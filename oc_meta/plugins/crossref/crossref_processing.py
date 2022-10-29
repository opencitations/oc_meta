#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


from typing import Dict, List, Tuple
from csv import DictReader
from bs4 import BeautifulSoup
from oc_idmanager import DOIManager, ISBNManager, ISSNManager, ORCIDManager
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.master_of_regex import *
import html
import re
import warnings


warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

class CrossrefProcessing:
    def __init__(self, orcid_index:str=None, doi_csv:str=None, publishers_filepath:str=None):
        self.doi_set = CSVManager.load_csv_column_as_set(doi_csv, 'id') if doi_csv else None
        self.publishers_mapping = self.load_publishers_mapping(publishers_filepath) if publishers_filepath else None
        orcid_index = orcid_index if orcid_index else None
        self.orcid_index = CSVManager(orcid_index)
    
    def csv_creator(self, item:dict) -> dict:
        row = dict()
        if not 'DOI' in item:
            return row
        doi_manager = DOIManager(use_api_service=False)
        if isinstance(item['DOI'], list):
            doi = doi_manager.normalise(str(item['DOI'][0]), include_prefix=False)
        else:
            doi = doi_manager.normalise(str(item['DOI']), include_prefix=False)
        if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
            # create empty row
            keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                    'publisher', 'editor']
            for k in keys:
                row[k] = ''

            if 'type' in item:
                if item['type']:
                    row['type'] = item['type'].replace('-', ' ')

            # row['id']
            idlist = list()
            idlist.append(str('doi:' + doi))

            if 'ISBN' in item:
                if row['type'] in {'book', 'dissertation', 'edited book', 'monograph', 'reference book', 'report', 'standard'}:
                    self.id_worker(item['ISBN'], idlist, self.isbn_worker)

            if 'ISSN' in item:
                if row['type'] in {'book series', 'book set', 'journal', 'proceedings series', 'series', 'standard series'}:
                    self.id_worker(item['ISSN'], idlist, self.issn_worker)
                elif row['type'] == 'report series':
                    br_id = True
                    if 'container-title' in item:
                        if item['container-title']:
                            br_id = False
                    if br_id:
                        self.id_worker(item['ISSN'], idlist, self.issn_worker)
            row['id'] = ' '.join(idlist)

            # row['title']
            if 'title' in item:
                if item['title']:
                    if isinstance(item['title'], list):
                        text_title = item['title'][0]
                    else:
                        text_title = item['title']
                    soup = BeautifulSoup(text_title, 'html.parser')
                    title_soup = soup.get_text().replace('\n', '')
                    title = html.unescape(title_soup)
                    row['title'] = title

            agents_list = []
            if 'author' in item:
                for author in item['author']:
                    author['role'] = 'author'
                agents_list.extend(item['author'])
            if 'editor' in item:
                for editor in item['editor']:
                    editor['role'] = 'editor'
                agents_list.extend(item['editor'])
            authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

            # row['author']
            if 'author' in item:
                row['author'] = '; '.join(authors_strings_list)

            # row['pub_date']
            if 'issued' in item:
                if item['issued']['date-parts'][0][0]:
                    row['pub_date'] = '-'.join([str(y) for y in item['issued']['date-parts'][0]])
                else:
                    row['pub_date'] = ''

            # row['venue']
            row['venue'] = self.get_venue_name(item, row)

            if 'volume' in item:
                row['volume'] = item['volume']
            if 'issue' in item:
                row['issue'] = item['issue']
            if 'page' in item:
                row['page'] = self.get_pages(item)

            row['publisher'] = self.get_publisher_name(doi, item)                        

            if 'editor' in item:
                row['editor'] = '; '.join(editors_string_list)
        return row
    
    def orcid_finder(self, doi:str) -> dict:
        found = dict()
        doi = doi.lower()
        people:List[str] = self.orcid_index.get_value(doi)
        if people:
            for person in people:
                orcid = re.search(orcid_pattern, person).group(0)
                name:str = person[:person.find(orcid)-1]
                found[orcid] = name.strip().lower()
        return found
    
    def get_pages(self, item:dict) -> str:
        '''
        This function returns the pages interval. 

        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        roman_letters = {'I', 'V', 'X', 'L', 'C', 'D', 'M'}
        pages_list = re.split(pages_separator, item['page'])
        clean_pages_list = list()
        for page in pages_list:
            # e.g. 583-584
            if all(c.isdigit() for c in page):
                clean_pages_list.append(page)
            # e.g. G27. It is a born digital document. PeerJ uses this approach, where G27 identifies the whole document, since it has no pages.
            elif len(pages_list) == 1:
                clean_pages_list.append(page)
            # e.g. iv-vii. This syntax is used in the prefaces.
            elif all(c.upper() in roman_letters for c in page):
                clean_pages_list.append(page)
            # 583b-584. It is an error. The b must be removed.
            elif any(c.isdigit() for c in page):
                page_without_letters = ''.join([c for c in page if c.isdigit()])
                clean_pages_list.append(page_without_letters)
        pages = '-'.join(clean_pages_list)
        return pages
    
    def get_publisher_name(self, doi:str, item:dict) -> str:
        '''
        This function aims to return a publisher's name and id. If a mapping was provided, 
        it is used to find the publisher's standardized name from its id or DOI prefix. 

        :params doi: the item's DOI
        :type doi: str
        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'American Medical Association (AMA) [crossref:10]'. If the id does not exist, the output is only the name. Finally, if there is no publisher, the output is an empty string.
        '''
        data = {
            'publisher': '',
            'member': None,
            'prefix': doi.split('/')[0]
        }
        for field in {'publisher', 'member', 'prefix'}:
            if field in item:
                if item[field]:
                    data[field] = item[field]
        publisher = data['publisher']
        member = data['member']
        prefix = data['prefix']
        relevant_member = False
        if self.publishers_mapping and member:
            if member in self.publishers_mapping:
                relevant_member = True
        if self.publishers_mapping:
            if relevant_member:
                name = self.publishers_mapping[member]['name']
                name_and_id = f'{name} [crossref:{member}]'
            else:
                member_dict = next(({member:data} for member, data in self.publishers_mapping.items() if prefix in data['prefixes']), None)
                if member_dict:
                    member = list(member_dict.keys())[0]
                    name_and_id = f"{member_dict[member]['name']} [crossref:{member}]"
                else:
                    name_and_id = publisher
        else:
            name_and_id = f'{publisher} [crossref:{member}]' if member else publisher
        return name_and_id
    
    def get_venue_name(self, item:dict, row:dict) -> str:
        '''
        This method deals with generating the venue's name, followed by id in square brackets, separated by spaces. 
        HTML tags are deleted and HTML entities escaped. In addition, any ISBN and ISSN are validated. 
        Finally, the square brackets in the venue name are replaced by round brackets to avoid conflicts with the ids enclosures.

        :params item: the item's dictionary
        :type item: dict
        :params row: a CSV row
        :type row: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'Nutrition & Food Science [issn:0034-6659]'. If the id does not exist, the output is only the name. Finally, if there is no venue, the output is an empty string.
        '''
        name_and_id = ''
        if 'container-title' in item:
            if item['container-title']:
                if isinstance(item['container-title'], list):
                    ventit = str(item['container-title'][0]).replace('\n', '')
                else:
                    ventit = str(item['container-title']).replace('\n', '')
                ven_soup = BeautifulSoup(ventit, 'html.parser')
                ventit = html.unescape(ven_soup.get_text())
                ambiguous_brackets = re.search(ids_inside_square_brackets, ventit)
                if ambiguous_brackets:
                    match = ambiguous_brackets.group(1)
                    open_bracket = ventit.find(match) - 1
                    close_bracket = ventit.find(match) + len(match)
                    ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
                    ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
                venidlist = list()
                if 'ISBN' in item:
                    if row['type'] in {'book chapter', 'book part', 'book section', 'book track', 'reference entry'}:
                        self.id_worker(item['ISBN'], venidlist, self.isbn_worker)

                if 'ISSN' in item:
                    if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article', 'journal volume', 'journal issue', 'monograph', 'proceedings', 'peer review', 'reference book', 'reference entry', 'report'}:
                        self.id_worker(item['ISSN'], venidlist, self.issn_worker)
                    elif row['type'] == 'report series':
                        if 'container-title' in item:
                            if item['container-title']:
                                self.id_worker(item['ISSN'], venidlist, self.issn_worker)
                if venidlist:
                    name_and_id = ventit + ' [' + ' '.join(venidlist) + ']'
                else:
                    name_and_id = ventit
        return name_and_id
    
    def get_agents_strings_list(self, doi:str, agents_list:List[dict]) -> Tuple[list, list]:
        authors_strings_list = list()
        editors_string_list = list()
        dict_orcid = None
        if not all('ORCID' in agent for agent in agents_list):
            dict_orcid = self.orcid_finder(doi)
        agents_list = [{k:Cleaner(v).remove_unwanted_characters() if k in {'family', 'given', 'name'} else v for k,v in agent_dict.items()} for agent_dict in agents_list]
        for agent in agents_list:
            cur_role = agent['role']
            f_name = None
            g_name = None
            agent_string = None
            if 'family' in agent:
                f_name = agent['family']
                if 'given' in agent:
                    g_name = agent['given']
                    agent_string = f_name + ', ' + g_name
                else:
                    agent_string = f_name + ', '
            elif 'name' in agent:
                agent_string = agent['name']
                f_name = agent_string.split()[-1] if ' ' in agent_string else None
            elif 'given' in agent and 'family' not in agent:
                agent_string = ', ' + agent['given']
            orcid = None
            if 'ORCID' in agent:
                if isinstance(agent['ORCID'], list):
                    orcid = str(agent['ORCID'][0])
                else:
                    orcid = str(agent['ORCID'])
                orcid_manager = ORCIDManager(use_api_service=False)
                orcid = orcid_manager.normalise(orcid, include_prefix=False)
                orcid = orcid if orcid_manager.check_digit(orcid) else None
            elif dict_orcid and f_name:
                for ori in dict_orcid:
                    orc_n:List[str] = dict_orcid[ori].split(', ')
                    orc_f = orc_n[0].lower()
                    orc_g = orc_n[1] if len(orc_n) == 2 else None
                    if f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower():
                        if g_name and orc_g:
                            # If there are several authors with the same surname
                            if len([person for person in agents_list if 'family' in person if person['family'] if person['family'].lower() in orc_f.lower() or orc_f.lower() in person['family'].lower()]) > 1:
                                # If there are several authors with the same surname and the same given names' initials
                                if len([person for person in agents_list if 'given' in person if person['given'] if person['given'][0].lower() == orc_g[0].lower()]) > 1:
                                    homonyms_list = [person for person in agents_list if 'given' in person if person['given'] if person['given'].lower() == orc_g.lower()]
                                    # If there are homonyms
                                    if len(homonyms_list) > 1:
                                        # If such homonyms have different roles from the current role
                                        if [person for person in homonyms_list if person['role'] != cur_role]:
                                            if orc_g.lower() == g_name.lower():
                                                orcid = ori
                                    else:
                                        if orc_g.lower() == g_name.lower():
                                            orcid = ori
                                elif orc_g[0].lower() == g_name[0].lower():
                                    orcid = ori
                            # If there is a person whose given name is equal to the family name of the current person (a common situation for cjk names)
                            elif any([person for person in agents_list if 'given' in person if person['given'] if person['given'].lower() == f_name.lower()]):
                                if orc_g.lower() == g_name.lower():
                                    orcid = ori
                            else:
                                orcid = ori
                        else:
                            orcid = ori
            if agent_string and orcid:
                agent_string += ' [' + 'orcid:' + str(orcid) + ']'
            if agent_string:
                if agent['role'] == 'author':
                    authors_strings_list.append(agent_string)
                elif agent['role'] == 'editor':
                    editors_string_list.append(agent_string)
        return authors_strings_list, editors_string_list
    
    @staticmethod
    def id_worker(field, idlist:list, func) -> None:
        if isinstance(field, list):
            for i in field:
                func(str(i), idlist)
        else:
            id = str(field)
            func(id, idlist)

    @staticmethod
    def issn_worker(issnid, idlist):
        issn_manager = ISSNManager()
        issnid = issn_manager.normalise(issnid, include_prefix=False)
        if issn_manager.check_digit(issnid):
            idlist.append('issn:' + issnid)

    @staticmethod
    def isbn_worker(isbnid, idlist):
        isbn_manager = ISBNManager()
        isbnid = isbn_manager.normalise(isbnid, include_prefix=False)
        if isbn_manager.check_digit(isbnid):
            idlist.append('isbn:' + isbnid)

    @staticmethod
    def load_publishers_mapping(publishers_filepath:str) -> dict:
        publishers_mapping: Dict[str, Dict[str, set]] = dict()
        with open(publishers_filepath, 'r', encoding='utf-8') as f:
            data = DictReader(f)
            for row in data:
                id = row['id']
                publishers_mapping.setdefault(id, dict())
                publishers_mapping[id]['name'] = row['name']
                publishers_mapping[id].setdefault('prefixes', set()).add(row['prefix'])
        return publishers_mapping