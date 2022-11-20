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


import html
import re
import warnings

from bs4 import BeautifulSoup
from oc_idmanager import DOIManager, ISBNManager, ISSNManager

from oc_meta.lib.master_of_regex import *
from oc_meta.plugins.ra_processor import RaProcessor

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

class CrossrefProcessing(RaProcessor):
    def __init__(self, orcid_index:str=None, doi_csv:str=None, publishers_filepath:str=None):
        super(CrossrefProcessing, self).__init__(orcid_index, doi_csv, publishers_filepath)

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
                row['page'] = self.get_crossref_pages(item)

            row['publisher'] = self.get_publisher_name(doi, item)                        

            if 'editor' in item:
                row['editor'] = '; '.join(editors_string_list)
        return row
        
    def get_crossref_pages(self, item:dict) -> str:
        '''
        This function returns the pages interval. 

        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        pages_list = re.split(pages_separator, item['page'])
        return self.get_pages(pages_list)
    
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