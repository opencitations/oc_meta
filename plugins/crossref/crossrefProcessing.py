import html
import re
import warnings
from typing import Dict
from csv import DictReader
from bs4 import BeautifulSoup
from meta.lib.id_manager.orcidmanager import ORCIDManager
from meta.lib.csvmanager import CSVManager
from meta.lib.id_manager.issnmanager import ISSNManager
from meta.lib.id_manager.isbnmanager import ISBNManager
from meta.lib.id_manager.doimanager import DOIManager
from meta.lib.cleaner import Cleaner
from meta.lib.master_of_regex import *

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

class crossrefProcessing:

    def __init__(self, orcid_index:str, doi_csv:str=None, publishers_filepath:str=None):
        self.doi_set = CSVManager.load_csv_column_as_set(doi_csv, 'doi') if doi_csv else None
        self.publishers_mapping = self.load_publishers_mapping(publishers_filepath) if publishers_filepath else None
        self.orcid_index = CSVManager(orcid_index)
    
    def csv_creator(self, data:dict) -> list:
        data = data['items']
        output = list()
        for x in data:
            if not 'DOI' in x:
                continue
            if isinstance(x['DOI'], list):
                doi = DOIManager().normalise(str(x['DOI'][0]))
            else:
                doi = DOIManager().normalise(str(x['DOI']))
            if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
                row = dict()

                # create empty row
                keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                        'publisher', 'editor']
                for k in keys:
                    row[k] = ''

                if 'type' in x:
                    if x['type']:
                        row['type'] = x['type'].replace('-', ' ')

                # row['id']
                idlist = list()
                idlist.append(str('doi:' + doi))

                if 'ISBN' in x:
                    if row['type'] in {'book', 'monograph', 'edited book'}:
                        self.id_worker(x['ISBN'], idlist, self.isbn_worker)

                if 'ISSN' in x:
                    if row['type'] in {'journal', 'series', 'report series', 'standard series'}:
                        self.id_worker(x['ISSN'], idlist, self.issn_worker)
                row['id'] = ' '.join(idlist)

                # row['title']
                if 'title' in x:
                    if x['title']:
                        if isinstance(x['title'], list):
                            text_title = x['title'][0]
                        else:
                            text_title = x['title']
                        soup = BeautifulSoup(text_title, 'html.parser')
                        title_soup = soup.get_text().replace('\n', '')
                        title = html.unescape(title_soup)
                        row['title'] = title

                # row['author']
                if 'author' in x:
                    autlist = self.get_agents_strings_list(doi, x['author'])
                    row['author'] = '; '.join(autlist)

                # row['pub_date']
                if 'issued' in x:
                    if x['issued']['date-parts'][0][0]:
                        row['pub_date'] = '-'.join([str(y) for y in x['issued']['date-parts'][0]])
                    else:
                        row['pub_date'] = ''

                # row['venue']
                if 'container-title' in x:
                    if x['container-title']:
                        if isinstance(x['container-title'], list):
                            ventit = str(x['container-title'][0]).replace('\n', '')
                        else:
                            ventit = str(x['container-title']).replace('\n', '')
                        ven_soup = BeautifulSoup(ventit, 'html.parser')
                        ventit = html.unescape(ven_soup.get_text())
                        venidlist = list()
                        if 'ISBN' in x:
                            if row['type'] in {'book chapter', 'book part'}:
                                self.id_worker(x['ISBN'], venidlist, self.isbn_worker)

                        if 'ISSN' in x:
                            if row['type'] in {'journal article', 'journal volume', 'journal issue'}:
                                self.id_worker(x['ISSN'], venidlist, self.issn_worker)
                        if venidlist:
                            row['venue'] = ventit + ' [' + ' '.join(venidlist) + ']'
                        else:
                            row['venue'] = ventit

                if 'volume' in x:
                    row['volume'] = x['volume']
                if 'issue' in x:
                    row['issue'] = x['issue']
                if 'page' in x:
                    pages = '-'.join(re.split(pages_separator, x['page']))
                    row['page'] = pages

                row['publisher'] = self.get_publisher_name(doi, x)                        

                if 'editor' in x:
                    editlist = self.get_agents_strings_list(doi, x['editor'])
                    row['editor'] = '; '.join(editlist)
                output.append(row)
        return output
    
    def orcid_finder(self, doi:str) -> dict:
        found = dict()
        doi = doi.lower()
        orcids = self.orcid_index.get_value(doi)
        if orcids:
            for orc in orcids:
                orc = orc.replace(']', '').split(' [')
                found[orc[1]] = orc[0].lower()
        return found
    
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
        if self.publishers_mapping:
            if member:
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
    
    def get_agents_strings_list(self, doi:str, agents_list:str) -> list:
        agents_strings_list = list()
        dict_orcid = None
        if not all('ORCID' in agent for agent in agents_list):
            dict_orcid = self.orcid_finder(doi)
        for agent in agents_list:
            agent_string = None
            if 'family' in agent:
                f_name = Cleaner(agent['family']).remove_unwanted_characters()
                if 'given' in agent:
                    g_name = Cleaner(agent['given']).remove_unwanted_characters()
                    agent_string = f_name + ', ' + g_name
                else:
                    agent_string = f_name + ', '
            elif 'name' in agent:
                agent_string = f_name = Cleaner(agent['name']).remove_unwanted_characters()
                f_name = agent_string.split()[-1] if ' ' in agent_string else None
            orcid = None
            if 'ORCID' in agent:
                if isinstance(agent['ORCID'], list):
                    orcid = str(agent['ORCID'][0])
                else:
                    orcid = str(agent['ORCID'])
                if ORCIDManager().is_valid(orcid):
                    orcid = ORCIDManager().normalise(orcid)
                else:
                    orcid = None
            elif dict_orcid and f_name:
                for ori in dict_orcid:
                    orc_n = dict_orcid[ori].split(', ')
                    orc_f = orc_n[0]
                    if f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower():
                        orcid = ori
            if orcid:
                agent_string += ' [' + 'orcid:' + str(orcid) + ']'
            if agent_string:
                agents_strings_list.append(agent_string)
        return agents_strings_list

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
        if ISSNManager().is_valid(issnid):
            issnid = ISSNManager().normalise(issnid, include_prefix=True)
            idlist.append(issnid)

    @staticmethod
    def isbn_worker(isbnid, idlist):
        if ISBNManager().is_valid(isbnid):
            isbnid = ISBNManager().normalise(isbnid, include_prefix=True)
            idlist.append(isbnid)

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
