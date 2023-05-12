#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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

import re
import unicodedata
from csv import DictReader
from typing import Dict, List, Tuple

from oc_idmanager import ISBNManager, ISSNManager, ORCIDManager
from zipfile import ZipFile
from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.master_of_regex import orcid_pattern
import os

class RaProcessor(object):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath: str = None, citing_entities: str = None):
        self.doi_set = CSVManager.load_csv_column_as_set(doi_csv, 'id') if doi_csv else None
        self.publishers_mapping = self.load_publishers_mapping(publishers_filepath) if publishers_filepath else None
        orcid_index = orcid_index if orcid_index else None
        self.orcid_index = CSVManager(orcid_index)
        if citing_entities:
            self.unzip_citing_entities(citing_entities)
            self.citing_entities_set = CSVManager.load_csv_column_as_set(citing_entities, 'id') if citing_entities else None

    def get_agents_strings_list(self, doi: str, agents_list: List[dict]) -> Tuple[list, list]:
        authors_strings_list = list()
        editors_string_list = list()
        dict_orcid = None
        if not all('orcid' in agent or 'ORCID' in agent for agent in agents_list):
            dict_orcid = self.orcid_finder(doi)
        agents_list = [
            {k: Cleaner(v).remove_unwanted_characters() if k in {'family', 'given', 'name'} and v is not None 
            else v for k, v in agent_dict.items()} for agent_dict in agents_list]
        for agent in agents_list:
            cur_role = agent['role']
            f_name = None
            g_name = None
            agent_string = None
            if agent.get('family') and agent.get('given'):
                f_name = agent['family']
                g_name = agent['given']
                agent_string = f_name + ', ' + g_name
            elif agent.get('name'):
                agent_string = agent['name']
                f_name = agent_string.split(",")[0].strip() if "," in agent_string else None
                g_name = agent_string.split(",")[-1].strip() if "," in agent_string else None
                if f_name and g_name:
                    agent_string = f_name + ', ' + g_name
            if agent_string is None:
                if agent.get('family') and not agent.get('given'):
                    if g_name:
                        agent_string = agent['family'] + ', ' + g_name
                    else:
                        agent_string = agent['family'] + ', '
                elif agent.get('given') and not agent.get('family'):
                    if f_name:
                        agent_string = f_name + ', ' + agent['given']
                    else:
                        agent_string = ', ' + agent['given']
            orcid = None
            if 'orcid' in agent:
                if isinstance(agent['orcid'], list):
                    orcid = str(agent['orcid'][0])
                else:
                    orcid = str(agent['orcid'])
            elif 'ORCID' in agent:
                if isinstance(agent['ORCID'], list):
                    orcid = str(agent['ORCID'][0])
                else:
                    orcid = str(agent['ORCID'])
            if orcid:
                orcid_manager = ORCIDManager(data=dict(), use_api_service=False)
                orcid = orcid_manager.normalise(orcid, include_prefix=False)
                orcid = orcid if orcid_manager.check_digit(orcid) else None
            elif dict_orcid and f_name:
                for ori in dict_orcid:
                    orc_n: List[str] = dict_orcid[ori].split(', ')
                    orc_f = orc_n[0].lower()
                    orc_g = orc_n[1] if len(orc_n) == 2 else None
                    if f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower():
                        if g_name and orc_g:
                            # If there are several authors with the same surname
                            if len([person for person in agents_list if 'family' in person if person['family'] if
                                    person['family'].lower() in orc_f.lower() or orc_f.lower() in person[
                                        'family'].lower()]) > 1:
                                # If there are several authors with the same surname and the same given names' initials
                                if len([person for person in agents_list if 'given' in person if person['given'] if
                                        person['given'][0].lower() == orc_g[0].lower()]) > 1:
                                    homonyms_list = [person for person in agents_list if 'given' in person if
                                                     person['given'] if person['given'].lower() == orc_g.lower()]
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
                            elif any([person for person in agents_list if 'given' in person if person['given'] if
                                      person['given'].lower() == f_name.lower()]):
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

    def orcid_finder(self, doi: str) -> dict:
        found = dict()
        doi = doi.lower()
        people:  List[str] = self.orcid_index.get_value(doi)
        if people:
            for person in people:
                orcid = re.search(orcid_pattern, person).group(0)
                name: str = person[:person.find(orcid)-1]
                found[orcid] = name.strip().lower()
        return found

    def unzip_citing_entities(self, citing_entities):
        for dirpath, _, filenames in os.walk(citing_entities):
            for filename in filenames:
                if filename.endswith('.zip'):
                    with ZipFile(os.path.join(citing_entities, filename), mode='r') as zipf:
                        zipf.extractall(citing_entities)
                    os.remove(os.path.join(citing_entities, filename))

    def get_pages(self, pages_list:list) -> str:
        '''
        This function returns the pages interval. 

        :params pages_list: a list of pages
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        roman_letters = {'I', 'V', 'X', 'L', 'C', 'D', 'M'}
        clean_pages_list = list()
        for page in pages_list:
            # e.g. 583-584 or 1_583-1_584
            if all(c.isdigit() or c == "_" for c in page):
                clean_pages_list.append(page)
            # e.g. G27. It is a born digital document. PeerJ uses this approach, where G27 identifies the whole document, since it has no pages.
            elif len(pages_list) == 1:
                clean_pages_list.append(page)
            # e.g. iv-vii. This syntax is used in the prefaces.
            elif all(c.upper() in roman_letters for c in page):
                clean_pages_list.append(page)
            # 583b-584. It is an error. The b must be removed.
            elif any(c.isdigit() for c in page):
                page_without_letters = ''.join([c for c in page if c.isdigit() or c == '_'])
                clean_pages_list.append(page_without_letters)
        if clean_pages_list:
            if len(clean_pages_list) == 1:
                clean_pages_list.append(clean_pages_list[0])
            return '-'.join(clean_pages_list)
        return ''
    
    @staticmethod
    def normalise_unicode(metadata: dict) -> dict:
        return {k:unicodedata.normalize('NFKC', v) for k, v in metadata.items()}

    @staticmethod
    def id_worker(field, ids:list, func) -> None:
        if isinstance(field, list):
            for i in field:
                func(str(i), ids)
        else:
            id = str(field)
            func(id, ids)

    @staticmethod
    def load_publishers_mapping(publishers_filepath: str) -> dict:
        publishers_mapping: Dict[str, Dict[str, set]] = dict()
        with open(publishers_filepath, 'r', encoding='utf-8') as f:
            data = DictReader(f)
            for row in data:
                id = row['id']
                publishers_mapping.setdefault(id, dict())
                publishers_mapping[id]['name'] = row['name']
                publishers_mapping[id].setdefault('prefixes', set()).add(row['prefix'])
        return publishers_mapping
    
    @staticmethod
    def issn_worker(issnid:str, ids:list):
        issn_manager = ISSNManager()
        issnid = issn_manager.normalise(issnid, include_prefix=False)
        if issn_manager.check_digit(issnid) and f'issn:{issnid}' not in ids:
            ids.append('issn:' + issnid)

    @staticmethod
    def isbn_worker(isbnid, ids:list):
        isbn_manager = ISBNManager()
        isbnid = isbn_manager.normalise(isbnid, include_prefix=False)
        if isbn_manager.check_digit(isbnid) and f'isbn:{isbnid}' not in ids:
            ids.append('isbn:' + isbnid)

    @staticmethod
    def uppercase_initials(inp_str: str):
        upper_word_list = []
        words_list = inp_str.split()
        for w in words_list:
            upper_word_list.append(w[0].upper() + w[1:]) if len(w)>1 else upper_word_list.append(w[0].upper())
        upper_str = " ".join(upper_word_list)
        return upper_str