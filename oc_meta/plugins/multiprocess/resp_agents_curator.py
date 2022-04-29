#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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


from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.finder import *
from oc_meta.lib.file_manager import *
from oc_meta.lib.master_of_regex import *
from oc_meta.scripts.curator import Curator
from typing import List
import json
import os
import re


class RespAgentsCurator(Curator):
    def __init__(self, data:List[dict], ts:str, prov_config:str, info_dir:str, base_iri:str='https://w3id.org/oc/meta', prefix:str='060', separator:str=None):
        self.finder = ResourceFinder(ts, base_iri)
        self.prov_config = prov_config
        self.separator = separator
        self.data = [{field:value.strip() for field,value in row.items()} for row in data]
        self.prefix = prefix
        self.id_info_path = info_dir + 'id.txt'
        self.ra_info_path = info_dir + 'ra.txt'
        self.radict:Dict[str, Dict[str, list]] = {}
        self.idra = {}  # key id; value metaid of id related to ra
        self.conflict_ra = {}
        self.rameta = dict()
        self.wnb_cnt = 0 # wannabe counter
        self.rowcnt = 0
        self.log = dict()
        self.preexisting_entities = set()

    def curator(self, filename:str=None, path_csv:str=None, path_index:str=None):
        for row in self.data:
            self.log[self.rowcnt] = {
                'id': {},
                'author': {},
                'venue': {},
                'editor': {},
                'publisher': {},
                'page': {},
                'volume': {},
                'issue': {},
                'pub_date': {},
                'type': {}
            }
            self.clean_ra(row, 'author')
            self.clean_ra(row, 'publisher')
            self.clean_ra(row, 'editor')
            self.rowcnt += 1
        self.radict.update(self.conflict_ra)
        self.meta_maker()
        self.log = self.log_update()
        self.enrich()
        if path_index:
            path_index = os.path.join(path_index, filename)
        self.filename = filename
        self.indexer(path_index, path_csv)

    def clean_ra(self, row, col_name):
        '''
        This method performs the deduplication process for responsible agents (authors, publishers and editors).

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :params col_name: the CSV column name. It can be 'author', 'publisher', or 'editor'
        :type col_name: str
        :returns: None -- This method modifies self.radict and self.idra, and returns None.
        '''
        if row[col_name]:
            if col_name in {'author', 'editor'}:
                ra_list = re.split(semicolon_in_people_field, row[col_name])
            elif col_name == 'publisher': 
                ra_list = [row[col_name]]
            ra_metaids = list()
            ra_list = Cleaner.clean_ra_list(ra_list)
            for ra in ra_list:
                ra_id = re.search(name_and_ids, ra)
                name = Cleaner(ra_id.group(1)).clean_name()
                ra_id = ra_id.group(2)
                if self.separator:
                    ra_id_list = re.sub(colon_and_spaces, ':', ra_id).split(self.separator)
                else:
                    ra_id_list = re.split(one_or_more_spaces, re.sub(colon_and_spaces, ':', ra_id))
                if col_name == 'publisher':
                    metaval = self.id_worker('publisher', name, ra_id_list, publ_entity=True)
                else:
                    metaval = self.id_worker(col_name, name, ra_id_list, publ_entity=False)
                if col_name != 'publisher' and metaval in self.radict:
                    full_name:str = self.radict[metaval]['title']
                    if ',' in name and ',' in full_name:
                        first_name = name.split(',')[1].strip()
                        if not full_name.split(',')[1].strip() and first_name:  # first name found!
                            given_name = full_name.split(',')[0]
                            self.radict[metaval]['title'] = given_name + ', ' + first_name
                ra_metaids.append(f'{name} [meta:ra/{metaval}]')
            row[col_name] = '; '.join(ra_metaids)

    def meta_maker(self):
        '''
        The MetaID dictionary 'rameta' is created from 'radict'.
        '''
        for identifier in self.radict:
            if 'wannabe' in identifier:
                other = identifier
                count = self._add_number(self.ra_info_path)
                meta = self.prefix + str(count)
                self.rameta[meta] = self.radict[identifier]
                self.rameta[meta]['others'].append(other)
                self.rameta[meta]['ids'].append('meta:ra/' + meta)
            else:
                self.rameta[identifier] = self.radict[identifier]
                self.rameta[identifier]['ids'].append('meta:ra/' + identifier)
                self.preexisting_entities.add(f'ra/{identifier}')

    def indexer(self, path_index:str, path_csv:str) -> None:
        '''
        This method is used to transform idra in such a way as to be saved as a csv file.
        Finally, it generates the enriched CSV and saves it.

        :params path_index: a directory path. It will contain the indexes
        :type path_index: str
        :params path_csv: a file path. It will be the output enriched CSV
        :type path_csv: str
        '''
        # ID
        self.index_id_ra = list()
        cur_index = self.idra
        if cur_index:
            for literal in cur_index:
                row = dict()
                row['id'] = str(literal)
                row['meta'] = str(cur_index[literal])
                self.index_id_ra.append(row)
        else:
            row = dict()
            row['id'] = ''
            row['meta'] = ''
            self.index_id_ra.append(row)
        if self.filename:
            if not os.path.exists(path_index):
                os.makedirs(path_index)
            ra_path = os.path.join(path_index, 'index_id_ra.csv')
            write_csv(ra_path, self.index_id_ra)
            if self.log:
                log_file = os.path.join(path_index + 'log.json')
                with open(log_file, 'w') as lf:
                    json.dump(self.log, lf)
            if self.data:
                name = self.filename + '.csv'
                data_file = os.path.join(path_csv, name)
                write_csv(data_file, self.data)

    @staticmethod
    def clean_id_list(id_list:List[str]) -> Tuple[list, str]:
        '''
        Clean IDs in the input list and check if there is a MetaID.

        :params: id_list: a list of IDs
        :type: id_list: List[str]
        :params: br: True if the IDs in id_list refer to bibliographic resources, False otherwise
        :type: br: bool
        :returns: Tuple[list, str]: -- it returns a two-elements tuple, where the first element is the list of cleaned IDs, while the second is a MetaID if any was found.
        '''
        pattern = 'ra/'
        metaid = ''
        id_list = list(filter(None, id_list))
        how_many_meta = [i for i in id_list if i.lower().startswith('meta')]
        if len(how_many_meta) > 1:
            for pos, elem in enumerate(list(id_list)):
                if 'meta' in elem.lower():
                    id_list[pos] = ''
        else:
            for pos, elem in enumerate(list(id_list)):
                elem = Cleaner(elem).normalize_hyphens()
                identifier = elem.split(':', 1)
                schema = identifier[0].lower()
                value = identifier[1]
                if schema == 'meta':
                    if 'meta:' + pattern in elem.lower():
                        metaid = value.replace(pattern, '')
                    id_list[pos] = ''
                else:
                    normalized_id = Cleaner(elem).normalize_id()
                    id_list[pos] = normalized_id
        id_list = list(filter(None, id_list))
        return id_list, metaid

    def id_worker(self, col_name, name, idslist:List[str], publ_entity:bool):
        id_dict = self.idra
        entity_dict = self.radict
        idslist, metaval = self.clean_id_list(idslist)
        # there's meta
        if metaval:
            # MetaID exists among data?
            # meta already in entity_dict (no care about conflicts, we have a meta specified)
            if metaval in entity_dict:
                self.merge_entities_in_csv(idslist, metaval, name, entity_dict, id_dict)
            else:
                found_meta_ts = None
                found_meta_ts = self.finder.retrieve_ra_from_meta(metaval, publisher=publ_entity)
                # meta in triplestore
                # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
                if found_meta_ts:
                    entity_dict[metaval] = dict()
                    entity_dict[metaval]['ids'] = list()
                    if col_name == 'author' or col_name == 'editor':
                        entity_dict[metaval]['title'] = self.name_check(found_meta_ts[0], name)
                    else:
                        entity_dict[metaval]['title'] = found_meta_ts[0]
                    entity_dict[metaval]['others'] = list()
                    self.merge_entities_in_csv(idslist, metaval, name, entity_dict, id_dict)
                    existing_ids = found_meta_ts[1]
                    for identifier in existing_ids:
                        if identifier[1] not in id_dict:
                            id_dict[identifier[1]] = identifier[0]
                        if identifier[1] not in entity_dict[metaval]['ids']:
                            entity_dict[metaval]['ids'].append(identifier[1])
                # Look for MetaId in the provenance
                else:
                    metaid_uri = f'{self.base_iri}/ra/{str(metaval)}'
                    # The entity MetaId after merge if it was merged, None otherwise. If None, the MetaId is considered invalid
                    metaval = self.finder.retrieve_metaid_from_merged_entity(metaid_uri=metaid_uri, prov_config=self.prov_config)
        # there's no meta or there was one but it didn't exist
        # Are there other IDs?
        if idslist and not metaval:
            local_match = self.__local_match(idslist, entity_dict)
            # IDs already exist among data?
            # check in entity_dict
            if local_match['existing']:
                # ids refer to multiple existing entities
                if len(local_match['existing']) > 1:
                    # !
                    return self.conflict(idslist, name, id_dict, col_name)
                # ids refer to ONE existing entity
                elif len(local_match['existing']) == 1:
                    metaval = str(local_match['existing'][0])
                    suspect_ids = list()
                    for identifier in idslist:
                        if identifier not in entity_dict[metaval]['ids']:
                            suspect_ids.append(identifier)
                    if suspect_ids:
                        sparql_match = self.finder_sparql(suspect_ids, br=False, ra=True, vvi=False, publ=publ_entity)
                        if len(sparql_match) > 1:
                            # !
                            return self.conflict(idslist, name, id_dict, col_name)
            # ids refers to 1 or more wannabe entities
            elif local_match['wannabe']:
                metaval = str(local_match['wannabe'].pop(0))
                # 5 Merge data from entityA (CSV) with data from EntityX (CSV)
                for old_meta in local_match['wannabe']:
                    self.merge(entity_dict, metaval, old_meta, name)
                suspect_ids = list()
                for identifier in idslist:
                    if identifier not in entity_dict[metaval]['ids']:
                        suspect_ids.append(identifier)
                if suspect_ids:
                    sparql_match = self.finder_sparql(suspect_ids, br=False, ra=True, vvi=False, publ=publ_entity)
                    if sparql_match:
                        if 'wannabe' not in metaval or len(sparql_match) > 1:
                            # Two entities previously disconnected on the triplestore now become connected
                            # !
                            return self.conflict(idslist, name, id_dict, col_name)
                        else:
                            existing_ids = sparql_match[0][2]
                            new_idslist = [x[1] for x in existing_ids]
                            new_sparql_match = self.finder_sparql(new_idslist, br=False, ra=True, vvi=False, publ=publ_entity)
                            if len(new_sparql_match) > 1:
                                # Two entities previously disconnected on the triplestore now become connected
                                # !
                                return self.conflict(idslist, name, id_dict, col_name)
                            else:
                                # 4 Merge data from EntityA (CSV) with data from EntityX (CSV) (it has already happened in # 5), update both with data from EntityA (RDF)
                                old_metaval = metaval
                                metaval = sparql_match[0][0]
                                entity_dict[metaval] = dict()
                                entity_dict[metaval]['ids'] = list()
                                entity_dict[metaval]['others'] = list()
                                entity_dict[metaval]['title'] = sparql_match[0][1] if sparql_match[0][1] else ''
                                self.merge(entity_dict, metaval, old_metaval, sparql_match[0][1])
                                for identifier in existing_ids:
                                    if identifier[1] not in id_dict:
                                        id_dict[identifier[1]] = identifier[0]
                                    if identifier[1] not in entity_dict[metaval]['ids']:
                                        entity_dict[metaval]['ids'].append(identifier[1])
            else:
                sparql_match = self.finder_sparql(idslist, br=False, ra=True, vvi=False, publ=publ_entity)
                if len(sparql_match) > 1:
                    # !
                    return self.conflict(idslist, name, id_dict, col_name)
                elif len(sparql_match) == 1:
                    existing_ids = sparql_match[0][2]
                    new_idslist = [x[1] for x in existing_ids]
                    new_sparql_match = self.finder_sparql(new_idslist, br=False, ra=True, vvi=False, publ=publ_entity)
                    if len(new_sparql_match) > 1:
                        # Two entities previously disconnected on the triplestore now become connected
                        # !
                        return self.conflict(idslist, name, id_dict, col_name)
                    # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
                    # 3 CONFLICT beteen MetaIDs. MetaID specified in EntityA inside CSV has precedence.
                    elif len(new_sparql_match) == 1:
                        metaval = sparql_match[0][0]
                        entity_dict[metaval] = dict()
                        entity_dict[metaval]['ids'] = list()
                        entity_dict[metaval]['others'] = list()
                        if col_name == 'author' or col_name == 'editor':
                            entity_dict[metaval]['title'] = self.name_check(sparql_match[0][1], name)
                        else:
                            entity_dict[metaval]['title'] = sparql_match[0][1]

                        if not entity_dict[metaval]['title'] and name:
                            entity_dict[metaval]['title'] = name

                        for identifier in existing_ids:
                            if identifier[1] not in id_dict:
                                id_dict[identifier[1]] = identifier[0]
                            if identifier[1] not in entity_dict[metaval]['ids']:
                                entity_dict[metaval]['ids'].append(identifier[1])
                else:
                    # 1 EntityA is a new one
                    metaval = self.new_entity(entity_dict, name)
            for identifier in idslist:
                if identifier not in id_dict:
                    self.__update_id_count(id_dict, identifier)
                if identifier not in entity_dict[metaval]['ids']:
                    entity_dict[metaval]['ids'].append(identifier)
            if not entity_dict[metaval]['title'] and name:
                entity_dict[metaval]['title'] = name
        # 1 EntityA is a new one
        if not idslist and not metaval:
            metaval = self.new_entity(entity_dict, name)
        return metaval

    def enrich(self):
        '''
        This method replaces the wannabeID placeholders with the
        actual data and MetaIDs as a result of the deduplication process.
        '''
        for row in self.data:
            for field in {'author', 'editor', 'publisher'}:
                if row[field]:
                    ras_list = list()
                    if field in {'author', 'editor'}:
                        ra_list = re.split(semicolon_in_people_field, row[field])
                    else:
                        ra_list = [row[field]]
                    for ra_entity in ra_list:
                        metaid = re.search(name_and_ids, ra_entity).group(2).replace('meta:ra/', '')
                        if 'wannabe' in metaid:
                            for ra_metaid in self.rameta:
                                if metaid in self.rameta[ra_metaid]['others']:
                                    metaid = ra_metaid
                        ra_name = self.rameta[metaid]['title']
                        ra_ids = ' '.join(self.rameta[metaid]['ids'])
                        ra = ra_name + ' [' + ra_ids + ']'
                        ras_list.append(ra)
                    row[field] = '; '.join(ras_list)

    @staticmethod
    def __local_match(list_to_match, dict_to_match:dict):
        match_elem = dict()
        match_elem['existing'] = list()
        match_elem['wannabe'] = list()
        for elem in list_to_match:
            for k, va in dict_to_match.items():
                if elem in va['ids']:
                    if 'wannabe' in k:
                        if k not in match_elem['wannabe']:
                            match_elem['wannabe'].append(k) # TODO: valutare uso di un set
                    else:
                        if k not in match_elem['existing']:
                            match_elem['existing'].append(k)
        return match_elem

    def __update_id_count(self, id_dict, identifier):
        count = self._add_number(self.id_info_path)
        id_dict[identifier] = self.prefix + str(count)

