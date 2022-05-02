from filelock import FileLock
from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.finder import *
from oc_meta.lib.file_manager import *
from oc_meta.lib.master_of_regex import *
from typing import List, Tuple, Dict
import json
import os
import re


class Curator:

    def __init__(self, data:List[dict], ts:str, prov_config:str, info_dir:str, base_iri:str='https://w3id.org/oc/meta', prefix:str='060', separator:str=None, valid_dois_cache:CSVManager=None):
        self.finder = ResourceFinder(ts, base_iri)
        self.base_iri = base_iri
        self.prov_config = prov_config
        self.separator = separator
        self.data = [{field:value.strip() for field,value in row.items()} for row in data if self.is_a_valid_row(row)]
        self.prefix = prefix
        # Counter local paths
        self.br_info_path = info_dir + 'br.txt'
        self.id_info_path = info_dir + 'id.txt'
        self.ra_info_path = info_dir + 'ra.txt'
        self.ar_info_path = info_dir + 'ar.txt'
        self.re_info_path = info_dir + 're.txt'
        self.brdict = {}
        self.radict:Dict[str, Dict[str, list]] = {}
        self.ardict:Dict[str, Dict[str, list]] = {}
        self.vvi = {}  # Venue, Volume, Issue
        self.idra = {}  # key id; value metaid of id related to ra
        self.idbr = {}  # key id; value metaid of id related to br
        self.conflict_br:Dict[str, Dict[str, list]] = {}
        self.conflict_ra = {}
        self.rameta = dict()
        self.brmeta = dict()
        self.armeta = dict()
        self.remeta = dict()
        self.wnb_cnt = 0 # wannabe counter
        self.rowcnt = 0
        self.log = dict()
        self.valid_dois_cache = valid_dois_cache
        self.preexisting_entities = set()

    def curator(self, filename:str=None, path_csv:str=None, path_index:str=None):
        for row in self.data:
            self.log[self.rowcnt] = {
                'id': {},
                'title': {},
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
            self.clean_id(row)
            self.rowcnt += 1
        self.merge_duplicate_entities()
        # reset row counter
        self.rowcnt = 0
        for row in self.data:
            self.clean_vvi(row)
            self.rowcnt += 1
        # reset row counter
        self.rowcnt = 0
        for row in self.data:
            self.clean_ra(row, 'author')
            self.clean_ra(row, 'publisher')
            self.clean_ra(row, 'editor')
            self.rowcnt += 1
        self.brdict.update(self.conflict_br)
        self.radict.update(self.conflict_ra)
        self.get_preexisting_entities()
        self.meta_maker()
        self.log = self.log_update()
        self.enrich()
        # remove duplicates
        self.data = list({v['id']: v for v in self.data}.values())
        if path_index:
            path_index = os.path.join(path_index, filename)
        self.filename = filename
        self.indexer(path_index, path_csv)

    # ID
    def clean_id(self, row:Dict[str,str]) -> None:
        '''
        The 'clean id()' function is executed for each CSV row. 
        In this process, any duplicates are detected by the IDs in the 'id' column. 
        For each line, a wannabeID or, if the bibliographic resource was found in the triplestore, 
        a MetaID is assigned.  
        Finally, this method enrich and clean the fields related to the 
        title, venue, volume, issue, page, publication date and type.

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :returns: None -- This method modifies the input CSV row without returning it.
        '''
        if row['title']:
            name = Cleaner(row['title']).clean_title()
        else:
            name = ''
        if row['id']:
            if self.separator:
                idslist = re.sub(colon_and_spaces, ':', row['id']).split(self.separator)
            else:
                idslist = re.split(one_or_more_spaces, re.sub(colon_and_spaces, ':', row['id']))
            metaval = self.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        else:
            metaval = self.new_entity(self.brdict, name)
        row['title'] = self.brdict[metaval]['title'] if metaval in self.brdict else self.conflict_br[metaval]['title']
        row['id'] = metaval
        if 'wannabe' not in metaval:
            self.equalizer(row, metaval)
        # page
        if row['page']:
            row['page'] = Cleaner(row['page']).normalize_hyphens()
        # date
        if row['pub_date']:
            date = Cleaner(row['pub_date']).normalize_hyphens()
            date = Cleaner(date).clean_date()
            row['pub_date'] = date
        # type
        if row['type']:
            entity_type = ' '.join((row['type'].lower()).split())
            if entity_type == 'edited book' or entity_type == 'monograph':
                entity_type = 'book'
            elif entity_type == 'report series' or entity_type == 'standard series' or entity_type == 'proceedings series':
                entity_type = 'series'
            elif entity_type == 'posted content':
                entity_type = 'web content'
            if entity_type in {'archival document', 'book', 'book chapter', 'book part', 'book section', 'book series',
                               'book set', 'data file', 'dataset', 'dissertation', 'journal', 'journal article', 'journal issue',
                               'journal volume', 'peer review', 'proceedings article', 'proceedings', 'reference book',
                               'reference entry', 'series', 'report', 'standard', 'web content'}:
                row['type'] = entity_type
            else:
                row['type'] = ''

    # VVI
    def clean_vvi(self, row:Dict[str, str]) -> None:
        '''
        This method performs the deduplication process for venues, volumes and issues.
        The acquired information is stored in the 'vvi' dictionary, that has the following format: ::

            {
                VENUE_IDENTIFIER: {
                    'issue': {SEQUENCE_IDENTIFIER: {'id': META_ID}},
                    'volume': {
                        SEQUENCE_IDENTIFIER: {
                            'id': META_ID,
                            'issue' {SEQUENCE_IDENTIFIER: {'id': META_ID}}
                        }
                    }
                }
            }

            {
                '4416': {
                    'issue': {}, 
                    'volume': {
                        '166': {'id': '4388', 'issue': {'4': {'id': '4389'}}}, 
                        '172': {'id': '4434', 
                            'issue': {
                                '22': {'id': '4435'}, 
                                '20': {'id': '4436'}, 
                                '21': {'id': '4437'}, 
                                '19': {'id': '4438'}
                            }
                        }
                    }
                }
            }   

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :returns: None -- This method modifies the input CSV row without returning it.
        '''
        Cleaner.clean_volume_and_issue(row=row)
        vol_meta = None
        br_id = row['id']
        venue = row['venue']
        volume = row['volume']
        issue = row['issue']
        br_type = row['type']
        # Venue
        if venue:
            # The data must be invalidated, because the resource is journal but a volume or an issue have also been specified
            if br_type == 'journal' and (volume or issue):
                row['venue'] = ''
                row['volume'] = ''
                row['issue'] = ''
            venue_id = re.search(name_and_ids, venue)
            if venue_id:
                name = Cleaner(venue_id.group(1)).clean_title()
                venue_id = venue_id.group(2)
                if self.separator:
                    idslist = re.sub(colon_and_spaces, ':', venue_id).split(self.separator)
                else:
                    idslist = re.split(one_or_more_spaces, re.sub(colon_and_spaces, ':', venue_id))
                metaval = self.id_worker('venue', name, idslist, ra_ent=False, br_ent=True, vvi_ent=True, publ_entity=False)
                if metaval not in self.vvi:
                    ts_vvi = None
                    if 'wannabe' not in metaval:
                        ts_vvi = self.finder.retrieve_venue_from_meta(metaval)
                    if 'wannabe' in metaval or not ts_vvi:
                        self.vvi[metaval] = dict()
                        self.vvi[metaval]['volume'] = dict()
                        self.vvi[metaval]['issue'] = dict()
                    elif ts_vvi:
                        self.vvi[metaval] = ts_vvi
            else:
                name = Cleaner(venue).clean_title()
                metaval = self.new_entity(self.brdict, name)
                self.vvi[metaval] = dict()
                self.vvi[metaval]['volume'] = dict()
                self.vvi[metaval]['issue'] = dict()
            row['venue'] = metaval
            # Volume
            if volume and (br_type == 'journal issue' or br_type == 'journal article'):
                if volume in self.vvi[metaval]['volume']:
                    vol_meta = self.vvi[metaval]['volume'][volume]['id']
                else:
                    vol_meta = self.new_entity(self.brdict, '')
                    self.vvi[metaval]['volume'][volume] = dict()
                    self.vvi[metaval]['volume'][volume]['id'] = vol_meta
                    self.vvi[metaval]['volume'][volume]['issue'] = dict()
            elif volume and br_type == 'journal volume':
                # The data must be invalidated, because the resource is journal volume but an issue has also been specified
                if issue:
                    row['volume'] = ''
                    row['issue'] = ''
                else:
                    vol_meta = br_id
                    self.volume_issue(vol_meta, self.vvi[metaval]['volume'], volume, row)
            # Issue
            if issue and br_type == 'journal article':
                row['issue'] = issue
                if vol_meta:
                    # issue inside volume
                    if issue not in self.vvi[metaval]['volume'][volume]['issue']:
                        issue_meta = self.new_entity(self.brdict, '')
                        self.vvi[metaval]['volume'][volume]['issue'][issue] = dict()
                        self.vvi[metaval]['volume'][volume]['issue'][issue]['id'] = issue_meta
                else:
                    # issue inside venue (without volume)
                    if issue not in self.vvi[metaval]['issue']:
                        issue_meta = self.new_entity(self.brdict, '')
                        self.vvi[metaval]['issue'][issue] = dict()
                        self.vvi[metaval]['issue'][issue]['id'] = issue_meta
            elif issue and br_type == 'journal issue':
                issue_meta = br_id
                if vol_meta:
                    self.volume_issue(issue_meta, self.vvi[metaval]['volume'][volume]['issue'], issue, row)
                else:
                    self.volume_issue(issue_meta, self.vvi[metaval]['issue'], issue, row)
        else:
            row['venue'] = ''
            row['volume'] = ''
            row['issue'] = ''

    # RA
    def clean_ra(self, row, col_name):
        '''
        This method performs the deduplication process for responsible agents (authors, publishers and editors).

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :params col_name: the CSV column name. It can be 'author', 'publisher', or 'editor'
        :type col_name: str
        :returns: None -- This method modifies self.ardict, self.radict, and self.idra, and returns None.
        '''
        if row[col_name]:
            if row['id'] in self.brdict or row['id'] in self.conflict_br:
                br_metaval = row['id']
            else:
                other_id = [id for id in self.brdict if row['id'] in self.brdict[id]['others']]
                conflict_id = [id for id in self.conflict_br if row['id'] in self.conflict_br[id]['others']]
                br_metaval = other_id[0] if other_id else conflict_id[0]
            if br_metaval not in self.ardict or not self.ardict[br_metaval][col_name]:
                # new sequence
                if 'wannabe' in br_metaval:
                    if br_metaval not in self.ardict:
                        self.ardict[br_metaval] = dict()
                        self.ardict[br_metaval]['author'] = list()
                        self.ardict[br_metaval]['editor'] = list()
                        self.ardict[br_metaval]['publisher'] = list()
                    sequence = []
                else:
                    # sequence can be in TS
                    sequence_found = self.finder.retrieve_ra_sequence_from_br_meta(br_metaval, col_name)
                    if sequence_found:
                        sequence = []
                        for agent in sequence_found:
                            for ar_metaid in agent:
                                ra_metaid = agent[ar_metaid][2]
                                sequence.append(tuple((ar_metaid, ra_metaid)))
                                if ra_metaid not in self.radict:
                                    self.radict[ra_metaid] = dict()
                                    self.radict[ra_metaid]['ids'] = list()
                                    self.radict[ra_metaid]['others'] = list()
                                    self.radict[ra_metaid]['title'] = agent[ar_metaid][0]
                                for identifier in agent[ar_metaid][1]:
                                    # other ids after meta
                                    id_metaid = identifier[0]
                                    literal = identifier[1]
                                    if id_metaid not in self.idra:
                                        self.idra[literal] = id_metaid
                                    if literal not in self.radict[ra_metaid]['ids']:
                                        self.radict[ra_metaid]['ids'].append(literal)

                        if br_metaval not in self.ardict:
                            self.ardict[br_metaval] = dict()
                            self.ardict[br_metaval]['author'] = list()
                            self.ardict[br_metaval]['editor'] = list()
                            self.ardict[br_metaval]['publisher'] = list()
                            self.ardict[br_metaval][col_name].extend(sequence)
                        else:
                            self.ardict[br_metaval][col_name].extend(sequence)
                    else:
                        # totally new sequence
                        if br_metaval not in self.ardict:
                            self.ardict[br_metaval] = dict()
                            self.ardict[br_metaval]['author'] = list()
                            self.ardict[br_metaval]['editor'] = list()
                            self.ardict[br_metaval]['publisher'] = list()
                        sequence = []
            else:
                sequence = self.ardict[br_metaval][col_name]
            new_sequence = list()
            change_order = False
            if col_name in {'author', 'editor'}:
                ra_list = re.split(semicolon_in_people_field, row[col_name])
            elif col_name == 'publisher': 
                ra_list = [row[col_name]]
            ra_list = Cleaner.clean_ra_list(ra_list)
            for pos, ra in enumerate(ra_list):
                new_elem_seq = True
                ra_id = re.search(name_and_ids, ra)
                if ra_id:
                    name = Cleaner(ra_id.group(1)).clean_name()
                    ra_id = ra_id.group(2)
                else:
                    name = Cleaner(ra).clean_name()
                if not ra_id and sequence:
                    for _, ra_metaid in sequence:
                        if self.radict[ra_metaid]['title'] == name:
                            ra_id = 'meta:ra/' + str(ra_metaid)
                            new_elem_seq = False
                            break
                if ra_id:
                    if self.separator:
                        ra_id_list = re.sub(colon_and_spaces, ':', ra_id).split(self.separator)
                    else:
                        ra_id_list = re.split(one_or_more_spaces, re.sub(colon_and_spaces, ':', ra_id))
                    if sequence:
                        ar_ra = None
                        for ps, el in enumerate(sequence):
                            ra_metaid = el[1]
                            for literal in ra_id_list:
                                if literal in self.radict[ra_metaid]['ids']:
                                    if ps != pos:
                                        change_order = True
                                    new_elem_seq = False
                                    if 'wannabe' not in ra_metaid:
                                        ar_ra = ra_metaid
                                        for pos, literal_value in enumerate(ra_id_list):
                                            if 'meta' in literal_value:
                                                ra_id_list[pos] = ''
                                            break
                                        ra_id_list = list(filter(None, ra_id_list))
                                        ra_id_list.append('meta:ra/' + ar_ra)
                        if not ar_ra:
                            # new element
                            for ar_metaid, ra_metaid in sequence:
                                if self.radict[ra_metaid]['title'] == name:
                                    new_elem_seq = False
                                    if 'wannabe' not in ra_metaid:
                                        ar_ra = ra_metaid
                                        for pos, i in enumerate(ra_id_list):
                                            if 'meta' in i:
                                                ra_id_list[pos] = ''
                                            break
                                        ra_id_list = list(filter(None, ra_id_list))
                                        ra_id_list.append('meta:ra/' + ar_ra)
                    if col_name == 'publisher':
                        metaval = self.id_worker('publisher', name, ra_id_list, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
                    else:
                        metaval = self.id_worker(col_name, name, ra_id_list, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
                    if col_name != 'publisher' and metaval in self.radict:
                        full_name:str = self.radict[metaval]['title']
                        if ',' in name and ',' in full_name:
                            first_name = name.split(',')[1].strip()
                            if not full_name.split(',')[1].strip() and first_name:  # first name found!
                                given_name = full_name.split(',')[0]
                                self.radict[metaval]['title'] = given_name + ', ' + first_name
                else:
                    metaval = self.new_entity(self.radict, name)
                if new_elem_seq:
                    role = self.prefix + str(self._add_number(self.ar_info_path))
                    new_sequence.append(tuple((role, metaval)))
            if change_order:
                self.log[self.rowcnt][col_name]['Info'] = 'New RA sequence proposed: refused'
            sequence.extend(new_sequence)
            self.ardict[br_metaval][col_name] = sequence

    @staticmethod
    def clean_id_list(id_list:List[str], br:bool, valid_dois_cache:CSVManager=None) -> Tuple[list, str]:
        '''
        Clean IDs in the input list and check if there is a MetaID.

        :params: id_list: a list of IDs
        :type: id_list: List[str]
        :params: br: True if the IDs in id_list refer to bibliographic resources, False otherwise
        :type: br: bool
        :returns: Tuple[list, str]: -- it returns a two-elements tuple, where the first element is the list of cleaned IDs, while the second is a MetaID if any was found.
        '''
        pattern = 'br/' if br else 'ra/'
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
                    normalized_id = Cleaner(elem).normalize_id(valid_dois_cache=valid_dois_cache)
                    id_list[pos] = normalized_id
        id_list = list(filter(None, id_list))
        return id_list, metaid

    def conflict(self, idslist:List[str], name:str, id_dict:dict, col_name:str) -> str:
        if col_name == 'id' or col_name == 'venue':
            entity_dict = self.conflict_br
        elif col_name == 'author' or col_name == 'editor' or col_name == 'publisher':
            entity_dict = self.conflict_ra
        metaval = self.new_entity(entity_dict, name)
        self.log[self.rowcnt][col_name]['Conflict entity'] = metaval
        for identifier in idslist:
            entity_dict[metaval]['ids'].append(identifier)
            if identifier not in id_dict:
                schema_value = identifier.split(':')
                found_metaid = self.finder.retrieve_metaid_from_id(schema_value[0], schema_value[1])
                if found_metaid:
                    id_dict[identifier] = found_metaid
                else:
                    self.__update_id_count(id_dict, identifier)
        return metaval

    def finder_sparql(self, list_to_find, br=True, ra=False, vvi=False, publ=False):
        match_elem = list()
        id_set = set()
        res = None
        for elem in list_to_find:
            if len(match_elem) < 2:
                identifier = elem.split(':')
                value = identifier[1]
                schema = identifier[0]
                if br:
                    res = self.finder.retrieve_br_from_id(schema, value)
                elif ra:
                    res = self.finder.retrieve_ra_from_id(schema, value, publ)
                if res:
                    for f in res:
                        if f[0] not in id_set:
                            match_elem.append(f)
                            id_set.add(f[0])
        return match_elem

    def ra_update(self, row:dict, br_key:str, col_name:str) -> None:
        if row[col_name]:
            sequence = self.armeta[br_key][col_name]
            ras_list = list()
            for _, ra_id in sequence:
                ra_name = self.rameta[ra_id]['title']
                ra_ids = ' '.join(self.rameta[ra_id]['ids'])
                ra = ra_name + ' [' + ra_ids + ']'
                ras_list.append(ra)
            row[col_name] = '; '.join(ras_list)

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

    def __meta_ar(self, newkey, oldkey, role):
        for x, k in self.ardict[oldkey][role]:
            if 'wannabe' in k:
                for m in self.rameta:
                    if k in self.rameta[m]['others']:
                        new_v = m
                        break
            else:
                new_v = k
            self.armeta[newkey][role].append(tuple((x, new_v)))

    def __tree_traverse(self, tree:dict, key:str, values:List[Tuple]) -> None:
        for k, v in tree.items():
            if k == key:
                values.append(v)
            elif isinstance(v, dict):
                found = self.__tree_traverse(v, key, values)
                if found is not None:  
                    values.append(found)
    
    def get_preexisting_entities(self) -> None:
        for entity_type in {'br', 'ra'}:
            for entity_metaid, data in getattr(self, f'{entity_type}dict').items():
                if not entity_metaid.startswith('wannabe'):
                    self.preexisting_entities.add(f'{entity_type}/{entity_metaid}')
                    for entity_id_literal in data['ids']:
                        preexisting_entity_id_metaid = getattr(self, f'id{entity_type}')[entity_id_literal]
                        self.preexisting_entities.add(f'id/{preexisting_entity_id_metaid}')
        for _, roles in self.ardict.items():
            for _, ar_ras in roles.items():
                for ar_ra in ar_ras:
                    if not ar_ra[1].startswith('wannabe'):
                        self.preexisting_entities.add(f'ar/{ar_ra[0]}')
        for venue_metaid, vi in self.vvi.items():
            if not venue_metaid.startswith('wannabe'):
                wannabe_preexisting_vis = list()
                self.__tree_traverse(vi, 'id', wannabe_preexisting_vis)
                self.preexisting_entities.update({f'br/{vi_metaid}' for vi_metaid in wannabe_preexisting_vis if not vi_metaid.startswith('wannabe')})
        for _, re_metaid in self.remeta.items():
            self.preexisting_entities.add(f're/{re_metaid[0]}')

    def meta_maker(self):
        '''
        For each dictionary ('brdict', 'ardict', 'radict') the corresponding MetaID dictionary is created
        ('brmeta', 'armeta', and 'rameta').
        '''
        for identifier in self.brdict:
            if 'wannabe' in identifier:
                other = identifier
                count = self._add_number(self.br_info_path)
                meta = self.prefix + str(count)
                self.brmeta[meta] = self.brdict[identifier]
                self.brmeta[meta]['others'].append(other)
                self.brmeta[meta]['ids'].append('meta:br/' + meta)
            else:
                self.brmeta[identifier] = self.brdict[identifier]
                self.brmeta[identifier]['ids'].append('meta:br/' + identifier)
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
        for ar_id in self.ardict:
            if 'wannabe' in ar_id:
                for br_id in self.brmeta:
                    if ar_id in self.brmeta[br_id]['others']:
                        br_key = br_id
                        break
            else:
                br_key = ar_id
            self.armeta[br_key] = dict()
            self.armeta[br_key]['author'] = list()
            self.armeta[br_key]['editor'] = list()
            self.armeta[br_key]['publisher'] = list()
            self.__meta_ar(br_key, ar_id, 'author')
            self.__meta_ar(br_key, ar_id, 'editor')
            self.__meta_ar(br_key, ar_id, 'publisher')

    def enrich(self):
        '''
        This method replaces the wannabeID placeholders with the
        actual data and MetaIDs as a result of the deduplication process.
        '''
        for row in self.data:
            if 'wannabe' in row['id']:
                for br_metaid in self.brmeta:
                    if row['id'] in self.brmeta[br_metaid]['others']:
                        metaid = br_metaid
            else:
                metaid = row['id']
            if row['page'] and (metaid not in self.remeta):
                re_meta = self.finder.retrieve_re_from_br_meta(metaid)
                if re_meta:
                    self.remeta[metaid] = re_meta
                    row['page'] = re_meta[1]
                else:
                    count = self.prefix + str(self._add_number(self.re_info_path))
                    page = row['page']
                    self.remeta[metaid] = (count, page)
                    row['page'] = page
            elif metaid in self.remeta:
                row['page'] = self.remeta[metaid][1]
            self.ra_update(row, metaid, 'author')
            self.ra_update(row, metaid, 'publisher')
            self.ra_update(row, metaid, 'editor')
            row['id'] = ' '.join(self.brmeta[metaid]['ids'])
            row['title'] = self.brmeta[metaid]['title']
            if row['venue']:
                venue = row['venue']
                if 'wannabe' in venue:
                    for i in self.brmeta:
                        if venue in self.brmeta[i]['others']:
                            ve = i
                else:
                    ve = venue
                row['venue'] = self.brmeta[ve]['title'] + ' [' + ' '.join(self.brmeta[ve]['ids']) + ']'

    @staticmethod
    def name_check(ts_name, name):
        if ',' in ts_name:
            names = ts_name.split(',')
            if names[0] and not names[1].strip():
                # there isn't a given name in ts
                if ',' in name:
                    gname = name.split(', ')[1]
                    if gname.strip():
                        ts_name = names[0] + ', ' + gname
        return ts_name

    @staticmethod
    def _read_number(file_path:str, line_number:int=1) -> int: 
        with open(file_path) as f:
            cur_number = int(f.readlines()[line_number - 1])
        return cur_number

    @staticmethod
    def _add_number(file_path:str, line_number:int=1) -> int:
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        lock = FileLock(f'{file_path}.lock')
        with lock:
            cur_number = Curator._read_number(file_path, line_number) + 1 if os.path.exists(file_path) else 1
            if os.path.exists(file_path):
                with open(file_path) as f:
                    all_lines = f.readlines()
            else:
                all_lines = []
            line_len = len(all_lines)
            zero_line_number = line_number - 1
            for i in range(line_number):
                if i >= line_len:
                    all_lines += ['\n']
                if i == zero_line_number:
                    all_lines[i] = str(cur_number) + '\n'
            with open(file_path, 'w') as f:
                f.writelines(all_lines)
        return cur_number
    
    def __update_id_and_entity_dict(self, existing_ids:list, id_dict:dict, entity_dict:Dict[str, Dict[str, list]], metaval:str) -> None:
        for identifier in existing_ids:
            if identifier[1] not in id_dict:
                id_dict[identifier[1]] = identifier[0]
            if identifier[1] not in entity_dict[metaval]['ids']:
                entity_dict[metaval]['ids'].append(identifier[1])

    def indexer(self, path_index:str, path_csv:str) -> None:
        '''
        This method is used to transform idra, idbr, armeta, remeta, brmeta and vvi in such a way as to be saved as csv and json files.
        As for venue, volume and issues, this method also takes care of replacing any wannabe_id with a meta_id.
        Finally, it generates the enriched CSV and saves it.

        :params path_index: a directory path. It will contain the indexes
        :type path_index: str
        :params path_csv: a file path. It will be the output enriched CSV
        :type path_csv: str
        '''
        # ID
        self.index_id_ra = list()
        self.index_id_br = list()
        for entity_type in {'ra', 'br'}:
            cur_index = getattr(self, f'id{entity_type}')
            if cur_index:
                for literal in cur_index:
                    row = dict()
                    row['id'] = str(literal)
                    row['meta'] = str(cur_index[literal])
                    getattr(self, f'index_id_{entity_type}').append(row)
            else:
                row = dict()
                row['id'] = ''
                row['meta'] = ''
                getattr(self, f'index_id_{entity_type}').append(row)
        # AR
        self.ar_index = list()
        if self.armeta:
            for metaid in self.armeta:
                index = dict()
                index['meta'] = metaid
                for role in self.armeta[metaid]:
                    list_ar = list()
                    for ar, ra in self.armeta[metaid][role]:
                        list_ar.append(str(ar) + ', ' + str(ra))
                    index[role] = '; '.join(list_ar)
                self.ar_index.append(index)
        else:
            row = dict()
            row['meta'] = ''
            row['author'] = ''
            row['editor'] = ''
            row['publisher'] = ''
            self.ar_index.append(row)
        # RE
        self.re_index = list()
        if self.remeta:
            for x in self.remeta:
                r = dict()
                r['br'] = x
                r['re'] = str(self.remeta[x][0])
                self.re_index.append(r)
        else:
            row = dict()
            row['br'] = ''
            row['re'] = ''
            self.re_index.append(row)
        # VI
        self.VolIss = dict()
        if self.vvi:
            for venue_meta in self.vvi:
                venue_issue = self.vvi[venue_meta]['issue']
                if venue_issue:
                    for issue in venue_issue:
                        issue_id = venue_issue[issue]['id']
                        if 'wannabe' in issue_id:
                            for br_meta in self.brmeta:
                                if issue_id in self.brmeta[br_meta]['others']:
                                    self.vvi[venue_meta]['issue'][issue]['id'] = str(br_meta)
                venue_volume = self.vvi[venue_meta]['volume']
                if venue_volume:
                    for volume in venue_volume:
                        volume_id = venue_volume[volume]['id']
                        if 'wannabe' in volume_id:
                            for br_meta in self.brmeta:
                                if volume_id in self.brmeta[br_meta]['others']:
                                    self.vvi[venue_meta]['volume'][volume]['id'] = str(br_meta)
                        if venue_volume[volume]['issue']:
                            volume_issue = venue_volume[volume]['issue']
                            for issue in volume_issue:
                                volume_issue_id = volume_issue[issue]['id']
                                if 'wannabe' in volume_issue_id:
                                    for br_meta in self.brmeta:
                                        if volume_issue_id in self.brmeta[br_meta]['others']:
                                            self.vvi[venue_meta]['volume'][volume]['issue'][issue]['id'] = str(br_meta)
                if 'wannabe' in venue_meta:
                    for br_meta in self.brmeta:
                        if venue_meta in self.brmeta[br_meta]['others']:
                            if br_meta in self.VolIss:
                                for vvi_v in self.vvi[venue_meta]['volume']:
                                    if vvi_v in self.VolIss[br_meta]['volume']:
                                        self.VolIss[br_meta]['volume'][vvi_v]['issue'].update(self.vvi[venue_meta]['volume'][vvi_v]['issue'])
                                    else:
                                        self.VolIss[br_meta]['volume'][vvi_v] = self.vvi[venue_meta]['volume'][vvi_v]
                                self.VolIss[br_meta]['issue'].update(self.vvi[venue_meta]['issue'])
                            else:
                                self.VolIss[br_meta] = self.vvi[venue_meta]
                else:
                    self.VolIss[venue_meta] = self.vvi[venue_meta]
        if self.filename:
            if not os.path.exists(path_index):
                os.makedirs(path_index)
            ra_path = os.path.join(path_index, 'index_id_ra.csv')
            write_csv(ra_path, self.index_id_ra)
            br_path = os.path.join(path_index, 'index_id_br.csv')
            write_csv(br_path, self.index_id_br)
            ar_path = os.path.join(path_index, 'index_ar.csv')
            write_csv(ar_path, self.ar_index)
            re_path = os.path.join(path_index, 'index_re.csv')
            write_csv(re_path, self.re_index)
            vvi_file = os.path.join(path_index, 'index_vi.json')
            with open(vvi_file, 'w') as fp:
                json.dump(self.VolIss, fp)
            if self.log:
                log_file = os.path.join(path_index + 'log.json')
                with open(log_file, 'w') as lf:
                    json.dump(self.log, lf)
            if self.data:
                name = self.filename + '.csv'
                data_file = os.path.join(path_csv, name)
                write_csv(data_file, self.data)
    
    def __update_id_count(self, id_dict, identifier):
        count = self._add_number(self.id_info_path)
        id_dict[identifier] = self.prefix + str(count)

    @staticmethod
    def merge(dict_to_match:Dict[str, Dict[str, list]], metaval:str, old_meta:str, temporary_name:str) -> None:
        for x in dict_to_match[old_meta]['ids']:
            if x not in dict_to_match[metaval]['ids']:
                dict_to_match[metaval]['ids'].append(x)
        for x in dict_to_match[old_meta]['others']:
            if x not in dict_to_match[metaval]['others']:
                dict_to_match[metaval]['others'].append(x)
        dict_to_match[metaval]['others'].append(old_meta)
        if not dict_to_match[metaval]['title']:
            if dict_to_match[old_meta]['title']:
                dict_to_match[metaval]['title'] = dict_to_match[old_meta]['title']
            else:
                dict_to_match[metaval]['title'] = temporary_name
        del dict_to_match[old_meta]
        
    def merge_entities_in_csv(self, idslist:list, metaval:str, name:str, entity_dict:Dict[str, Dict[str, list]], id_dict:dict) -> None:
        found_others = self.__local_match(idslist, entity_dict)
        if found_others['wannabe']:
            for old_meta in found_others['wannabe']:
                self.merge(entity_dict, metaval, old_meta, name)
        for identifier in idslist:
            if identifier not in entity_dict[metaval]['ids']:
                entity_dict[metaval]['ids'].append(identifier)
            if identifier not in id_dict:
                self.__update_id_count(id_dict, identifier)
        self.__update_title(entity_dict, metaval, name)
    
    def __update_title(self, entity_dict:dict, metaval:str, name:str) -> None:
        if not entity_dict[metaval]['title'] and name:
            entity_dict[metaval]['title'] = name
            self.log[self.rowcnt]['title']['status'] = 'New value proposed'
    
    def id_worker(self, col_name, name, idslist:List[str], ra_ent=False, br_ent=False, vvi_ent=False, publ_entity=False):
        if not ra_ent:
            id_dict = self.idbr
            entity_dict = self.brdict
            idslist, metaval = self.clean_id_list(idslist, br=True, valid_dois_cache=self.valid_dois_cache)
        else:
            id_dict = self.idra
            entity_dict = self.radict
            idslist, metaval = self.clean_id_list(idslist, br=False, valid_dois_cache=self.valid_dois_cache)
        # there's meta
        if metaval:
            # MetaID exists among data?
            # meta already in entity_dict (no care about conflicts, we have a meta specified)
            if metaval in entity_dict:
                self.merge_entities_in_csv(idslist, metaval, name, entity_dict, id_dict)
            else:
                found_meta_ts = None
                if ra_ent:
                    found_meta_ts = self.finder.retrieve_ra_from_meta(metaval, publisher=publ_entity)
                elif br_ent:
                    found_meta_ts = self.finder.retrieve_br_from_meta(metaval)
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
                    self.__update_id_and_entity_dict(existing_ids, id_dict, entity_dict, metaval)
                # Look for MetaId in the provenance
                else:
                    entity_type = 'br' if br_ent or vvi_ent else 'ra' 
                    metaid_uri = f'{self.base_iri}/{entity_type}/{str(metaval)}'
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
                        sparql_match = self.finder_sparql(suspect_ids, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
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
                    sparql_match = self.finder_sparql(suspect_ids, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                    if sparql_match:
                        if 'wannabe' not in metaval or len(sparql_match) > 1:
                            # Two entities previously disconnected on the triplestore now become connected
                            # !
                            return self.conflict(idslist, name, id_dict, col_name)
                        else:
                            existing_ids = sparql_match[0][2]
                            new_idslist = [x[1] for x in existing_ids]
                            new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
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
                                self.__update_id_and_entity_dict(existing_ids, id_dict, entity_dict, metaval)
            else:
                sparql_match = self.finder_sparql(idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                if len(sparql_match) > 1:
                    # !
                    return self.conflict(idslist, name, id_dict, col_name)
                elif len(sparql_match) == 1:
                    existing_ids = sparql_match[0][2]
                    new_idslist = [x[1] for x in existing_ids]
                    new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
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
                        self.__update_title(entity_dict, metaval, name)
                        self.__update_id_and_entity_dict(existing_ids, id_dict, entity_dict, metaval)
                else:
                    # 1 EntityA is a new one
                    metaval = self.new_entity(entity_dict, name)
            for identifier in idslist:
                if identifier not in id_dict:
                    self.__update_id_count(id_dict, identifier)
                if identifier not in entity_dict[metaval]['ids']:
                    entity_dict[metaval]['ids'].append(identifier)
            self.__update_title(entity_dict, metaval, name)
        # 1 EntityA is a new one
        if not idslist and not metaval:
            metaval = self.new_entity(entity_dict, name)
        return metaval

    def new_entity(self, entity_dict, name):
        metaval = 'wannabe_' + str(self.wnb_cnt)
        self.wnb_cnt += 1
        entity_dict[metaval] = dict()
        entity_dict[metaval]['ids'] = list()
        entity_dict[metaval]['others'] = list()
        entity_dict[metaval]['title'] = name
        return metaval

    def volume_issue(self, meta:str, path:Dict[str, Dict[str, str]], value:str, row:Dict[str, str]) -> None:
        if 'wannabe' not in meta:
            if value in path:
                if 'wannabe' in path[value]['id']:
                    old_meta = path[value]['id']
                    self.merge(self.brdict, meta, old_meta, row['title'])
                    path[value]['id'] = meta
            else:
                path[value] = dict()
                path[value]['id'] = meta
                if 'issue' not in path:
                    path[value]['issue'] = dict()
        else:
            if value in path:
                if 'wannabe' in path[value]['id']:
                    old_meta = path[value]['id']
                    if meta != old_meta:
                        self.merge(self.brdict, meta, old_meta, row['title'])
                        path[value]['id'] = meta
                else:
                    old_meta = path[value]['id']
                    if 'wannabe' not in old_meta and old_meta not in self.brdict:
                        br4dict = self.finder.retrieve_br_from_meta(old_meta)
                        self.brdict[old_meta] = dict()
                        self.brdict[old_meta]['ids'] = list()
                        self.brdict[old_meta]['others'] = list()
                        self.brdict[old_meta]['title'] = br4dict[0]
                        for x in br4dict[1]:
                            identifier = x[1]
                            self.brdict[old_meta]['ids'].append(identifier)
                            if identifier not in self.idbr:
                                self.idbr[identifier] = x[0]
                    self.merge(self.brdict, old_meta, meta, row['title'])
            else:
                path[value] = dict()
                path[value]['id'] = meta
                if 'issue' not in path:  # it's a Volume
                    path[value]['issue'] = dict()

    def log_update(self):
        new_log = dict()
        for x in self.log:
            if any(self.log[x][y].values() for y in self.log[x]):
                for y in self.log[x]:
                    if 'Conflict entity' in self.log[x][y]:
                        v = self.log[x][y]['Conflict entity']
                        if 'wannabe' in v:
                            if y == 'id' or y == 'venue':
                                for brm in self.brmeta:
                                    if v in self.brmeta[brm]['others']:
                                        m = 'br/' + str(brm)
                            elif y == 'author' or y == 'editor' or y == 'publisher':
                                for ram in self.rameta:
                                    if v in self.rameta[ram]['others']:
                                        m = 'ra/' + str(ram)
                        else:
                            m = v
                        self.log[x][y]['Conflict entity'] = m
                new_log[x] = self.log[x]

                if 'wannabe' in self.data[x]['id']:
                    for brm in self.brmeta:
                        if self.data[x]['id'] in self.brmeta[brm]['others']:
                            met = 'br/' + str(brm)
                else:
                    met = 'br/' + str(self.data[x]['id'])
                new_log[x]['id']['meta'] = met
        return new_log

    def merge_duplicate_entities(self) -> None:
        '''
        The 'merge_duplicate_entities()' function merge duplicate entities. 
        Moreover, it modifies the CSV cells, giving precedence to the first found information 
        or data in the triplestore in the case of already existing entities. 

        :returns: None -- This method updates the CSV rows and returns None.
        '''
        self.rowcnt = 0
        for row in self.data:
            if 'wannabe' in row['id']:
                for id in self.brdict:
                    if row['id'] in self.brdict[id]['others'] and 'wannabe' not in id:
                        row['id'] = id
                        self.equalizer(row, id)
                        break
                other_rowcnt = 0
                for other_row in self.data:
                    if other_row['id'] == row['id'] and self.rowcnt != other_rowcnt: # if it is another row
                        for field in ['pub_date', 'page', 'type', 'venue', 'volume', 'issue']:
                            if row[field] and row[field] != other_row[field]:
                                if other_row[field]:
                                    self.log[other_rowcnt][field]['status'] = 'New value proposed'
                                other_row[field] = row[field]
                    other_rowcnt += 1
            self.rowcnt += 1

    def equalizer(self, row:Dict[str, str], metaval:str) -> None:
        '''
        Given a CSV row and its MetaID, this function equates the information present in the CSV with that present on the triplestore.

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :params metaval: the MetaID identifying the bibliographic resource contained in the input CSV row
        :type metaval: str
        :returns: None -- This method modifies the input CSV row without returning it.
        '''
        self.log[self.rowcnt]['id']['status'] = 'Entity already exists'
        known_data = self.finder.retrieve_br_info_from_meta(metaval)
        known_data['author'] = self.__get_resp_agents(metaval, 'author')
        known_data['editor'] = self.__get_resp_agents(metaval, 'editor')
        known_data['publisher'] = self.finder.retrieve_publisher_from_br_metaid(metaval)
        for datum in ['venue', 'volume', 'issue', 'pub_date', 'type']:
            if known_data[datum]:
                row[datum] = known_data[datum]
            elif row[datum]:
                self.log[self.rowcnt][datum]['status'] = 'New value proposed'
        for role in ['author', 'editor', 'publisher']:
            if known_data[role] and not row[role]:
                row[role] = known_data[role]
        if known_data['page']:
            row['page'] = known_data['page'][1]
            self.remeta[metaval] = known_data['page']
        elif row['page']:
            self.log[self.rowcnt]['page']['status'] = 'New value proposed'

    def __get_resp_agents(self, metaid:str, column:str) -> str:
        resp_agents = self.finder.retrieve_ra_sequence_from_br_meta(metaid, column)
        output = ''
        if resp_agents:
            full_resp_agents = list()
            for item in resp_agents:
                for _, resp_agent in item.items():
                    author_name = resp_agent[0]
                    ids = [f'meta:ra/{resp_agent[2]}']
                    ids.extend([id[1] for id in resp_agent[1]])
                    author_ids = '[' + ' '.join(ids) + ']'
                    full_resp_agent =  author_name + ' ' + author_ids
                    full_resp_agents.append(full_resp_agent)
            output = '; '.join(full_resp_agents)
        return output
    
    def is_a_valid_row(self, row:Dict[str, str]) -> bool:
        '''
        This method discards invalid rows in the input CSV file.

        :params row: a dictionary representing a CSV row
        :type row: Dict[str, str]
        :returns: bool -- This method returns True if the row is valid, False if it is invalid.
        '''
        if row['id']:
            return True
        if all(not row[value] for value in row):
            return False
        br_type = ' '.join((row['type'].lower()).split())
        br_title = row['title']
        br_author = row['author']
        br_editor = row['editor']
        br_pub_date = row['pub_date']
        br_venue = row['venue']
        br_volume = row['volume']
        br_issue = row['issue']
        if not br_type or br_type in {'book', 'data file', 'dataset', 'dissertation', 'edited book', 'journal article', 'monograph', 
                        'other', 'peer review', 'posted content', 'web content', 'proceedings article', 'report', 'reference book'}:
            is_a_valid_row = True if br_title and br_pub_date and (br_author or br_editor) else False
        elif br_type in {'book chapter', 'book part', 'book section', 'book track', 'component', 'reference entry'}:
            is_a_valid_row = True if br_title and br_venue else False
        elif br_type in {'book series', 'book set', 'journal', 'proceedings', 'proceedings series', 'report series', 'standard', 'standard series'}:
            is_a_valid_row = True if br_title else False
        elif br_type == 'journal volume':
            is_a_valid_row = True if br_venue and (br_volume or br_title) else False
        elif br_type == 'journal issue':
            is_a_valid_row = True if br_venue and (br_issue or br_title) else False
        return is_a_valid_row