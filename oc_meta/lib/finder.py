from __future__ import annotations

import os
from time import sleep
from typing import Dict, List, Tuple
import yaml
from dateutil import parser
from oc_ocdm.graph import GraphEntity
from oc_ocdm.prov.prov_entity import ProvEntity
from oc_ocdm.support import get_count, get_resource_number
from rdflib import RDF, Graph, Literal, URIRef
from SPARQLWrapper import JSON, POST, SPARQLWrapper
from time_agnostic_library.agnostic_entity import AgnosticEntity
from oc_ocdm.graph.graph_entity import GraphEntity
from oc_meta.plugins.fixer.ar_order import check_roles
from oc_meta.plugins.editor import MetaEditor

class ResourceFinder:

    def __init__(self, ts_url, base_iri:str, local_g: Graph = Graph(), settings: dict = dict(), meta_config_path: str = None):
        self.ts = SPARQLWrapper(ts_url)
        self.ts.setMethod(POST)
        self.base_iri = base_iri[:-1] if base_iri[-1] == '/' else base_iri
        self.local_g = local_g
        self.ids_in_local_g = set()
        self.meta_config_path = meta_config_path
        self.meta_settings = settings
        self.blazegraph_full_text_search = settings['blazegraph_full_text_search'] if settings and 'blazegraph_full_text_search' in settings else False
        self.virtuoso_full_text_search = settings['virtuoso_full_text_search'] if settings and 'virtuoso_full_text_search' in settings else False

    def __query(self, query, return_format = JSON):
        self.ts.setReturnFormat(return_format)
        self.ts.setQuery(query)
        tentative = 3
        result = None
        while tentative:
            tentative -= 1
            try:
                result = self.ts.queryAndConvert()
                return result
            except Exception:
                sleep(5)
        return result
        
    # _______________________________BR_________________________________ #

    def retrieve_br_from_id(self, schema:str, value:str) -> List[Tuple[str, str, list]]:
        '''
        Given an identifier, it retrieves bibliographic resources associated with that identifier, related titles and other identifiers MetaIDs and literal values.

        :params schema: an identifier schema
        :type schema: str
        :params value: an identifier literal value
        :type value: str
        :returns List[Tuple[str, str, list]]: -- it returns a list of three elements tuples. The first element is the MetaID of a resource associated with the input ID. The second element is a title of that resourse, if present. The third element is a list of MetaID-ID tuples related to identifiers associated with that resource. 
        '''
        schema_uri = URIRef(GraphEntity.DATACITE + schema)
        value = value.replace('\\', '\\\\')
        result_list = list()
        identifier_uri = None
        for starting_triple in self.local_g.triples((None, GraphEntity.iri_has_literal_value, Literal(value))):
            for known_id_triple in self.local_g.triples((starting_triple[0], None, None)):
                if known_id_triple[1] == GraphEntity.iri_uses_identifier_scheme and known_id_triple[2] == schema_uri:
                    identifier_uri = known_id_triple[0]
                    break
            if identifier_uri:
                break
        if identifier_uri:
            metaid_id_list = [(identifier_uri.replace(f'{self.base_iri}/id/', ''), f'{schema}:{value}')]
            for triple in self.local_g.triples((None, GraphEntity.iri_has_identifier, identifier_uri)):
                title = ''
                res = triple[0]
                for res_triple in self.local_g.triples((res, None, None)):
                    if res_triple[1] == GraphEntity.iri_title:
                        title = str(res_triple[2])
                    elif res_triple[1] == GraphEntity.iri_has_identifier and res_triple[2] != identifier_uri:
                        for id_triple in self.local_g.triples((res_triple[2], None, None)):
                            if id_triple[1] == GraphEntity.iri_uses_identifier_scheme:
                                id_schema = id_triple[2]
                            elif id_triple[1] == GraphEntity.iri_has_literal_value:
                                id_literal_value = id_triple[2]
                        full_id = f'{id_schema.replace(GraphEntity.DATACITE, "")}:{id_literal_value}'
                        metaid_id_tuple = (res_triple[2].replace(f'{self.base_iri}/id/', ''), full_id)
                        metaid_id_list.append(metaid_id_tuple)
                result_list.append((res.replace(f'{self.base_iri}/br/', ''), title, metaid_id_list))
        return result_list
        
    def retrieve_br_from_meta(self, metaid: str) -> Tuple[str, List[Tuple[str, str]], bool]:
        '''
        Given a MetaID, it retrieves the title of the bibliographic resource having that MetaID and other identifiers of that entity.

        :params metaid: a MetaID
        :type metaid: str
        :returns Tuple[str, List[Tuple[str, str]]]: -- it returns a tuple of two elements. The first element is the resource's title associated with the input MetaID. The second element is a list of MetaID-ID tuples related to identifiers associated with that entity.
        '''
        metaid_uri = f'{self.base_iri}/br/{metaid}'
        title = ''
        identifiers = []
        it_exists = False

        for triple in self.local_g.triples((URIRef(metaid_uri), None, None)):
            it_exists = True
            if triple[1] == GraphEntity.iri_title:
                title = str(triple[2])
            elif triple[1] == GraphEntity.iri_has_identifier:
                id_scheme = ''
                literal_value = ''
                identifier = triple[2]
                for triple_inner in self.local_g.triples((identifier, None, None)):
                    if triple_inner[1] == GraphEntity.iri_uses_identifier_scheme:
                        id_scheme = str(triple_inner[2]).replace(GraphEntity.DATACITE, '')
                    elif triple_inner[1] == GraphEntity.iri_has_literal_value:
                        literal_value = str(triple_inner[2])
                if id_scheme and literal_value:  # Ensure both id_scheme and literal_value are found before appending
                    full_id = f'{id_scheme}:{literal_value}'
                    identifiers.append((str(identifier).replace(self.base_iri + '/id/', ''), full_id))

        if not it_exists:
            return "", [], False

        return title, identifiers, True

    # _______________________________ID_________________________________ #

    def retrieve_metaid_from_id(self, schema:str, value:str) -> str:
        '''
        Given the schema and value of an ID, it returns the MetaID associated with that identifier.

        :params schema: an identifier schema
        :type schema: str
        :params value: an identifier literal value
        :type value: str
        :returns str: -- it returns the MetaID associated with the input ID.
        '''
        schema = URIRef(GraphEntity.DATACITE + schema)
        value = value.replace('\\', '\\\\')
        for starting_triple in self.local_g.triples((None, GraphEntity.iri_has_literal_value, Literal(value))):
            for known_id_triple in self.local_g.triples((starting_triple[0], None, None)):
                if known_id_triple[1] == GraphEntity.iri_uses_identifier_scheme and known_id_triple[2] == schema:
                    return known_id_triple[0].replace(f'{self.base_iri}/id/', '')

    def retrieve_metaid_from_merged_entity(self, metaid_uri:str, prov_config:str) -> str:
        '''
        It looks for MetaId in the provenance. If the input entity was deleted due to a merge, this function returns the target entity. Otherwise, it returns None.

        :params metaid_uri: a MetaId URI
        :type metaid_uri: str
        :params prov_config: the path of the configuration file required by time-agnostic-library
        :type prov_config: str
        :returns str: -- It returns the MetaID associated with the target entity after a merge. If there was no merge, it returns None.
        '''
        metaval = None
        with open(prov_config, 'r', encoding='utf8') as f:
            prov_config_dict = yaml.safe_load(f)
        agnostic_meta = AgnosticEntity(res=metaid_uri, config=prov_config_dict, related_entities_history=False)
        agnostic_meta_history = agnostic_meta.get_history(include_prov_metadata=True)
        meta_history_data = agnostic_meta_history[0][metaid_uri]
        if meta_history_data:
            meta_history_metadata = agnostic_meta_history[1][metaid_uri]
            penultimate_snapshot = sorted(
                meta_history_metadata.items(),
                key=lambda x: parser.parse(x[1]['generatedAtTime']).replace(tzinfo=None),
                reverse=True
            )[1][0]
            query_if_it_was_merged = f'''
                SELECT DISTINCT ?se
                WHERE {{
                    ?se a <{ProvEntity.iri_entity}>;
                        <{ProvEntity.iri_was_derived_from}> <{penultimate_snapshot}>.
                }}
            '''
            results = self.__query(query_if_it_was_merged)['results']['bindings']
            # The entity was merged to another
            merged_entity = [se for se in results if metaid_uri not in se['se']['value']]
            if merged_entity:
                merged_entity:str = merged_entity[0]['se']['value']
                merged_entity = merged_entity.split('/prov/')[0]
                merged_entity = get_count(merged_entity)
                metaval = merged_entity
            return metaval

    # _______________________________RA_________________________________ #
    def retrieve_ra_from_meta(self, metaid: str) -> Tuple[str, List[Tuple[str, str]]]:
        '''
        Given a MetaID, it retrieves the name and id of the responsible agent associated with it, whether it is an author or a publisher.
        The output has the following format:

            ('NAME', [('METAID_OF_THE_IDENTIFIER', 'LITERAL_VALUE')])
            ('American Medical Association (ama)', [('4274', 'crossref:10')])

        :params metaid: a responsible agent's MetaID
        :type metaid: str
        :returns str: -- it returns a tuple, where the first element is the responsible agent's name, and the second element is a list containing its identifier's MetaID and literal value
        '''
        metaid_uri = f'{self.base_iri}/ra/{metaid}'
        family_name = ''
        given_name = ''
        name = ''
        identifiers = []
        it_exists = False

        for triple in self.local_g.triples((URIRef(metaid_uri), None, None)):
            it_exists = True
            if triple[1] == GraphEntity.iri_family_name:
                family_name = str(triple[2])
            elif triple[1] == GraphEntity.iri_given_name:
                given_name = str(triple[2])
            elif triple[1] == GraphEntity.iri_name:
                name = str(triple[2])
            elif triple[1] == GraphEntity.iri_has_identifier:
                identifier = triple[2]
                id_scheme = ''
                literal_value = ''
                for triple_inner in self.local_g.triples((identifier, None, None)):
                    if triple_inner[1] == GraphEntity.iri_uses_identifier_scheme:
                        id_scheme = str(triple_inner[2]).replace(GraphEntity.DATACITE, '')
                    elif triple_inner[1] == GraphEntity.iri_has_literal_value:
                        literal_value = str(triple_inner[2])
                if id_scheme and literal_value:
                    full_id = f'{id_scheme}:{literal_value}'
                    identifiers.append((str(identifier).replace(self.base_iri + '/id/', ''), full_id))
        
        if family_name and not given_name:
            full_name = f'{family_name}, '
        elif family_name and given_name:
            full_name = f'{family_name}, {given_name}'
        elif not family_name and given_name:
            full_name = f', {given_name}'
        elif name:
            full_name = name
        else:
            full_name = ''
            
        return full_name, identifiers, it_exists

    def retrieve_ra_from_id(self, schema:str, value:str, publisher:bool) -> List[Tuple[str, str, list]]:
        '''
        Given an identifier, it retrieves responsible agents associated with that identifier, related names and other identifiers MetaIDs and literal values.
        The output has the following format: ::

            [(METAID, NAME, [(METAID_OF_THE_IDENTIFIER, LITERAL_VALUE)])]
            [('3309', 'American Medical Association (ama)', [('4274', 'crossref:10')])]

        :params schema: an identifier schema
        :type schema: str
        :params value: an identifier literal value
        :type value: str
        :params publisher: True if the identifier is associated with a publisher, False otherwise.
        :type publisher: bool
        :returns List[Tuple[str, str, list]]: -- it returns a list of three elements tuples. The first element is the MetaID of a responsible agent associated with the input ID. The second element is the name of that responsible agent, if present. The third element is a list of MetaID-ID tuples related to identifiers associated with that responsible agent. 
        '''
        schema = URIRef(GraphEntity.DATACITE + schema)
        value = value.replace('\\', '\\\\')
        result_list = list()
        identifier_uri = None
        for starting_triple in self.local_g.triples((None, GraphEntity.iri_has_literal_value, Literal(value))):
            for known_id_triple in self.local_g.triples((starting_triple[0], None, None)):
                if known_id_triple[1] == GraphEntity.iri_uses_identifier_scheme and known_id_triple[2] == schema:
                    identifier_uri = known_id_triple[0]
                    break
            if identifier_uri:
                break
        if identifier_uri:
            metaid_id_list = [(identifier_uri.replace(f'{self.base_iri}/id/', ''), f'{schema.replace(GraphEntity.DATACITE, "")}:{value}')]
            for triple in self.local_g.triples((None, GraphEntity.iri_has_identifier, identifier_uri)):
                name = ''
                family_name = ''
                given_name = ''
                res = triple[0]
                for res_triple in self.local_g.triples((res, None, None)):
                    if res_triple[1] == GraphEntity.iri_name:
                        name = str(res_triple[2])
                    elif res_triple[1] == GraphEntity.iri_family_name:
                        family_name = str(res_triple[2])
                    elif res_triple[1] == GraphEntity.iri_given_name:
                        given_name = str(res_triple[2])
                    elif res_triple[1] == GraphEntity.iri_has_identifier and res_triple[2] != identifier_uri:
                        for id_triple in self.local_g.triples((res_triple[2], None, None)):
                            if id_triple[1] == GraphEntity.iri_uses_identifier_scheme:
                                id_schema = id_triple[2]
                            elif id_triple[1] == GraphEntity.iri_has_literal_value:
                                id_literal_value = id_triple[2]
                        full_id = f'{id_schema.replace(GraphEntity.DATACITE, "")}:{id_literal_value}'
                        metaid_id_tuple = (res_triple[2].replace(f'{self.base_iri}/id/', ''), full_id)
                        metaid_id_list.append(metaid_id_tuple)
                if name and not family_name and not given_name:
                    full_name = name
                elif not name and family_name and not given_name:
                    full_name = f'{family_name},'
                elif not name and not family_name and given_name:
                    full_name = f', {given_name}'
                elif not name and family_name and given_name:
                    full_name = f'{family_name}, {given_name}'
                else:
                    full_name = ''
                result_list.append((res.replace(f'{self.base_iri}/ra/', ''), full_name, metaid_id_list))
        return result_list

    # _______________________________VVI_________________________________ #

    def retrieve_venue_from_meta(self, meta_id:str) -> Dict[str, Dict[str, str]]:
        '''
        Given a MetaID, it returns the structure of volumes and issues contained in the related venue.
        The output has the following format: ::

            {
                'issue': {SEQUENCE_IDENTIFIER: {'id': META_ID}},
                'volume': {
                    SEQUENCE_IDENTIFIER: {
                        'id': META_ID,
                        'issue' {SEQUENCE_IDENTIFIER: {'id': META_ID}}
                    }
                }
            }

            {
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

        The first level 'issue' field includes the issues contained directly in the venue, 
        while the 'volume' field includes the volumes in the venue and the related issues.

        :params meta_id: a MetaID
        :type meta_id: str
        :returns: Dict[str, Dict[str, str]] -- the string with normalized hyphens
        '''
        content = dict()
        content['issue'] = dict()
        content['volume'] = dict()
        content = self.__retrieve_vvi(meta_id, content)
        return content

    def __retrieve_vvi(self, meta:str, content:Dict[str, dict]) -> dict:
        venue_iri = URIRef(f'{self.base_iri}/br/{meta}')
        ress = []
        for triple in self.local_g.triples((None, GraphEntity.iri_part_of, venue_iri)):
            res = {'res': None, 'type': None, 'sequence_identifier': None, 'container': None}
            res['res'] = triple[0].replace(f'{self.base_iri}/br/', '')
            for res_triple in self.local_g.triples((triple[0], None, None)):
                if res_triple[1] == RDF.type and res_triple[2] != GraphEntity.iri_expression:
                    res['type'] = res_triple[2]
                elif res_triple[1] == GraphEntity.iri_has_sequence_identifier:
                    res['sequence_identifier'] = str(res_triple[2])
                elif res_triple[1] == GraphEntity.iri_part_of:
                    res['container'] = res_triple[2]
            ress.append(res)
        for res in ress:
            if res['res'] is not None:
                if res['type'] == GraphEntity.iri_journal_issue and res['container'] == venue_iri:
                    content['issue'].setdefault(res['sequence_identifier'], dict())
                    content['issue'][res['sequence_identifier']]['id'] = res['res']
                elif res['type'] == GraphEntity.iri_journal_volume:
                    content['volume'].setdefault(res['sequence_identifier'], dict())
                    content['volume'][res['sequence_identifier']]['id'] = res['res']
                    content['volume'][res['sequence_identifier']]['issue'] = self.__retrieve_issues_by_volume(URIRef(f"{self.base_iri}/br/{res['res']}"))
        return content

    def __retrieve_issues_by_volume(self, res:URIRef) -> dict:
        content = dict()
        for triple in self.local_g.triples((None, GraphEntity.iri_part_of, res)):
            for res_triple in self.local_g.triples((triple[0], None, None)):
                if res_triple[1] == GraphEntity.iri_has_sequence_identifier:
                    content.setdefault(str(res_triple[2]), dict())
                    content[str(res_triple[2])]['id'] = res_triple[0].replace(f'{self.base_iri}/br/', '')
        return content
    
    def retrieve_ra_sequence_from_br_meta(self, metaid:str, col_name:str) -> List[Dict[str, tuple]]:
        '''
        Given a bibliographic resource's MetaID and a field name, it returns its agent roles and responsible agents in the correct order according to the specified field.
        The output has the following format: ::

            [
                {METAID_AR_1: (NAME_RA_1, [(METAID_ID_RA_1, LITERAL_VALUE_ID_RA_1)], METAID_RA_1)}, 
                {METAID_AR_2: (NAME_RA_2, [(METAID_ID_RA_2, LITERAL_VALUE_ID_RA_2)], METAID_RA_2)}, 
                {METAID_AR_N: (NAME_RA_N, [(METAID_ID_RA_N, LITERAL_VALUE_ID_RA_N)], METAID_RA_N)}, 
            ]

            [
                {'5343': ('Hodge, James G.', [], '3316')}, 
                {'5344': ('Anderson, Evan D.', [], '3317')}, 
                {'5345': ('Kirsch, Thomas D.', [], '3318')}, 
                {'5346': ('Kelen, Gabor D.', [('4278', 'orcid:0000-0002-3236-8286')], '3319')}
            ]   

        :params metaid: a MetaID
        :type meta_id: str
        :params col_name: a MetaID
        :type col_name: str
        :returns: List[Dict[str, tuple]] -- the output is a list of three-elements tuples. Each tuple's first and third elements are the MetaIDs of an agent role and responsible agent related to the specified bibliographic resource. The second element is a two-elements tuple, where the first element is the MetaID of the identifier of the responsible agent. In contrast, the second one is the literal value of that id.
        '''
        if col_name == 'author':
            role = GraphEntity.iri_author
        elif col_name == 'editor':
            role = GraphEntity.iri_editor
        else:
            role = GraphEntity.iri_publisher
        metaid_uri = URIRef(f'{self.base_iri}/br/{str(metaid)}')
        dict_ar = dict()
        br_ars = list()
        roles_with_next = set()
        for triple in self.local_g.triples((metaid_uri, GraphEntity.iri_is_document_context_for, None)):
            for ar_triple in self.local_g.triples((triple[2], None, None)):
                if ar_triple[2] == role:
                    br_ars.append(str(triple[2]))
                    role_value = str(triple[2]).replace(f'{self.base_iri}/ar/', '')
                    next_role = ''
                    for relevant_ar_triple in self.local_g.triples((triple[2], None, None)):                            
                        if relevant_ar_triple[1] == GraphEntity.iri_has_next:
                            next_role = str(relevant_ar_triple[2]).replace(f'{self.base_iri}/ar/', '')
                            roles_with_next.add(next_role)
                        elif relevant_ar_triple[1] == GraphEntity.iri_is_held_by:
                            ra = str(relevant_ar_triple[2]).replace(f'{self.base_iri}/ra/', '')
                    try:
                        dict_ar[role_value] = {'next': next_role, 'ra': ra}
                    except UnboundLocalError:
                        print(list(self.local_g.triples((triple[2], None, None))))
                        raise(UnboundLocalError)
                        
        # Find the start_role by excluding all roles that are "next" for others from the set of all roles.
        all_roles = set(dict_ar.keys())
        start_role_candidates = all_roles - roles_with_next
        # Handle the edge cases for start role determination
        if len(all_roles) == 0:
            return []
        elif len(start_role_candidates) != 1:
            # If more than one start candidate exists or none exist in a multi-role situation, resolve automatically
            chains = []
            for start_candidate in start_role_candidates:
                current_role = start_candidate
                chain = []
                visited_roles = set()
                while current_role and current_role not in visited_roles:
                    visited_roles.add(current_role)
                    ra_info = self.retrieve_ra_from_meta(dict_ar[current_role]['ra'])[0:2]
                    ra_tuple = ra_info + (dict_ar[current_role]['ra'],)
                    chain.append({current_role: ra_tuple})
                    current_role = dict_ar[current_role]['next']  # Move to the next role.
                chains.append(chain)
            # Sort chains by length, then by the lowest sequential number of the starting role
            chains.sort(key=lambda chain: (-len(chain), get_resource_number(f'{self.base_iri}/ar/{list(chain[0].keys())[0]}')))
            ordered_ar_list = chains[0]
            meta_editor = MetaEditor(meta_config=self.meta_config_path, resp_agent='https://w3id.org/oc/meta/prov/pa/1')
            for chain in chains[1:]:
                for ar_dict in chain:
                    for ar in ar_dict.keys():
                        meta_editor.delete(res=f"{self.base_iri}/ar/{ar}")
        else:
        # Follow the "next" chain from the start_role to construct an ordered list.
            ordered_ar_list = []
            start_role = start_role_candidates.pop()
            current_role = start_role
            while current_role:
                ra_info = self.retrieve_ra_from_meta(dict_ar[current_role]['ra'])[0:2]
                ra_tuple = ra_info + (dict_ar[current_role]['ra'],)
                ordered_ar_list.append({current_role: ra_tuple})
                current_role = dict_ar[current_role]['next']  # Move to the next role.
        # Now 'ordered_ar_list' should contain the roles in the correct order, starting from 'start_role'.
        return ordered_ar_list

    def retrieve_re_from_br_meta(self, metaid:str) -> Tuple[str, str]:
        '''
            Given a bibliographic resource's MetaID, it returns its resource embodiment's MetaID and pages.
            The output has the following format: ::

                (METAID, PAGES)
                ('2011', '391-397')

            :params metaid: a bibliographic resource's MetaID
            :type meta_id: str
            :returns: Tuple[str, str] -- the output is a two-elements tuple, where the first element is the MetaID of the resource embodiment, and the second is a pages' interval. 
        '''
        metaid_uri = URIRef(f'{self.base_iri}/br/{str(metaid)}')
        re_uri = None
        starting_page = None
        ending_page = None
        for triple in self.local_g.triples((metaid_uri, GraphEntity.iri_embodiment, None)):
            re_uri = triple[2].replace(f'{self.base_iri}/re/', '')
            for re_triple in self.local_g.triples((triple[2], None, None)):
                if re_triple[1] == GraphEntity.iri_starting_page:
                    starting_page = str(re_triple[2])
                elif re_triple[1] == GraphEntity.iri_ending_page:
                    ending_page = str(re_triple[2])
        if re_uri:
            if starting_page and ending_page:
                pages = f'{starting_page}-{ending_page}'
            elif starting_page and not ending_page:
                pages = f'{starting_page}-{starting_page}'
            elif not starting_page and ending_page:
                pages = f'{ending_page}-{ending_page}'
            elif not starting_page and not ending_page:
                pages = ''
            return re_uri, pages

    def retrieve_br_info_from_meta(self, metaid:str) -> dict:
        '''
            Given a bibliographic resource's MetaID, it returns all the information about that resource.
            The output has the following format: ::

                {
                    'pub_date': PUB_DATE, 
                    'type': TYPE, 
                    'page': (METAID, PAGES), 
                    'issue': ISSUE, 
                    'volume': VOLUME, 
                    'venue': VENUE
                }
                {
                    'pub_date': '2006-02-27', 
                    'type': 'journal article', 
                    'page': ('2011', '391-397'), 
                    'issue': '4', 
                    'volume': '166', 
                    'venue': 'Archives Of Internal Medicine [omid:br/4387]'
                }

            :params metaid: a bibliographic resource's MetaID
            :type meta_id: str
            :returns: Tuple[str, str] -- the output is a dictionary including the publication date, type, page, issue, volume, and venue of the specified bibliographic resource.
        '''
        metaid = str(metaid)
        metaid_uri = URIRef(f'{self.base_iri}/br/{metaid}') if self.base_iri not in metaid else URIRef(metaid)
        res_dict = {
            'pub_date': '',
            'type': '',
            'page': self.retrieve_re_from_br_meta(metaid),
            'issue': '',
            'volume': '',
            'venue': ''
        }
        for triple in self.local_g.triples((metaid_uri, None, None)):
            if triple[1] == GraphEntity.iri_has_publication_date:
                res_dict['pub_date'] = str(triple[2])
            elif triple[1] == RDF.type and triple[2] != GraphEntity.iri_expression:
                res_dict['type'] = self._type_it(triple[2])
            elif triple[1] == GraphEntity.iri_has_sequence_identifier:
                for inner_triple in self.local_g.triples((metaid_uri, None, None)):
                    if inner_triple[2] == GraphEntity.iri_journal_issue:
                        res_dict['issue'] = str(triple[2])
                    elif inner_triple[2] == GraphEntity.iri_journal_volume:
                        res_dict['volume'] = str(triple[2])
            elif triple[1] == GraphEntity.iri_part_of:
                for vvi_triple in self.local_g.triples((triple[2], None, None)):
                    if vvi_triple[2] == GraphEntity.iri_journal_issue:
                        for inner_vvi_triple in self.local_g.triples((triple[2], None, None)):
                            if inner_vvi_triple[1] == GraphEntity.iri_has_sequence_identifier:
                                res_dict['issue'] = str(inner_vvi_triple[2])
                    elif vvi_triple[2] == GraphEntity.iri_journal_volume:
                        for inner_vvi_triple in self.local_g.triples((triple[2], None, None)):
                            if inner_vvi_triple[1] == GraphEntity.iri_has_sequence_identifier:
                                res_dict['volume'] = str(inner_vvi_triple[2])
                    elif vvi_triple[2] == GraphEntity.iri_journal:
                        for inner_vvi_triple in self.local_g.triples((triple[2], None, None)):
                            if inner_vvi_triple[1] == GraphEntity.iri_title:
                                res_dict['venue'] = str(inner_vvi_triple[2])+ ' [omid:' + inner_vvi_triple[0].replace(f'{self.base_iri}/', '') + ']'
                    elif vvi_triple[1] == GraphEntity.iri_part_of:
                        for vi_triple in self.local_g.triples((vvi_triple[2], None, None)):
                            if vi_triple[2] == GraphEntity.iri_journal_volume:
                                for inner_vvi_triple in self.local_g.triples((vvi_triple[2], None, None)):
                                    if inner_vvi_triple[1] == GraphEntity.iri_has_sequence_identifier:
                                        res_dict['volume'] = str(inner_vvi_triple[2])
                            elif vi_triple[1] == GraphEntity.iri_journal:
                                for inner_vvi_triple in self.local_g.triples((vvi_triple[2], None, None)):
                                    if inner_vvi_triple[1] == GraphEntity.iri_title:
                                        res_dict['venue'] = str(inner_vvi_triple[2])+ ' [omid:' + inner_vvi_triple[0].replace(f'{self.base_iri}/', '') + ']'
                            elif vi_triple[1] == GraphEntity.iri_part_of:
                                for venue_triple in self.local_g.triples((vi_triple[2], None, None)):
                                    if venue_triple[1] == GraphEntity.iri_title:
                                        res_dict['venue'] = str(venue_triple[2])+ ' [omid:' + venue_triple[0].replace(f'{self.base_iri}/', '') + ']'
        return res_dict

    @staticmethod
    def _type_it(br_type: URIRef) -> str:
        output_type = ''
        if br_type == GraphEntity.iri_archival_document:
            output_type = 'archival document'
        if br_type == GraphEntity.iri_book:
            output_type = 'book'
        if br_type == GraphEntity.iri_book_chapter:
            output_type = 'book chapter'
        if br_type == GraphEntity.iri_part:
            output_type = 'book part'
        if br_type == GraphEntity.iri_expression_collection:
            output_type = 'book section'
        if br_type == GraphEntity.iri_book_series:
            output_type = 'book series'
        if br_type == GraphEntity.iri_book_set:
            output_type = 'book set'
        if br_type == GraphEntity.iri_data_file:
            output_type = 'data file'
        if br_type == GraphEntity.iri_thesis:
            output_type = 'dissertation'
        if br_type == GraphEntity.iri_journal:
            output_type = 'journal'
        if br_type == GraphEntity.iri_journal_article:
            output_type = 'journal article'
        if br_type == GraphEntity.iri_journal_issue:
            output_type = 'journal issue'
        if br_type == GraphEntity.iri_journal_volume:
            output_type = 'journal volume'
        if br_type == GraphEntity.iri_proceedings_paper:
            output_type = 'proceedings article'
        if br_type == GraphEntity.iri_academic_proceedings:
            output_type = 'proceedings'
        if br_type == GraphEntity.iri_reference_book:
            output_type = 'reference book'
        if br_type == GraphEntity.iri_reference_entry:
            output_type = 'reference entry'
        if br_type == GraphEntity.iri_series:
            output_type = 'series'
        if br_type == GraphEntity.iri_report_document:
            output_type = 'report'
        if br_type == GraphEntity.iri_specification_document:
            output_type = 'standard'
        return output_type
    
    def retrieve_publisher_from_br_metaid(self, metaid:str):
        metaid_uri = URIRef(f'{self.base_iri}/br/{metaid}')
        publishers = set()
        for triple in self.local_g.triples((metaid_uri, None, None)):
            if triple[1] == GraphEntity.iri_is_document_context_for:
                for document_triple in self.local_g.triples((triple[2], None, None)):
                    if document_triple[2] == GraphEntity.iri_publisher:
                        publishers.add(triple[2])
            elif triple[1] == GraphEntity.iri_part_of:
                for inner_triple in self.local_g.triples((triple[2], None, None)):
                    if inner_triple[1] == GraphEntity.iri_is_document_context_for:
                        for document_triple in self.local_g.triples((inner_triple[2], None, None)):
                            if document_triple[2] == GraphEntity.iri_publisher:
                                publishers.add(inner_triple[2])
                    elif inner_triple[1] == GraphEntity.iri_part_of:
                        for inner_inner_triple in self.local_g.triples((inner_triple[2], None, None)):
                            if inner_inner_triple[1] == GraphEntity.iri_is_document_context_for:
                                for document_triple in self.local_g.triples((inner_inner_triple[2], None, None)):
                                    if document_triple[2] == GraphEntity.iri_publisher:
                                        publishers.add(inner_inner_triple[2])
        publishers_output = []
        for publisher_uri in publishers:
            pub_identifiers = []
            pub_name = None
            for triple in self.local_g.triples((publisher_uri, None, None)):
                if triple[1] == GraphEntity.iri_is_held_by:
                    pub_metaid = triple[2].replace(f'{self.base_iri}/', 'omid:')
                    pub_identifiers.append(pub_metaid)
                    for ra_triple in self.local_g.triples((triple[2], None, None)):
                        pub_schema = None
                        pub_literal = None
                        if ra_triple[1] == GraphEntity.iri_name:
                            pub_name = ra_triple[2]
                        elif ra_triple[1] == GraphEntity.iri_has_identifier:
                            for id_triple in self.local_g.triples((ra_triple[2], None, None)):
                                if id_triple[1] == GraphEntity.iri_uses_identifier_scheme:
                                    pub_schema = id_triple[2].replace(f'{str(GraphEntity.DATACITE)}', '')
                                elif id_triple[1] == GraphEntity.iri_has_literal_value:
                                    pub_literal = id_triple[2]
                        if pub_schema is not None and pub_literal is not None:
                            pub_id = f'{pub_schema}:{pub_literal}'
                            pub_identifiers.append(pub_id)
            if pub_name is not None:
                pub_full = f'{pub_name} [{" ".join(pub_identifiers)}]'
            else:
                pub_full = f'[{" ".join(pub_identifiers)}]'
            publishers_output.append(pub_full)
        return '; '.join(publishers_output)
            
    def get_everything_about_res(self, metavals: set, identifiers: set, vvis: set, max_depth: int = 10) -> None:
        BATCH_SIZE = 10
        def batch_process(input_set, batch_size):
            """Generator to split input data into smaller batches if batch_size is not None."""
            if batch_size is None:
                yield input_set
            else:
                for i in range(0, len(input_set), batch_size):
                    yield input_set[i:i + batch_size]

        def process_batch(subjects, cur_depth):
            """Process each batch of subjects up to the specified depth."""
            if not subjects or (max_depth and cur_depth > max_depth):
                return

            next_subjects = set()
            for batch in batch_process(list(subjects), BATCH_SIZE):
                query_prefix = f'''
                    SELECT ?s ?p ?o
                    WHERE {{
                        VALUES ?s {{ {' '.join([f"<{s}>" for s in batch])} }}
                        ?s ?p ?o.
                    }}'''
                
                result = self.__query(query_prefix)
                if result:
                    for row in result['results']['bindings']:
                        s = URIRef(row['s']['value'])
                        p = URIRef(row['p']['value'])
                        o = row['o']['value']
                        o_type = row['o']['type']
                        o_datatype = URIRef(row['o']['datatype']) if 'datatype' in row['o'] else None
                        o = URIRef(o) if o_type == 'uri' else Literal(lexical_or_value=o, datatype=o_datatype)
                        self.local_g.add((s, p, o))
                        if isinstance(o, URIRef) and p not in {RDF.type, GraphEntity.iri_with_role, GraphEntity.iri_uses_identifier_scheme}:
                            next_subjects.add(str(o))

            # Dopo aver processato tutti i batch di questo livello, procedi con il prossimo livello di profondità
            process_batch(next_subjects, cur_depth + 1)

        def get_initial_subjects_from_metavals(metavals):
            """Convert metavals to a set of subjects."""
            return {f"{self.base_iri}/{mid.replace('omid:', '')}" for mid in metavals}

        def get_initial_subjects_from_identifiers(identifiers):
            """Convert identifiers to a set of subjects based on batch queries."""
            subjects = set()
            for batch in batch_process(list(identifiers), BATCH_SIZE):
                if not batch:
                    continue

                if self.blazegraph_full_text_search:
                    # Processing for text search enabled databases
                    for identifier in batch:
                        scheme, literal = identifier.split(":", 1)
                        escaped_identifier = literal.replace('\\', '\\\\').replace('"', '\\"')
                        query = f'''
                            PREFIX bds: <http://www.bigdata.com/rdf/search#>
                            SELECT ?s WHERE {{
                                ?literal bds:search "{escaped_identifier}" ;
                                        bds:matchAllTerms "true" ;
                                        ^<{GraphEntity.iri_has_literal_value}> ?id.
                                ?id <{GraphEntity.iri_uses_identifier_scheme}> <{GraphEntity.DATACITE + scheme}>;
                                    ^<{GraphEntity.iri_has_identifier}> ?s .
                            }}
                        '''
                        result = self.__query(query)
                        for row in result['results']['bindings']:
                            subjects.add(str(row['s']['value']))
                elif self.virtuoso_full_text_search:
                    union_blocks = []
                    for identifier in batch:
                        scheme, literal = identifier.split(':', maxsplit=1)[0], identifier.split(':', maxsplit=1)[1]
                        escaped_literal = literal.replace('\\', '\\\\').replace('"', '\\"')
                        union_blocks.append(f"""
                            {{  
                                ?id <{GraphEntity.iri_has_literal_value}> ?literal .
                                ?literal bif:contains "'{escaped_literal}'" .
                                ?id <{GraphEntity.iri_uses_identifier_scheme}> <{GraphEntity.DATACITE + scheme}> .
                                ?s <{GraphEntity.iri_has_identifier}> ?id .                                    
                            }}
                        """)
                    union_query = " UNION ".join(union_blocks)
                    query = f'''
                        SELECT ?s WHERE {{
                            {union_query}
                        }}
                    '''
                    result = self.__query(query)
                    for row in result['results']['bindings']:
                        subjects.add(str(row['s']['value']))
                else:
                    identifiers_values = []
                    for identifier in batch:
                        scheme, literal = identifier.split(':', maxsplit=1)[0], identifier.split(':', maxsplit=1)[1]
                        escaped_literal = literal.replace('\\', '\\\\').replace('"', '\\"')
                        identifiers_values.append(f"(<{GraphEntity.DATACITE + scheme}> \"{escaped_literal}\")")
                    identifiers_values_str = " ".join(identifiers_values)
                    query = f'''
                        SELECT ?s WHERE {{
                            VALUES (?scheme ?literal) {{ {identifiers_values_str} }}
                            ?id <{GraphEntity.iri_uses_identifier_scheme}> ?scheme;
                                <{GraphEntity.iri_has_literal_value}> ?literal;
                                ^<{GraphEntity.iri_has_identifier}> ?s .
                        }}
                    '''
                    result = self.__query(query)
                    for row in result['results']['bindings']:
                        subjects.add(str(row['s']['value']))
            return subjects

        def get_initial_subjects_from_vvis(vvis):
            """Convert vvis to a set of subjects based on batch queries."""
            subjects = set()
            for batch in batch_process(list(vvis), BATCH_SIZE):
                if not batch:
                    continue

                for volume, issue, venue_metaid in batch:
                    vvi_type = GraphEntity.iri_journal_issue if issue else GraphEntity.iri_journal_volume
                    query = f'''
                        SELECT ?s WHERE {{
                            ?s a <{vvi_type}>;
                                <{GraphEntity.iri_part_of}>+ <{self.base_iri}/{venue_metaid.replace("omid:", "")}>;
                                <{GraphEntity.iri_has_sequence_identifier}> "{issue if issue else volume}".
                        }}
                    '''
                    result = self.__query(query)
                    for row in result['results']['bindings']:
                        subjects.add(str(row['s']['value']))
            return subjects

        initial_subjects = set()

        if metavals:
            initial_subjects.update(get_initial_subjects_from_metavals(metavals))
        
        if identifiers:
            initial_subjects.update(get_initial_subjects_from_identifiers(identifiers))

        if vvis:
            initial_subjects.update(get_initial_subjects_from_vvis(vvis))

        # Now start the depth-based processing
        process_batch(initial_subjects, 0)

    def get_subgraph(self, res: str, graphs_dict: dict) -> Graph|None:
        if res in graphs_dict:
            return graphs_dict[res]
        subgraph = Graph()
        for triple in self.local_g.triples((res, None, None)):
            subgraph.add(triple)
        if len(subgraph):
            graphs_dict[res] = subgraph
            return subgraph