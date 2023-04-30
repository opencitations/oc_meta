import json
from time import sleep
from typing import Dict, List, Tuple

from dateutil import parser
from oc_ocdm.graph import GraphEntity
from oc_ocdm.prov.prov_entity import ProvEntity
from oc_ocdm.support import get_count
from rdflib import Graph, Literal, URIRef
from SPARQLWrapper import GET, JSON, XML, SPARQLWrapper
from time_agnostic_library.agnostic_entity import AgnosticEntity


class ResourceFinder:

    def __init__(self, ts_url, base_iri:str, local_g: Graph = Graph()):
        self.ts = SPARQLWrapper(ts_url)
        self.ts.setMethod(GET)
        self.base_iri = base_iri[:-1] if base_iri[-1] == '/' else base_iri
        self.local_g = local_g

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
        
    def retrieve_br_from_meta(self, metaid:str) -> Tuple[str, List[Tuple[str, str]]]:
        '''
        Given a MetaID, it retrieves the title of the bibliographic resource having that MetaID and other identifiers of that entity.

        :params metaid: a MetaID
        :type metaid: str
        :returns Tuple[str, List[Tuple[str, str]]]: -- it returns a tuple of two elements. The first element is the resource's title associated with the input MetaID. The second element is a list of MetaID-ID tuples related to identifiers associated with that entity.
        '''
        metaid_uri = f'{self.base_iri}/br/{str(metaid)}'
        title = None
        identifiers_found = []
        identifiers = []
        id_scheme = None
        for triple in self.local_g.triples((URIRef(metaid_uri), None, None)):
            if triple[1] == GraphEntity.iri_title:
                title = str(triple[2])
            elif triple[1] == GraphEntity.iri_has_identifier:
                identifiers_found.append(triple[2])
        if identifiers_found:
            for identifier in identifiers_found:
                for triple in self.local_g.triples((identifier, None, None)):
                    if triple[1] == GraphEntity.iri_uses_identifier_scheme:
                        id_scheme = str(triple[2]).replace(GraphEntity.DATACITE, '')
                    elif triple[1] == GraphEntity.iri_has_literal_value:
                        literal_value = str(triple[2])
                full_id = f'{id_scheme}:{literal_value}'
                identifiers.append((str(identifier).replace(self.base_iri + '/id/', ''), full_id))
        if not title and not identifiers:
            return None
        return title, identifiers

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
        agnostic_meta = AgnosticEntity(res=metaid_uri, related_entities_history=False, config_path=prov_config)
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
    def retrieve_ra_from_meta(self, metaid:str, publisher:bool=False) -> Tuple[str, List[Tuple[str, str]]]:
        '''
        Given a MetaID, it retrieves the name and id of the responsible agent associated with it, whether it is an author or a publisher.
        The output has the following format:

            ('NAME', [('METAID_OF_THE_IDENTIFIER', 'LITERAL_VALUE')])
            ('American Medical Association (ama)', [('4274', 'crossref:10')])

        :params metaid: a responsible agent's MetaID
        :type metaid: str
        :params publisher: True if the MetaID is associated with a publisher, False otherwise.  
        :type publisher: bool
        :returns str: -- it returns a tuple, where the first element is the responsible agent's name, and the second element is a list containing its identifier's MetaID and literal value
        '''
        metaid_uri = f'{self.base_iri}/ra/{str(metaid)}'
        family_name = None
        given_name = None
        name = None
        identifiers_found = []
        identifiers = []
        id_scheme = None
        for triple in self.local_g.triples((URIRef(metaid_uri), None, None)):
            if triple[1] == GraphEntity.iri_family_name:
                family_name = str(triple[2])
            elif triple[1] == GraphEntity.iri_given_name:
                given_name = str(triple[2])
            elif triple[1] == GraphEntity.iri_name:
                name = str(triple[2])
            elif triple[1] == GraphEntity.iri_has_identifier:
                identifiers_found.append(triple[2])
        if family_name and not given_name:
            full_name = f'{family_name},'
        elif family_name and given_name:
            full_name = f'{family_name}, {given_name}'
        elif not family_name and given_name:
            full_name = f', {given_name}'
        elif name:
            full_name = name
        else:
            full_name = ''
        if identifiers_found:
            for identifier in identifiers_found:
                for triple in self.local_g.triples((identifier, None, None)):
                    if triple[1] == GraphEntity.iri_uses_identifier_scheme:
                        id_scheme = str(triple[2]).replace(GraphEntity.DATACITE, '')
                    elif triple[1] == GraphEntity.iri_has_literal_value:
                        literal_value = str(triple[2])
                full_id = f'{id_scheme}:{literal_value}'
                identifiers.append((str(identifier).replace(self.base_iri + '/id/', ''), full_id))
        if not full_name and not identifiers:
            return None
        return full_name, identifiers

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
        query = f'''
            SELECT DISTINCT ?res
                (GROUP_CONCAT(DISTINCT ?container; separator=' ;and; ') AS ?container_)
                (GROUP_CONCAT(DISTINCT ?type; separator=' ;and; ') AS ?type_)
                (GROUP_CONCAT(DISTINCT ?title) AS ?title_)
            WHERE {{
                ?res <{GraphEntity.iri_part_of}>+ <{self.base_iri}/br/{meta}>;
                    <{GraphEntity.iri_part_of}> ?container;
                    a ?type;
                    <{GraphEntity.iri_has_sequence_identifier}> ?title.
            }} GROUP BY ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            results = result['results']['bindings']
            for x in results:
                res = str(x['res']['value']).replace(f'{self.base_iri}/br/', '')
                container = str(x['container_']['value'])
                title = str(x['title_']['value'])
                types = str(x['type_']['value']).split(' ;and; ')
                if str(GraphEntity.iri_journal_issue) in types and container == f'{self.base_iri}/br/{meta}':
                    content['issue'].setdefault(title, dict())
                    content['issue'][title]['id'] = res
                elif str(GraphEntity.iri_journal_volume) in types:
                    content['volume'].setdefault(title, dict())
                    content['volume'][title]['id'] = res
                    content['volume'][title]['issue'] = self.__retrieve_issues_by_volume(results, res)
        return content

    def __retrieve_issues_by_volume(self, data:List[Dict[str, Dict[str, str]]], res:str) -> dict:
        content = dict()
        for item in data:
            if res in item['container_']['value'] and str(GraphEntity.iri_journal_issue in item['type_']['value']):
                title = item['title_']['value']
                content[title] = dict()
                content[title]['id'] = item['res']['value'].replace(f'{self.base_iri}/br/', '')
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
        metaid_uri = f'{self.base_iri}/br/{str(metaid)}'
        query = f'''
            SELECT DISTINCT ?role ?next ?ra
            WHERE {{
                <{metaid_uri}> <{GraphEntity.iri_is_document_context_for}> ?role.
                ?role <{GraphEntity.iri_with_role}> <{role}>;
                    <{GraphEntity.iri_is_held_by}> ?ra
                OPTIONAL {{?role <{GraphEntity.iri_has_next}> ?next.}}
            }}
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            results = result['results']['bindings']
            dict_ar = dict()
            for ra_dict in results:
                role = str(ra_dict['role']['value']).replace(f'{self.base_iri}/ar/', '')
                if 'next' in ra_dict:
                    next_role = str(ra_dict['next']['value']).replace(f'{self.base_iri}/ar/', '')
                else:
                    next_role = ''
                ra = str(ra_dict['ra']['value']).replace(f'{self.base_iri}/ra/', '')
                dict_ar[role] = dict()
                dict_ar[role]['next'] = next_role
                dict_ar[role]['ra'] = ra
            ar_list = list()
            last = ''
            while dict_ar:
                for ar_metaid in dict_ar:
                    if dict_ar[ar_metaid]['next'] == last:
                        if col_name == 'publisher':
                            ra_info = self.retrieve_ra_from_meta(dict_ar[ar_metaid]['ra'], publisher=True) +\
                                         (dict_ar[ar_metaid]['ra'],)
                        else:
                            ra_info = self.retrieve_ra_from_meta(dict_ar[ar_metaid]['ra'], publisher=False) +\
                                         (dict_ar[ar_metaid]['ra'],)
                        ar_dic = dict()
                        ar_dic[ar_metaid] = ra_info
                        ar_list.append(ar_dic)
                        last = ar_metaid
                        del dict_ar[ar_metaid]
                        break
            ar_list.reverse()
            return ar_list
        else:
            return None

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
        metaid_uri = f'{self.base_iri}/br/{str(metaid)}'
        query = f'''
            SELECT DISTINCT ?re ?sp ?ep
            WHERE {{
                <{metaid_uri}> <{GraphEntity.iri_embodiment}> ?re.
                ?re <{GraphEntity.iri_starting_page}> ?sp;
                    <{GraphEntity.iri_ending_page}> ?ep.
            }}
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            meta = result['results']['bindings'][0]['re']['value'].replace(f'{self.base_iri}/re/', '')
            pages = result['results']['bindings'][0]['sp']['value'] + '-' +\
                result['results']['bindings'][0]['ep']['value']
            return meta, pages
        else:
            return None

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
        metaid_uri = f'{self.base_iri}/br/{metaid}' if self.base_iri not in metaid else metaid
        query = f'''
            SELECT ?res 
            (GROUP_CONCAT(DISTINCT ?type; separator=' ;and; ') AS ?type_)
            (GROUP_CONCAT(DISTINCT ?date; separator=' ;and; ') AS ?date_)
            (GROUP_CONCAT(DISTINCT ?num; separator=' ;and; ') AS ?num_)
            (GROUP_CONCAT(DISTINCT ?part1; separator=' ;and; ') AS ?part1_)
            (GROUP_CONCAT(DISTINCT ?title1; separator=' ;and; ') AS ?title1_)
            (GROUP_CONCAT(DISTINCT ?num1; separator=' ;and; ') AS ?num1_)
            (GROUP_CONCAT(DISTINCT ?type1; separator=' ;and; ') AS ?type1_)
            (GROUP_CONCAT(DISTINCT ?part2; separator=' ;and; ') AS ?part2_)
            (GROUP_CONCAT(DISTINCT ?title2; separator=' ;and; ') AS ?title2_)
            (GROUP_CONCAT(DISTINCT ?num2; separator=' ;and; ') AS ?num2_)
            (GROUP_CONCAT(DISTINCT ?type2; separator=' ;and; ') AS ?type2_)
            (GROUP_CONCAT(DISTINCT ?part3; separator=' ;and; ') AS ?part3_)
            (GROUP_CONCAT(DISTINCT ?title3; separator=' ;and; ') AS ?title3_)
            (GROUP_CONCAT(DISTINCT ?num3; separator=' ;and; ') AS ?num3_)
            (GROUP_CONCAT(DISTINCT ?type3; separator=' ;and; ') AS ?type3_)
            WHERE {{
                ?res a ?type.
                OPTIONAL {{?res <{GraphEntity.iri_has_publication_date}> ?date.}}
                OPTIONAL {{?res <{GraphEntity.iri_has_sequence_identifier}> ?num.}}
                OPTIONAL {{
                    ?res <{GraphEntity.iri_part_of}> ?part1.
                    OPTIONAL {{?part1 <{GraphEntity.iri_title}> ?title1.}}
                    OPTIONAL {{?part1 <{GraphEntity.iri_has_sequence_identifier}> ?num1.}}
                    ?part1 a ?type1.
                    OPTIONAL {{
                        ?part1 <{GraphEntity.iri_part_of}> ?part2.
                        OPTIONAL {{?part2 <{GraphEntity.iri_title}> ?title2.}}
                        OPTIONAL {{?part2 <{GraphEntity.iri_has_sequence_identifier}> ?num2.}}
                        ?part2 a ?type2.
                        OPTIONAL{{
                            ?part2 <{GraphEntity.iri_part_of}> ?part3.
                            OPTIONAL {{?part3 <{GraphEntity.iri_title}> ?title3.}}
                            OPTIONAL {{?part3 <{GraphEntity.iri_has_sequence_identifier}> ?num3.}}
                            ?part3 a ?type3.
                        }}
                    }}
                }}
                BIND (<{metaid_uri}> AS ?res)
            }} 
            GROUP BY ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            bindings = result['results']['bindings'][0]
            res_dict = {
                'pub_date': '',
                'type': '',
                'page': '',
                'issue': '',
                'volume': '',
                'venue': ''
            }
            if 'date_' in bindings:
                res_dict['pub_date'] = bindings['date_']['value']
            res_dict['type'] = self._type_it(bindings, 'type_')
            res_dict['page'] = self.retrieve_re_from_br_meta(metaid)
            if 'num_' in bindings:
                if 'issue' in self._type_it(bindings, 'type_'):
                    res_dict['issue'] = str(bindings['num_']['value'])
                elif 'volume' in self._type_it(bindings, 'type_'):
                    res_dict['volume'] = str(bindings['num_']['value'])
            res_dict = self._vvi_find(bindings, 'part1_', 'type1_', 'title1_', 'num1_', res_dict)
            res_dict = self._vvi_find(bindings, 'part2_', 'type2_', 'title2_', 'num2_', res_dict)
            res_dict = self._vvi_find(bindings, 'part3_', 'type3_', 'title3_', 'num3_', res_dict)
            return res_dict
        else:
            return None

    @staticmethod
    def _type_it(result:Dict[str, Dict[str, str]], variable:str) -> str:
        output_type = ''
        if variable in result:
            types = result[variable]['value'].split(' ;and; ')
            for type in types:
                if type != str(GraphEntity.iri_expression):
                    output_type = str(type)
                    if str(output_type) == str(GraphEntity.iri_archival_document):
                        output_type = 'archival document'
                    if str(output_type) == str(GraphEntity.iri_book):
                        output_type = 'book'
                    if str(output_type) == str(GraphEntity.iri_book_chapter):
                        output_type = 'book chapter'
                    if str(output_type) == str(GraphEntity.iri_part):
                        output_type = 'book part'
                    if str(output_type) == str(GraphEntity.iri_expression_collection):
                        output_type = 'book section'
                    if str(output_type) == str(GraphEntity.iri_book_series):
                        output_type = 'book series'
                    if str(output_type) == str(GraphEntity.iri_book_set):
                        output_type = 'book set'
                    if str(output_type) == str(GraphEntity.iri_data_file):
                        output_type = 'data file'
                    if str(output_type) == str(GraphEntity.iri_thesis):
                        output_type = 'dissertation'
                    if str(output_type) == str(GraphEntity.iri_journal):
                        output_type = 'journal'
                    if str(output_type) == str(GraphEntity.iri_journal_article):
                        output_type = 'journal article'
                    if str(output_type) == str(GraphEntity.iri_journal_issue):
                        output_type = 'journal issue'
                    if str(output_type) == str(GraphEntity.iri_journal_volume):
                        output_type = 'journal volume'
                    if str(output_type) == str(GraphEntity.iri_proceedings_paper):
                        output_type = 'proceedings article'
                    if str(output_type) == str(GraphEntity.iri_academic_proceedings):
                        output_type = 'proceedings'
                    if str(output_type) == str(GraphEntity.iri_reference_book):
                        output_type = 'reference book'
                    if str(output_type) == str(GraphEntity.iri_reference_entry):
                        output_type = 'reference entry'
                    if str(output_type) == str(GraphEntity.iri_series):
                        output_type = 'series'
                    if str(output_type) == str(GraphEntity.iri_report_document):
                        output_type = 'report'
                    if str(output_type) == str(GraphEntity.iri_specification_document):
                        output_type = 'standard'
        return output_type

    def _vvi_find(self, result:Dict[str, Dict[str, str]], part_:str, type_:str, title_:str, num_:str, res_dict:dict) -> dict:
        type_value = self._type_it(result, type_)
        if 'issue' in type_value:
            res_dict['issue'] = str(result[num_]['value'])
        elif 'volume' in type_value:
            res_dict['volume'] = str(result[num_]['value'])
        elif type_value:
            res_dict['venue'] = result[title_]['value'] + ' [omid:' + result[part_]['value'] \
                .replace(f'{self.base_iri}/', '') + ']'
        return res_dict
    
    def retrieve_publisher_from_br_metaid(self, metaid:str):
        query = f'''
            SELECT DISTINCT ?ra ?schema ?literal_value ?name
            WHERE {{
                <{self.base_iri}/br/{metaid}> ^<{str(GraphEntity.iri_part_of)}>*/<{str(GraphEntity.iri_is_document_context_for)}> ?ar.
                ?ar <{str(GraphEntity.iri_with_role)}> <{str(GraphEntity.iri_publisher)}>;
                    <{str(GraphEntity.iri_is_held_by)}> ?ra.
                ?ra <{str(GraphEntity.iri_has_identifier)}>/<{str(GraphEntity.iri_uses_identifier_scheme)}> ?schema;
                    <{str(GraphEntity.iri_has_identifier)}>/<{str(GraphEntity.iri_has_literal_value)}> ?literal_value;
                    <{str(GraphEntity.iri_name)}> ?name.
            }}
        '''
        result = self.__query(query)
        publisher = ''
        if result['results']['bindings']:
            bindings = result['results']['bindings']
            publishers = list()
            for binding in bindings:
                pub_metaid = binding['ra']['value'].replace(f'{self.base_iri}/ra/', '')
                pub_name = binding['name']['value']
                pub_schema = binding['schema']['value'].replace(f'{str(GraphEntity.DATACITE)}', '')
                pub_literal = binding['literal_value']['value']
                pub_id = f'{pub_schema}:{pub_literal}'
                pub_full_name = f'{pub_name} [omid:ra/{pub_metaid} {pub_id}]'
                publishers.append(pub_full_name)
            publisher = '; '.join(publishers)
        return publisher
    
    def check_type(self, res:str, forbidden_types:set) -> bool:
        allowed_type = False
        query = f'''
            SELECT
            (GROUP_CONCAT(DISTINCT ?type; separator=' ;and; ') AS ?type_)
            WHERE {{
                <{res}> a ?type.
            }}
        '''
        results = self.__query(query)
        bindings = results['results']['bindings']
        for binding in bindings:
            type_label = self._type_it(binding, 'type_')
            if type_label and type_label not in forbidden_types:
                allowed_type = True
        return allowed_type
    
    def get_preexisting_graph(self, res:str, preexisting_graphs:dict) -> Graph:
        if res in preexisting_graphs:
            return preexisting_graphs[res]
        else:
            query_subj = f"""
                CONSTRUCT {{
                    <{res}> ?p ?o.
                }}
                WHERE {{
                    <{res}> ?p ?o.
                }}
            """
            graph_subj = self.__query(query_subj, XML)
            untyped_graph_subj = Graph()
            for triple in graph_subj.triples((None, None, None)):
                remove_datatype = False
                if isinstance(triple[2], Literal):
                    if triple[2].datatype == URIRef('http://www.w3.org/2001/XMLSchema#string'):
                        remove_datatype = True
                        untyped_literal = Literal(lexical_or_value=str(triple[2]), datatype=None)
                        untyped_triple = (triple[0], triple[1], untyped_literal)
                untyped_graph_subj.add(untyped_triple) if remove_datatype else untyped_graph_subj.add(triple)
            untyped_graph_subj = untyped_graph_subj if len(untyped_graph_subj) else None
            preexisting_graphs[res] = untyped_graph_subj
        return untyped_graph_subj
    
    def get_everything_about_res(self, metaval_ids_list: List[Tuple[str, List[str]]]) -> None:
        if not metaval_ids_list:
            return
        relevant_ids = []
        for x in metaval_ids_list:
            if x[0]:
                relevant_ids.append(x[0])
            else:
                relevant_ids.extend(x[1])
        omids = [f'{self.base_iri}/{x.replace("omid:", "")}' for x in relevant_ids if x.startswith('omid:')]
        identifiers = [(GraphEntity.DATACITE+x.split(':')[0], x.split(':')[1]) for x in relevant_ids if not x.startswith('omid:')]
        if omids and identifiers:
            query = f'''
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{ 
                    {{
                        ?res (<>|!<>)* ?s. 
                        ?s ?p ?o.
                        VALUES ?res {{<{'> <'.join(omids)}>}}
                    }} UNION {{
                        ?br <{GraphEntity.iri_has_identifier}> ?id.
                        ?id <{GraphEntity.iri_uses_identifier_scheme}> ?scheme;
                            <{GraphEntity.iri_has_literal_value}> ?literal.
                        VALUES (?scheme ?literal) {{({') ('.join(map(lambda x: f'<{x[0]}> "{x[1]}"', identifiers))})}}
                        ?br (<>|!<>)* ?s. ?s ?p ?o. 
                    }}
                }}
            '''
        elif omids and not identifiers:
            query = f'''
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{ 
                    ?res (<>|!<>)* ?s. 
                    ?s ?p ?o.
                    VALUES ?res {{<{'> <'.join(omids)}>}}
                }}
            '''
        elif identifiers and not omids:
            query = f'''
                CONSTRUCT {{ ?s ?p ?o }}
                WHERE {{ 
                    ?br <{GraphEntity.iri_has_identifier}> ?id.
                    ?id <{GraphEntity.iri_uses_identifier_scheme}> ?scheme;
                        <{GraphEntity.iri_has_literal_value}> ?literal.
                    VALUES (?scheme ?literal) {{({') ('.join(map(lambda x: f'<{x[0]}> "{x[1]}"', identifiers))})}}
                    ?br (<>|!<>)* ?s. ?s ?p ?o. 
                }}
            '''
        result = self.__query(query, XML)
        if result:
            for triple in result.triples((None, None, None)):
                if isinstance(triple[2], Literal):
                    new_triple = (triple[0], triple[1], Literal(lexical_or_value=str(triple[2])))
                    self.local_g.add(new_triple)
                else:
                    self.local_g.add(triple)

    def _get_subgraph(self, graph: Graph, res: str) -> str:
        subgraph = Graph()
        for triple in graph.triples((res, None, None)):
            subgraph.add(triple)
        return subgraph