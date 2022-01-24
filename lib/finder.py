from typing import List, Dict, Tuple
from oc_ocdm.graph import GraphEntity
from pymantic import sparql


class ResourceFinder:

    def __init__(self, ts_url):
        self.ts = sparql.SPARQLServer(ts_url)

    def __query(self, query):
        result = self.ts.query(query)
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
        schema = GraphEntity.DATACITE + schema
        query = f'''
            SELECT DISTINCT ?res (GROUP_CONCAT(DISTINCT ?title; separator=' ;and; ') AS ?title_)
                (GROUP_CONCAT(DISTINCT ?otherId; separator=' ;and; ') AS ?otherId_)
                (GROUP_CONCAT(?schema; separator=' ;and; ') AS ?schema_)
                (GROUP_CONCAT(DISTINCT ?value; separator=' ;and; ') AS ?value_)
            WHERE {{
                ?res a <{GraphEntity.iri_expression}>;
                    <{GraphEntity.iri_has_identifier}> ?knownId;
                    <{GraphEntity.iri_has_identifier}> ?otherId.
                OPTIONAL {{?res <{GraphEntity.iri_title}> ?title.}}
                ?otherId <{GraphEntity.iri_uses_identifier_scheme}> ?schema;
                    <{GraphEntity.iri_has_literal_value}> ?value.
                ?knownId <{GraphEntity.iri_uses_identifier_scheme}> <{schema}>;
                    <{GraphEntity.iri_has_literal_value}> '{value}'.
            }} GROUP BY ?res
        '''
        results = self.__query(query)
        if results['results']['bindings']:
            bindings = results['results']['bindings']
            result_list = list()
            for result in bindings:
                res = str(result['res']['value']).replace('https://w3id.org/oc/meta/br/', '')
                title = str(result['title_']['value'])
                metaid_list = str(result['otherId_']['value']).replace('https://w3id.org/oc/meta/id/', '').split(' ;and; ')
                id_schema_list = str(result['schema_']['value']).replace(GraphEntity.DATACITE, '').split(' ;and; ')
                id_value_list = str(result['value_']['value']).split(' ;and; ')
                schema_value_list = list(zip(id_schema_list, id_value_list))
                id_list = list()
                for schema, value in schema_value_list:
                    identifier = f'{schema}:{value}'
                    id_list.append(identifier)
                metaid_id_list = list(zip(metaid_list, id_list))
                result_list.append(tuple((res, title, metaid_id_list)))
            return result_list
        else:
            return None

    def retrieve_br_from_meta(self, metaid:str) -> Tuple[str, List[Tuple[str, str]]]:
        '''
        Given a MetaID, it retrieves the title of the bibliographic resource having that MetaID and other identifiers of that entity.

        :params metaid: a MetaID
        :type metaid: str
        :returns Tuple[str, List[Tuple[str, str]]]: -- it returns a tuple of two elements. The first element is the resource's title associated with the input MetaID. The second element is a list of MetaID-ID tuples related to identifiers associated with that entity.
        '''
        metaid_uri = f'https://w3id.org/oc/meta/br/{str(metaid)}'
        query = f'''
            SELECT DISTINCT ?res 
                (GROUP_CONCAT(DISTINCT  ?title;separator=' ;and; ') AS ?title_)
                (GROUP_CONCAT(DISTINCT  ?id;separator=' ;and; ') AS ?id_)
                (GROUP_CONCAT(?schema;separator=' ;and; ') AS ?schema_)
                (GROUP_CONCAT(DISTINCT  ?value;separator=' ;and; ') AS ?value_)
            WHERE {{
                ?res a <{GraphEntity.iri_expression}>.
                OPTIONAL {{?res <{GraphEntity.iri_title}> ?title.}}
                OPTIONAL {{
                    ?res <{GraphEntity.iri_has_identifier}> ?id.
                    ?id <{GraphEntity.iri_uses_identifier_scheme}> ?schema;
                        <{GraphEntity.iri_has_literal_value}> ?value.}}
                BIND (<{metaid_uri}> AS ?res)
            }} GROUP BY ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            # By definition, there is only one resource associated with a MetaID
            bindings = result['results']['bindings'][0]
            title = str(bindings['title_']['value'])
            metaid_list = str(bindings['id_']['value']).replace('https://w3id.org/oc/meta/id/', '').split(' ;and; ')
            id_schema_list = str(bindings['schema_']['value']).replace(GraphEntity.DATACITE, '').split(' ;and; ')
            id_value_list = str(bindings['value_']['value']).split(' ;and; ')
            schema_value_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for schema, value in schema_value_list:
                identifier = f'{schema}:{value}'
                id_list.append(identifier)
            metaid_id_list = list(zip(metaid_list, id_list))
            return title, metaid_id_list
        else:
            return None

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
        schema = GraphEntity.DATACITE + schema
        query = f'''
            SELECT DISTINCT ?res 
            WHERE {{
                ?res a <{GraphEntity.iri_identifier}>;
                    <{GraphEntity.iri_uses_identifier_scheme}> <{schema}>;
                    <{GraphEntity.iri_has_literal_value}> '{value}'.
            }} 
            GROUP BY ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            bindings = result['results']['bindings']
            return str(bindings[0]['res']['value']).replace('https://w3id.org/oc/meta/id/', '')
        else:
            return None

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
        metaid_uri = f'https://w3id.org/oc/meta/ra/{str(metaid)}'
        query = f'''
            SELECT DISTINCT ?res 
                (GROUP_CONCAT(DISTINCT ?name ;separator=' ;and; ') AS ?name_)
                (GROUP_CONCAT(DISTINCT ?givenName ;separator=' ;and; ') AS ?givenName_)
                (GROUP_CONCAT(DISTINCT ?familyName ;separator=' ;and; ') AS ?familyName_)
                (GROUP_CONCAT(DISTINCT ?id; separator=' ;and; ') AS ?id_)
                (GROUP_CONCAT(?schema; separator=' ;and; ') AS ?schema_)
                (GROUP_CONCAT(DISTINCT ?value; separator=' ;and; ') AS ?value_)
            WHERE {{
                ?res a <{GraphEntity.iri_agent}>.
                OPTIONAL {{?res <{GraphEntity.iri_given_name}> ?givenName.}}
                OPTIONAL {{?res <{GraphEntity.iri_family_name}> ?familyName.}}
                OPTIONAL {{?res <{GraphEntity.iri_name}> ?name.}}
                OPTIONAL {{
                    ?res <{GraphEntity.iri_has_identifier}> ?id.
                    ?id <{GraphEntity.iri_uses_identifier_scheme}> ?schema;
                        <{GraphEntity.iri_has_literal_value}> ?value.
                }}
                BIND (<{metaid_uri}> AS ?res)
            }} 
            GROUP BY ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            bindings = result['results']['bindings'][0]
            if str(bindings['name_']['value']) and publisher:
                name = str(bindings['name_']['value'])
            elif str(bindings['familyName_']['value']) and not publisher:
                name = str(bindings['familyName_']['value']) + ', ' + str(bindings['givenName_']['value'])
            else:
                name = ''
            meta_id_list = str(bindings['id_']['value']).replace('https://w3id.org/oc/meta/id/', '').split(' ;and; ')
            id_schema_list = str(bindings['schema_']['value']).replace(GraphEntity.DATACITE, '').split(' ;and; ')
            id_value_list = str(bindings['value_']['value']).split(' ;and; ')

            schema_value_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for schema, value in schema_value_list:
                if schema and value:
                    identifier = f'{schema}:{value}'
                    id_list.append(identifier)
            metaid_id_list = list(zip(meta_id_list, id_list))
            return name, metaid_id_list
        else:
            return None

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
        schema = GraphEntity.DATACITE + schema
        query = f'''
            SELECT DISTINCT ?res
                (GROUP_CONCAT(DISTINCT ?name; separator=' ;and; ') AS ?name_)
                (GROUP_CONCAT(DISTINCT ?givenName; separator=' ;and; ') AS ?givenName_)
                (GROUP_CONCAT(DISTINCT ?familyName; separator=' ;and; ') AS ?familyName_)
                (GROUP_CONCAT(DISTINCT ?otherId; separator=' ;and; ') AS ?otherId_)
                (GROUP_CONCAT(?schema; separator=' ;and; ') AS ?schema_)
                (GROUP_CONCAT(DISTINCT ?value; separator=' ;and; ') AS ?value_)
            WHERE {{
                ?res a <{GraphEntity.iri_agent}>;
                    <{GraphEntity.iri_has_identifier}> ?knownId;
                    <{GraphEntity.iri_has_identifier}> ?otherId.
                ?knownId <{GraphEntity.iri_uses_identifier_scheme}> <{schema}>;
                    <{GraphEntity.iri_has_literal_value}> '{value}'.
                OPTIONAL {{?res <{GraphEntity.iri_given_name}> ?givenName.}}
                OPTIONAL {{?res <{GraphEntity.iri_family_name}> ?familyName.}}
                OPTIONAL {{?res <{GraphEntity.iri_name}> ?name.}}
                ?otherId <{GraphEntity.iri_uses_identifier_scheme}> ?schema;
                    <{GraphEntity.iri_has_literal_value}> ?value.
            }} GROUP BY ?res
        '''
        results = self.__query(query)
        if results['results']['bindings']:
            result_list = list()
            for result in results['results']['bindings']:
                res = str(result['res']['value']).replace('https://w3id.org/oc/meta/ra/', '')
                if str(result['name_']['value']) and publisher:
                    name = str(result['name_']['value'])
                elif str(result['familyName_']['value']) and not publisher:
                    name = str(result['familyName_']['value']) + ', ' + str(result['givenName_']['value'])
                else:
                    name = ''
                meta_id_list = str(result['otherId_']['value']).replace('https://w3id.org/oc/meta/id/', '').split(' ;and; ')
                id_schema_list = str(result['schema_']['value']).replace(GraphEntity.DATACITE, '').split(' ;and; ')
                id_value_list = str(result['value_']['value']).split(' ;and; ')
                schema_value_list = list(zip(id_schema_list, id_value_list))
                id_list = list()
                for schema, value in schema_value_list:
                    identifier = f'{schema}:{value}'
                    id_list.append(identifier)
                metaid_id_list = list(zip(meta_id_list, id_list))
                result_list.append(tuple((res, name, metaid_id_list)))
            return result_list
        else:
            return None

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
                ?res <{GraphEntity.iri_part_of}>+ <https://w3id.org/oc/meta/br/{meta}>;
                    <{GraphEntity.iri_part_of}> ?container;
                    a ?type;
                    <{GraphEntity.iri_has_sequence_identifier}> ?title.
            }} GROUP BY ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            results = result['results']['bindings']
            for x in results:
                res = str(x['res']['value']).replace('https://w3id.org/oc/meta/br/', '')
                container = str(x['container_']['value'])
                title = str(x['title_']['value'])
                types = str(x['type_']['value']).split(' ;and; ')
                if str(GraphEntity.iri_journal_issue) in types and container == f'https://w3id.org/oc/meta/br/{meta}':
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
                content[title]['id'] = item['res']['value'].replace('https://w3id.org/oc/meta/br/', '')
        return content
    
    def retrieve_ra_sequence_from_br_meta(self, metaid:str, col_name:str) -> List[Tuple[str, list, str]]:
        '''
        Given a bibliographic resource's MetaID and a field name, it returns its agent roles and responsible agents in the correct order according to the specified field.
        The output has the following format: ::

            [
                {METAID_AR_1: (NAME_RA_1, [(METAID_ID_RA_1, LITERAL_VALUE_ID_RA_1)], METAID_RA_1)}, 
                {METAID_AR_2: (NAME_RA_2, [(METAID_ID_RA_2, LITERAL_VALUE_ID_RA_2)], 'METAID_RA_2)}, 
                {METAID_AR_N: (NAME_RA_N, [(METAID_ID_RA_N, LITERAL_VALUE_ID_RA_N)], 'METAID_RA_N)}, 
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
        :returns: List[Tuple[str, tuple, str]] -- the output is a list of three-elements tuples. Each tuple's first and third elements are the MetaIDs of an agent role and responsible agent related to the specified bibliographic resource. The second element is a two-elements tuple, where the first element is the MetaID of the identifier of the responsible agent. In contrast, the second one is the literal value of that id.
        '''
        if col_name == 'author':
            role = GraphEntity.iri_author
        elif col_name == 'editor':
            role = GraphEntity.iri_editor
        else:
            role = GraphEntity.iri_publisher
        metaid_uri = f'https://w3id.org/oc/meta/br/{str(metaid)}'
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
                role = str(ra_dict['role']['value']).replace('https://w3id.org/oc/meta/ar/', '')
                if 'next' in ra_dict:
                    next_role = str(ra_dict['next']['value']).replace('https://w3id.org/oc/meta/ar/', '')
                else:
                    next_role = ''
                ra = str(ra_dict['ra']['value']).replace('https://w3id.org/oc/meta/ra/', '')
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
        metaid_uri = f'https://w3id.org/oc/meta/br/{str(metaid)}'
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
            meta = result['results']['bindings'][0]['re']['value'].replace('https://w3id.org/oc/meta/re/', '')
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
                    'venue': 'Archives Of Internal Medicine [meta:br/4387]'
                }

            :params metaid: a bibliographic resource's MetaID
            :type meta_id: str
            :returns: Tuple[str, str] -- the output is a dictionary including the publication date, type, page, issue, volume, and venue of the specified bibliographic resource.
        '''
        metaid_uri = f'https://w3id.org/oc/meta/br/{str(metaid)}'
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
            res_dict['venue'] = result[title_]['value'] + ' [meta:' + result[part_]['value'] \
                .replace('https://w3id.org/oc/meta/', '') + ']'
        return res_dict
