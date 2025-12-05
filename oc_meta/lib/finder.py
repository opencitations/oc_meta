import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Tuple

import yaml
from dateutil import parser
from oc_ocdm.graph import GraphEntity
from oc_ocdm.graph.graph_entity import GraphEntity
from oc_ocdm.prov.prov_entity import ProvEntity
from oc_ocdm.support import get_count, get_resource_number
from rdflib import RDF, XSD, Graph, Literal, URIRef
from sparqlite import SPARQLClient
from time_agnostic_library.agnostic_entity import AgnosticEntity


def _execute_sparql_queries(args: tuple) -> list:
    ts_url, queries = args
    results = []
    with SPARQLClient(ts_url, max_retries=5, backoff_factor=5) as client:
        for query in queries:
            result = client.query(query)
            results.append(result['results']['bindings'] if result else [])
    return results


class ResourceFinder:

    def __init__(self, ts_url, base_iri: str, local_g: Graph = Graph(), settings: dict = dict(), meta_config_path: str = None):
        self.ts_url = ts_url
        self.base_iri = base_iri[:-1] if base_iri[-1] == '/' else base_iri
        self.local_g = local_g
        self.prebuilt_subgraphs = {}
        self.ids_in_local_g = set()
        self.meta_config_path = meta_config_path
        self.meta_settings = settings
        self.virtuoso_full_text_search = settings['virtuoso_full_text_search'] if settings and 'virtuoso_full_text_search' in settings else False

    # _______________________________BR_________________________________ #

    def retrieve_br_from_id(self, schema: str, value: str) -> List[Tuple[str, str, list]]:
        '''
        Given an identifier, it retrieves bibliographic resources associated with that identifier, related titles and other identifiers MetaIDs and literal values.

        :params schema: an identifier schema
        :type schema: str
        :params value: an identifier literal value
        :type value: str
        :returns List[Tuple[str, str, list]]: -- it returns a list of three elements tuples. The first element is the MetaID of a resource associated with the input ID. The second element is a title of that resource, if present. The third element is a list of MetaID-ID tuples related to identifiers associated with that resource. 
        '''
        schema_uri = URIRef(GraphEntity.DATACITE + schema)
        value = value.replace('\\', '\\\\')
        result_list = []
        identifier_uri = None

        # Search for both string-typed and untyped literals
        for literal_value in [Literal(value, datatype=XSD.string), Literal(value)]:
            for starting_triple in self.local_g.triples((None, GraphEntity.iri_has_literal_value, literal_value)):
                for known_id_triple in self.local_g.triples((starting_triple[0], None, None)):
                    if known_id_triple[1] == GraphEntity.iri_uses_identifier_scheme and known_id_triple[2] == schema_uri:
                        identifier_uri = known_id_triple[0]
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
        
    def retrieve_br_from_meta(self, metaid: str) -> Tuple[str, List[Tuple[str, str]]]:
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

    def retrieve_metaid_from_id(self, schema: str, value: str) -> str:
        '''
        Given the schema and value of an ID, it returns the MetaID associated with that identifier.

        :params schema: an identifier schema
        :type schema: str
        :params value: an identifier literal value
        :type value: str
        :returns str: -- it returns the MetaID associated with the input ID.
        '''
        schema_uri = URIRef(GraphEntity.DATACITE + schema)
        value = value.replace('\\', '\\\\')
        
        # Create both untyped and string-typed literals        
        for literal in [Literal(value, datatype=XSD.string), Literal(value)]:
            for starting_triple in self.local_g.triples((None, GraphEntity.iri_has_literal_value, literal)):
                for known_id_triple in self.local_g.triples((starting_triple[0], None, None)):
                    if known_id_triple[1] == GraphEntity.iri_uses_identifier_scheme and known_id_triple[2] == schema_uri:
                        return known_id_triple[0].replace(f'{self.base_iri}/id/', '')
        
        # If no match is found, return None or an appropriate value
        return None

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
        agnostic_meta = AgnosticEntity(res=metaid_uri, config=prov_config_dict, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
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
            with SPARQLClient(self.ts_url, max_retries=5, backoff_factor=5) as client:
                results = client.query(query_if_it_was_merged)['results']['bindings']
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
        
        full_name = self._construct_full_name(name, family_name, given_name)
            
        return full_name, identifiers, it_exists

    def retrieve_ra_from_id(self, schema: str, value: str, publisher: bool) -> List[Tuple[str, str, list]]:
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
        schema_uri = URIRef(GraphEntity.DATACITE + schema)
        value = value.replace('\\', '\\\\')
        result_list = []
        identifier_uri = None

        # Search for both string-typed and untyped literals
        for literal_value in [Literal(value, datatype=XSD.string), Literal(value)]:
            for starting_triple in self.local_g.triples((None, GraphEntity.iri_has_literal_value, literal_value)):
                for known_id_triple in self.local_g.triples((starting_triple[0], None, None)):
                    if known_id_triple[1] == GraphEntity.iri_uses_identifier_scheme and known_id_triple[2] == schema_uri:
                        identifier_uri = known_id_triple[0]
                        break
                if identifier_uri:
                    break
        if identifier_uri:
            metaid_id_list = [(identifier_uri.replace(f'{self.base_iri}/id/', ''), f'{schema}:{value}')]
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
                
                full_name = self._construct_full_name(name, family_name, given_name)
                result_list.append((res.replace(f'{self.base_iri}/ra/', ''), full_name, metaid_id_list))

        return result_list

    def _construct_full_name(self, name: str, family_name: str, given_name: str) -> str:
        if name and not family_name and not given_name:
            return name
        elif not name and family_name and not given_name:
            return f'{family_name},'
        elif not name and not family_name and given_name:
            return f', {given_name}'
        elif not name and family_name and given_name:
            return f'{family_name}, {given_name}'
        else:
            return ''

    def retrieve_ra_sequence_from_br_meta(self, metaid: str, col_name: str) -> List[Dict[str, tuple]]:
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

        for triple in self.local_g.triples((metaid_uri, GraphEntity.iri_is_document_context_for, None)):
            for ar_triple in self.local_g.triples((triple[2], None, None)):
                if ar_triple[2] == role:
                    role_value = str(triple[2]).replace(f'{self.base_iri}/ar/', '')
                    next_role = ''
                    ra = None
                    for relevant_ar_triple in self.local_g.triples((triple[2], None, None)):
                        if relevant_ar_triple[1] == GraphEntity.iri_has_next:
                            next_role = str(relevant_ar_triple[2]).replace(f'{self.base_iri}/ar/', '')
                        elif relevant_ar_triple[1] == GraphEntity.iri_is_held_by:
                            ra = str(relevant_ar_triple[2]).replace(f'{self.base_iri}/ra/', '')
                    # Skip AR if it has no associated RA (malformed data)
                    if ra is not None:
                        dict_ar[role_value] = {'next': next_role, 'ra': ra}

        # Find the start_role by excluding all roles that are "next" for others from the set of all roles
        all_roles = set(dict_ar.keys())
        roles_with_next = set(details['next'] for details in dict_ar.values() if details['next'])
        start_role_candidates = all_roles - roles_with_next

        MAX_ITERATIONS = 10000

        if len(all_roles) == 0:
            return []

        # If no start candidates (circular loop), pick the AR with lowest number as arbitrary start
        if len(start_role_candidates) == 0:
            sorted_ars = sorted(all_roles, key=lambda ar: get_resource_number(f'{self.base_iri}/ar/{ar}'))
            start_role_candidates = {sorted_ars[0]}

        if len(start_role_candidates) != 1:
            # If more than one start candidate exists or none exist, build all chains and return the best one
            chains = []
            for start_candidate in start_role_candidates:
                current_role = start_candidate
                chain = []
                visited_roles = set()
                iteration_count = 0
                while current_role and current_role not in visited_roles and iteration_count < MAX_ITERATIONS:
                    visited_roles.add(current_role)
                    if current_role in dict_ar:
                        ra_info = self.retrieve_ra_from_meta(dict_ar[current_role]['ra'])[0:2]
                        ra_tuple = ra_info + (dict_ar[current_role]['ra'],)
                        chain.append({current_role: ra_tuple})
                        current_role = dict_ar[current_role]['next']
                    else:
                        break
                    iteration_count += 1

                if iteration_count == MAX_ITERATIONS:
                    print(f"Warning: Possible infinite loop detected for BR: {metaid}, column: {col_name}")
                    return []

                chains.append(chain)
            # Sort chains by length, then by the lowest sequential number of the starting role
            chains.sort(key=lambda chain: (-len(chain), get_resource_number(f'{self.base_iri}/ar/{list(chain[0].keys())[0]}')))
            try:
                ordered_ar_list = chains[0]
            except Exception as e:
                print(f"\nWarning: Error processing BR: {metaid} for column: {col_name}")
                print(f"dict_ar: {dict_ar}")
                print(f"All roles: {all_roles}")
                print(f"Start role candidates: {start_role_candidates}")
                print(f"Roles with next: {roles_with_next}")
                print(f"Error: {str(e)}")
                return []
        else:
            start_role = start_role_candidates.pop()
            # Follow the "next" chain from the start_role to construct an ordered list
            ordered_ar_list = []
            current_role = start_role
            visited_roles = set()
            iteration_count = 0
            while current_role and current_role not in visited_roles and iteration_count < MAX_ITERATIONS:
                visited_roles.add(current_role)
                if current_role in dict_ar:
                    ra_info = self.retrieve_ra_from_meta(dict_ar[current_role]['ra'])[0:2]
                    ra_tuple = ra_info + (dict_ar[current_role]['ra'],)
                    ordered_ar_list.append({current_role: ra_tuple})
                    current_role = dict_ar[current_role]['next']
                else:
                    break
                iteration_count += 1

            if iteration_count == MAX_ITERATIONS:
                print(f"Warning: Possible infinite loop detected for BR: {metaid}, column: {col_name}")
                return []

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

    def retrieve_br_info_from_meta(self, metaid: str) -> dict:
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

        :param metaid: a bibliographic resource's MetaID
        :type metaid: str
        :returns: dict -- the output is a dictionary including the publication date, type, page, issue, volume, and venue of the specified bibliographic resource.
        '''

        venue_iris = [
            GraphEntity.iri_archival_document,
            GraphEntity.iri_journal,
            GraphEntity.iri_book,
            GraphEntity.iri_book_series,
            GraphEntity.iri_series,
            GraphEntity.iri_academic_proceedings,
            GraphEntity.iri_proceedings_series,
            GraphEntity.iri_reference_book,
            GraphEntity.iri_series,

            GraphEntity.iri_expression
        ]

        def extract_identifiers(entity_uri):
            identifiers = [f"omid:{entity_uri.replace(f'{self.base_iri}/', '')}"]
            for id_triple in self.local_g.triples((entity_uri, GraphEntity.iri_has_identifier, None)):
                id_obj = id_triple[2]
                scheme = value = None
                for detail_triple in self.local_g.triples((id_obj, None, None)):
                    if detail_triple[1] == GraphEntity.iri_uses_identifier_scheme:
                        scheme = str(detail_triple[2])
                    elif detail_triple[1] == GraphEntity.iri_has_literal_value:
                        value = str(detail_triple[2])
                if scheme and value:
                    scheme = scheme.replace(GraphEntity.DATACITE, '')
                    identifiers.append(f"{scheme}:{value}")
            return identifiers

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
            predicate, obj = triple[1], triple[2]

            if predicate == GraphEntity.iri_has_publication_date:
                res_dict['pub_date'] = str(obj)
            elif predicate == RDF.type and obj != GraphEntity.iri_expression:
                res_dict['type'] = self._type_it(obj)
            elif predicate == GraphEntity.iri_has_sequence_identifier:
                for inner_triple in self.local_g.triples((metaid_uri, None, None)):
                    inner_obj = inner_triple[2]
                    if inner_obj == GraphEntity.iri_journal_issue:
                        res_dict['issue'] = str(triple[2])
                    elif inner_obj == GraphEntity.iri_journal_volume:
                        res_dict['volume'] = str(triple[2])
            elif predicate == GraphEntity.iri_part_of:
                for vvi_triple in self.local_g.triples((obj, None, None)):
                    vvi_obj = vvi_triple[2]
                    if vvi_obj == GraphEntity.iri_journal_issue:
                        for inner_vvi_triple in self.local_g.triples((obj, None, None)):
                            if inner_vvi_triple[1] == GraphEntity.iri_has_sequence_identifier:
                                res_dict['issue'] = str(inner_vvi_triple[2])
                    elif vvi_obj == GraphEntity.iri_journal_volume:
                        for inner_vvi_triple in self.local_g.triples((obj, None, None)):
                            if inner_vvi_triple[1] == GraphEntity.iri_has_sequence_identifier:
                                res_dict['volume'] = str(inner_vvi_triple[2])
                    elif vvi_obj in venue_iris:
                        for inner_vvi_triple in self.local_g.triples((obj, None, None)):
                            if inner_vvi_triple[1] == GraphEntity.iri_title:
                                venue_title = str(inner_vvi_triple[2])
                                venue_ids = extract_identifiers(obj)
                                res_dict['venue'] = f"{venue_title} [{' '.join(venue_ids)}]"

                    if vvi_triple[1] == GraphEntity.iri_part_of:
                        for vi_triple in self.local_g.triples((vvi_obj, None, None)):
                            vi_obj = vi_triple[2]
                            if vi_obj == GraphEntity.iri_journal_volume:
                                for inner_vvi_triple in self.local_g.triples((vvi_obj, None, None)):
                                    if inner_vvi_triple[1] == GraphEntity.iri_has_sequence_identifier:
                                        res_dict['volume'] = str(inner_vvi_triple[2])
                            elif vi_obj in venue_iris:
                                for inner_vvi_triple in self.local_g.triples((vvi_obj, None, None)):
                                    if inner_vvi_triple[1] == GraphEntity.iri_title:
                                        venue_title = str(inner_vvi_triple[2])
                                        venue_ids = extract_identifiers(vvi_obj)
                                        res_dict['venue'] = f"{venue_title} [{' '.join(venue_ids)}]"

                            if vi_triple[1] == GraphEntity.iri_part_of:
                                for venue_triple in self.local_g.triples((vi_obj, None, None)):
                                    if venue_triple[1] == GraphEntity.iri_title:
                                        venue_title = str(venue_triple[2])
                                        venue_ids = extract_identifiers(vi_obj)
                                        res_dict['venue'] = f"{venue_title} [{' '.join(venue_ids)}]"
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
        MAX_WORKERS = 4

        def batch_process(input_set, batch_size):
            """Generator to split input data into smaller batches if batch_size is not None."""
            if batch_size is None:
                yield input_set
            else:
                for i in range(0, len(input_set), batch_size):
                    yield input_set[i:i + batch_size]

        def process_batch_parallel(subjects, cur_depth, visited_subjects):
            """Process batches of subjects in parallel up to the specified depth."""
            if not subjects or (max_depth and cur_depth > max_depth):
                return

            new_subjects = subjects - visited_subjects
            if not new_subjects:
                return

            visited_subjects.update(new_subjects)

            subject_list = list(new_subjects)
            batches = list(batch_process(subject_list, BATCH_SIZE))
            batch_queries = []
            ts_url = self.ts_url

            for batch in batches:
                query = f'''
                    SELECT ?s ?p ?o
                    WHERE {{
                        VALUES ?s {{ {' '.join([f"<{s}>" for s in batch])} }}
                        ?s ?p ?o.
                    }}'''
                batch_queries.append(query)

            next_subjects = set()
            if len(batch_queries) > 1 and MAX_WORKERS > 1:
                queries_per_worker = max(1, len(batch_queries) // MAX_WORKERS)
                grouped_queries = []
                for i in range(0, len(batch_queries), queries_per_worker):
                    grouped_queries.append((ts_url, batch_queries[i:i + queries_per_worker]))
                with ProcessPoolExecutor(
                    max_workers=min(len(grouped_queries), MAX_WORKERS),
                    mp_context=multiprocessing.get_context('spawn')
                ) as executor:
                    grouped_results = list(executor.map(_execute_sparql_queries, grouped_queries))
                results = [item for sublist in grouped_results for item in sublist]
            else:
                results = _execute_sparql_queries((ts_url, batch_queries)) if batch_queries else []

            for result in results:
                for row in result:
                    s = URIRef(row['s']['value'])
                    p = URIRef(row['p']['value'])
                    o = row['o']['value']
                    o_type = row['o']['type']
                    o_datatype = URIRef(row['o']['datatype']) if 'datatype' in row['o'] else None
                    o = URIRef(o) if o_type == 'uri' else Literal(lexical_or_value=o, datatype=o_datatype)
                    self.local_g.add((s, p, o))
                    if s not in self.prebuilt_subgraphs:
                        self.prebuilt_subgraphs[s] = Graph()
                    self.prebuilt_subgraphs[s].add((s, p, o))
                    if isinstance(o, URIRef) and p not in {RDF.type, GraphEntity.iri_with_role, GraphEntity.iri_uses_identifier_scheme}:
                        next_subjects.add(str(o))

            process_batch_parallel(next_subjects, cur_depth + 1, visited_subjects)

        def get_initial_subjects_from_metavals(metavals):
            """Convert metavals to a set of subjects."""
            return {f"{self.base_iri}/{mid.replace('omid:', '')}" for mid in metavals}

        def get_initial_subjects_from_identifiers(identifiers):
            """Convert identifiers to a set of subjects based on batch queries executed in parallel.

            Returns:
                tuple: (subjects set, id_to_subjects mapping)
                    - subjects: set of subject URIs found
                    - id_to_subjects: dict mapping identifier string to set of subject URIs
            """
            subjects = set()
            id_to_subjects = {}
            ts_url = self.ts_url
            batches = list(batch_process(list(identifiers), BATCH_SIZE))

            if not batches:
                return subjects, id_to_subjects

            batch_queries = []
            for batch in batches:
                if not batch:
                    continue

                if self.virtuoso_full_text_search:
                    union_blocks = []
                    for identifier in batch:
                        scheme, literal = identifier.split(':', maxsplit=1)[0], identifier.split(':', maxsplit=1)[1]
                        escaped_literal = literal.replace('\\', '\\\\').replace('"', '\\"')
                        union_blocks.append(f"""
                            {{
                                {{
                                    ?id <{GraphEntity.iri_has_literal_value}> "{escaped_literal}" .
                                }}
                                UNION
                                {{
                                    ?id <{GraphEntity.iri_has_literal_value}> "{escaped_literal}"^^<{XSD.string}> .
                                }}
                                ?id <{GraphEntity.iri_uses_identifier_scheme}> <{GraphEntity.DATACITE + scheme}> .
                                ?s <{GraphEntity.iri_has_identifier}> ?id .
                                BIND("{scheme}" AS ?schemeLabel)
                                BIND("{escaped_literal}" AS ?literalLabel)
                            }}
                        """)
                    union_query = " UNION ".join(union_blocks)
                    query = f'''
                        SELECT ?s ?schemeLabel ?literalLabel WHERE {{
                            {union_query}
                        }}
                    '''
                    batch_queries.append(query)
                else:
                    identifiers_values = []
                    for identifier in batch:
                        scheme, literal = identifier.split(':', maxsplit=1)[0], identifier.split(':', maxsplit=1)[1]
                        escaped_literal = literal.replace('\\', '\\\\').replace('"', '\\"')
                        identifiers_values.append(f"(<{GraphEntity.DATACITE + scheme}> \"{escaped_literal}\")")
                    identifiers_values_str = " ".join(identifiers_values)
                    query = f'''
                        SELECT DISTINCT ?s ?scheme ?literal WHERE {{
                            VALUES (?scheme ?literal) {{ {identifiers_values_str} }}
                            ?id <{GraphEntity.iri_uses_identifier_scheme}> ?scheme .
                            ?id <{GraphEntity.iri_has_literal_value}> ?literalValue  .
                            FILTER(str(?literalValue) = str(?literal))
                            ?s <{GraphEntity.iri_has_identifier}> ?id .
                        }}
                    '''
                    batch_queries.append(query)

            if len(batch_queries) > 1 and MAX_WORKERS > 1:
                queries_per_worker = max(1, len(batch_queries) // MAX_WORKERS)
                grouped_queries = []
                for i in range(0, len(batch_queries), queries_per_worker):
                    grouped_queries.append((ts_url, batch_queries[i:i + queries_per_worker]))
                with ProcessPoolExecutor(
                    max_workers=min(len(grouped_queries), MAX_WORKERS),
                    mp_context=multiprocessing.get_context('spawn')
                ) as executor:
                    grouped_results = list(executor.map(_execute_sparql_queries, grouped_queries))
                results = [item for sublist in grouped_results for item in sublist]
            elif batch_queries:
                results = _execute_sparql_queries((ts_url, batch_queries))
            else:
                results = []

            for result in results:
                for row in result:
                    subject = str(row['s']['value'])
                    subjects.add(subject)
                    if 'schemeLabel' in row:
                        scheme = str(row['schemeLabel']['value'])
                        literal = str(row['literalLabel']['value'])
                    else:
                        scheme = str(row['scheme']['value']).replace(str(GraphEntity.DATACITE), '')
                        literal = str(row['literal']['value'])
                    identifier = f"{scheme}:{literal}"
                    if identifier not in id_to_subjects:
                        id_to_subjects[identifier] = set()
                    id_to_subjects[identifier].add(subject)

            return subjects, id_to_subjects

        def get_initial_subjects_from_vvis(vvis):
            """Convert vvis to a set of subjects based on batch queries executed in parallel."""
            subjects = set()
            ts_url = self.ts_url
            vvi_queries = []
            venue_uris_to_add = set()

            # First pass: collect all venue IDs and prepare queries
            all_venue_ids = set()
            for volume, issue, venue_metaid, venue_ids_tuple in vvis:
                if venue_ids_tuple:
                    all_venue_ids.update(venue_ids_tuple)

            # Get venue subjects from identifiers with mapping
            venue_id_to_uris = {}
            if all_venue_ids:
                venue_id_subjects, venue_id_to_uris = get_initial_subjects_from_identifiers(all_venue_ids)
                subjects.update(venue_id_subjects)

            # Second pass: prepare VVI queries
            for volume, issue, venue_metaid, venue_ids_tuple in vvis:
                venues_to_search = set()

                if venue_metaid:
                    venues_to_search.add(venue_metaid)

                if venue_ids_tuple:
                    # Convert venue URIs to metaid format for VVI search
                    # Only use venues matching THIS tuple's identifiers
                    for venue_id in venue_ids_tuple:
                        if venue_id in venue_id_to_uris:
                            for venue_uri in venue_id_to_uris[venue_id]:
                                if '/br/' in venue_uri:
                                    metaid = venue_uri.replace(f'{self.base_iri}/br/', '')
                                    venues_to_search.add(f"omid:br/{metaid}")

                # Prepare VVI queries for each venue
                for venue_metaid_to_search in venues_to_search:
                    venue_uri = f"{self.base_iri}/{venue_metaid_to_search.replace('omid:', '')}"
                    sequence_value = issue if issue else volume
                    if not sequence_value:
                        continue
                    escaped_sequence = sequence_value.replace('\\', '\\\\').replace('"', '\\"')

                    if issue:
                        if volume:
                            escaped_volume = volume.replace('\\', '\\\\').replace('"', '\\"')
                            query = f'''
                                SELECT ?s WHERE {{
                                    {{
                                        ?volume a <{GraphEntity.iri_journal_volume}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_volume}" .
                                        ?s a <{GraphEntity.iri_journal_issue}> ;
                                            <{GraphEntity.iri_part_of}> ?volume ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}" .
                                    }}
                                    UNION
                                    {{
                                        ?volume a <{GraphEntity.iri_journal_volume}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_volume}"^^<{XSD.string}> .
                                        ?s a <{GraphEntity.iri_journal_issue}> ;
                                            <{GraphEntity.iri_part_of}> ?volume ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}" .
                                    }}
                                    UNION
                                    {{
                                        ?volume a <{GraphEntity.iri_journal_volume}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_volume}" .
                                        ?s a <{GraphEntity.iri_journal_issue}> ;
                                            <{GraphEntity.iri_part_of}> ?volume ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}"^^<{XSD.string}> .
                                    }}
                                    UNION
                                    {{
                                        ?volume a <{GraphEntity.iri_journal_volume}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_volume}"^^<{XSD.string}> .
                                        ?s a <{GraphEntity.iri_journal_issue}> ;
                                            <{GraphEntity.iri_part_of}> ?volume ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}"^^<{XSD.string}> .
                                    }}
                                }}
                            '''
                        else:
                            query = f'''
                                SELECT ?s WHERE {{
                                    {{
                                        ?s a <{GraphEntity.iri_journal_issue}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}" .
                                    }}
                                    UNION
                                    {{
                                        ?s a <{GraphEntity.iri_journal_issue}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}"^^<{XSD.string}> .
                                    }}
                                }}
                            '''
                    else:
                        if volume:
                            query = f'''
                                SELECT ?s WHERE {{
                                    {{
                                        ?s a <{GraphEntity.iri_journal_volume}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}" .
                                    }}
                                    UNION
                                    {{
                                        ?s a <{GraphEntity.iri_journal_volume}> ;
                                            <{GraphEntity.iri_part_of}> <{venue_uri}> ;
                                            <{GraphEntity.iri_has_sequence_identifier}> "{escaped_sequence}"^^<{XSD.string}> .
                                    }}
                                }}
                            '''
                        else:
                            continue

                    vvi_queries.append(query)
                    venue_uris_to_add.add(venue_uri)

            # Execute VVI queries in parallel
            if len(vvi_queries) > 1 and MAX_WORKERS > 1:
                queries_per_worker = max(1, len(vvi_queries) // MAX_WORKERS)
                grouped_queries = []
                for i in range(0, len(vvi_queries), queries_per_worker):
                    grouped_queries.append((ts_url, vvi_queries[i:i + queries_per_worker]))
                with ProcessPoolExecutor(
                    max_workers=min(len(grouped_queries), MAX_WORKERS),
                    mp_context=multiprocessing.get_context('spawn')
                ) as executor:
                    grouped_results = list(executor.map(_execute_sparql_queries, grouped_queries))
                results = [item for sublist in grouped_results for item in sublist]
            elif vvi_queries:
                results = _execute_sparql_queries((ts_url, vvi_queries))
            else:
                results = []

            for result in results:
                for row in result:
                    subjects.add(str(row['s']['value']))

            subjects.update(venue_uris_to_add)

            return subjects

        initial_subjects = set()

        if metavals:
            initial_subjects.update(get_initial_subjects_from_metavals(metavals))

        if identifiers:
            id_subjects, _ = get_initial_subjects_from_identifiers(identifiers)
            initial_subjects.update(id_subjects)

        if vvis:
            initial_subjects.update(get_initial_subjects_from_vvis(vvis))

        visited_subjects = set()
        process_batch_parallel(initial_subjects, 0, visited_subjects)

    def get_subgraph(self, res: str) -> Graph|None:
        if res in self.prebuilt_subgraphs:
            return self.prebuilt_subgraphs[res]
        return None

    def retrieve_venue_from_local_graph(self, meta_id: str) -> Dict[str, Dict[str, str]]:
        """
        Retrieve venue VVI structure from local graph instead of querying triplestore.
        
        :params meta_id: a MetaID
        :type meta_id: str
        :returns: Dict[str, Dict[str, str]] -- the venue structure with volumes and issues
        """
        content = {
            'issue': {},
            'volume': {}
        }

        volumes = {}
        venue_uri = URIRef(f'{self.base_iri}/br/{meta_id}')
        
        # Find all volumes directly part of this venue
        for triple in self.local_g.triples((None, RDF.type, GraphEntity.iri_journal_volume)):
            entity = triple[0]
            # Check if this volume is part of our venue
            for part_triple in self.local_g.triples((entity, GraphEntity.iri_part_of, venue_uri)):
                entity_id = str(entity).replace(f'{self.base_iri}/br/', '')
                for seq_triple in self.local_g.triples((entity, GraphEntity.iri_has_sequence_identifier, None)):
                    seq = str(seq_triple[2])
                    volumes[entity_id] = seq
                    content['volume'][seq] = {
                        'id': entity_id,
                        'issue': {}
                    }

        # Find all issues
        for triple in self.local_g.triples((None, RDF.type, GraphEntity.iri_journal_issue)):
            entity = triple[0]
            entity_id = str(entity).replace(f'{self.base_iri}/br/', '')
            seq = None
            container = None
            
            # Get sequence identifier
            for seq_triple in self.local_g.triples((entity, GraphEntity.iri_has_sequence_identifier, None)):
                seq = str(seq_triple[2])
            
            # Get container (could be venue or volume)
            for container_triple in self.local_g.triples((entity, GraphEntity.iri_part_of, None)):
                container = str(container_triple[2])

            if seq:
                if container:
                    container_id = container.replace(f'{self.base_iri}/br/', '')
                    # Check if container is a volume of our venue
                    if container_id in volumes:
                        volume_seq = volumes[container_id]
                        content['volume'][volume_seq]['issue'][seq] = {'id': entity_id}
                    # Check if container is directly our venue
                    elif container == str(venue_uri):
                        content['issue'][seq] = {'id': entity_id}

        return content