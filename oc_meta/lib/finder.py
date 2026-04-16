# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Tuple, TypedDict

import orjson

if TYPE_CHECKING:
    from rich.progress import Progress
from dateutil import parser
from oc_ocdm.graph.graph_entity import GraphEntity
from oc_ocdm.prov.prov_entity import ProvEntity
from oc_ocdm.support import get_resource_number
from triplelite import RDFTerm, TripleLite
from sparqlite import SPARQLClient
from time_agnostic_library.agnostic_entity import AgnosticEntity
from rich.console import Console

from oc_meta.constants import QLEVER_BATCH_SIZE, QLEVER_MAX_WORKERS, QLEVER_QUERIES_PER_GROUP
from oc_meta.lib.sparql import execute_sparql_queries

_XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
_DATACITE = "http://purl.org/spar/datacite/"

_P_HAS_LITERAL_VALUE = GraphEntity.iri_has_literal_value
_P_USES_ID_SCHEME = GraphEntity.iri_uses_identifier_scheme
_P_HAS_IDENTIFIER = GraphEntity.iri_has_identifier
_P_TITLE = GraphEntity.iri_title
_P_NAME = GraphEntity.iri_name
_P_FAMILY_NAME = GraphEntity.iri_family_name
_P_GIVEN_NAME = GraphEntity.iri_given_name
_P_IS_DOC_CONTEXT_FOR = GraphEntity.iri_is_document_context_for
_P_HAS_NEXT = GraphEntity.iri_has_next
_P_IS_HELD_BY = GraphEntity.iri_is_held_by
_P_WITH_ROLE = GraphEntity.iri_with_role
_P_EMBODIMENT = GraphEntity.iri_embodiment
_P_STARTING_PAGE = GraphEntity.iri_starting_page
_P_ENDING_PAGE = GraphEntity.iri_ending_page
_P_PUB_DATE = GraphEntity.iri_has_publication_date
_P_SEQ_ID = GraphEntity.iri_has_sequence_identifier
_P_PART_OF = GraphEntity.iri_part_of
_P_TYPE = _RDF_TYPE
_T_JOURNAL_VOLUME = GraphEntity.iri_journal_volume
_T_JOURNAL_ISSUE = GraphEntity.iri_journal_issue
_T_EXPRESSION = GraphEntity.iri_expression
_R_AUTHOR = GraphEntity.iri_author
_R_EDITOR = GraphEntity.iri_editor
_R_PUBLISHER = GraphEntity.iri_publisher


class IssueEntry(TypedDict):
    id: str


class VolumeEntry(TypedDict):
    id: str
    issue: Dict[str, IssueEntry]


class VenueStructure(TypedDict):
    issue: Dict[str, IssueEntry]
    volume: Dict[str, VolumeEntry]


class ResourceFinder:

    def __init__(self, ts_url: str, base_iri: str, settings: dict = dict(), meta_config_path: str | None = None, workers: int = 1):
        self.ts_url = ts_url
        self.base_iri = base_iri[:-1] if base_iri[-1] == '/' else base_iri
        self.graph = TripleLite(
            reverse_index_predicates=frozenset(self._PO_S_INDEXED_PREDICATES)
        )
        self.meta_config_path = meta_config_path
        self.meta_settings = settings
        self.virtuoso_full_text_search = settings['virtuoso_full_text_search'] if settings and 'virtuoso_full_text_search' in settings else False
        self.workers = workers

    _PO_S_INDEXED_PREDICATES = {_P_HAS_LITERAL_VALUE, _P_HAS_IDENTIFIER, _P_PART_OF}

    def add_triple(self, s: str, p: str, o: str, o_datatype: str = '') -> None:
        if o_datatype:
            term = RDFTerm("literal", o, o_datatype)
        elif o.startswith('http'):
            term = RDFTerm("uri", o)
        else:
            term = RDFTerm("literal", o, _XSD_STRING)
        self.graph.add((s, p, term))

    def __contains__(self, uri: str) -> bool:
        return self.graph.has_subject(uri)

    def _get_objects(self, subject: str, predicate: str) -> list[str]:
        return [t.value for t in self.graph.objects(subject, predicate)]

    def _get_all_po(self, subject: str) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for p, o in self.graph.predicate_objects(subject):
            result.setdefault(p, []).append(o.value)
        return result

    def _get_subjects(self, predicate: str, obj: str) -> set[str]:
        if predicate == _P_HAS_LITERAL_VALUE:
            term = RDFTerm("literal", obj, _XSD_STRING)
        else:
            term = RDFTerm("uri", obj)
        return set(self.graph.subjects(predicate, term))

    # _______________________________BR_________________________________ #

    def _find_id_uri(self, schema: str, value: str) -> str | None:
        schema_uri = _DATACITE + schema
        for id_uri in self._get_subjects(_P_HAS_LITERAL_VALUE, value):
            schemes = self._get_objects(id_uri, _P_USES_ID_SCHEME)
            if schemes[0] == schema_uri:
                return id_uri
        return None

    def _collect_entity_ids(self, entity_uri: str, exclude_id_uri: str | None = None) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for id_uri in self._get_objects(entity_uri, _P_HAS_IDENTIFIER):
            if id_uri == exclude_id_uri:
                continue
            po = self._get_all_po(id_uri)
            schemes = po.get(_P_USES_ID_SCHEME, [])
            literals = po.get(_P_HAS_LITERAL_VALUE, [])
            if not schemes or not literals:
                raise ValueError(f"Identifier {id_uri} missing schema or literal value")
            full_id = f'{schemes[0].replace(_DATACITE, "")}:{literals[0]}'
            result.append((id_uri.replace(f'{self.base_iri}/', ''), full_id))
        return result

    def retrieve_br_from_id(self, schema: str, value: str) -> List[Tuple[str, str, list]]:
        id_uri = self._find_id_uri(schema, value)
        if not id_uri:
            return []
        metaid_id_list = [(id_uri.replace(f'{self.base_iri}/', ''), f'{schema}:{value}')]
        result_list = []
        for entity_uri in self._get_subjects(_P_HAS_IDENTIFIER, id_uri):
            title = ''
            titles = self._get_objects(entity_uri, _P_TITLE)
            if titles:
                title = titles[0]
            other_ids = self._collect_entity_ids(entity_uri, exclude_id_uri=id_uri)
            result_list.append((entity_uri.replace(f'{self.base_iri}/', ''), title, metaid_id_list + other_ids))
        return result_list
        
    def retrieve_br_from_meta(self, metaid: str) -> Tuple[str, List[Tuple[str, str]], bool]:
        metaid_uri = f'{self.base_iri}/{metaid}'
        po = self._get_all_po(metaid_uri)
        if not po:
            return "", [], False
        title = ''
        titles = po.get(_P_TITLE, [])
        if titles:
            title = titles[0]
        identifiers = self._collect_entity_ids(metaid_uri)
        return title, identifiers, True

    # _______________________________ID_________________________________ #

    def retrieve_metaid_from_id(self, schema: str, value: str) -> str | None:
        id_uri = self._find_id_uri(schema, value)
        if id_uri:
            return id_uri.replace(f'{self.base_iri}/', '')
        return None

    def retrieve_metaid_from_merged_entity(self, metaid_uri: str, prov_config: str) -> str | None:
        '''
        It looks for MetaId in the provenance. If the input entity was deleted due to a merge, this function returns the target entity. Otherwise, it returns None.

        :params metaid_uri: a MetaId URI
        :type metaid_uri: str
        :params prov_config: the path of the configuration file required by time-agnostic-library
        :type prov_config: str
        :returns str | None: -- It returns the MetaID associated with the target entity after a merge. If there was no merge, it returns None.
        '''
        metaval: str | None = None
        with open(prov_config, 'rb') as f:
            prov_config_dict = orjson.loads(f.read())
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
            prov_endpoint = prov_config_dict['provenance']['triplestore_urls'][0]
            with SPARQLClient(prov_endpoint, max_retries=5, backoff_factor=5, timeout=3600) as client:
                results = client.query(query_if_it_was_merged)['results']['bindings']
            merged_entities = [se for se in results if metaid_uri not in se['se']['value']]
            if merged_entities:
                merged_entity_uri = merged_entities[0]['se']['value']
                merged_entity_uri = merged_entity_uri.split('/prov/')[0]
                metaval = merged_entity_uri.split('/')[-1]
        return metaval

    # _______________________________RA_________________________________ #
    def retrieve_ra_from_meta(self, metaid: str) -> Tuple[str, List[Tuple[str, str]], bool]:
        metaid_uri = f'{self.base_iri}/{metaid}'
        po = self._get_all_po(metaid_uri)
        if not po:
            return '', [], False
        family_names = po.get(_P_FAMILY_NAME, [])
        given_names = po.get(_P_GIVEN_NAME, [])
        names = po.get(_P_NAME, [])
        full_name = self._construct_full_name(
            names[0] if names else '',
            family_names[0] if family_names else '',
            given_names[0] if given_names else '',
        )
        identifiers = self._collect_entity_ids(metaid_uri)
        return full_name, identifiers, True

    def retrieve_ra_from_id(self, schema: str, value: str) -> List[Tuple[str, str, list]]:
        id_uri = self._find_id_uri(schema, value)
        if not id_uri:
            return []
        metaid_id_list: List[Tuple[str, str]] = [(id_uri.replace(f'{self.base_iri}/', ''), f'{schema}:{value}')]
        result_list = []
        for entity_uri in self._get_subjects(_P_HAS_IDENTIFIER, id_uri):
            po = self._get_all_po(entity_uri)
            names = po.get(_P_NAME, [])
            family_names = po.get(_P_FAMILY_NAME, [])
            given_names = po.get(_P_GIVEN_NAME, [])
            full_name = self._construct_full_name(
                names[0] if names else '',
                family_names[0] if family_names else '',
                given_names[0] if given_names else '',
            )
            other_ids = self._collect_entity_ids(entity_uri, exclude_id_uri=id_uri)
            result_list.append((entity_uri.replace(f'{self.base_iri}/', ''), full_name, metaid_id_list + other_ids))
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
        if col_name == 'author':
            role_str = _R_AUTHOR
        elif col_name == 'editor':
            role_str = _R_EDITOR
        else:
            role_str = _R_PUBLISHER

        metaid_uri = f'{self.base_iri}/{metaid}'
        dict_ar: dict[str, dict[str, str]] = {}

        for ar_uri in self._get_objects(metaid_uri, _P_IS_DOC_CONTEXT_FOR):
            ar_po = self._get_all_po(ar_uri)
            roles = ar_po.get(_P_WITH_ROLE, [])
            if role_str in roles:
                role_value = ar_uri.replace(f'{self.base_iri}/', '')
                next_list = ar_po.get(_P_HAS_NEXT, [])
                next_role = next_list[0].replace(f'{self.base_iri}/', '') if next_list else ''
                held_by = ar_po.get(_P_IS_HELD_BY, [])
                ra = held_by[0].replace(f'{self.base_iri}/', '') if held_by else None
                if ra is not None:
                    dict_ar[role_value] = {'next': next_role, 'ra': ra}

        all_roles = set(dict_ar.keys())
        roles_with_next = set(details['next'] for details in dict_ar.values() if details['next'])
        start_role_candidates = all_roles - roles_with_next

        MAX_ITERATIONS = 10000

        if len(all_roles) == 0:
            return []

        if len(start_role_candidates) == 0:
            sorted_ars = sorted(all_roles, key=lambda ar: get_resource_number(f'{self.base_iri}/{ar}'))
            start_role_candidates = {sorted_ars[0]}

        if len(start_role_candidates) != 1:
            chains = []
            for start_candidate in start_role_candidates:
                current_role = start_candidate
                chain: list[dict[str, tuple]] = []
                visited_roles: set[str] = set()
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
            chains.sort(key=lambda chain: (-len(chain), get_resource_number(f'{self.base_iri}/{list(chain[0].keys())[0]}')))
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
            ordered_ar_list: list[dict[str, tuple]] = []
            current_role = start_role
            visited_roles: set[str] = set()
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

    def retrieve_re_from_br_meta(self, metaid: str) -> Tuple[str, str] | None:
        metaid_uri = f'{self.base_iri}/{metaid}'
        re_uris = self._get_objects(metaid_uri, _P_EMBODIMENT)
        if not re_uris:
            return None
        re_full_uri = re_uris[0]
        re_metaid = re_full_uri.replace(f'{self.base_iri}/', '')
        re_po = self._get_all_po(re_full_uri)
        starting_pages = re_po.get(_P_STARTING_PAGE, [])
        ending_pages = re_po.get(_P_ENDING_PAGE, [])
        starting_page = starting_pages[0] if starting_pages else None
        ending_page = ending_pages[0] if ending_pages else None
        pages = ''
        if starting_page and ending_page:
            pages = f'{starting_page}-{ending_page}'
        elif starting_page:
            pages = f'{starting_page}-{starting_page}'
        elif ending_page:
            pages = f'{ending_page}-{ending_page}'
        return re_metaid, pages

    def retrieve_br_info_from_meta(self, metaid: str) -> dict:
        venue_type_strs = {
            GraphEntity.iri_archival_document,
            GraphEntity.iri_journal,
            GraphEntity.iri_book,
            GraphEntity.iri_book_series,
            GraphEntity.iri_series,
            GraphEntity.iri_academic_proceedings,
            GraphEntity.iri_proceedings_series,
            GraphEntity.iri_reference_book,
            _T_EXPRESSION,
        }

        def extract_identifiers(entity_uri: str) -> list[str]:
            identifiers = [f"omid:{entity_uri.replace(f'{self.base_iri}/', '')}"]
            for id_uri in self._get_objects(entity_uri, _P_HAS_IDENTIFIER):
                id_po = self._get_all_po(id_uri)
                schemes = id_po.get(_P_USES_ID_SCHEME, [])
                literals = id_po.get(_P_HAS_LITERAL_VALUE, [])
                if schemes and literals:
                    scheme = schemes[0].replace(_DATACITE, '')
                    identifiers.append(f"{scheme}:{literals[0]}")
            return identifiers

        def check_venue(entity_uri: str) -> str | None:
            entity_types = self._get_objects(entity_uri, _P_TYPE)
            if any(t in venue_type_strs for t in entity_types):
                titles = self._get_objects(entity_uri, _P_TITLE)
                if titles:
                    venue_ids = extract_identifiers(entity_uri)
                    return f"{titles[0]} [{' '.join(venue_ids)}]"
            return None

        metaid_uri = f'{self.base_iri}/{metaid}' if self.base_iri not in metaid else metaid
        po = self._get_all_po(metaid_uri)
        res_dict: dict = {
            'pub_date': '',
            'type': '',
            'page': self.retrieve_re_from_br_meta(metaid),
            'issue': '',
            'volume': '',
            'venue': ''
        }

        pub_dates = po.get(_P_PUB_DATE, [])
        if pub_dates:
            res_dict['pub_date'] = pub_dates[0]

        types = po.get(_P_TYPE, [])
        for t in types:
            if t != _T_EXPRESSION and t.startswith('http'):
                res_dict['type'] = self._type_it(t)
                break

        seq_ids = po.get(_P_SEQ_ID, [])
        if seq_ids:
            entity_types = types
            if _T_JOURNAL_ISSUE in entity_types:
                res_dict['issue'] = seq_ids[0]
            elif _T_JOURNAL_VOLUME in entity_types:
                res_dict['volume'] = seq_ids[0]

        for container_uri in po.get(_P_PART_OF, []):
            container_po = self._get_all_po(container_uri)
            container_types = container_po.get(_P_TYPE, [])

            if _T_JOURNAL_ISSUE in container_types:
                container_seqs = container_po.get(_P_SEQ_ID, [])
                if container_seqs:
                    res_dict['issue'] = container_seqs[0]
            elif _T_JOURNAL_VOLUME in container_types:
                container_seqs = container_po.get(_P_SEQ_ID, [])
                if container_seqs:
                    res_dict['volume'] = container_seqs[0]
            else:
                venue_str = check_venue(container_uri)
                if venue_str:
                    res_dict['venue'] = venue_str

            for inner_uri in container_po.get(_P_PART_OF, []):
                inner_po = self._get_all_po(inner_uri)
                inner_types = inner_po.get(_P_TYPE, [])

                if _T_JOURNAL_VOLUME in inner_types:
                    inner_seqs = inner_po.get(_P_SEQ_ID, [])
                    if inner_seqs:
                        res_dict['volume'] = inner_seqs[0]
                else:
                    venue_str = check_venue(inner_uri)
                    if venue_str:
                        res_dict['venue'] = venue_str

                for venue_uri in inner_po.get(_P_PART_OF, []):
                    titles = self._get_objects(venue_uri, _P_TITLE)
                    if titles:
                        venue_ids = extract_identifiers(venue_uri)
                        res_dict['venue'] = f"{titles[0]} [{' '.join(venue_ids)}]"

        return res_dict

    _IRI_TO_TYPE = {
        GraphEntity.iri_archival_document: 'archival document',
        GraphEntity.iri_book: 'book',
        GraphEntity.iri_book_chapter: 'book chapter',
        GraphEntity.iri_part: 'book part',
        GraphEntity.iri_expression_collection: 'book section',
        GraphEntity.iri_book_series: 'book series',
        GraphEntity.iri_book_set: 'book set',
        GraphEntity.iri_data_file: 'data file',
        GraphEntity.iri_thesis: 'dissertation',
        GraphEntity.iri_journal: 'journal',
        GraphEntity.iri_journal_article: 'journal article',
        GraphEntity.iri_journal_issue: 'journal issue',
        GraphEntity.iri_journal_volume: 'journal volume',
        GraphEntity.iri_proceedings_paper: 'proceedings article',
        GraphEntity.iri_academic_proceedings: 'proceedings',
        GraphEntity.iri_reference_book: 'reference book',
        GraphEntity.iri_reference_entry: 'reference entry',
        GraphEntity.iri_series: 'series',
        GraphEntity.iri_report_document: 'report',
        GraphEntity.iri_specification_document: 'standard',
    }

    @staticmethod
    def _type_it(br_type: str) -> str:
        return ResourceFinder._IRI_TO_TYPE.get(br_type, '')
    
    def retrieve_publisher_from_br_metaid(self, metaid: str):
        metaid_uri = f'{self.base_iri}/{metaid}'
        publisher_ar_uris: set[str] = set()

        def find_publisher_ars(entity_uri: str) -> None:
            for ar_uri in self._get_objects(entity_uri, _P_IS_DOC_CONTEXT_FOR):
                roles = self._get_objects(ar_uri, _P_WITH_ROLE)
                if _R_PUBLISHER in roles:
                    publisher_ar_uris.add(ar_uri)

        find_publisher_ars(metaid_uri)
        for parent_uri in self._get_objects(metaid_uri, _P_PART_OF):
            find_publisher_ars(parent_uri)
            for grandparent_uri in self._get_objects(parent_uri, _P_PART_OF):
                find_publisher_ars(grandparent_uri)

        publishers_output = []
        for ar_uri in publisher_ar_uris:
            pub_identifiers: List[str] = []
            pub_name: str | None = None
            for ra_uri in self._get_objects(ar_uri, _P_IS_HELD_BY):
                pub_identifiers.append(ra_uri.replace(f'{self.base_iri}/', 'omid:'))
                ra_po = self._get_all_po(ra_uri)
                names = ra_po.get(_P_NAME, [])
                if names:
                    pub_name = names[0]
                for id_uri in ra_po.get(_P_HAS_IDENTIFIER, []):
                    id_po = self._get_all_po(id_uri)
                    schemes = id_po.get(_P_USES_ID_SCHEME, [])
                    literals = id_po.get(_P_HAS_LITERAL_VALUE, [])
                    if schemes and literals:
                        pub_identifiers.append(f'{schemes[0].replace(_DATACITE, "")}:{literals[0]}')
            if pub_name is not None:
                pub_full = f'{pub_name} [{" ".join(pub_identifiers)}]'
            else:
                pub_full = f'[{" ".join(pub_identifiers)}]'
            publishers_output.append(pub_full)
        return '; '.join(publishers_output)
            
    def get_everything_about_res(self, metavals: set, identifiers: set, vvis: set, max_depth: int = 10, progress: Progress | None = None) -> None:
        BATCH_SIZE = QLEVER_BATCH_SIZE
        MAX_WORKERS = min(self.workers, QLEVER_MAX_WORKERS)

        def batch_process(input_set, batch_size):
            """Generator to split input data into smaller batches if batch_size is not None."""
            if batch_size is None:
                yield input_set
            else:
                for i in range(0, len(input_set), batch_size):
                    yield input_set[i:i + batch_size]

        task_metavals = None
        task_identifiers = None
        task_vvis = None
        if progress:
            if metavals:
                task_metavals = progress.add_task(
                    "  [dim]Resolving OMIDs[/dim]", total=len(metavals)
                )
            if identifiers:
                task_identifiers = progress.add_task(
                    "  [dim]Resolving identifiers[/dim]", total=len(identifiers)
                )
            if vvis:
                task_vvis = progress.add_task(
                    "  [dim]Resolving VVI[/dim]", total=len(vvis)
                )

        max_depth_reached = 0

        def process_batch_parallel(subjects, cur_depth, visited_subjects):
            nonlocal max_depth_reached
            if not subjects or (max_depth and cur_depth > max_depth):
                return

            new_subjects = subjects - visited_subjects
            if not new_subjects:
                return

            if cur_depth > max_depth_reached:
                max_depth_reached = cur_depth

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
                query_groups = [
                    batch_queries[i:i + queries_per_worker]
                    for i in range(0, len(batch_queries), queries_per_worker)
                ]
                worker = partial(execute_sparql_queries, ts_url)
                with ProcessPoolExecutor(
                    max_workers=min(len(query_groups), MAX_WORKERS),
                    mp_context=multiprocessing.get_context('forkserver')
                ) as executor:
                    grouped_results = list(executor.map(worker, query_groups))
                results = [item for sublist in grouped_results for item in sublist]
            else:
                results = execute_sparql_queries(endpoint_url=ts_url, queries=batch_queries) if batch_queries else []

            _skip_preds = {_P_TYPE, _P_WITH_ROLE, _P_USES_ID_SCHEME}
            for result in results:
                for row in result:
                    s_str = row['s']['value']
                    p_str = row['p']['value']
                    o_binding = row['o']
                    o_str = o_binding['value']
                    o_datatype = o_binding.get('datatype', '') if o_binding['type'] in ('literal', 'typed-literal') else ''
                    self.add_triple(s_str, p_str, o_str, o_datatype=o_datatype)
                    if o_binding['type'] == 'uri' and p_str not in _skip_preds:
                        next_subjects.add(o_str)

            process_batch_parallel(next_subjects, cur_depth + 1, visited_subjects)

        def get_initial_subjects_from_metavals(metavals):
            """Convert metavals to a set of subjects."""
            return {f"{self.base_iri}/{mid.replace('omid:', '')}" for mid in metavals}

        def get_initial_subjects_from_identifiers(identifiers, progress_task=None):
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
            batch_sizes = []
            for batch in batches:
                if not batch:
                    continue

                batch_sizes.append(len(batch))
                if self.virtuoso_full_text_search:
                    union_blocks = []
                    for identifier in batch:
                        scheme, literal = identifier.split(':', maxsplit=1)[0], identifier.split(':', maxsplit=1)[1]
                        escaped_literal = literal.replace('\\', '\\\\').replace('"', '\\"')
                        union_blocks.append(f"""
                            {{
                                ?id <{_P_HAS_LITERAL_VALUE}> "{escaped_literal}"^^<{_XSD_STRING}> .
                                ?id <{_P_USES_ID_SCHEME}> <{_DATACITE}{scheme}> .
                                ?s <{_P_HAS_IDENTIFIER}> ?id .
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
                        identifiers_values.append(f'(<{_DATACITE}{scheme}> "{escaped_literal}"^^<{_XSD_STRING}>)')
                    identifiers_values_str = " ".join(identifiers_values)
                    query = f'''
                        SELECT DISTINCT ?s ?scheme ?literal WHERE {{
                            VALUES (?scheme ?literal) {{ {identifiers_values_str} }}
                            ?id <{_P_USES_ID_SCHEME}> ?scheme .
                            ?id <{_P_HAS_LITERAL_VALUE}> ?literal .
                            ?s <{_P_HAS_IDENTIFIER}> ?id .
                        }}
                    '''
                    batch_queries.append(query)

            if len(batch_queries) > 1 and MAX_WORKERS > 1:
                query_groups = []
                grouped_batch_sizes = []
                for i in range(0, len(batch_queries), QLEVER_QUERIES_PER_GROUP):
                    query_groups.append(batch_queries[i:i + QLEVER_QUERIES_PER_GROUP])
                    grouped_batch_sizes.append(sum(batch_sizes[i:i + QLEVER_QUERIES_PER_GROUP]))
                worker = partial(execute_sparql_queries, ts_url)
                with ProcessPoolExecutor(
                    max_workers=MAX_WORKERS,
                    mp_context=multiprocessing.get_context('forkserver')
                ) as executor:
                    results = []
                    for idx, grouped_result in enumerate(executor.map(worker, query_groups)):
                        results.extend(grouped_result)
                        if progress and progress_task is not None:
                            progress.advance(progress_task, grouped_batch_sizes[idx])
            elif batch_queries:
                results = execute_sparql_queries(endpoint_url=ts_url, queries=batch_queries)
                if progress and progress_task is not None:
                    progress.advance(progress_task, sum(batch_sizes))
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
                        scheme = str(row['scheme']['value']).replace(_DATACITE, '')
                        literal = str(row['literal']['value'])
                    identifier = f"{scheme}:{literal}"
                    if identifier not in id_to_subjects:
                        id_to_subjects[identifier] = set()
                    id_to_subjects[identifier].add(subject)

            return subjects, id_to_subjects

        def _build_values_queries(issue_vol_tuples, issue_no_vol_tuples, vol_only_tuples):
            queries = []

            for i in range(0, len(issue_vol_tuples), BATCH_SIZE):
                chunk = issue_vol_tuples[i:i + BATCH_SIZE]
                values_block = ' '.join(
                    f'(<{venue}> "{vol_seq}"^^<{_XSD_STRING}> "{issue_seq}"^^<{_XSD_STRING}>)'
                    for venue, vol_seq, issue_seq in chunk
                )
                queries.append(f'''
                    SELECT ?s WHERE {{
                        VALUES (?venueUri ?volSeq ?issSeq) {{ {values_block} }}
                        ?volume a <{_T_JOURNAL_VOLUME}> ;
                            <{_P_PART_OF}> ?venueUri ;
                            <{_P_SEQ_ID}> ?volSeq .
                        ?s a <{_T_JOURNAL_ISSUE}> ;
                            <{_P_PART_OF}> ?volume ;
                            <{_P_SEQ_ID}> ?issSeq .
                    }}
                ''')

            for i in range(0, len(issue_no_vol_tuples), BATCH_SIZE):
                chunk = issue_no_vol_tuples[i:i + BATCH_SIZE]
                values_block = ' '.join(
                    f'(<{venue}> "{issue_seq}"^^<{_XSD_STRING}>)'
                    for venue, issue_seq in chunk
                )
                queries.append(f'''
                    SELECT ?s WHERE {{
                        VALUES (?venueUri ?issSeq) {{ {values_block} }}
                        ?s a <{_T_JOURNAL_ISSUE}> ;
                            <{_P_PART_OF}> ?venueUri ;
                            <{_P_SEQ_ID}> ?issSeq .
                    }}
                ''')

            for i in range(0, len(vol_only_tuples), BATCH_SIZE):
                chunk = vol_only_tuples[i:i + BATCH_SIZE]
                values_block = ' '.join(
                    f'(<{venue}> "{vol_seq}"^^<{_XSD_STRING}>)'
                    for venue, vol_seq in chunk
                )
                queries.append(f'''
                    SELECT ?s WHERE {{
                        VALUES (?venueUri ?volSeq) {{ {values_block} }}
                        ?s a <{_T_JOURNAL_VOLUME}> ;
                            <{_P_PART_OF}> ?venueUri ;
                            <{_P_SEQ_ID}> ?volSeq .
                    }}
                ''')

            return queries

        def get_initial_subjects_from_vvis(vvis, progress_task=None):
            """Convert vvis to a set of subjects based on batched VALUES queries."""
            subjects = set()
            ts_url = self.ts_url
            venue_uris_to_add = set()
            vvis_list = list(vvis)
            total_vvis = len(vvis_list)

            # First pass: collect all venue IDs and prepare queries
            all_venue_ids = set()
            for volume, issue, venue_metaid, venue_ids_tuple in vvis_list:
                if venue_ids_tuple:
                    all_venue_ids.update(venue_ids_tuple)

            # Get venue subjects from identifiers with mapping
            venue_id_to_uris = {}
            if all_venue_ids:
                venue_id_subjects, venue_id_to_uris = get_initial_subjects_from_identifiers(all_venue_ids)
                subjects.update(venue_id_subjects)

            # Second pass: collect tuples grouped by query pattern
            issue_vol_tuples = []
            issue_no_vol_tuples = []
            vol_only_tuples = []

            for volume, issue, venue_metaid, venue_ids_tuple in vvis_list:
                venues_to_search = set()

                if venue_metaid:
                    venues_to_search.add(venue_metaid)

                if venue_ids_tuple:
                    for venue_id in venue_ids_tuple:
                        if venue_id in venue_id_to_uris:
                            for venue_uri in venue_id_to_uris[venue_id]:
                                if '/br/' in venue_uri:
                                    venues_to_search.add(venue_uri.replace(f'{self.base_iri}/', 'omid:'))

                for venue_metaid_to_search in venues_to_search:
                    venue_uri = f"{self.base_iri}/{venue_metaid_to_search.replace('omid:', '')}"
                    if not (issue or volume):
                        continue
                    escaped_issue = issue.replace('\\', '\\\\').replace('"', '\\"') if issue else None
                    escaped_volume = volume.replace('\\', '\\\\').replace('"', '\\"') if volume else None

                    if issue:
                        if volume:
                            issue_vol_tuples.append((venue_uri, escaped_volume, escaped_issue))
                        else:
                            issue_no_vol_tuples.append((venue_uri, escaped_issue))
                    else:
                        vol_only_tuples.append((venue_uri, escaped_volume))

                    venue_uris_to_add.add(venue_uri)

            vvi_queries = _build_values_queries(issue_vol_tuples, issue_no_vol_tuples, vol_only_tuples)

            # Execute batched VVI queries in parallel
            if len(vvi_queries) > 1 and MAX_WORKERS > 1:
                query_groups = []
                grouped_vvi_counts = []
                queries_per_group = max(1, len(vvi_queries) // MAX_WORKERS)
                for i in range(0, len(vvi_queries), queries_per_group):
                    group = vvi_queries[i:i + queries_per_group]
                    query_groups.append(group)
                    vvi_count = int(total_vvis * len(group) / len(vvi_queries))
                    grouped_vvi_counts.append(max(1, vvi_count))
                worker = partial(execute_sparql_queries, ts_url)
                with ProcessPoolExecutor(
                    max_workers=MAX_WORKERS,
                    mp_context=multiprocessing.get_context('forkserver')
                ) as executor:
                    results = []
                    for idx, grouped_result in enumerate(executor.map(worker, query_groups)):
                        results.extend(grouped_result)
                        if progress and progress_task is not None:
                            progress.advance(progress_task, grouped_vvi_counts[idx])
            elif vvi_queries:
                results = execute_sparql_queries(endpoint_url=ts_url, queries=vvi_queries)
                if progress and progress_task is not None:
                    progress.advance(progress_task, total_vvis)
            else:
                results = []
                if progress and progress_task is not None:
                    progress.advance(progress_task, total_vvis)

            for result in results:
                for row in result:
                    subjects.add(str(row['s']['value']))

            subjects.update(venue_uris_to_add)

            return subjects

        initial_subjects = set()

        if metavals:
            initial_subjects.update(get_initial_subjects_from_metavals(metavals))
            if progress and task_metavals is not None:
                progress.advance(task_metavals, len(metavals))
                progress.remove_task(task_metavals)

        if identifiers:
            id_subjects, _ = get_initial_subjects_from_identifiers(identifiers, progress_task=task_identifiers)
            initial_subjects.update(id_subjects)
            if progress and task_identifiers is not None:
                progress.remove_task(task_identifiers)

        if vvis:
            initial_subjects.update(get_initial_subjects_from_vvis(vvis, progress_task=task_vvis))
            if progress and task_vvis is not None:
                progress.remove_task(task_vvis)

        visited_subjects = set()
        process_batch_parallel(initial_subjects, 0, visited_subjects)

        console = Console()
        style = "bold red" if max_depth_reached >= max_depth else "bold green"
        console.print(f"  Max traversal depth reached: {max_depth_reached}/{max_depth}", style=style)

    def retrieve_venue_from_local_graph(self, meta_id: str) -> VenueStructure:
        content: VenueStructure = {
            'issue': {},
            'volume': {}
        }
        venue_uri = f'{self.base_iri}/{meta_id}'

        venue_children = self._get_subjects(_P_PART_OF, venue_uri)
        volumes: dict[str, str] = {}

        for child_uri in venue_children:
            types = self._get_objects(child_uri, _P_TYPE)
            child_id = child_uri.replace(f'{self.base_iri}/', '')
            if _T_JOURNAL_VOLUME in types:
                seqs = self._get_objects(child_uri, _P_SEQ_ID)
                for seq in seqs:
                    volumes[child_uri] = seq
                    content['volume'][seq] = {'id': child_id, 'issue': {}}
            elif _T_JOURNAL_ISSUE in types:
                seqs = self._get_objects(child_uri, _P_SEQ_ID)
                seq = seqs[0] if seqs else None
                if seq:
                    content['issue'][seq] = {'id': child_id}

        for volume_uri, volume_seq in volumes.items():
            volume_children = self._get_subjects(_P_PART_OF, volume_uri)
            for child_uri in volume_children:
                types = self._get_objects(child_uri, _P_TYPE)
                if _T_JOURNAL_ISSUE in types:
                    child_id = child_uri.replace(f'{self.base_iri}/', '')
                    seqs = self._get_objects(child_uri, _P_SEQ_ID)
                    seq = seqs[0] if seqs else None
                    if seq:
                        content['volume'][volume_seq]['issue'][seq] = {'id': child_id}

        return content