#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import os
import re
import time
from concurrent.futures import as_completed
from typing import Dict, List, Optional, Tuple

from oc_meta.lib.file_manager import get_csv_data_fast
from oc_meta.lib.master_of_regex import (ids_inside_square_brackets,
                                         semicolon_in_people_field)
from pebble import ProcessPool
from SPARQLWrapper import JSON, POST, SPARQLWrapper
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

URI_TYPE_DICT = {
    'http://purl.org/spar/doco/Abstract': 'abstract',
    'http://purl.org/spar/fabio/ArchivalDocument': 'archival document',
    'http://purl.org/spar/fabio/AudioDocument': 'audio document',
    'http://purl.org/spar/fabio/Book': 'book',
    'http://purl.org/spar/fabio/BookChapter': 'book chapter',
    'http://purl.org/spar/fabio/ExpressionCollection': 'book section',
    'http://purl.org/spar/fabio/BookSeries': 'book series',
    'http://purl.org/spar/fabio/BookSet': 'book set',
    'http://purl.org/spar/fabio/ComputerProgram': 'computer program',
    'http://purl.org/spar/doco/Part': 'book part',
    'http://purl.org/spar/fabio/Expression': '',
    'http://purl.org/spar/fabio/DataFile': 'dataset',
    'http://purl.org/spar/fabio/DataManagementPlan': 'data management plan',
    'http://purl.org/spar/fabio/Thesis': 'dissertation',
    'http://purl.org/spar/fabio/Editorial': 'editorial',
    'http://purl.org/spar/fabio/Journal': 'journal',
    'http://purl.org/spar/fabio/JournalArticle': 'journal article',
    'http://purl.org/spar/fabio/JournalEditorial': 'journal editorial',
    'http://purl.org/spar/fabio/JournalIssue': 'journal issue',
    'http://purl.org/spar/fabio/JournalVolume': 'journal volume',
    'http://purl.org/spar/fabio/Newspaper': 'newspaper',
    'http://purl.org/spar/fabio/NewspaperArticle': 'newspaper article',
    'http://purl.org/spar/fabio/NewspaperIssue': 'newspaper issue',
    'http://purl.org/spar/fr/ReviewVersion': 'peer review',
    'http://purl.org/spar/fabio/AcademicProceedings': 'proceedings',
    'http://purl.org/spar/fabio/Preprint': 'preprint',
    'http://purl.org/spar/fabio/Presentation': 'presentation',
    'http://purl.org/spar/fabio/ProceedingsPaper': 'proceedings article',
    'http://purl.org/spar/fabio/ReferenceBook': 'reference book',
    'http://purl.org/spar/fabio/ReferenceEntry': 'reference entry',
    'http://purl.org/spar/fabio/ReportDocument': 'report',
    'http://purl.org/spar/fabio/RetractionNotice': 'retraction notice',
    'http://purl.org/spar/fabio/Series': 'series',
    'http://purl.org/spar/fabio/SpecificationDocument': 'standard',
    'http://purl.org/spar/fabio/WebContent': 'web content'
}


def postprocess_type(type_uri: str) -> str:
    """Convert type URI to human-readable string"""
    if type_uri:
        return URI_TYPE_DICT.get(type_uri, type_uri)
    return ''

def normalize_ids_in_field(id_field: str) -> str:
    """
    Normalize ID field by putting OMID first and sorting other IDs alphabetically
    
    Args:
        id_field: Space-separated list of IDs (e.g., "doi:10.1000/123 pmid:456 omid:br/123")
        
    Returns:
        Normalized ID field with OMID first and others alphabetically sorted
    """
    if not id_field:
        return ""
    
    ids = id_field.strip().split()
    if not ids:
        return ""
    
    omid_ids = []
    other_ids = []
    
    for id_item in ids:
        if id_item.startswith('omid:'):
            omid_ids.append(id_item)
        else:
            other_ids.append(id_item)
    
    omid_ids.sort()
    other_ids.sort()
    
    normalized_ids = omid_ids + other_ids
    return " ".join(normalized_ids)

def normalize_ids_in_brackets(text: str) -> str:
    """
    Normalize IDs inside square brackets in text
    
    Args:
        text: Text containing IDs in square brackets (e.g., "John Doe [doi:123 omid:ra/456]")
        
    Returns:
        Text with normalized IDs in brackets (OMID first, others alphabetically)
    """
    if not text:
        return ""
    
    def normalize_bracket_content(match):
        bracket_content = match.group(1).strip()
        if not bracket_content:
            return match.group(0)
        
        ids = bracket_content.split()
        omid_ids = []
        other_ids = []
        
        for id_item in ids:
            if ':' in id_item:
                if id_item.startswith('omid:'):
                    omid_ids.append(id_item)
                else:
                    other_ids.append(id_item)
        
        omid_ids.sort()
        other_ids.sort()
        
        normalized_ids = omid_ids + other_ids
        normalized_content = " ".join(normalized_ids)
        
        return f"[{normalized_content}]"
    
    normalized_text = re.sub(ids_inside_square_brackets, normalize_bracket_content, text)
    return normalized_text

def normalize_people_field(field: str) -> str:
    """
    Normalize people field (author, editor, publisher) by normalizing IDs in brackets
    while preserving the order of people (which is significant for roles)
    
    Args:
        field: People field with semicolon-separated entries
        
    Returns:
        Field with normalized IDs but preserved people order
    """
    if not field:
        return ""
    
    people_entries = re.split(semicolon_in_people_field, field)
    
    normalized_entries = []
    for entry in people_entries:
        if entry.strip():
            normalized_entry = normalize_ids_in_brackets(entry.strip())
            normalized_entries.append(normalized_entry)
    
    return "; ".join(normalized_entries)

def normalize_page_field(page_field: str) -> str:
    """
    Normalize page field by simplifying ranges where start and end pages are identical
    
    Examples:
        "333-333" -> "333"
        "333-334" -> "333-334" (unchanged)
        "333" -> "333" (unchanged)
        "" -> ""
        
    Args:
        page_field: Page field that may contain page ranges
        
    Returns:
        Normalized page field with simplified identical ranges
    """
    if not page_field:
        return ""
    
    page_field = page_field.strip()
    if not page_field:
        return ""
    
    dash_match = re.search(r'(.+?)\s*[-–—]\s*(.+)', page_field)
    if dash_match:
        start_page = dash_match.group(1).strip()
        end_page = dash_match.group(2).strip()
            
        if start_page and end_page and start_page == end_page:
            return start_page
    
    return page_field

def get_existing_output_files(output_dir: str) -> set:
    """
    Get set of existing CSV filenames in output directory for caching
    
    Args:
        output_dir: Output directory path
        
    Returns:
        Set of filenames (without path) that already exist in output directory
    """
    if not os.path.exists(output_dir):
        return set()
    
    existing_files = set()
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            existing_files.add(filename)
    
    return existing_files

def _process_new_file(args):
    """Process a new CSV file that doesn't need database verification"""
    file_path, output_dir, endpoint_url, existing_output_files = args
    
    filename = os.path.basename(file_path)
    
    # Always read the file to extract OMIDs, even if cached
    rows = get_csv_data_fast(file_path)
    
    if not rows:
        return None, 0, 0, filename, set(), False
    
    merger = CSVDumpMerger(endpoint_url)
    
    normalized_rows = []
    file_omids = set()
    
    for row in rows:
        omid = merger.extract_omid_from_id_field(row.get('id', ''))
        if omid:
            file_omids.add(omid)
            normalized_row = merger.normalize_row_data(row)
            normalized_rows.append(normalized_row)
    
    # Check if file is cached after extracting OMIDs
    if filename in existing_output_files:
        return filename, 0, 0, filename, file_omids, True  # True indicates skipped due to cache
    
    output_file = None
    if normalized_rows:
        output_file = os.path.join(output_dir, filename)
        
        fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in normalized_rows:
                writer.writerow({key: row.get(key, '') for key in fieldnames})
    
    return output_file, len(normalized_rows), 0, filename, file_omids, False

def _process_single_file(args):
    """Process a single CSV file completely from start to finish"""
    file_path, output_dir, endpoint_url, batch_size, verbose_diff, excluded_omids, existing_output_files = args
    
    filename = os.path.basename(file_path)
    
    if filename in existing_output_files:
        return filename, 0, 0, filename, True  # True indicates skipped due to cache
    
    merger = CSVDumpMerger(endpoint_url, batch_size)
    
    rows = get_csv_data_fast(file_path)
    
    if not rows:
        return None, 0, 0, filename, False
    
    file_omids = []
    omid_to_row = {}
    
    for row in rows:
        omid = merger.extract_omid_from_id_field(row.get('id', ''))
        if omid:
            if omid not in excluded_omids:
                file_omids.append(omid)
                omid_to_row[omid] = row
    
    verified_data = {}
    any_query_failed = False
    if file_omids:
        verified_data, any_query_failed = merger.verify_file_data(file_omids)
        
        if any_query_failed:
            return None, 0, 0, filename, False
    
    final_rows = []
    updated_count = 0
    removed_count = 0
    
    for omid in file_omids:
        original_row = omid_to_row[omid]
        if omid in verified_data:
            db_row = verified_data[omid]
            if merger.rows_are_different(original_row, db_row, log_differences=verbose_diff):
                final_rows.append(db_row)
                updated_count += 1
            else:
                final_rows.append(original_row)
        else:
            removed_count += 1
            logger.info(f"Removing row for non-existent entity: {omid}")
    
    # Don't include rows_without_omid - OMID is mandatory
    
    output_file = None
    if final_rows:
        output_file = os.path.join(output_dir, filename)
        
        fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in final_rows:
                writer.writerow({key: row.get(key, '') for key in fieldnames})
    
    if removed_count > 0:
        logger.info(f"File {filename}: removed {removed_count} rows for non-existent entities")
    
    return output_file, len(final_rows), updated_count, filename, False

def process_ordered_list(items: str) -> str:
    """
    Process ordered list items with role information
    
    Includes protection against infinite loops by tracking visited roles
    and limiting maximum iterations to prevent circular references.
    
    Args:
        items: Pipe-separated string of items with role information
        
    Returns:
        Semicolon-separated string of processed items in correct order
    """
    if not items:
        return items
    items_dict = {}
    role_to_name = {}
    for item in items.split('|'):
        parts = item.split(':')
        if len(parts) >= 3:
            name = ':'.join(parts[:-2])
            current_role = parts[-2]
            next_role = parts[-1] if parts[-1] != '' else None
            items_dict[current_role] = next_role
            role_to_name[current_role] = name

    if not items_dict:
        return items

    ordered_items = []
    visited_roles = set()
    max_iterations = len(items_dict) * 2  # Reasonable upper bound to prevent infinite loops
    
    start_roles = [role for role in items_dict.keys() if role not in items_dict.values()]
    if not start_roles:
        start_role = next(iter(items_dict.keys()))
    else:
        start_role = start_roles[0]

    current_role = start_role
    iteration_count = 0
    
    while current_role and current_role in role_to_name and iteration_count < max_iterations:
        # Check for circular references
        if current_role in visited_roles:
            logger.warning(f"Circular reference detected in role chain at role: {current_role}")
            break
            
        visited_roles.add(current_role)
        ordered_items.append(role_to_name[current_role])
        current_role = items_dict.get(current_role, '')
        iteration_count += 1
    
    if iteration_count >= max_iterations:
        logger.warning(f"Maximum iterations reached ({max_iterations}) in process_ordered_list, possible infinite loop prevented")

    return "; ".join(ordered_items)


class CSVDumpMerger:
    """
    CSV Dump Merger for processing and verifying OpenCitations metadata
    
    This class processes CSV dump files by verifying their content against a SPARQL endpoint
    and provides detailed progress information including per-file statistics and update percentages.
    
    Features:
    - Processes CSV files individually with multiprocessing support
    - Verifies data against SPARQL endpoint with batched queries
    - Shows detailed progress information for each file processed
    - Displays update percentages per file and overall statistics
    - Handles files with and without OMID identifiers
    """
    
    def __init__(self, endpoint_url: str, batch_size: int = 50):
        """
        Initialize CSVDumpMerger
        
        Args:
            endpoint_url: SPARQL endpoint URL (mandatory)
            batch_size: Batch size for SPARQL queries
        """
        if not endpoint_url:
            raise ValueError("SPARQL endpoint URL is mandatory")
        
        self.endpoint_url = endpoint_url
        self.batch_size = batch_size
        self.sparql = SPARQLWrapper(endpoint_url)
        self.sparql.setMethod(POST)
        self.sparql.setReturnFormat(JSON)
        
    def extract_omid_from_id_field(self, id_field: str) -> Optional[str]:
        """Extract OMID from ID field containing multiple identifiers"""
        if not id_field:
            return None
        ids = id_field.split()
        for id_item in ids:
            if id_item.startswith('omid:'):
                return id_item
        return None
    
    def get_all_csv_files(self, directory: str) -> List[str]:
        """Get all CSV files from a directory"""
        if not os.path.exists(directory):
            return []
        
        csv_files = []
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                csv_files.append(os.path.join(directory, filename))
        return csv_files
    
    
    def build_sparql_query(self, omids: List[str]) -> str:
        """Build SPARQL query for a batch of OMIDs"""
        values_clause = "VALUES ?res {"
        for omid in omids:
            entity_id = omid.replace('omid:br/', 'https://w3id.org/oc/meta/br/')
            values_clause += f"<{entity_id}>"
        values_clause += "}"
        
        base_query = """
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX pro: <http://purl.org/spar/pro/>
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX dcterm: <http://purl.org/dc/terms/>
PREFIX frbr: <http://purl.org/vocab/frbr/core#>
PREFIX fabio: <http://purl.org/spar/fabio/>
PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/>
PREFIX oco: <https://w3id.org/oc/ontology/>

SELECT DISTINCT
?id
(STR(?title) AS ?title)
(GROUP_CONCAT(DISTINCT ?author_info; SEPARATOR="|") AS ?author)
(STR(?pub_date) AS ?pub_date)
(STR(?issue) AS ?issue)
(STR(?volume) AS ?volume)
?venue
?type
?page
(GROUP_CONCAT(DISTINCT ?publisher_info; SEPARATOR="|") AS ?publisher)
(GROUP_CONCAT(DISTINCT ?combined_editor_info; SEPARATOR="|") AS ?editor)
WHERE {
    {
        SELECT ?res ?title ?author_info ?combined_editor_info ?publisher_info ?type ?pub_date ?page ?issue ?volume ?venueName ?venueMetaid
        (GROUP_CONCAT(DISTINCT ?id ; SEPARATOR=" ") AS ?ids)
        (GROUP_CONCAT(DISTINCT ?venue_ids_; SEPARATOR=' ') AS ?venue_ids)
        WHERE {
            VALUES_PLACEHOLDER
            OPTIONAL {
                ?res datacite:hasIdentifier ?allIdentifiers.
                ?allIdentifiers datacite:usesIdentifierScheme ?allSchemes;
                                literal:hasLiteralValue ?allLiteralValues.
                BIND(CONCAT(STRAFTER(STR(?allSchemes), "http://purl.org/spar/datacite/"), ":", ?allLiteralValues) AS ?id)
            }
            OPTIONAL {
                ?res pro:isDocumentContextFor ?arAuthor.
                ?arAuthor pro:withRole pro:author;
                        pro:isHeldBy ?raAuthor.
                OPTIONAL {
                    ?arAuthor oco:hasNext ?nextAuthorRole .
                }
                BIND(STRAFTER(STR(?arAuthor), "https://w3id.org/oc/meta/ar/") AS ?roleUri)
                BIND(STRAFTER(STR(?nextAuthorRole), "https://w3id.org/oc/meta/ar/") AS ?nextRoleUri)
                BIND(CONCAT("omid:ra/", STRAFTER(STR(?raAuthor), "/ra/")) AS ?author_metaid)
                OPTIONAL {?raAuthor foaf:familyName ?familyName.}
                OPTIONAL {?raAuthor foaf:givenName ?givenName.}
                OPTIONAL {?raAuthor foaf:name ?name.}
                OPTIONAL {
                    ?raAuthor datacite:hasIdentifier ?authorIdentifier.
                    ?authorIdentifier datacite:usesIdentifierScheme ?authorIdSchema;
                                        literal:hasLiteralValue ?authorIdLiteralValue.
                    BIND(CONCAT(STRAFTER(STR(?authorIdSchema), "http://purl.org/spar/datacite/"), ":", ?authorIdLiteralValue) AS ?author_id)
                }
                BIND(
                    IF(
                        STRLEN(STR(?familyName)) > 0 && STRLEN(STR(?givenName)) > 0,
                        CONCAT(?familyName, ", ", ?givenName),
                        IF(
                            STRLEN(STR(?familyName)) > 0,
                            CONCAT(?familyName, ","),
                            ?name
                        )
                    )
                AS ?authorName)
                BIND(
                    IF(
                        STRLEN(STR(?author_id)) > 0,
                        CONCAT(?authorName, " [", ?author_id, " ", ?author_metaid, "]"),
                        CONCAT(?authorName, " [", ?author_metaid, "]")
                    )
                AS ?author_)
                BIND(CONCAT(?author_, ":", ?roleUri, ":", COALESCE(?nextRoleUri, "")) AS ?author_info)
            }
            OPTIONAL {
                ?res pro:isDocumentContextFor ?arEditor.
                ?arEditor pro:withRole pro:editor;
                            pro:isHeldBy ?raEditor.
                OPTIONAL {
                    ?arEditor oco:hasNext ?nextEditorRole .
                }
                BIND(STRAFTER(STR(?arEditor), "https://w3id.org/oc/meta/ar/") AS ?editorRoleUri)
                BIND(STRAFTER(STR(?nextEditorRole), "https://w3id.org/oc/meta/ar/") AS ?nextEditorRoleUri)
                BIND(CONCAT("omid:ra/", STRAFTER(STR(?raEditor), "/ra/")) AS ?editor_metaid)
                OPTIONAL {?raEditor foaf:familyName ?editorFamilyName.}
                OPTIONAL {?raEditor foaf:givenName ?editorGivenName.}
                OPTIONAL {?raEditor foaf:name ?editor_name.}
                OPTIONAL {
                    ?raEditor datacite:hasIdentifier ?editorIdentifier.
                    ?editorIdentifier datacite:usesIdentifierScheme ?editorIdSchema;
                                    literal:hasLiteralValue ?editorIdLiteralValue.
                    BIND(CONCAT(STRAFTER(STR(?editorIdSchema), "http://purl.org/spar/datacite/"), ":", ?editorIdLiteralValue) AS ?editor_id)
                }
                BIND(
                    IF(
                        STRLEN(STR(?editorFamilyName)) > 0 && STRLEN(STR(?editorGivenName)) > 0,
                        CONCAT(?editorFamilyName, ", ", ?editorGivenName),
                        IF(
                            STRLEN(STR(?editorFamilyName)) > 0,
                            CONCAT(?editorFamilyName, ","),
                            ?editor_name
                        )
                    )
                AS ?editorName)
                BIND(
                    IF(
                        STRLEN(STR(?editor_id)) > 0,
                        CONCAT(?editorName, " [", ?editor_id, " ", ?editor_metaid, "]"),
                        CONCAT(?editorName, " [", ?editor_metaid, "]")
                    )
                AS ?editor_)
                BIND(CONCAT(?editor_, ":", ?editorRoleUri, ":", COALESCE(?nextEditorRoleUri, "")) AS ?editor_info)
            }
            OPTIONAL {
                ?res frbr:partOf ?container.
                ?container pro:isDocumentContextFor ?arContainerEditor.
                ?arContainerEditor pro:withRole pro:editor;
                                pro:isHeldBy ?raContainerEditor.
                OPTIONAL {
                    ?arContainerEditor oco:hasNext ?nextContainerEditorRole .
                }
                BIND(STRAFTER(STR(?arContainerEditor), "https://w3id.org/oc/meta/ar/") AS ?containerEditorRoleUri)
                BIND(STRAFTER(STR(?nextContainerEditorRole), "https://w3id.org/oc/meta/ar/") AS ?nextContainerEditorRoleUri)
                BIND(CONCAT("omid:ra/", STRAFTER(STR(?raContainerEditor), "/ra/")) AS ?container_editor_metaid)
                OPTIONAL {?raContainerEditor foaf:familyName ?containerEditorFamilyName.}
                OPTIONAL {?raContainerEditor foaf:givenName ?containerEditorGivenName.}
                OPTIONAL {?raContainerEditor foaf:name ?container_editor_name.}
                OPTIONAL {
                    ?raContainerEditor datacite:hasIdentifier ?containerEditorIdentifier.
                    ?containerEditorIdentifier datacite:usesIdentifierScheme ?containerEditorIdSchema;
                                                literal:hasLiteralValue ?containerEditorIdLiteralValue.
                    BIND(CONCAT(STRAFTER(STR(?containerEditorIdSchema), "http://purl.org/spar/datacite/"), ":", ?containerEditorIdLiteralValue) AS ?container_editor_id)
                }
                BIND(
                    IF(
                        STRLEN(STR(?containerEditorFamilyName)) > 0 && STRLEN(STR(?containerEditorGivenName)) > 0,
                        CONCAT(?containerEditorFamilyName, ", ", ?containerEditorGivenName),
                        IF(
                            STRLEN(STR(?containerEditorFamilyName)) > 0,
                            CONCAT(?containerEditorFamilyName, ","),
                            ?container_editor_name
                        )
                    )
                AS ?containerEditorName)
                BIND(
                    IF(
                        STRLEN(STR(?container_editor_id)) > 0,
                        CONCAT(?containerEditorName, " [", ?container_editor_id, " ", ?container_editor_metaid, "]"),
                        CONCAT(?containerEditorName, " [", ?container_editor_metaid, "]")
                    )
                AS ?container_editor_)
                BIND(CONCAT(?container_editor_, ":", ?containerEditorRoleUri, ":", COALESCE(?nextContainerEditorRoleUri, "")) AS ?container_editor_info)
            }
            BIND(
                IF(BOUND(?editor_info),
                IF(BOUND(?container_editor_info),
                    CONCAT(?editor_info, "|", ?container_editor_info),
                    ?editor_info),
                IF(BOUND(?container_editor_info),
                    ?container_editor_info,
                    "")
                )
            AS ?combined_editor_info)
            OPTIONAL {
                ?res pro:isDocumentContextFor ?arPublisher.
                ?arPublisher pro:withRole pro:publisher;
                            pro:isHeldBy ?raPublisher.
                OPTIONAL {
                    ?arPublisher oco:hasNext ?nextPublisherRole .
                }
                BIND(STRAFTER(STR(?arPublisher), "https://w3id.org/oc/meta/ar/") AS ?publisherRoleUri)
                BIND(STRAFTER(STR(?nextPublisherRole), "https://w3id.org/oc/meta/ar/") AS ?nextPublisherRoleUri)
                ?raPublisher foaf:name ?publisherName_.
                BIND(CONCAT("omid:ra/", STRAFTER(STR(?raPublisher), "/ra/")) AS ?publisher_metaid)
                ?raPublisher foaf:name ?publisher_name.
                OPTIONAL {
                    ?raPublisher datacite:hasIdentifier ?publisherIdentifier__.
                    ?publisherIdentifier__ datacite:usesIdentifierScheme ?publisherIdSchema;
                                        literal:hasLiteralValue ?publisherIdLiteralValue.
                    BIND(CONCAT(STRAFTER(STR(?publisherIdSchema), "http://purl.org/spar/datacite/"), ":", ?publisherIdLiteralValue) AS ?publisher_id)
                }
                BIND(
                    IF(
                        STRLEN(STR(?publisher_id)) > 0,
                        CONCAT(?publisher_name, " [", ?publisher_id, " ", ?publisher_metaid, "]"),
                        CONCAT(?publisher_name, " [", ?publisher_metaid, "]")
                    )
                AS ?publisher_)
                BIND(CONCAT(?publisher_, ":", ?publisherRoleUri, ":", COALESCE(?nextPublisherRoleUri, "")) AS ?publisher_info)
            }
            OPTIONAL {
                {
                    ?res a fabio:JournalArticle;
                        frbr:partOf+ ?journal.
                        BIND(CONCAT("omid:br/", STRAFTER(STR(?journal), "/br/")) AS ?venueMetaid)
                    ?journal a fabio:Journal.
                } UNION {
                    ?res frbr:partOf ?journal.
                    BIND(CONCAT("omid:br/", STRAFTER(STR(?journal), "/br/")) AS ?venueMetaid)
                }
                ?journal dcterm:title ?venueName.
                OPTIONAL {
                    ?journal datacite:hasIdentifier ?journalIdentifier__.
                    ?journalIdentifier__ datacite:usesIdentifierScheme ?journalIdScheme;
                                literal:hasLiteralValue ?journalIdLiteralValue.
                    BIND(CONCAT(STRAFTER(STR(?journalIdScheme), "http://purl.org/spar/datacite/"), ":", ?journalIdLiteralValue) AS ?venue_ids_)
                }
            }
            OPTIONAL {?res a ?type. FILTER (?type != fabio:Expression)}
            OPTIONAL {?res dcterm:title ?title.}
            OPTIONAL {?res prism:publicationDate ?pub_date.}
            OPTIONAL {
                ?res frbr:embodiment ?re.
                ?re prism:startingPage ?startingPage;
                    prism:endingPage ?endingPage.
                BIND(IF(STR(?startingPage) = STR(?endingPage), STR(?startingPage), CONCAT(?startingPage, '-', ?endingPage)) AS ?page)
            }
            OPTIONAL {
                ?res frbr:partOf ?resIssue.
                ?resIssue a fabio:JournalIssue;
                            fabio:hasSequenceIdentifier ?issue.
            }
            OPTIONAL {
                ?res frbr:partOf+ ?resVolume.
                ?resVolume a fabio:JournalVolume;
                            fabio:hasSequenceIdentifier ?volume.
            }
        } GROUP BY ?res ?title ?author_info ?combined_editor_info ?publisher_info ?type ?issue ?volume ?pub_date ?page  ?venueName ?venueMetaid
    }
    BIND(CONCAT(?ids, IF(STR(?ids) != "", " ", ""), "omid:br/", STRAFTER(STR(?res), "/br/")) AS ?id)
    BIND(
        IF(BOUND(?venueMetaid),
           IF(STR(?venue_ids) != "",
              CONCAT(" [", ?venue_ids, " ", ?venueMetaid, "]"),
              CONCAT(" [", ?venueMetaid, "]")
           ),
           ""
        )
    AS ?venueIdentifiers)
    BIND(CONCAT(?venueName, ?venueIdentifiers) AS ?venue)
} GROUP BY ?id ?title ?type ?issue ?volume ?venue ?pub_date ?page
"""
        
        return base_query.replace("VALUES_PLACEHOLDER", values_clause)
    
    def execute_sparql_query(self, query: str) -> Optional[List[Dict[str, str]]]:
        """
        Execute SPARQL query with retry logic
        
        Returns:
            List of results if successful, None if query failed
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.sparql.setQuery(query)
                results = self.sparql.query().convert()
                
                formatted_results = []
                for binding in results["results"]["bindings"]:
                    result = {}
                    for key, value in binding.items():
                        if key == "type" and "value" in value:
                            type_uri = value["value"]
                            result[key] = postprocess_type(type_uri)
                        elif key in ["author", "editor", "publisher"] and "value" in value:
                            ordered_value = process_ordered_list(value.get("value", ""))
                            result[key] = ordered_value
                        else:
                            result[key] = value.get("value", "")
                    formatted_results.append(result)
                
                return formatted_results
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    logger.error(f"Error executing SPARQL query after {max_retries} attempts: {e}")
                    return None
    
    def verify_file_data(self, omids: List[str]) -> Tuple[Dict[str, Dict[str, str]], bool]:
        """
        Verify data for a single file's OMIDs against database
        
        Returns:
            Tuple of (verified_data, any_query_failed)
            - verified_data: Dict mapping OMIDs to their database records
            - any_query_failed: True if any query failed, False otherwise
        """
        verified_data = {}
        
        if not omids or not self.endpoint_url:
            return verified_data, False
        
        batches = [omids[i:i + self.batch_size] for i in range(0, len(omids), self.batch_size)]
        
        for batch in batches:
            query = self.build_sparql_query(batch)
            db_results = self.execute_sparql_query(query)
            
            if db_results is None:
                # If any query fails, return immediately with failure flag
                return {}, True
            else:
                for db_result in db_results:
                    db_omid = self.extract_omid_from_id_field(db_result.get('id', ''))
                    if db_omid:
                        verified_data[db_omid] = db_result
        
        return verified_data, False
    
    def normalize_row_data(self, row: Dict[str, str]) -> Dict[str, str]:
        """
        Normalize row data for comparison by standardizing various field formats
        
        This method normalizes:
        - ID field: OMID first, other IDs alphabetically sorted
        - People fields (author, editor, publisher): IDs in brackets normalized
        - Venue field: IDs in brackets normalized
        - Page field: simplified ranges where start and end pages are identical (e.g., "333-333" -> "333")
        - Other fields: basic string normalization
        """
        normalized = {}
        for key, value in row.items():
            if value is None:
                normalized_value = ""
            else:
                str_value = str(value).strip()
                
                if key == 'id':
                    normalized_value = normalize_ids_in_field(str_value)
                elif key in ['author', 'editor', 'publisher']:
                    normalized_value = normalize_people_field(str_value)
                elif key == 'venue':
                    normalized_value = normalize_ids_in_brackets(str_value)
                elif key == 'page':
                    normalized_value = normalize_page_field(str_value)
                else:
                    normalized_value = str_value
                    
            normalized[key] = normalized_value
        return normalized
    
    def rows_are_different(self, row1: Dict[str, str], row2: Dict[str, str], log_differences: bool = True) -> bool:
        """
        Check if two rows have different content with detailed logging
        
        This method compares normalized versions of the rows and logs
        detailed information about any differences found, including:
        - Which fields differ
        - Original values before normalization  
        - Normalized values after standardization
        
        Args:
            row1: First row (usually from CSV file)
            row2: Second row (usually from database)
            
        Returns:
            True if rows differ in any significant field, False otherwise
        """
        norm_row1 = self.normalize_row_data(row1)
        norm_row2 = self.normalize_row_data(row2)
        
        omid = self.extract_omid_from_id_field(row1.get('id', ''))
        differences_found = False
        
        for key in ['title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']:
            val1 = norm_row1.get(key, "")
            val2 = norm_row2.get(key, "")
            
            if val1 != val2:
                differences_found = True
                
                if log_differences:
                    logger.info(f"DIFFERENCE FOUND - OMID: {omid}")
                    logger.info(f"  Field: {key}")
                    logger.info(f"  File value (normalized): '{val1}'")
                    logger.info(f"  DB value (normalized): '{val2}'")        
                    logger.info("")
                    
        return differences_found
    
    def merge_dumps(self, existing_dir: str, new_dir: str, output_dir: str, max_workers: int = 4, verbose_diff: bool = False) -> None:
        """
        Process CSV dumps by handling new files first (without database verification) 
        and then existing files (with database verification but excluding OMIDs from new files)
        
        This method processes files in two phases:
        1. New CSV files: normalized but not verified against database, OMIDs collected
        2. Existing CSV files: verified against database but excluding OMIDs from new files
        
        This ensures that new files (which are correct by design) take precedence over
        existing files when the same OMID appears in both.
        
        Args:
            existing_dir: Directory containing existing CSV files to verify against database
            new_dir: Directory containing new CSV files (correct by design, no database verification)
            output_dir: Directory to save processed CSV files
            max_workers: Maximum number of worker processes
            verbose_diff: Enable verbose logging of row differences
        """
        if not self.endpoint_url:
            raise ValueError("SPARQL endpoint is mandatory. Please provide a valid endpoint URL.")
        
        os.makedirs(output_dir, exist_ok=True)
        
        existing_files = self.get_all_csv_files(existing_dir)
        new_files = self.get_all_csv_files(new_dir)
        existing_output_files = get_existing_output_files(output_dir)
        
        print(f"Found {len(existing_files)} existing files and {len(new_files)} new files")
        print(f"Found {len(existing_output_files)} existing output files (will be skipped for caching)")
        
        total_files_processed = 0
        total_rows_processed = 0
        total_updates = 0
        all_new_omids = set()
        
        if new_files:
            print(f"Phase 1: Processing {len(new_files)} new files (no database verification)...")
            
            with ProcessPool(max_workers=max_workers) as pool:
                new_tasks = [(file_path, output_dir, self.endpoint_url, existing_output_files) for file_path in new_files]
                new_futures = [pool.schedule(_process_new_file, args=[task]) for task in new_tasks]
                
                try:
                    with tqdm(total=len(new_files), desc="Processing new files") as pbar:
                        for future in as_completed(new_futures):
                            try:
                                output_file, row_count, updated_count, filename, file_omids, skipped = future.result()
                                if skipped:
                                    tqdm.write(f"⚡ {filename}: Skipped (cached)")
                                elif output_file:
                                    total_files_processed += 1
                                    total_rows_processed += row_count
                                    all_new_omids.update(file_omids)
                                    
                                    tqdm.write(f"✓ {filename}: {row_count} rows (new file, no verification)")
                                else:
                                    tqdm.write(f"⚠ {filename}: No output (empty file)")
                                    
                            except Exception as e:
                                tqdm.write(f"✗ Error processing new file: {e}")
                            finally:
                                pbar.update(1)
                except Exception as e:
                    print(f"Error processing new files: {e}")
        
        if existing_files:
            print(f"Phase 2: Processing {len(existing_files)} existing files (with database verification)...")
            print(f"Excluding {len(all_new_omids)} OMIDs already present in new files")
            
            with ProcessPool(max_workers=max_workers) as pool:
                existing_tasks = [(file_path, output_dir, self.endpoint_url, self.batch_size, verbose_diff, all_new_omids, existing_output_files) 
                                for file_path in existing_files]
                existing_futures = [pool.schedule(_process_single_file, args=[task]) for task in existing_tasks]
                
                try:
                    with tqdm(total=len(existing_files), desc="Processing existing files") as pbar:
                        for future in as_completed(existing_futures):
                            try:
                                output_file, row_count, updated_count, filename, skipped = future.result()
                                if skipped:
                                    tqdm.write(f"⚡ {filename}: Skipped (cached)")
                                elif output_file:
                                    total_files_processed += 1
                                    total_rows_processed += row_count
                                    total_updates += updated_count
                                    
                                    update_percentage = (updated_count / row_count * 100) if row_count > 0 else 0
                                    
                                    tqdm.write(f"✓ {filename}: {row_count} rows, {updated_count} updated ({update_percentage:.1f}%)")
                                elif output_file is None and not skipped:
                                    tqdm.write(f"⚠ {filename}: Skipped (all queries failed or empty file)")
                                else:
                                    tqdm.write(f"⚠ {filename}: No output (empty file)")
                                    
                            except Exception as e:
                                tqdm.write(f"✗ Error processing existing file: {e}")
                            finally:
                                pbar.update(1)
                except Exception as e:
                    print(f"Error processing existing files: {e}")
        
        overall_update_percentage = (total_updates / total_rows_processed * 100) if total_rows_processed > 0 else 0
        
        print(f"\n" + "="*60)
        print(f"PROCESSING COMPLETE - SUMMARY")
        print(f"="*60)
        print(f"📁 Files processed: {total_files_processed}")
        print(f"📄 Total rows processed: {total_rows_processed:,}")
        print(f"🔄 Rows updated from database: {total_updates:,}")
        print(f"📊 Overall update percentage: {overall_update_percentage:.2f}%")
        print(f"🆕 New OMIDs processed: {len(all_new_omids):,}")
        print(f"💾 Output files created in: {output_dir}")
        print(f"="*60)


def main():
    """Main function to run CSV dump merger from command line"""
    parser = argparse.ArgumentParser(description='Merge CSV metadata dumps with SPARQL verification')
    parser.add_argument('existing_dir', help='Directory containing existing CSV files (will be verified against database)')
    parser.add_argument('new_dir', help='Directory containing new CSV files (correct by design, no database verification)')
    parser.add_argument('output_dir', help='Directory to save merged CSV files')
    parser.add_argument('sparql_endpoint', help='SPARQL endpoint URL')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for SPARQL queries')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of worker processes')
    parser.add_argument('--verbose-diff', action='store_true', 
                       help='Enable verbose logging of row differences (shows detailed comparison info)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Set logging level')
    
    args = parser.parse_args()
    
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    if not args.verbose_diff:
        logging.getLogger(__name__).setLevel(logging.WARNING)
    
    merger = CSVDumpMerger(
        endpoint_url=args.sparql_endpoint,
        batch_size=args.batch_size
    )
    
    merger.merge_dumps(args.existing_dir, args.new_dir, args.output_dir, args.max_workers, args.verbose_diff)


if __name__ == '__main__':
    main()