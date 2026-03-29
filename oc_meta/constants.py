#!/usr/bin/python

# Copyright (C) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2022-2025 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_ocdm.graph.graph_entity import GraphEntity


FORBIDDEN_IDS = {'issn:0000-0000'}
VENUES = {'archival-document', 'book', 'book-part', 'book-section', 'book-series', 'book-set', 'edited-book', 'journal', 'journal-volume', 'journal-issue', 'monograph', 'proceedings-series', 'proceedings', 'reference-book', 'report-series', 'standard-series'}
CONTAINER_EDITOR_TYPES = {'book chapter', 'book part', 'book section', 'book track', 'component', 'proceedings article', 'reference entry'}
VALID_ENTITY_TYPES = frozenset({
    'abstract', 'archival document', 'audio document', 'book', 'book chapter',
    'book part', 'book section', 'book series', 'book set', 'computer program',
    'data file', 'data management plan', 'dataset', 'dissertation', 'editorial',
    'journal', 'journal article', 'journal editorial', 'journal issue',
    'journal volume', 'newspaper', 'newspaper article', 'newspaper editorial',
    'newspaper issue', 'peer review', 'preprint', 'presentation', 'proceedings',
    'proceedings article', 'proceedings series', 'reference book', 'reference entry',
    'retraction notice', 'series', 'report', 'standard', 'web content',
})

# Constants for bibliographic resources that cannot contain other resources
ROOT_CONTAINER_TYPES = {
    GraphEntity.iri_journal_article,              # fabio:JournalArticle
    GraphEntity.iri_book_chapter,                 # fabio:BookChapter
    GraphEntity.iri_proceedings_paper,            # fabio:ProceedingsPaper
    GraphEntity.iri_journal_editorial,            # fabio:JournalEditorial
    GraphEntity.iri_newspaper_article,            # fabio:NewspaperArticle
    GraphEntity.iri_newspaper_editorial,          # fabio:NewspaperEditorial
    GraphEntity.iri_reference_entry,              # fabio:ReferenceEntry
    GraphEntity.iri_retraction_notice,            # fabio:RetractionNotice
    GraphEntity.iri_peer_review,                  # fabio:PeerReview
    GraphEntity.iri_data_file,                    # fabio:DataFile
    GraphEntity.iri_computer_program,             # fabio:ComputerProgram
    GraphEntity.iri_presentation,                 # fabio:Presentation
    GraphEntity.iri_web_content,                  # fabio:WebContent
    GraphEntity.iri_data_management_plan,         # fabio:DataManagementPlan
    GraphEntity.iri_abstract,                     # fabio:Abstract
    GraphEntity.iri_editorial,                    # fabio:Editorial
    GraphEntity.iri_preprint,                     # fabio:Preprint
}
