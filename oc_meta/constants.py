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

from oc_ocdm.graph.graph_entity import GraphEntity


FORBIDDEN_IDS = {'issn:0000-0000'}
VENUES = {'archival-document', 'book', 'book-part', 'book-section', 'book-series', 'book-set', 'edited-book', 'journal', 'journal-volume', 'journal-issue', 'monograph', 'proceedings-series', 'proceedings', 'reference-book', 'report-series', 'standard-series'}
CONTAINER_EDITOR_TYPES = {'book chapter', 'book part', 'book section', 'book track', 'component', 'proceedings article', 'reference entry'}

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
