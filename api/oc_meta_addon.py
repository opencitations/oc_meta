#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

__author__ = 'arcangelo7'

from typing import ClassVar
from rdflib import Namespace, URIRef

class GraphEntity():
    BIRO: ClassVar[Namespace] = Namespace("http://purl.org/spar/biro/")
    C4O: ClassVar[Namespace] = Namespace("http://purl.org/spar/c4o/")
    CO: ClassVar[Namespace] = Namespace("http://purl.org/co/")
    CITO: ClassVar[Namespace] = Namespace("http://purl.org/spar/cito/")
    DATACITE: ClassVar[Namespace] = Namespace("http://purl.org/spar/datacite/")
    DCTERMS: ClassVar[Namespace] = Namespace("http://purl.org/dc/terms/")
    DEO: ClassVar[Namespace] = Namespace("http://purl.org/spar/deo/")
    DOCO: ClassVar[Namespace] = Namespace("http://purl.org/spar/doco/")
    FABIO: ClassVar[Namespace] = Namespace("http://purl.org/spar/fabio/")
    FOAF: ClassVar[Namespace] = Namespace("http://xmlns.com/foaf/0.1/")
    FR: ClassVar[Namespace] = Namespace("http://purl.org/spar/fr/")
    FRBR: ClassVar[Namespace] = Namespace("http://purl.org/vocab/frbr/core#")
    LITERAL: ClassVar[Namespace] = Namespace("http://www.essepuntato.it/2010/06/literalreification/")
    OA: ClassVar[Namespace] = Namespace("http://www.w3.org/ns/oa#")
    OCO: ClassVar[Namespace] = Namespace("https://w3id.org/oc/ontology/")
    PRISM: ClassVar[Namespace] = Namespace("http://prismstandard.org/namespaces/basic/2.0/")
    PRO: ClassVar[Namespace] = Namespace("http://purl.org/spar/pro/")

    iri_has_subtitle: ClassVar[URIRef] = FABIO.hasSubtitle
    iri_has_publication_date: ClassVar[URIRef] = PRISM.publicationDate
    iri_bibliographic_reference: ClassVar[URIRef] = BIRO.BibliographicReference
    iri_references: ClassVar[URIRef] = BIRO.references
    iri_denotes: ClassVar[URIRef] = C4O.denotes
    iri_has_content: ClassVar[URIRef] = C4O.hasContent
    iri_intextref_pointer: ClassVar[URIRef] = C4O.InTextReferencePointer
    iri_is_context_of: ClassVar[URIRef] = C4O.isContextOf
    iri_singleloc_pointer_list: ClassVar[URIRef] = C4O.SingleLocationPointerList
    iri_has_element: ClassVar[URIRef] = CO.element
    iri_citation: ClassVar[URIRef] = CITO.Citation
    iri_cites: ClassVar[URIRef] = CITO.cites
    iri_citation_characterisation: ClassVar[URIRef] = CITO.hasCitationCharacterisation
    iri_has_citing_entity: ClassVar[URIRef] = CITO.hasCitingEntity
    iri_has_cited_entity: ClassVar[URIRef] = CITO.hasCitedEntity
    iri_oci: ClassVar[URIRef] = DATACITE.oci
    iri_doi: ClassVar[URIRef] = DATACITE.doi
    iri_pmid: ClassVar[URIRef] = DATACITE.pmid
    iri_pmcid: ClassVar[URIRef] = DATACITE.pmcid
    iri_orcid: ClassVar[URIRef] = DATACITE.orcid
    iri_xpath: ClassVar[URIRef] = DATACITE["local-resource-identifier-scheme"]
    iri_intrepid: ClassVar[URIRef] = DATACITE.intrepid
    iri_xmlid: ClassVar[URIRef] = DATACITE["local-resource-identifier-scheme"]
    iri_has_identifier: ClassVar[URIRef] = DATACITE.hasIdentifier
    iri_identifier: ClassVar[URIRef] = DATACITE.Identifier
    iri_isbn: ClassVar[URIRef] = DATACITE.isbn
    iri_issn: ClassVar[URIRef] = DATACITE.issn
    iri_url: ClassVar[URIRef] = DATACITE.url
    iri_uses_identifier_scheme: ClassVar[URIRef] = DATACITE.usesIdentifierScheme
    iri_title: ClassVar[URIRef] = DCTERMS["title"]
    iri_caption: ClassVar[URIRef] = DEO.Caption
    iri_discourse_element: ClassVar[URIRef] = DEO.DiscourseElement
    iri_footnote: ClassVar[URIRef] = DOCO.Footnote
    iri_paragraph: ClassVar[URIRef] = DOCO.Paragraph
    iri_part: ClassVar[URIRef] = DOCO.Part
    iri_section: ClassVar[URIRef] = DOCO.Section
    iri_section_title: ClassVar[URIRef] = DOCO.SectionTitle
    iri_sentence: ClassVar[URIRef] = DOCO.Sentence
    iri_table: ClassVar[URIRef] = DOCO.Table
    iri_text_chunk: ClassVar[URIRef] = DOCO.TextChunk
    iri_academic_proceedings: ClassVar[URIRef] = FABIO.AcademicProceedings
    iri_book: ClassVar[URIRef] = FABIO.Book
    iri_book_chapter: ClassVar[URIRef] = FABIO.BookChapter
    iri_book_series: ClassVar[URIRef] = FABIO.BookSeries
    iri_book_set: ClassVar[URIRef] = FABIO.BookSet
    iri_data_file: ClassVar[URIRef] = FABIO.DataFile
    iri_expression: ClassVar[URIRef] = FABIO.Expression
    iri_expression_collection: ClassVar[URIRef] = FABIO.ExpressionCollection
    iri_has_sequence_identifier: ClassVar[URIRef] = FABIO.hasSequenceIdentifier
    iri_journal: ClassVar[URIRef] = FABIO.Journal
    iri_journal_article: ClassVar[URIRef] = FABIO.JournalArticle
    iri_journal_issue: ClassVar[URIRef] = FABIO.JournalIssue
    iri_journal_volume: ClassVar[URIRef] = FABIO.JournalVolume
    iri_manifestation: ClassVar[URIRef] = FABIO.Manifestation
    iri_peer_review: ClassVar[URIRef] = FR.ReviewVersion
    iri_proceedings_paper: ClassVar[URIRef] = FABIO.ProceedingsPaper
    iri_proceedings_series: ClassVar[URIRef] = FABIO.Series
    iri_reference_book: ClassVar[URIRef] = FABIO.ReferenceBook
    iri_reference_entry: ClassVar[URIRef] = FABIO.ReferenceEntry
    iri_report_document: ClassVar[URIRef] = FABIO.ReportDocument
    iri_series: ClassVar[URIRef] = FABIO.Series
    iri_specification_document: ClassVar[URIRef] = FABIO.SpecificationDocument
    iri_thesis: ClassVar[URIRef] = FABIO.Thesis
    iri_web_content: ClassVar[URIRef] = FABIO.WebContent
    iri_agent: ClassVar[URIRef] = FOAF.Agent
    iri_family_name: ClassVar[URIRef] = FOAF.familyName
    iri_given_name: ClassVar[URIRef] = FOAF.givenName
    iri_name: ClassVar[URIRef] = FOAF.name
    iri_embodiment: ClassVar[URIRef] = FRBR.embodiment
    iri_part_of: ClassVar[URIRef] = FRBR.partOf
    iri_contains_reference: ClassVar[URIRef] = FRBR.part
    iri_contains_de: ClassVar[URIRef] = FRBR.part
    iri_has_literal_value: ClassVar[URIRef] = LITERAL.hasLiteralValue
    iri_ending_page: ClassVar[URIRef] = PRISM.endingPage
    iri_starting_page: ClassVar[URIRef] = PRISM.startingPage
    iri_author: ClassVar[URIRef] = PRO.author
    iri_editor: ClassVar[URIRef] = PRO.editor
    iri_is_held_by: ClassVar[URIRef] = PRO.isHeldBy
    iri_publisher: ClassVar[URIRef] = PRO.publisher
    iri_is_document_context_for: ClassVar[URIRef] = PRO.isDocumentContextFor
    iri_role_in_time: ClassVar[URIRef] = PRO.RoleInTime
    iri_with_role: ClassVar[URIRef] = PRO.withRole
    iri_note: ClassVar[URIRef] = OA.Annotation
    iri_has_body: ClassVar[URIRef] = OA.hasBody
    iri_has_annotation: ClassVar[URIRef] = OCO.hasAnnotation  # inverse of OA.hasTarget
    iri_has_next: ClassVar[URIRef] = OCO.hasNext
    iri_archival_document: ClassVar[URIRef] = FABIO.ArchivalDocument
    iri_viaf: ClassVar[URIRef] = DATACITE.viaf
    iri_crossref: ClassVar[URIRef] = DATACITE.crossref  # TODO: add to datacite!
    iri_wikidata: ClassVar[URIRef] = DATACITE.wikidata  # TODO: add to datacite!
    iri_wikipedia: ClassVar[URIRef] = DATACITE.wikipedia  # TODO: add to datacite!
    iri_has_edition: ClassVar[URIRef] = PRISM.edition
    iri_relation: ClassVar[URIRef] = DCTERMS.relation
    iri_has_citation_creation_date: ClassVar[URIRef] = CITO.hasCitationCreationDate
    iri_has_citation_time_span: ClassVar[URIRef] = CITO.hasCitationTimeSpan
    iri_digital_manifestation: ClassVar[URIRef] = FABIO.DigitalManifestation
    iri_print_object: ClassVar[URIRef] = FABIO.PrintObject
    iri_has_url: ClassVar[URIRef] = FRBR.exemplar
    iri_self_citation: ClassVar[URIRef] = CITO.SelfCitation
    iri_affiliation_self_citation: ClassVar[URIRef] = CITO.AffiliationSelfCitation
    iri_author_network_self_citation: ClassVar[URIRef] = CITO.AuthorNetworkSelfCitation
    iri_author_self_citation: ClassVar[URIRef] = CITO.AuthorSelfCitation
    iri_funder_self_citation: ClassVar[URIRef] = CITO.FunderSelfCitation
    iri_journal_self_citation: ClassVar[URIRef] = CITO.JournalSelfCitation
    iri_journal_cartel_citation: ClassVar[URIRef] = CITO.JournalCartelCitation
    iri_distant_citation: ClassVar[URIRef] = CITO.DistantCitation
    iri_has_format: ClassVar[URIRef] = DCTERMS["format"]

def split_ids(literal_values:str) -> str:
    return "\"%s\"" % "\" \"".join(literal_values.split("__")),

def create_metadata_output(results):
    header:list = results[0]
    output_results = [header]
    for result in results[1:]:
        output_result = list()
        for i, data in enumerate(result):
            if i == header.index('type'):
                output_result.append((data[0], __postprocess_type(data[1])))
            else:
                output_result.append(data)
        output_results.append(output_result)
    return output_results, True

def __postprocess_type(ugly_types:str) -> str:
    ugly_types_list = [URIRef(type_uri) for type_uri in ugly_types.split(' ;and; ')]
    beautiful_type = ''
    if GraphEntity.iri_archival_document in ugly_types_list:
        beautiful_type = 'archival document'
    elif GraphEntity.iri_book in ugly_types_list:
        beautiful_type = 'book'
    elif GraphEntity.iri_book_chapter in ugly_types_list:
        beautiful_type = 'book chapter'
    elif GraphEntity.iri_part in ugly_types_list:
        beautiful_type = 'book part'
    elif GraphEntity.iri_expression_collection in ugly_types_list:
        beautiful_type = 'book section'
    elif GraphEntity.iri_book_series in ugly_types_list:
        beautiful_type = 'book series'
    elif GraphEntity.iri_book_set in ugly_types_list:
        beautiful_type = 'book set'
    elif GraphEntity.iri_data_file in ugly_types_list:
        beautiful_type = 'dataset'
    elif GraphEntity.iri_thesis in ugly_types_list:
        beautiful_type = 'dissertation'
    elif GraphEntity.iri_journal in ugly_types_list:
        beautiful_type = 'journal'
    elif GraphEntity.iri_journal_article in ugly_types_list:
        beautiful_type = 'journal article'
    elif GraphEntity.iri_journal_issue in ugly_types_list:
        beautiful_type = 'journal issue'
    elif GraphEntity.iri_journal_volume in ugly_types_list:
        beautiful_type = 'journal volume'
    elif GraphEntity.iri_peer_review in ugly_types_list:
        beautiful_type = 'peer_review'
    elif GraphEntity.iri_academic_proceedings in ugly_types_list:
        beautiful_type = 'proceedings'
    elif GraphEntity.iri_proceedings_paper in ugly_types_list:
        beautiful_type = 'proceedings article'
    elif GraphEntity.iri_reference_book in ugly_types_list:
        beautiful_type = 'reference book'
    elif GraphEntity.iri_reference_entry in ugly_types_list:
        beautiful_type = 'reference entry'
    elif GraphEntity.iri_report_document in ugly_types_list:
        beautiful_type = 'report'
    elif GraphEntity.iri_series in ugly_types_list:
        beautiful_type = 'series'
    elif GraphEntity.iri_specification_document in ugly_types_list:
        beautiful_type = 'standard'
    elif GraphEntity.iri_web_content in ugly_types_list:
        beautiful_type = 'web content'
    return beautiful_type