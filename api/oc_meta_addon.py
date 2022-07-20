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

from oc_ocdm.graph import GraphEntity
from rdflib import URIRef


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