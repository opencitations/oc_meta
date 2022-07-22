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

import re

URI_TYPE_DICT = {
    'http://purl.org/spar/fabio/ArchivalDocument': 'archival document', 
    'http://purl.org/spar/fabio/Book': 'book', 
    'http://purl.org/spar/fabio/BookChapter': 'book chapter', 
    'http://purl.org/spar/doco/Part': 'book part', 
    'http://purl.org/spar/fabio/ExpressionCollection': 'book section', 
    'http://purl.org/spar/fabio/BookSeries': 'book series', 
    'http://purl.org/spar/fabio/BookSet': 'book set', 
    'http://purl.org/spar/fabio/DataFile': 'dataset', 
    'http://purl.org/spar/fabio/Thesis': 'dissertation', 
    'http://purl.org/spar/fabio/Journal': 'journal', 
    'http://purl.org/spar/fabio/JournalArticle': 'journal article', 
    'http://purl.org/spar/fabio/JournalIssue': 'journal issue', 
    'http://purl.org/spar/fabio/JournalVolume': 'journal volume', 
    'http://purl.org/spar/fr/ReviewVersion': 'peer_review', 
    'http://purl.org/spar/fabio/AcademicProceedings': 'proceedings', 
    'http://purl.org/spar/fabio/ProceedingsPaper': 'proceedings article', 
    'http://purl.org/spar/fabio/ReferenceBook': 'reference book', 
    'http://purl.org/spar/fabio/ReferenceEntry': 'reference entry', 
    'http://purl.org/spar/fabio/ReportDocument': 'report', 
    'http://purl.org/spar/fabio/Series': 'series', 
    'http://purl.org/spar/fabio/SpecificationDocument': 'standard', 
    'http://purl.org/spar/fabio/WebContent': 'web content'}


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

def __postprocess_type(types_uris:str) -> str:
    types_uris_list = types_uris.split(' ;and; ')
    types_uris_list.remove('http://purl.org/spar/fabio/Expression')
    type_uri = types_uris_list[0]
    type_string = URI_TYPE_DICT[type_uri]
    return type_string

class TextSearch():
    def __init__(self, text:str):
        self.text = text
    
    def get_text_search_on_title(self) -> str:
        return f'''
            ?res dcterm:title ?tsTitle.
            {self.__gen_text_search('tsTitle', self.text, True)}
        '''
    
    def get_text_search_on_person(self, role:str) -> str:
        role = role.title()
        return f'''
            ?res pro:isDocumentContextFor ?ts{role}.
            ?ts{role} pro:withRole pro:{role.lower()};
                    pro:isHeldBy ?ts{role}Ra.
            {{
                ?ts{role}Ra foaf:familyName ?ts{role}Fn
                {self.__gen_text_search(f'ts{role}Fn', self.text, False)}
            }}
            UNION
            {{
                ?ts{role}Ra foaf:name ?ts{role}Name
                {self.__gen_text_search(f'ts{role}Name', self.text, False)}
            }}
            UNION
            {{
                ?ts{role}Ra foaf:givenName ?ts{role}Gn
                {self.__gen_text_search(f'ts{role}Gn', self.text, False)}
            }}
        '''
    
    def get_text_search_on_publisher(self) -> str:
        return f'''
            ?res pro:isDocumentContextFor ?tsPublisher.
            ?tsPublisher pro:withRole pro:publisher;
                    pro:isHeldBy ?tsPublisherRa.
            ?tsPublisherRa foaf:name ?tsPublisherName.
            {self.__gen_text_search(f'tsPublisherName', self.text, True)}
        '''
    
    def get_text_search_on_page(self) -> str:
        pages_list = re.split('[^A-Za-z\d]+(?=[A-Za-z\d]+)', self.text)
        starting_page = pages_list[0]
        ending_page = pages_list[1] if len(pages_list) == 2 else None
        text_search = f'''
            ?res frbr:embodiment ?tsEmbodiment.
            ?tsEmbodiment prism:startingPage ?tsStartingPage;
                        prism:endingPage ?tsEndingPage.
            {self.__gen_text_search(f'tsStartingPage', starting_page, True)}
        '''
        if ending_page:
            text_search += self.__gen_text_search(f'tsEndingPage', ending_page, True)
        return text_search
    
    def get_text_search_on_vi(self, vi:str) -> str:
        v_or_i = vi.title()
        return f'''
            ?res frbr:partOf+ ?ts{v_or_i}.
            ?ts{v_or_i} a fabio:Journal{v_or_i};
                    fabio:hasSequenceIdentifier ?ts{v_or_i}Number.
            {self.__gen_text_search(f'ts{v_or_i}Number', self.text, True)}
        '''
    
    def get_text_search_on_venue(self) -> str:
        return f'''
            ?res frbr:partOf+ ?tsVenue.
            ?tsVenue a fabio:Journal;
                    dcterm:title ?tsVenueTitle.
            {self.__gen_text_search('tsVenueTitle', self.text, True)}
        '''

    def __gen_text_search(self, variable:str, text:str, match_all_terms:bool) -> str:
        header = 'SERVICE <http://www.bigdata.com/rdf/search#search> {\n'
        body = f"\t?{variable} bds:search '{text}'"
        body = body + "; bds:matchAllTerms 'true'." if match_all_terms else body
        footer = '}'
        text_search = header + body + footer
        return text_search

def generate_text_search(fields:str, text:str) -> str:
    text_searches = []
    text_search = TextSearch(text)
    fields_list = \
        ['title', 'author', 'editor', 'publisher', 'page', 'issue', 'volume', 'venue'] if fields == 'all' \
        else fields.split('__') if '__' in fields \
        else [fields]
    for field in fields_list:
        if field in {'editor', 'author'}:
            text_searches.append(getattr(text_search, f'get_text_search_on_person')(field))
        elif field in {'volume', 'issue'}:
            text_searches.append(getattr(text_search, f'get_text_search_on_vi')(field))
        else:
            text_searches.append(getattr(text_search, f'get_text_search_on_{field}')())
    return '{' + '} UNION \n {'.join(text_searches) + '}',