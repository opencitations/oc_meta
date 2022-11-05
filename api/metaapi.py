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

__author__ = 'Arcangelo Massari'

from typing import List, Tuple
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


def generate_id_search(ids:str) -> Tuple[str]:
    ids_search = f'''
        ?res a fabio:Expression;
            datacite:hasIdentifier ?identifier.
    '''
    id_searches = list()
    for identifier in ids.split('__'):
        scheme_literal_value = identifier.split(':')
        scheme = scheme_literal_value[0].lower()
        literal_value = scheme_literal_value[1]
        literal_value = literal_value.lower() if scheme == 'doi' else literal_value
        if scheme == 'meta':
            id_searches.append(f'''{{BIND(<https://w3id.org/oc/meta/{literal_value}> AS ?res)}}''')
        elif scheme in {'doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia'}:
            id_searches.append(f'''
                {{?identifier datacite:usesIdentifierScheme datacite:{scheme};
                                literal:hasLiteralValue "{literal_value}".}}''')
    ids_search += 'UNION'.join(id_searches)
    ids_search += 'OPTIONAL {?res a ?type__. FILTER (?type__ != fabio:Expression)}'
    return ids_search, 

def create_metadata_output(results):
    header:list = results[0]
    output_results = [header]
    for result in results[1:]:
        output_result = list()
        for i, data in enumerate(result):
            if i == header.index('type'):
                beautiful_type = __postprocess_type(data[1])
                output_result.append((data[0], beautiful_type))
            else:
                output_result.append(data)
        output_results.append(output_result)
    return output_results, True

def __postprocess_type(type_uri:str) -> str:
    if type_uri:
        type_string = URI_TYPE_DICT[type_uri]
    else:
        type_string = ''
    return type_string

class TextSearch():
    def __init__(self, text:str):
        self.text = text

    def get_text_search_on_id(self, ts_index:bool) -> str:
        schema_and_literal_value = self.text.split(':')
        schema = self.text = schema_and_literal_value[0].lower()
        literal_value = schema_and_literal_value[1]
        literal_value = literal_value.lower() if schema == 'doi' else literal_value
        return f'''
            {self.__gen_text_search(f'tsId{ts_index}', literal_value, True, ts_index)}
            ?res a fabio:Expression; datacite:hasIdentifier ?tsIdentifier{ts_index}.
            ?tsIdentifier{ts_index} datacite:usesIdentifierScheme datacite:{schema};
                          literal:hasLiteralValue ?tsId{ts_index}.
        '''
    
    def get_text_search_on_title(self, ts_index:bool) -> str:
        return f'''
            {self.__gen_text_search(f'tsTitle{ts_index}', self.text, False, ts_index)}
            ?res dcterm:title ?tsTitle{ts_index};
                a fabio:Expression.
        '''
    
    def get_text_search_on_person(self, role:str, ts_index:bool) -> str:
        family_name = None
        given_name = None
        name = None
        if ',' in self.text:
            name_parts = [part.strip() for part in self.text.split(',')]
            if name_parts:
                family_name = ' '.join([s.title() for s in name_parts[0].split()])
                if len(name_parts) == 2:
                    given_name = name_parts[1]
                    given_name = '. '.join(given_name.split('.'))
                    given_name = ' '.join([f"{name_part.rstrip('.').title()}.+?" if len(name_part.rstrip('.')) == 1 else name_part.title() for name_part in given_name.split()])
                    given_name = given_name.replace('*', '.*?')
        else:
            name = self.text
        role = role.title()
        text_search = ''
        base_query = f'''
            ?res pro:isDocumentContextFor ?ts{role}{ts_index};
                a fabio:Expression.
            ?ts{role}{ts_index} pro:withRole pro:{role.lower()};
                pro:isHeldBy ?ts{role}Ra{ts_index}.
        '''
        if name:
            text_search += f"{self.__gen_text_search(f'ts{role}Name{ts_index}', name, False, ts_index)}"
            base_query += f'?ts{role}Ra{ts_index} ?namePredicate ?ts{role}Name{ts_index}.'
            base_query += 'VALUES (?namePredicate) {(foaf:name) (foaf:familyName)}'
        else:
            if family_name:
                text_search += f"{self.__gen_text_search(f'ts{role}Fn{ts_index}', family_name, True, ts_index)}"
                base_query += f'?ts{role}Ra{ts_index} foaf:familyName ?ts{role}Fn{ts_index}.'
                if given_name:
                    base_query += f'?ts{role}Ra{ts_index} foaf:givenName ?ts{role}Gn{ts_index}.'
                    text_search += f"FILTER REGEX (?ts{role}Gn{ts_index}, '^{given_name}$')"
            elif given_name:
                base_query += f'?ts{role}Ra{ts_index} foaf:givenName ?ts{role}Gn{ts_index}.'
                text_search += f"{self.__gen_text_search(f'ts{role}Gn{ts_index}', given_name, True, ts_index)}"
        return text_search + base_query

    def get_text_search_on_publisher(self, ts_index:bool) -> str:
        return f'''
            ?res pro:isDocumentContextFor ?tsPublisher{ts_index};
                 a fabio:Expression.
            
            ?tsPublisher{ts_index} pro:withRole pro:publisher;
                    pro:isHeldBy ?tsPublisherRa{ts_index}.
            ?tsPublisherRa{ts_index} foaf:name ?tsPublisherName{ts_index}.
            {self.__gen_text_search(f'tsPublisherName{ts_index}', self.text, False, ts_index)}
        '''
        
    def get_text_search_on_vi(self, vi:str, ts_index:bool) -> str:
        v_or_i = vi.title()
        return f'''
            {self.__gen_text_search(f'ts{v_or_i}Number{ts_index}', self.text, False, ts_index)}
            ?res frbr:partOf+ ?ts{v_or_i}{ts_index};
                a fabio:Expression.
            ?ts{v_or_i}{ts_index} a fabio:Journal{v_or_i};
                    fabio:hasSequenceIdentifier ?ts{v_or_i}Number{ts_index}.
        '''
    
    def get_text_search_on_venue(self, ts_index:bool) -> str:
        return f'''
            {self.__gen_text_search(f'tsVenueTitle{ts_index}', self.text, False, ts_index)}
            ?res frbr:partOf+ ?tsVenue{ts_index}.
            ?res a fabio:Expression.
            FILTER ((!BOUND(?type__) || ?type__ != fabio:JournalVolume) && (!BOUND(?type__) ||?type__ != fabio:JournalIssue))
            ?tsVenue{ts_index} dcterm:title ?tsVenueTitle{ts_index}.
        '''

    def __gen_text_search(self, variable:str, text:str, perfect_match:bool, ts_index:int) -> str:
        if str(ts_index).startswith('0'):
            min_relevance = f"bds:minRelevance '0.6'." if not perfect_match else f"bds:matchRegex '^{text}$'."
            text_search = f"?{variable} bds:search '{text}'; {min_relevance}"
        else:
            pattern = f'^{text}$' if perfect_match else text
            text_search = f"FILTER REGEX (?{variable}, '{pattern}', 'i')"
        return text_search


def to_text_search(request:str, ts_index:bool) -> Tuple[str, str]:
    text_search = None
    field = request[0]
    value = request[1]
    ts = TextSearch(value)
    if field in {'editor', 'author'}:
        text_search = getattr(ts, f'get_text_search_on_person')(field, ts_index)
    elif field in {'volume', 'issue'}:
        text_search = getattr(ts, f'get_text_search_on_vi')(field, ts_index)
    else:
        text_search = getattr(ts, f'get_text_search_on_{field}')(ts_index)
    return text_search

def generate_text_search(text_search:str) -> str:
    requests = reorder_requests(text_search)
    text_searches = []
    for or_request in requests:
        and_text_search = ''
        for i, and_request in enumerate(or_request):
            and_text_search += f'{to_text_search(and_request, i)}'
        text_searches.append(and_text_search)
    if len(text_searches) > 1:
        query = '{' + '} UNION {'.join(text_searches) + '}'
    elif len(text_searches) == 1:
        query = text_searches[0]
    return query + 'OPTIONAL {?res a ?type__. FILTER (?type__ != fabio:Expression)}',

def reorder_requests(text_search:str) -> list:
    preferred_order = ['id', 'editor', 'author', 'title', 'venue', 'publisher', 'volume', 'issue']
    reordered_requests = []
    split_by_or = text_search.split('||')
    for or_request in split_by_or:
        split_by_and = or_request.split('&&')
        parsed_and_requests = parse_requests(split_by_and)
        sorted_and_requests = sorted(parsed_and_requests, key=lambda x: preferred_order.index(x[0]))
        reordered_requests.append(sorted_and_requests)
    return reordered_requests

def parse_requests(requests:list) -> List[Tuple]:
    parsed_requests = list()
    for request in requests:
        field_value = re.search(r'(id|title|author|editor|publisher|venue|volume|issue)=((?:(?!&&|\|\|).)+)', request)
        field = field_value.group(1)
        value = field_value.group(2)
        parsed_requests.append((field, value))
    if (any(field in {'volume', 'issue'} for field, _ in parsed_requests) and not any(field == 'venue' for field, _ in parsed_requests)):
        raise(ValueError('Specify the venue if you want to search a volume or an issue'))
    return parsed_requests