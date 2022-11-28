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

from difflib import get_close_matches
from publishers import PUBLISHERS
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
    id_searches = list()
    for identifier in ids.split('__'):
        scheme_literal_value = identifier.split(':')
        scheme = scheme_literal_value[0].lower()
        literal_value = scheme_literal_value[1]
        literal_value = literal_value.lower() if scheme == 'doi' else literal_value
        if scheme == 'meta':
            id_searches.append(f'''{{?res a fabio:Expression. BIND(<https://w3id.org/oc/meta/{literal_value}> AS ?res)}}''')
        elif scheme in {'doi', 'issn', 'isbn', 'pmid', 'pmcid', 'url', 'wikidata', 'wikipedia'}:
            id_searches.append(f'''
                {{?identifier literal:hasLiteralValue "{literal_value}";
                              datacite:usesIdentifierScheme datacite:{scheme}.
                ?res datacite:hasIdentifier ?identifier;
                     a fabio:Expression.}}''')
    ids_search = 'UNION'.join(id_searches)
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

def clean_name(name: str) -> str:
    if ',' in name:
        split_name = re.split('\s*,\s*', name)
        first_name = split_name[1].split()
        for i, w in enumerate(first_name):
            first_name[i] = clean_title(w)
        new_first_name = ' '.join(first_name)
        surname = split_name[0].split()
        for i, w in enumerate(surname):
            surname[i] = clean_title(w)
        new_surname = ' '.join(surname)
        if new_surname and new_first_name:
            new_name = new_surname + ', ' + new_first_name
        elif not new_surname and new_first_name:
            new_name = ', ' + new_first_name
        else:
            new_name = ''
    else:
        split_name = name.split()
        for i, w in enumerate(split_name):
            split_name[i] = clean_title(w)
        new_name = ' '.join(split_name)
    return new_name

def clean_title(title: str) -> str:
    if title.isupper():
        title = title.lower()
    words = title.split()
    for i, w in enumerate(words):
        if not any(x.isupper() for x in w):
            words[i] = w.title()
    new_title = ' '.join(words)
    return new_title

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
            ?tsIdentifier{ts_index} literal:hasLiteralValue ?tsId{ts_index};
                                    datacite:usesIdentifierScheme datacite:{schema}.
            ?res datacite:hasIdentifier ?tsIdentifier{ts_index};
                a fabio:Expression.
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
        clean_test = clean_name(self.text)
        if ',' in clean_test:
            name_parts = [part.strip() for part in clean_test.split(',')]
            if name_parts:
                family_name = name_parts[0]
                if len(name_parts) == 2:
                    given_name = name_parts[1]
                    given_name = '. '.join(given_name.split('.'))
                    given_name = ' '.join([f"{name_part.rstrip('.')}.+?" if len(name_part.rstrip('.')) == 1 else name_part for name_part in given_name.split()])
                    given_name = given_name.replace('*', '.*?')
        else:
            name = clean_test
        role = role.title()
        text_search = ''
        base_query = f'''
            ?ts{role}{ts_index} pro:isHeldBy ?ts{role}Ra{ts_index};
                                pro:withRole pro:{role.lower()}.
            ?res pro:isDocumentContextFor ?ts{role}{ts_index};
                a fabio:Expression.
        '''
        if name:
            base_query = f'''
                ?ts{role}Ra{ts_index} ?namePredicate '{name}'.
                VALUES (?namePredicate) {{(foaf:name) (foaf:familyName)}}''' + base_query
        else:
            if family_name and given_name:
                base_query = f'''
                    ?ts{role}Ra{ts_index} foaf:familyName '{family_name}';
                                          foaf:givenName ?ts{role}Gn{ts_index}.
                    FILTER(REGEX(?ts{role}Gn{ts_index}, '{given_name}'))''' + base_query
            elif family_name and not given_name:
                base_query = f"?ts{role}Ra{ts_index} foaf:familyName '{family_name}'." + base_query
            elif not family_name and given_name:
                base_query = f"?ts{role}Ra{ts_index} foaf:givenName '{given_name}'." + base_query
        return text_search + base_query

    def get_text_search_on_publisher(self, ts_index:bool) -> str:
        close_match = get_close_matches(self.text.lower(), PUBLISHERS, n=1)
        if close_match:
            publisher = clean_name(close_match[0])
            text_search_on_publisher = f'''
                ?tsPublisherRa{ts_index} foaf:name '{publisher}'.
                ?tsPublisher{ts_index} pro:isHeldBy ?tsPublisherRa{ts_index};
                                    pro:withRole pro:publisher.
                ?res pro:isDocumentContextFor ?tsPublisher{ts_index};
                    a fabio:Expression.            
            '''
        else:
            text_search_on_publisher = f'''
                {self.__gen_text_search(f'tsPublisherName{ts_index}', self.text, False, ts_index)}
                ?tsPublisherRa{ts_index} foaf:name ?tsPublisherName{ts_index}.
                ?tsPublisher{ts_index} pro:isHeldBy ?tsPublisherRa{ts_index};
                                    pro:withRole pro:publisher.
                ?res pro:isDocumentContextFor ?tsPublisher{ts_index};
                    a fabio:Expression.            
            '''
        return text_search_on_publisher
        
    def get_text_search_on_vi(self, vi:str, ts_index:bool) -> str:
        v_or_i = vi.title()
        return f'''
            {self.__gen_text_search(f'ts{v_or_i}Number{ts_index}', self.text, False, ts_index)}
            ?ts{v_or_i}{ts_index} fabio:hasSequenceIdentifier ?ts{v_or_i}Number{ts_index};
                                  a fabio:Journal{v_or_i}.
            ?res frbr:partOf+ ?ts{v_or_i}{ts_index};
                a fabio:Expression.
        '''
    
    def get_text_search_on_venue(self, ts_index:bool) -> str:
        return f'''
            {self.__gen_text_search(f'tsVenueTitle{ts_index}', self.text, False, ts_index)}
            ?tsVenue{ts_index} dcterm:title ?tsVenueTitle{ts_index}.
            ?res frbr:partOf+ ?tsVenue{ts_index}.
            FILTER NOT EXISTS {{?res a fabio:JournalVolume}}
            FILTER NOT EXISTS {{?res a fabio:JournalIssue}}
        '''

    def __gen_text_search(self, variable:str, text:str, perfect_match:bool, ts_index:int) -> str:
        if str(ts_index).startswith('0'):
            min_relevance = f"bds:minRelevance '0.6'; bds:matchAllTerms 'true'." if not perfect_match else f"bds:matchRegex '^{text}$'."
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
    return query,

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