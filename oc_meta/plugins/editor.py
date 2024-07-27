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

from __future__ import annotations

import os
import re

import validators
import yaml
from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader
from rdflib import URIRef
from oc_ocdm.graph.graph_entity import GraphEntity
from oc_ocdm.support.support import build_graph_from_results
from SPARQLWrapper import JSON, SPARQLWrapper
from rdflib import RDF, ConjunctiveGraph

class MetaEditor:
    property_to_remove_method = {
        'http://purl.org/spar/datacite/hasIdentifier': 'remove_identifier',
        'http://purl.org/spar/pro/isHeldBy': 'remove_is_held_by',
        'http://purl.org/vocab/frbr/core#embodiment': 'remove_format',
        'http://purl.org/spar/pro/isDocumentContextFor': 'remove_is_held_by',
        'https://w3id.org/oc/ontology/hasNext': 'remove_next'
    }

    def __init__(self, meta_config: str, resp_agent: str):
        with open(meta_config, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        self.endpoint = settings['triplestore_url']
        self.base_dir = os.path.join(settings['output_rdf_dir'], 'rdf') + os.sep
        self.info_dir = os.path.join(settings['output_rdf_dir'], 'info_dir')
        self.base_iri = settings['base_iri']
        self.resp_agent = resp_agent
        self.dir_split = settings['dir_split_number']
        self.n_file_item = settings['items_per_file']
        self.zip_output_rdf = settings['zip_output_rdf']
        self.reader = Reader()
    
    def update_property(self, res: URIRef, property: str, new_value: str|URIRef) -> None:
        info_dir = self.__get_info_dir(res)
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(self.base_iri, info_dir, supplier_prefix=supplier_prefix)
        self.reader.import_entity_from_triplestore(g_set, self.endpoint, res, self.resp_agent, enable_validation=False)
        if validators.url(new_value):
            new_value = URIRef(new_value)
            self.reader.import_entity_from_triplestore(g_set, self.endpoint, new_value, self.resp_agent, enable_validation=False)
            getattr(g_set.get_entity(res), property)(g_set.get_entity(new_value))
        else:
            getattr(g_set.get_entity(res), property)(new_value)
        self.save(g_set, info_dir, supplier_prefix)
    
    def delete(self, res: str, property: str = None, object: str = None) -> None:
        info_dir = self.__get_info_dir(res)
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(self.base_iri, info_dir, supplier_prefix=supplier_prefix)
        try:
            self.reader.import_entity_from_triplestore(g_set, self.endpoint, res, self.resp_agent, enable_validation=False)
        except ValueError as e:
            print(f"ValueError for entity {res}: {e}")
            inferred_type = self.infer_type_from_uri(res)
            if inferred_type:
                print(f"Inferred type {inferred_type} for entity {res}")
                sparql: SPARQLWrapper = SPARQLWrapper(self.endpoint)
                query: str = f"SELECT ?s ?p ?o WHERE {{BIND (<{res}> AS ?s). ?s ?p ?o.}}"
                sparql.setQuery(query)
                sparql.setMethod('GET')
                sparql.setReturnFormat(JSON)
                result = sparql.queryAndConvert()['results']['bindings']
                preexisting_graph: ConjunctiveGraph = build_graph_from_results(result)
                self.add_entity_with_type(g_set, res, inferred_type, preexisting_graph)
            else:
                return
        if not g_set.get_entity(URIRef(res)):
            return
        if property:
            remove_method = self.property_to_remove_method[property] if property in self.property_to_remove_method else property.replace('has', 'remove') if property.startswith('has') else f'remove_{property}'
            if object:
                if validators.url(object):
                    self.reader.import_entity_from_triplestore(g_set, self.endpoint, object, self.resp_agent, enable_validation=False)
                    try:
                        getattr(g_set.get_entity(URIRef(res)), remove_method)(g_set.get_entity(URIRef(object)))
                    # TypeError: AgentRole.remove_is_held_by() takes 1 positional argument but 2 were given
                    except TypeError:
                        getattr(g_set.get_entity(URIRef(res)), remove_method)()
                else:
                    getattr(g_set.get_entity(URIRef(res)), remove_method)(object)
            else:
                getattr(g_set.get_entity(URIRef(res)), remove_method)()
        else:
            sparql = SPARQLWrapper(endpoint=self.endpoint)
            query = f'SELECT ?s WHERE {{?s ?p <{res}>.}}'       
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            result = sparql.queryAndConvert()
            for entity in result['results']['bindings']:
                self.reader.import_entity_from_triplestore(g_set, self.endpoint, URIRef(entity['s']['value']), self.resp_agent, enable_validation=False)
            entity_to_purge = g_set.get_entity(URIRef(res))
            entity_to_purge.mark_as_to_be_deleted()
        self.save(g_set, info_dir, supplier_prefix)
    
    def merge(self, res: URIRef, other: URIRef) -> None:
        info_dir = self.__get_info_dir(res)
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(self.base_iri, info_dir, supplier_prefix=supplier_prefix)
        self.reader.import_entity_from_triplestore(g_set, self.endpoint, res, self.resp_agent, enable_validation=False)
        self.reader.import_entity_from_triplestore(g_set, self.endpoint, other, self.resp_agent, enable_validation=False)
        sparql = SPARQLWrapper(endpoint=self.endpoint)
        query_other_as_obj = f'''
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?entity WHERE {{
                {{?entity ?p <{other}>}} UNION {{<{res}> ?p ?entity}} UNION {{<{other}> ?p ?entity}} 
                FILTER (?p != rdf:type) FILTER (?p != datacite:usesIdentifierScheme) FILTER (?p != pro:withRole)}}'''          
        sparql.setQuery(query_other_as_obj)
        sparql.setReturnFormat(JSON)
        data_obj = sparql.queryAndConvert()
        for data in data_obj["results"]["bindings"]:
            if data['entity']['type'] == 'uri':
                res_other_as_obj = URIRef(data["entity"]["value"])
                try:
                    self.reader.import_entity_from_triplestore(g_set, self.endpoint, res_other_as_obj, self.resp_agent, enable_validation=False)
                except ValueError:
                    print(res_other_as_obj)
                    raise(ValueError)
        res_as_entity = g_set.get_entity(res)
        other_as_entity = g_set.get_entity(other)
        res_as_entity.merge(other_as_entity)
        self.save(g_set, info_dir, supplier_prefix)
    
    def sync_rdf_with_triplestore(self, res: str, source_uri: str = None) -> bool:
        info_dir = self.__get_info_dir(res)
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(self.base_iri, info_dir, supplier_prefix=supplier_prefix)
        try:
            self.reader.import_entity_from_triplestore(g_set, self.endpoint, res, self.resp_agent, enable_validation=False)
            self.save(g_set, info_dir, supplier_prefix)
            return True
        except ValueError:
            try:
                self.reader.import_entity_from_triplestore(g_set, self.endpoint, source_uri, self.resp_agent, enable_validation=False)
            except ValueError:
                res_filepath = self.find_file(self.base_dir, self.dir_split, self.n_file_item, source_uri, self.zip_output_rdf)
                if not res_filepath:
                    return False
                imported_graph = self.reader.load(res_filepath)
                self.reader.import_entities_from_graph(g_set, imported_graph, self.resp_agent)
                res_entity = g_set.get_entity(URIRef(source_uri))
                if res_entity:
                    for res, entity in g_set.res_to_entity.items():
                        triples_list = list(entity.g.triples((URIRef(source_uri), None, None)))
                        for triple in triples_list:
                            entity.g.remove(triple)
                    self.save(g_set, info_dir, supplier_prefix)
                return False
            
    def save(self, g_set: GraphSet, info_dir: str, supplier_prefix: str):
        provset = ProvSet(g_set, self.base_iri, info_dir, wanted_label=False, supplier_prefix=supplier_prefix)
        provset.generate_provenance()
        graph_storer = Storer(g_set, dir_split=self.dir_split, n_file_item=self.n_file_item, zip_output=self.zip_output_rdf)
        prov_storer = Storer(provset, dir_split=self.dir_split, n_file_item=self.n_file_item, zip_output=self.zip_output_rdf)
        graph_storer.store_all(self.base_dir, self.base_iri)
        prov_storer.store_all(self.base_dir, self.base_iri)
        graph_storer.upload_all(self.endpoint)
        g_set.commit_changes()
    
    def __get_info_dir(self, uri: str):
        supplier_prefix = self.__get_supplier_prefix(uri)
        return os.path.join(self.info_dir, supplier_prefix, 'creator') + os.sep if supplier_prefix != '060' else os.path.join(self.info_dir, 'creator') + os.sep
    
    def __get_supplier_prefix(self, uri: str) -> str:
        entity_regex: str = r'^(.+)/([a-z][a-z])/(0[1-9]+0)?([1-9][0-9]*)$'
        entity_match = re.match(entity_regex, uri)
        return entity_match.group(3)

    def find_file(self, rdf_dir: str, dir_split_number: str, items_per_file: str, uri: str, zip_output_rdf: bool) -> str|None:
        entity_regex: str = r'^(https:\/\/w3id\.org\/oc\/meta)\/([a-z][a-z])\/(0[1-9]+0)?([1-9][0-9]*)$'
        entity_match = re.match(entity_regex, uri)
        if entity_match:
            cur_number = int(entity_match.group(4))
            cur_file_split: int = 0
            while True:
                if cur_number > cur_file_split:
                    cur_file_split += items_per_file
                else:
                    break
            cur_split: int = 0
            while True:
                if cur_number > cur_split:
                    cur_split += dir_split_number
                else:
                    break
            short_name = entity_match.group(2)
            sub_folder = entity_match.group(3)
            cur_dir_path = os.path.join(rdf_dir, short_name, sub_folder, str(cur_split))
            extension = '.zip' if zip_output_rdf else '.json'
            cur_file_path = os.path.join(cur_dir_path, str(cur_file_split)) + extension
            return cur_file_path

    def infer_type_from_uri(self, uri: str) -> str:
        if os.path.join(self.base_iri, 'br') in uri:
            return GraphEntity.iri_expression
        elif os.path.join(self.base_iri, 'ar') in uri:
            return GraphEntity.iri_role_in_time
        elif os.path.join(self.base_iri, 'ra') in uri:
            return GraphEntity.iri_agent
        elif os.path.join(self.base_iri, 're') in uri:
            return GraphEntity.iri_manifestation
        elif os.path.join(self.base_iri, 'id') in uri:
            return GraphEntity.iri_identifier
        return None

    def add_entity_with_type(self, g_set: GraphSet, res: str, entity_type: str, preexisting_graph: ConjunctiveGraph):
        subject = URIRef(res)
        if entity_type == GraphEntity.iri_expression:
            g_set.add_br(resp_agent=self.resp_agent, res=subject, preexisting_graph=preexisting_graph)
        elif entity_type == GraphEntity.iri_role_in_time:
            g_set.add_ar(resp_agent=self.resp_agent, res=subject, preexisting_graph=preexisting_graph)
        elif entity_type == GraphEntity.iri_agent:
            g_set.add_ra(resp_agent=self.resp_agent, res=subject, preexisting_graph=preexisting_graph)
        elif entity_type == GraphEntity.iri_manifestation:
            g_set.add_re(resp_agent=self.resp_agent, res=subject, preexisting_graph=preexisting_graph)
        elif entity_type == GraphEntity.iri_identifier:
            g_set.add_id(resp_agent=self.resp_agent, res=subject, preexisting_graph=preexisting_graph)