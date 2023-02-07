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
from SPARQLWrapper import JSON, SPARQLWrapper


class MetaEditor:
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
        self.save(g_set, info_dir)
    
    def delete_property(self, res: str, property: str, object: str = None) -> None:
        info_dir = self.__get_info_dir(res)
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(self.base_iri, info_dir, supplier_prefix=supplier_prefix)
        self.reader.import_entity_from_triplestore(g_set, self.endpoint, res, self.resp_agent, enable_validation=False)
        remove_method = property.replace('has', 'remove')
        if object:
            if validators.url(object):
                self.reader.import_entity_from_triplestore(g_set, self.endpoint, object, self.resp_agent, enable_validation=False)
                getattr(g_set.get_entity(URIRef(res)), remove_method)(g_set.get_entity(URIRef(object)))
            else:
                getattr(g_set.get_entity(URIRef(res)), remove_method)(object)
        else:
            getattr(g_set.get_entity(URIRef(res)), remove_method)()
        self.save(g_set, info_dir)

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
            SELECT DISTINCT ?entity WHERE {{
                {{?entity ?p <{other}>}} UNION {{<{res}> ?p ?entity}} UNION {{<{other}> ?p ?entity}} 
                FILTER (?p != rdf:type) FILTER (?p != datacite:usesIdentifierScheme)}}'''          
        sparql.setQuery(query_other_as_obj)
        sparql.setReturnFormat(JSON)
        data_obj = sparql.queryAndConvert()
        for data in data_obj["results"]["bindings"]:
            if data['entity']['type'] == 'uri':
                res_other_as_obj = URIRef(data["entity"]["value"])
                self.reader.import_entity_from_triplestore(g_set, self.endpoint, res_other_as_obj, self.resp_agent, enable_validation=False)
        res_as_entity = g_set.get_entity(res)
        other_as_entity = g_set.get_entity(other)
        res_as_entity.merge(other_as_entity)
        self.save(g_set, info_dir)
    
    def sync_rdf_with_triplestore(self, res: str) -> None:
        info_dir = self.__get_info_dir(res)
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(self.base_iri, info_dir, supplier_prefix=supplier_prefix)
        self.reader.import_entity_from_triplestore(g_set, self.endpoint, res, self.resp_agent, enable_validation=False)
        self.save(g_set, info_dir)
    
    def save(self, g_set: GraphSet, info_dir: str):
        provset = ProvSet(g_set, self.base_iri, info_dir, wanted_label=False)
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
