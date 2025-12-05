#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
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
from typing import Set

import validators
import yaml
from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.graph.graph_entity import GraphEntity
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader
from oc_ocdm.support.support import build_graph_from_results
from rdflib import RDF, Graph, URIRef
from sparqlite import SPARQLClient


class EntityCache:
    def __init__(self):
        self.cache: Set[URIRef] = set()

    def add(self, entity: URIRef) -> None:
        """Add an entity to the cache"""
        self.cache.add(entity)

    def is_cached(self, entity: URIRef) -> bool:
        """Check if an entity is in the cache"""
        return entity in self.cache

    def clear(self) -> None:
        """Clear all cached entities"""
        self.cache.clear()


class MetaEditor:
    property_to_remove_method = {
        "http://purl.org/spar/datacite/hasIdentifier": "remove_identifier",
        "http://purl.org/spar/pro/isHeldBy": "remove_is_held_by",
        "http://purl.org/vocab/frbr/core#embodiment": "remove_format",
        "http://purl.org/spar/pro/isDocumentContextFor": "remove_is_held_by",
        "https://w3id.org/oc/ontology/hasNext": "remove_next",
    }

    def __init__(self, meta_config: str, resp_agent: str, save_queries: bool = False):
        with open(meta_config, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        self.endpoint = settings["triplestore_url"]
        self.provenance_endpoint = settings["provenance_triplestore_url"]
        output_dir = settings.get("base_output_dir")
        self.data_hotfix_dir = os.path.join(output_dir, "to_be_uploaded_hotfix")
        self.prov_hotfix_dir = os.path.join(output_dir, "to_be_uploaded_hotfix")
        self.base_dir = os.path.join(output_dir, "rdf") + os.sep
        self.base_iri = settings["base_iri"]
        self.resp_agent = resp_agent
        self.dir_split = settings["dir_split_number"]
        self.n_file_item = settings["items_per_file"]
        self.zip_output_rdf = settings["zip_output_rdf"]
        self.generate_rdf_files = settings.get("generate_rdf_files", True)
        self.reader = Reader()
        self.save_queries = save_queries
        self.update_queries = []

        # Redis configuration
        self.redis_host = settings.get("redis_host", "localhost")
        self.redis_port = settings.get("redis_port", 6379)
        self.redis_db = settings.get("redis_db", 5)
        self.counter_handler = RedisCounterHandler(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )

        self.entity_cache = EntityCache()
        self.relationship_cache = {}

    def update_property(
        self, res: URIRef, property: str, new_value: str | URIRef
    ) -> None:
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(
            self.base_iri,
            supplier_prefix=supplier_prefix,
            custom_counter_handler=self.counter_handler,
        )
        self.reader.import_entity_from_triplestore(
            g_set, self.endpoint, res, self.resp_agent, enable_validation=False
        )
        if validators.url(new_value):
            new_value = URIRef(new_value)
            self.reader.import_entity_from_triplestore(
                g_set,
                self.endpoint,
                new_value,
                self.resp_agent,
                enable_validation=False,
            )
            getattr(g_set.get_entity(res), property)(g_set.get_entity(new_value))
        else:
            getattr(g_set.get_entity(res), property)(new_value)
        self.save(g_set, supplier_prefix)

    def delete(self, res: str, property: str = None, object: str = None) -> None:
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(
            self.base_iri,
            supplier_prefix=supplier_prefix,
            custom_counter_handler=self.counter_handler,
        )
        try:
            self.reader.import_entity_from_triplestore(
                g_set, self.endpoint, res, self.resp_agent, enable_validation=False
            )
        except ValueError as e:
            print(f"ValueError for entity {res}: {e}")
            inferred_type = self.infer_type_from_uri(res)
            if inferred_type:
                print(f"Inferred type {inferred_type} for entity {res}")
                query: str = (
                    f"SELECT ?s ?p ?o WHERE {{BIND (<{res}> AS ?s). ?s ?p ?o.}}"
                )
                with SPARQLClient(self.endpoint, max_retries=3, backoff_factor=0.3) as client:
                    result = client.query(query)["results"]["bindings"]
                preexisting_graph: Graph = build_graph_from_results(result)
                self.add_entity_with_type(g_set, res, inferred_type, preexisting_graph)
            else:
                return
        if not g_set.get_entity(URIRef(res)):
            return
        if property:
            remove_method = (
                self.property_to_remove_method[property]
                if property in self.property_to_remove_method
                else (
                    property.replace("has", "remove")
                    if property.startswith("has")
                    else f"remove_{property}"
                )
            )
            if object:
                if validators.url(object):
                    self.reader.import_entity_from_triplestore(
                        g_set,
                        self.endpoint,
                        object,
                        self.resp_agent,
                        enable_validation=False,
                    )
                    # try:
                    getattr(g_set.get_entity(URIRef(res)), remove_method)(
                        g_set.get_entity(URIRef(object))
                    )
                    # TypeError: AgentRole.remove_is_held_by() takes 1 positional argument but 2 were given
                    # except TypeError:
                    #     getattr(g_set.get_entity(URIRef(res)), remove_method)()
                else:
                    getattr(g_set.get_entity(URIRef(res)), remove_method)(object)
            else:
                getattr(g_set.get_entity(URIRef(res)), remove_method)()
        else:
            query = f"SELECT ?s WHERE {{?s ?p <{res}>.}}"
            with SPARQLClient(self.endpoint, max_retries=3, backoff_factor=0.3) as client:
                result = client.query(query)
            for entity in result["results"]["bindings"]:
                self.reader.import_entity_from_triplestore(
                    g_set,
                    self.endpoint,
                    URIRef(entity["s"]["value"]),
                    self.resp_agent,
                    enable_validation=False,
                )
            entity_to_purge = g_set.get_entity(URIRef(res))
            entity_to_purge.mark_as_to_be_deleted()
        self.save(g_set, supplier_prefix)

    def merge(self, g_set: GraphSet, res: URIRef, other: URIRef) -> None:
        """
        Merge two entities and their related entities using batch import with caching.

        Args:
            g_set: The GraphSet containing the entities
            res: The main entity that will absorb the other
            other: The entity to be merged into the main one
        """
        # First get all related entities with a single SPARQL query
        related_entities = set()
        with SPARQLClient(self.endpoint, max_retries=5, backoff_factor=0.3) as client:
            if other in self.relationship_cache:
                related_entities.update(self.relationship_cache[other])
            else:
                query = f"""
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX datacite: <http://purl.org/spar/datacite/>
                    PREFIX pro: <http://purl.org/spar/pro/>
                    SELECT DISTINCT ?entity WHERE {{
                        {{?entity ?p <{other}>}} UNION
                        {{<{other}> ?p ?entity}}
                        FILTER (?p != rdf:type)
                        FILTER (?p != datacite:usesIdentifierScheme)
                        FILTER (?p != pro:withRole)
                    }}"""

                data = client.query(query)
                other_related = {
                    URIRef(result["entity"]["value"])
                    for result in data["results"]["bindings"]
                    if result["entity"]["type"] == "uri"
                }

                self.relationship_cache[other] = other_related
                related_entities.update(other_related)
            if res in self.relationship_cache:
                related_entities.update(self.relationship_cache[res])
            else:
                # Query only for objects of the surviving entity if not in cache
                query = f"""
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX datacite: <http://purl.org/spar/datacite/>
                    PREFIX pro: <http://purl.org/spar/pro/>
                    SELECT DISTINCT ?entity WHERE {{
                        <{res}> ?p ?entity
                        FILTER (?p != rdf:type)
                        FILTER (?p != datacite:usesIdentifierScheme)
                        FILTER (?p != pro:withRole)
                    }}"""

                data = client.query(query)
                res_related = {
                    URIRef(result["entity"]["value"])
                    for result in data["results"]["bindings"]
                    if result["entity"]["type"] == "uri"
                }

                self.relationship_cache[res] = res_related
                related_entities.update(res_related)

        entities_to_import = set([res, other])
        entities_to_import.update(related_entities)
        entities_to_import = {
            e for e in entities_to_import if not self.entity_cache.is_cached(e)
        }
        # Import only non-cached entities if there are any
        if entities_to_import:
            try:
                self.reader.import_entities_from_triplestore(
                    g_set=g_set,
                    ts_url=self.endpoint,
                    entities=list(entities_to_import),
                    resp_agent=self.resp_agent,
                    enable_validation=False,
                    batch_size=10,
                )

                # Add newly imported entities to cache
                for entity in entities_to_import:
                    self.entity_cache.add(entity)

            except ValueError as e:
                print(f"Error importing entities: {e}")
                return

        # Perform the merge
        res_as_entity = g_set.get_entity(res)
        other_as_entity = g_set.get_entity(other)

        is_both_expression = all(
            GraphEntity.iri_expression in entity.g.objects(entity.res, RDF.type)
            for entity in [res_as_entity, other_as_entity]
        )

        if is_both_expression:
            res_as_entity.merge(other_as_entity, prefer_self=True)
        else:
            res_as_entity.merge(other_as_entity)

    def sync_rdf_with_triplestore(self, res: str, source_uri: str = None) -> bool:
        supplier_prefix = self.__get_supplier_prefix(res)
        g_set = GraphSet(
            self.base_iri,
            supplier_prefix=supplier_prefix,
            custom_counter_handler=self.counter_handler,
        )
        try:
            self.reader.import_entity_from_triplestore(
                g_set, self.endpoint, res, self.resp_agent, enable_validation=False
            )
            self.save(g_set, supplier_prefix)
            return True
        except ValueError:
            try:
                self.reader.import_entity_from_triplestore(
                    g_set,
                    self.endpoint,
                    source_uri,
                    self.resp_agent,
                    enable_validation=False,
                )
            except ValueError:
                res_filepath = self.find_file(
                    self.base_dir,
                    self.dir_split,
                    self.n_file_item,
                    source_uri,
                    self.zip_output_rdf,
                )
                if not res_filepath:
                    return False
                imported_graph = self.reader.load(res_filepath)
                self.reader.import_entities_from_graph(
                    g_set, imported_graph, self.resp_agent
                )
                res_entity = g_set.get_entity(URIRef(source_uri))
                if res_entity:
                    for res, entity in g_set.res_to_entity.items():
                        triples_list = list(
                            entity.g.triples((URIRef(source_uri), None, None))
                        )
                        for triple in triples_list:
                            entity.g.remove(triple)
                    self.save(g_set, supplier_prefix)
                return False

    def save(self, g_set: GraphSet, supplier_prefix: str = "") -> None:        
        provset = ProvSet(
            g_set,
            self.base_iri,
            wanted_label=False,
            supplier_prefix=supplier_prefix,
            custom_counter_handler=self.counter_handler,
        )
        provset.generate_provenance()
        graph_storer = Storer(
            g_set,
            dir_split=self.dir_split,
            n_file_item=self.n_file_item,
            zip_output=self.zip_output_rdf,
        )
        prov_storer = Storer(
            provset,
            dir_split=self.dir_split,
            n_file_item=self.n_file_item,
            zip_output=self.zip_output_rdf,
        )
        
        if self.generate_rdf_files:
            graph_storer.store_all(self.base_dir, self.base_iri)
            prov_storer.store_all(self.base_dir, self.base_iri)

        graph_storer.upload_all(
            self.endpoint, base_dir=self.data_hotfix_dir, save_queries=self.save_queries
        )
        prov_storer.upload_all(
            self.provenance_endpoint, base_dir=self.prov_hotfix_dir, save_queries=self.save_queries
        )
        g_set.commit_changes()

    def __get_supplier_prefix(self, uri: str) -> str:
        entity_regex: str = r"^(.+)/([a-z][a-z])/(0[1-9]+0)?([1-9][0-9]*)$"
        entity_match = re.match(entity_regex, uri)
        return entity_match.group(3)

    def find_file(
        self,
        rdf_dir: str,
        dir_split_number: str,
        items_per_file: str,
        uri: str,
        zip_output_rdf: bool,
    ) -> str | None:
        entity_regex: str = (
            r"^(https:\/\/w3id\.org\/oc\/meta)\/([a-z][a-z])\/(0[1-9]+0)?([1-9][0-9]*)$"
        )
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
            extension = ".zip" if zip_output_rdf else ".json"
            cur_file_path = os.path.join(cur_dir_path, str(cur_file_split)) + extension
            return cur_file_path

    def infer_type_from_uri(self, uri: str) -> str:
        if os.path.join(self.base_iri, "br") in uri:
            return GraphEntity.iri_expression
        elif os.path.join(self.base_iri, "ar") in uri:
            return GraphEntity.iri_role_in_time
        elif os.path.join(self.base_iri, "ra") in uri:
            return GraphEntity.iri_agent
        elif os.path.join(self.base_iri, "re") in uri:
            return GraphEntity.iri_manifestation
        elif os.path.join(self.base_iri, "id") in uri:
            return GraphEntity.iri_identifier
        return None

    def add_entity_with_type(
        self,
        g_set: GraphSet,
        res: str,
        entity_type: str,
        preexisting_graph: Graph,
    ):
        subject = URIRef(res)
        if entity_type == GraphEntity.iri_expression:
            g_set.add_br(
                resp_agent=self.resp_agent,
                res=subject,
                preexisting_graph=preexisting_graph,
            )
        elif entity_type == GraphEntity.iri_role_in_time:
            g_set.add_ar(
                resp_agent=self.resp_agent,
                res=subject,
                preexisting_graph=preexisting_graph,
            )
        elif entity_type == GraphEntity.iri_agent:
            g_set.add_ra(
                resp_agent=self.resp_agent,
                res=subject,
                preexisting_graph=preexisting_graph,
            )
        elif entity_type == GraphEntity.iri_manifestation:
            g_set.add_re(
                resp_agent=self.resp_agent,
                res=subject,
                preexisting_graph=preexisting_graph,
            )
        elif entity_type == GraphEntity.iri_identifier:
            g_set.add_id(
                resp_agent=self.resp_agent,
                res=subject,
                preexisting_graph=preexisting_graph,
            )
