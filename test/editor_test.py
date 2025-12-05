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

import json
import os
import unittest
from shutil import rmtree
from test.test_utils import (PROV_SERVER, REDIS_CACHE_DB, REDIS_DB, REDIS_HOST,
                             REDIS_PORT, SERVER, reset_redis_counters,
                             reset_server)

import yaml
from oc_meta.plugins.editor import EntityCache, MetaEditor
from oc_meta.run.meta_process import run_meta_process
from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader
from rdflib import URIRef
from sparqlite import SPARQLClient

BASE = os.path.join("test", "editor")
OUTPUT = os.path.join(BASE, "output")
META_CONFIG = os.path.join(BASE, "meta_config.yaml")


def get_counter_handler():
    return RedisCounterHandler(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


class TestEditor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.counter_handler = get_counter_handler()
        cls.original_generate_rdf_files = None

    def setUp(self):
        reset_server()
        reset_redis_counters()
        if os.path.exists(OUTPUT):
            rmtree(OUTPUT)

        # Create temporary directory for cache files
        self.temp_dir = os.path.join("test", "temp_editor_test")
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        os.makedirs(self.temp_dir)

        # Setup cache files
        self.cache_file = os.path.join(self.temp_dir, "ts_upload_cache.json")
        self.failed_file = os.path.join(self.temp_dir, "failed_queries.txt")
        self.stop_file = os.path.join(self.temp_dir, ".stop_upload")
        
        # Create separate directories for data and provenance update queries
        self.data_update_dir = os.path.join(self.temp_dir, "to_be_uploaded_data")
        self.prov_update_dir = os.path.join(self.temp_dir, "to_be_uploaded_prov")
        os.makedirs(self.data_update_dir, exist_ok=True)
        os.makedirs(self.prov_update_dir, exist_ok=True)

        with open(META_CONFIG, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        # Update settings to use Redis and cache files
        settings.update(
            {
                "redis_host": REDIS_HOST,
                "redis_port": REDIS_PORT,
                "redis_db": REDIS_DB,
                "redis_cache_db": REDIS_CACHE_DB,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
                "triplestore_url": SERVER,
                "provenance_triplestore_url": PROV_SERVER,
                "data_update_dir": self.data_update_dir,
                "prov_update_dir": self.prov_update_dir
            }
        )
        run_meta_process(settings=settings, meta_config_path=META_CONFIG)

    def tearDown(self):
        if os.path.exists(OUTPUT):
            rmtree(OUTPUT)
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        reset_redis_counters()
        
        if self.original_generate_rdf_files is not None:
            with open(META_CONFIG, encoding="utf-8") as file:
                settings = yaml.full_load(file)
            settings["generate_rdf_files"] = self.original_generate_rdf_files
            with open(META_CONFIG, "w", encoding="utf-8") as file:
                yaml.dump(settings, file)
            self.original_generate_rdf_files = None

    def test_update_property(self):
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")
        editor.update_property(
            URIRef("https://w3id.org/oc/meta/ar/0601"),
            "has_next",
            URIRef("https://w3id.org/oc/meta/ar/0604"),
        )
        editor.update_property(
            URIRef("https://w3id.org/oc/meta/ar/0604"),
            "has_next",
            URIRef("https://w3id.org/oc/meta/ar/0603"),
        )
        editor.update_property(
            URIRef("https://w3id.org/oc/meta/ar/0603"),
            "has_next",
            URIRef("https://w3id.org/oc/meta/ar/0602"),
        )
        editor.update_property(
            URIRef("https://w3id.org/oc/meta/ar/0602"),
            "has_next",
            URIRef("https://w3id.org/oc/meta/ar/0605"),
        )
        
        with SPARQLClient(SERVER) as client:
            result = client.query("""
            ASK {
                GRAPH <https://w3id.org/oc/meta/ar/> {
                    <https://w3id.org/oc/meta/ar/0601> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0604> .
                }
            }
            """)
            self.assertTrue(result["boolean"], "AR/0601 → AR/0604 relationship not found in triplestore")

            result = client.query("""
            ASK {
                GRAPH <https://w3id.org/oc/meta/ar/> {
                    <https://w3id.org/oc/meta/ar/0604> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0603> .
                }
            }
            """)
            self.assertTrue(result["boolean"], "AR/0604 → AR/0603 relationship not found in triplestore")

            result = client.query("""
            ASK {
                GRAPH <https://w3id.org/oc/meta/ar/> {
                    <https://w3id.org/oc/meta/ar/0603> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0602> .
                }
            }
            """)
            self.assertTrue(result["boolean"], "AR/0603 → AR/0602 relationship not found in triplestore")

            result = client.query("""
            ASK {
                GRAPH <https://w3id.org/oc/meta/ar/> {
                    <https://w3id.org/oc/meta/ar/0602> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0605> .
                }
            }
            """)
            self.assertTrue(result["boolean"], "AR/0602 → AR/0605 relationship not found in triplestore")

        with SPARQLClient(PROV_SERVER) as client:
            prov_result = client.query("""
            ASK {
                ?s <http://www.w3.org/ns/prov#specializationOf> <https://w3id.org/oc/meta/ar/0601> ;
                   <http://www.w3.org/ns/prov#generatedAtTime> ?time .
            }
            """)
            self.assertTrue(prov_result["boolean"], "Provenance for AR/0601 not found in triplestore")
        
        with open(
            os.path.join(OUTPUT, "rdf", "ar", "060", "10000", "1000.json"),
            "r",
            encoding="utf-8",
        ) as file:
            ar_data = json.load(file)
            for graph in ar_data:
                graph_data = graph["@graph"]
                for ar in graph_data:
                    if ar["@id"] == "https://w3id.org/oc/meta/ar/0601":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasNext"][0]["@id"],
                            "https://w3id.org/oc/meta/ar/0604",
                        )
                    elif ar["@id"] == "https://w3id.org/oc/meta/ar/0603":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasNext"][0]["@id"],
                            "https://w3id.org/oc/meta/ar/0602",
                        )
                    elif ar["@id"] == "https://w3id.org/oc/meta/ar/0604":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasNext"][0]["@id"],
                            "https://w3id.org/oc/meta/ar/0603",
                        )
                    elif ar["@id"] == "https://w3id.org/oc/meta/ar/0602":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasNext"][0]["@id"],
                            "https://w3id.org/oc/meta/ar/0605",
                        )
        with open(
            os.path.join(
                OUTPUT, "rdf", "ar", "060", "10000", "1000", "prov", "se.json"
            ),
            "r",
            encoding="utf8",
        ) as f:
            ar_prov = json.load(f)
            for graph in ar_prov:
                graph_prov = graph["@graph"]
                for ar in graph_prov:
                    if ar["@id"] == "https://w3id.org/oc/meta/ar/0601/prov/se/2":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                                "@value"
                            ],
                            "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0601> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0602> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0601> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0604> . } }",
                        )
                    if ar["@id"] == "https://w3id.org/oc/meta/ar/0603/prov/se/2":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                                "@value"
                            ],
                            "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0603> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0604> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0603> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0602> . } }",
                        )
                    if ar["@id"] == "https://w3id.org/oc/meta/ar/0604/prov/se/2":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                                "@value"
                            ],
                            "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0604> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0605> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0604> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0603> . } }",
                        )
                    if ar["@id"] == "https://w3id.org/oc/meta/ar/0602/prov/se/2":
                        self.assertEqual(
                            ar["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                                "@value"
                            ],
                            "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0602> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0603> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0602> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0605> . } }",
                        )

    def test_delete_property(self):
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")
        editor.delete(URIRef("https://w3id.org/oc/meta/br/0601"), "has_title")
        with open(
            os.path.join(OUTPUT, "rdf", "br", "060", "10000", "1000.json"),
            "r",
            encoding="utf8",
        ) as f:
            br_data = json.load(f)
            for graph in br_data:
                graph_data = graph["@graph"]
                for br in graph_data:
                    if br["@id"] == "https://w3id.org/oc/meta/br/0601":
                        self.assertFalse("http://purl.org/dc/terms/title" in br)
        with open(
            os.path.join(
                OUTPUT, "rdf", "br", "060", "10000", "1000", "prov", "se.json"
            ),
            "r",
            encoding="utf8",
        ) as f:
            br_prov = json.load(f)
            for graph in br_prov:
                graph_prov = graph["@graph"]
                for br in graph_prov:
                    if br["@id"] == "https://w3id.org/oc/meta/br/0601/prov/se/2":
                        self.assertEqual(
                            br["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                                "@value"
                            ],
                            'DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { <https://w3id.org/oc/meta/br/0601> <http://purl.org/dc/terms/title> "A Review Of Hemolytic Uremic Syndrome In Patients Treated With Gemcitabine Therapy"^^<http://www.w3.org/2001/XMLSchema#string> . } }',
                        )

    def test_delete_entity(self):
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")
        editor.delete(URIRef("https://w3id.org/oc/meta/id/0601"))
        with open(
            os.path.join(OUTPUT, "rdf", "id", "060", "10000", "1000.json"),
            "r",
            encoding="utf8",
        ) as f:
            id_data = json.load(f)
            for graph in id_data:
                graph_data = graph["@graph"]
                for identifier in graph_data:
                    if identifier["@id"] == "https://w3id.org/oc/meta/id/0601":
                        self.fail()
        with open(
            os.path.join(
                OUTPUT, "rdf", "id", "060", "10000", "1000", "prov", "se.json"
            ),
            "r",
            encoding="utf8",
        ) as f:
            id_prov = json.load(f)
            for graph in id_prov:
                graph_prov = graph["@graph"]
                for identifier in graph_prov:
                    if (
                        identifier["@id"]
                        == "https://w3id.org/oc/meta/id/0601/prov/se/2"
                    ):
                        update_query = (
                            identifier["https://w3id.org/oc/ontology/hasUpdateQuery"][
                                0
                            ]["@value"]
                            .replace(
                                "DELETE DATA { GRAPH <https://w3id.org/oc/meta/id/> { ",
                                "",
                            )
                            .replace(" . } }", "")
                            .replace("\n", "")
                            .split(" .")
                        )
                        self.assertEqual(
                            set(update_query),
                            {
                                "<https://w3id.org/oc/meta/id/0601> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/doi>",
                                "<https://w3id.org/oc/meta/id/0601> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>",
                                '<https://w3id.org/oc/meta/id/0601> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1002/(sici)1097-0142(19990501)85:9<2023::aid-cncr21>3.0.co;2-2"^^<http://www.w3.org/2001/XMLSchema#string>',
                            },
                        )
        with open(
            os.path.join(
                OUTPUT, "rdf", "br", "060", "10000", "1000", "prov", "se.json"
            ),
            "r",
            encoding="utf8",
        ) as f:
            ra_prov = json.load(f)
            for graph in ra_prov:
                graph_prov = graph["@graph"]
                for ra in graph_prov:
                    if ra["@id"] == "https://w3id.org/oc/meta/br/0601/prov/se/2":
                        self.assertEqual(
                            ra["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                                "@value"
                            ],
                            "DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { <https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0601> . } }",
                        )

    def test_merge(self):
        base_iri = "https://w3id.org/oc/meta/"
        resp_agent = "https://orcid.org/0000-0002-8420-0696"
        g_set = GraphSet(
            base_iri,
            supplier_prefix="060",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        endpoint = "http://127.0.0.1:8805/sparql"

        # Create entities  testing
        ra = g_set.add_ra(
            resp_agent=resp_agent, res=URIRef("https://w3id.org/oc/meta/ra/06010")
        )
        ra.has_name("Wiley")

        reader = Reader()
        id_0605 = reader.import_entity_from_triplestore(
            g_set,
            endpoint,
            URIRef("https://w3id.org/oc/meta/id/0605"),
            resp_agent,
            enable_validation=False,
        )
        id_0609 = g_set.add_id(resp_agent=resp_agent)
        id_0609.create_crossref("313")

        ra.has_identifier(id_0605)
        ra.has_identifier(id_0609)

        # Generate provenance
        provset = ProvSet(
            g_set,
            base_iri,
            wanted_label=False,
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )
        provset.generate_provenance()

        # Store and upload data
        rdf_dir = os.path.join(OUTPUT, "rdf") + os.sep
        graph_storer = Storer(
            g_set, dir_split=10000, n_file_item=1000, zip_output=False
        )
        prov_storer = Storer(
            provset, dir_split=10000, n_file_item=1000, zip_output=False
        )

        graph_storer.store_all(rdf_dir, base_iri)
        prov_storer.store_all(rdf_dir, base_iri)
        graph_storer.upload_all(endpoint)
        g_set.commit_changes()

        # Perform merge
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")
        editor.merge(
            g_set,
            URIRef("https://w3id.org/oc/meta/ra/0607"),
            URIRef("https://w3id.org/oc/meta/ra/06010"),
        )
        editor.save(g_set)

        # Check Redis counters
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=1, supplier_prefix="060"
            ),
            1,
        )
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=2, supplier_prefix="060"
            ),
            1,
        )
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=3, supplier_prefix="060"
            ),
            1,
        )
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=4, supplier_prefix="060"
            ),
            1,
        )
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=5, supplier_prefix="060"
            ),
            1,
        )
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=6, supplier_prefix="060"
            ),
            1,
        )
        self.assertEqual(
            self.counter_handler.read_counter(
                "ra", prov_short_name="se", identifier=7, supplier_prefix="060"
            ),
            2,
        )

        # Verify merged data
        for filepath in [
            os.path.join(OUTPUT, "rdf", "ra", "060", "10000", "1000.json"),
            os.path.join(
                OUTPUT, "rdf", "ra", "060", "10000", "1000", "prov", "se.json"
            ),
        ]:
            with open(filepath, "r", encoding="utf8") as f:
                data = json.load(f)
                for graph in data:
                    graph_data = graph["@graph"]
                    for entity in graph_data:
                        if entity["@id"] == "https://w3id.org/oc/meta/ra/0607":
                            identifiers = {
                                identifier["@id"]
                                for identifier in entity[
                                    "http://purl.org/spar/datacite/hasIdentifier"
                                ]
                            }
                            self.assertEqual(
                                identifiers,
                                {
                                    str(id_0605.res),
                                    str(id_0609.res),
                                },
                            )
                        elif entity["@id"] == "https://w3id.org/oc/meta/ra/06010":
                            self.fail()
                        # elif entity['@id'] == 'https://w3id.org/oc/meta/ar/06010':
                        #     self.assertEqual(entity['http://purl.org/spar/pro/isHeldBy'][0]['@id'], 'https://w3id.org/oc/meta/ra/0607')
                        elif entity["@id"] in {
                            "https://w3id.org/oc/meta/ra/0607/prov/se/1",
                            "https://w3id.org/oc/meta/ra/06010/prov/se/1",
                        }:
                            self.assertTrue(
                                "http://www.w3.org/ns/prov#invalidatedAtTime" in entity
                            )
                        elif (
                            entity["@id"]
                            == "https://w3id.org/oc/meta/ra/0607/prov/se/3"
                        ):
                            self.assertEqual(
                                entity["http://purl.org/dc/terms/description"][0][
                                    "@value"
                                ],
                                "The entity 'https://w3id.org/oc/meta/ra/0607' has been merged with 'https://w3id.org/oc/meta/ra/06010'.",
                            )
                            self.assertEqual(
                                entity["https://w3id.org/oc/ontology/hasUpdateQuery"][
                                    0
                                ]["@value"],
                                "INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/0607> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06011> . } }",
                            )
                        elif (
                            entity["@id"]
                            == "https://w3id.org/oc/meta/ra/06010/prov/se/2"
                        ):
                            update_query = (
                                entity["https://w3id.org/oc/ontology/hasUpdateQuery"][
                                    0
                                ]["@value"]
                                .replace(
                                    "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { ",
                                    "",
                                )
                                .replace(" . } }", "")
                                .replace("\n", "")
                                .split(" .")
                            )
                            self.assertEqual(
                                set(update_query),
                                {
                                    '<https://w3id.org/oc/meta/ra/06010> <http://xmlns.com/foaf/0.1/name> "Wiley"^^<http://www.w3.org/2001/XMLSchema#string>',
                                    f"<https://w3id.org/oc/meta/ra/06010> <http://purl.org/spar/datacite/hasIdentifier> <{id_0609.res}>",
                                    f"<https://w3id.org/oc/meta/ra/06010> <http://purl.org/spar/datacite/hasIdentifier> <{id_0605.res}>",
                                    "<https://w3id.org/oc/meta/ra/06010> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>",
                                },
                            )

    def test_delete_entity_with_inferred_type(self):
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")

        with SPARQLClient(SERVER) as client:
            # Remove the type from the entity
            delete_type_query = """
            DELETE {
                GRAPH <https://w3id.org/oc/meta/br/> {
                    <https://w3id.org/oc/meta/br/0605> a <http://purl.org/spar/fabio/Expression> .
                }
            }
            WHERE {
                GRAPH <https://w3id.org/oc/meta/br/> {
                    <https://w3id.org/oc/meta/br/0605> a <http://purl.org/spar/fabio/Expression> .
                }
            }
            """
            client.update(delete_type_query)

            # Ensure the entity exists before deletion
            select_query = """
            SELECT ?s WHERE {
                GRAPH <https://w3id.org/oc/meta/br/> {
                    ?s <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date> .
                }
            }
            """
            result = client.query(select_query)
            self.assertEqual(len(result["results"]["bindings"]), 1)

        # Perform deletion
        editor.delete(URIRef("https://w3id.org/oc/meta/br/0605"))

        # Ensure the entity is deleted
        with SPARQLClient(SERVER) as client:
            result = client.query(select_query)
            self.assertEqual(len(result["results"]["bindings"]), 0)

        # Verify provenance information
        prov_path = os.path.join(
            OUTPUT, "rdf", "br", "060", "10000", "1000", "prov", "se.json"
        )
        with open(prov_path, "r", encoding="utf8") as f:
            prov_data = json.load(f)
            br_0605_prov_se_2 = None
            br_0605_prov_se_1 = None
            for graph in prov_data:
                for entity in graph["@graph"]:
                    if entity["@id"] == "https://w3id.org/oc/meta/br/0605/prov/se/2":
                        br_0605_prov_se_2 = entity
                    if entity["@id"] == "https://w3id.org/oc/meta/br/0605/prov/se/1":
                        br_0605_prov_se_1 = entity

            self.assertIsNotNone(br_0605_prov_se_2)
            self.assertEqual(
                br_0605_prov_se_2["http://purl.org/dc/terms/description"][0]["@value"],
                "The entity 'https://w3id.org/oc/meta/br/0605' has been deleted.",
            )
            self.assertEqual(
                br_0605_prov_se_2["@type"][0], "http://www.w3.org/ns/prov#Entity"
            )
            self.assertEqual(
                br_0605_prov_se_2["http://www.w3.org/ns/prov#specializationOf"][0][
                    "@id"
                ],
                "https://w3id.org/oc/meta/br/0605",
            )
            self.assertEqual(
                br_0605_prov_se_2["http://www.w3.org/ns/prov#wasAttributedTo"][0][
                    "@id"
                ],
                "https://orcid.org/0000-0002-8420-0696",
            )
            self.assertIn(
                "http://www.w3.org/ns/prov#invalidatedAtTime", br_0605_prov_se_2
            )
            self.assertIn(
                "http://www.w3.org/ns/prov#generatedAtTime", br_0605_prov_se_2
            )
            self.assertEqual(
                len(br_0605_prov_se_1["http://www.w3.org/ns/prov#generatedAtTime"]), 1
            )
            self.assertIn(
                "https://w3id.org/oc/ontology/hasUpdateQuery", br_0605_prov_se_2
            )
            update_query_value = br_0605_prov_se_2[
                "https://w3id.org/oc/ontology/hasUpdateQuery"
            ][0]["@value"]
            update_query_triples = (
                update_query_value.replace(
                    "DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { ", ""
                )
                .replace(" } }", "")
                .strip()
            )
            actual_triples = set(
                triple.strip()
                for triple in update_query_triples.split(" .")
                if triple.strip()
            )
            expected_triples = {
                "<https://w3id.org/oc/meta/br/0605> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0606>",
                '<https://w3id.org/oc/meta/br/0605> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date>',
            }
            self.assertEqual(actual_triples, expected_triples)

            self.assertIsNotNone(br_0605_prov_se_1)
            self.assertEqual(
                br_0605_prov_se_1["http://purl.org/dc/terms/description"][0]["@value"],
                "The entity 'https://w3id.org/oc/meta/br/0605' has been created.",
            )
            self.assertEqual(
                br_0605_prov_se_1["@type"][0], "http://www.w3.org/ns/prov#Entity"
            )
            self.assertEqual(
                br_0605_prov_se_1["http://www.w3.org/ns/prov#specializationOf"][0][
                    "@id"
                ],
                "https://w3id.org/oc/meta/br/0605",
            )
            self.assertEqual(
                br_0605_prov_se_1["http://www.w3.org/ns/prov#wasAttributedTo"][0][
                    "@id"
                ],
                "https://w3id.org/oc/meta/prov/pa/1",
            )
            self.assertIn(
                "http://www.w3.org/ns/prov#generatedAtTime", br_0605_prov_se_1
            )
            self.assertEqual(
                len(br_0605_prov_se_1["http://www.w3.org/ns/prov#generatedAtTime"]), 1
            )
            self.assertEqual(
                len(br_0605_prov_se_2["http://www.w3.org/ns/prov#invalidatedAtTime"]),
                1,
            )
            self.assertIn(
                "http://www.w3.org/ns/prov#hadPrimarySource", br_0605_prov_se_1
            )

        # Reinsert the publication date
        sparql_update_query = """
        INSERT DATA {
            GRAPH <https://w3id.org/oc/meta/br/> {
                <https://w3id.org/oc/meta/br/0605> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date> .
            }
        }
        """
        with SPARQLClient(SERVER) as client:
            client.update(sparql_update_query)

        # Perform deletion again
        editor.delete(URIRef("https://w3id.org/oc/meta/br/0605"))

        # Verify and print the provenance graph for the entity
        prov_path = os.path.join(
            OUTPUT, "rdf", "br", "060", "10000", "1000", "prov", "se.json"
        )
        with open(prov_path, "r", encoding="utf8") as f:
            prov_data = json.load(f)
            for graph in prov_data:
                for entity in graph["@graph"]:
                    if "https://w3id.org/oc/meta/br/0605" in entity["@id"]:
                        if (
                            entity["@id"]
                            == "https://w3id.org/oc/meta/br/0605/prov/se/1"
                        ):
                            self.assertEqual(
                                len(
                                    entity["http://www.w3.org/ns/prov#generatedAtTime"]
                                ),
                                1,
                            )
                            self.assertEqual(
                                len(
                                    entity[
                                        "http://www.w3.org/ns/prov#invalidatedAtTime"
                                    ]
                                ),
                                1,
                            )
                        elif (
                            entity["@id"]
                            == "https://w3id.org/oc/meta/br/0605/prov/se/2"
                        ):
                            self.assertEqual(
                                len(
                                    entity["http://www.w3.org/ns/prov#generatedAtTime"]
                                ),
                                1,
                            )
                            # self.assertEqual(len(entity['http://www.w3.org/ns/prov#invalidatedAtTime']), 2)
                        elif (
                            entity["@id"]
                            == "https://w3id.org/oc/meta/br/0605/prov/se/3"
                        ):
                            self.assertEqual(
                                entity["http://purl.org/dc/terms/description"][0][
                                    "@value"
                                ],
                                "The entity 'https://w3id.org/oc/meta/br/0605' has been deleted.",
                            )
                            self.assertIn(
                                "https://w3id.org/oc/ontology/hasUpdateQuery", entity
                            )
                            update_query_value = entity[
                                "https://w3id.org/oc/ontology/hasUpdateQuery"
                            ][0]["@value"]
                            update_query_triples = (
                                update_query_value.replace(
                                    "DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { ",
                                    "",
                                )
                                .replace(" } }", "")
                                .strip()
                            )
                            actual_triples = set(
                                triple.strip()
                                for triple in update_query_triples.split(" .")
                                if triple.strip()
                            )
                            expected_triples = {
                                '<https://w3id.org/oc/meta/br/0605> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date>'
                            }
                            self.assertEqual(actual_triples, expected_triples)
                            self.assertEqual(
                                entity["@type"][0], "http://www.w3.org/ns/prov#Entity"
                            )
                            self.assertEqual(
                                entity["http://www.w3.org/ns/prov#specializationOf"][0][
                                    "@id"
                                ],
                                "https://w3id.org/oc/meta/br/0605",
                            )
                            self.assertEqual(
                                entity["http://www.w3.org/ns/prov#wasAttributedTo"][0][
                                    "@id"
                                ],
                                "https://orcid.org/0000-0002-8420-0696",
                            )
                            self.assertIn(
                                "http://www.w3.org/ns/prov#invalidatedAtTime", entity
                            )
                            self.assertIn(
                                "http://www.w3.org/ns/prov#generatedAtTime", entity
                            )
                            self.assertEqual(
                                len(
                                    entity["http://www.w3.org/ns/prov#generatedAtTime"]
                                ),
                                1,
                            )
                            self.assertEqual(
                                len(
                                    entity[
                                        "http://www.w3.org/ns/prov#invalidatedAtTime"
                                    ]
                                ),
                                1,
                            )
                            self.assertEqual(
                                entity["http://www.w3.org/ns/prov#wasDerivedFrom"][0][
                                    "@id"
                                ],
                                "https://w3id.org/oc/meta/br/0605/prov/se/2",
                            )

    def test_no_rdf_files_generation(self):
        """Test that when generate_rdf_files is False, data is still updated in triplestore but not in files"""
        with open(META_CONFIG, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        self.original_generate_rdf_files = settings.get("generate_rdf_files", True)
        
        settings["generate_rdf_files"] = False
        with open(META_CONFIG, "w", encoding="utf-8") as file:
            yaml.dump(settings, file)
        
        os.makedirs(os.path.join(OUTPUT, "rdf", "br", "060", "10000"), exist_ok=True)
        
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")
        
        self.assertFalse(editor.generate_rdf_files, "generate_rdf_files should be False")
        
        g_set = GraphSet(base_iri="https://w3id.org/oc/meta/")
        br = g_set.add_br(res=URIRef("https://w3id.org/oc/meta/br/0603"), resp_agent="https://orcid.org/0000-0002-8420-0696")
        br.has_title("Original Title")
        editor.save(g_set)
        
        editor.update_property(
            URIRef("https://w3id.org/oc/meta/br/0603"),
            "has_title",
            "New Test Title",
        )
        
        with SPARQLClient(SERVER) as client:
            debug_result = client.query("""
            SELECT ?p ?o
            WHERE {
                GRAPH ?g {
                    <https://w3id.org/oc/meta/br/0603> ?p ?o .
                }
            }
            """)

            title_found = False
            if debug_result["results"]["bindings"]:
                for binding in debug_result["results"]["bindings"]:
                    predicate = binding.get('p', {}).get('value')
                    obj = binding.get('o', {}).get('value')

                    # Check if this is our title property with the expected value
                    if predicate == "http://purl.org/dc/terms/title" and obj == "New Test Title":
                        title_found = True
            else:
                print("No properties found for BR/0603")

            self.assertTrue(title_found, "Title update not found in triplestore")

        with SPARQLClient(PROV_SERVER) as client:
            prov_result = client.query("""
            ASK {
                ?s <http://www.w3.org/ns/prov#specializationOf> <https://w3id.org/oc/meta/br/0603> .
            }
            """)
            self.assertTrue(prov_result["boolean"], "Provenance for BR/0603 not found in triplestore")
        
        target_file = os.path.join(OUTPUT, "rdf", "br", "060", "10000", "1000.json")
        if os.path.exists(target_file):
            with open(target_file, "r", encoding="utf-8") as file:
                try:
                    data = json.load(file)
                    contains_update = False
                    for graph in data:
                        for entity in graph.get("@graph", []):
                            if entity.get("@id") == "https://w3id.org/oc/meta/br/0603":
                                titles = entity.get("http://purl.org/dc/terms/title", [])
                                for title in titles:
                                    if title.get("@value") == "New Test Title":
                                        contains_update = True
                                        break
                    self.assertFalse(contains_update, "RDF file should not contain the update")
                except json.JSONDecodeError:
                    pass
        
    def test_merge_caches_entities(self):
        """Verifica che le entità vengano correttamente cachate durante merge successivi"""
        base_iri = "https://w3id.org/oc/meta/"
        resp_agent = "https://orcid.org/0000-0002-8420-0696"
        g_set = GraphSet(
            base_iri,
            supplier_prefix="060",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        endpoint = "http://127.0.0.1:8805/sparql"

        # Prepara le entità di test
        ra = g_set.add_ra(
            resp_agent=resp_agent, res=URIRef("https://w3id.org/oc/meta/ra/06010")
        )
        ra.has_name("Wiley")

        reader = Reader()
        id_0605 = reader.import_entity_from_triplestore(
            g_set,
            endpoint,
            URIRef("https://w3id.org/oc/meta/id/0605"),
            resp_agent,
            enable_validation=False,
        )
        id_0609 = g_set.add_id(resp_agent=resp_agent)
        id_0609.create_crossref("313")

        ra.has_identifier(id_0605)
        ra.has_identifier(id_0609)

        # Genera provenance
        provset = ProvSet(
            g_set,
            base_iri,
            wanted_label=False,
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )
        provset.generate_provenance()

        # Salva e carica i dati
        rdf_dir = os.path.join(OUTPUT, "rdf") + os.sep
        graph_storer = Storer(
            g_set, dir_split=10000, n_file_item=1000, zip_output=False
        )
        prov_storer = Storer(
            provset, dir_split=10000, n_file_item=1000, zip_output=False
        )

        graph_storer.store_all(rdf_dir, base_iri)
        prov_storer.store_all(rdf_dir, base_iri)
        graph_storer.upload_all(endpoint)
        g_set.commit_changes()

        # Esegui il test della cache
        editor = MetaEditor(META_CONFIG, "https://orcid.org/0000-0002-8420-0696")

        # Prima fusione
        editor.merge(
            g_set,
            URIRef("https://w3id.org/oc/meta/ra/0607"),
            URIRef("https://w3id.org/oc/meta/ra/06010"),
        )

        # Verifica che le entità principali siano in cache
        self.assertTrue(
            editor.entity_cache.is_cached(URIRef("https://w3id.org/oc/meta/ra/0607"))
        )
        self.assertTrue(
            editor.entity_cache.is_cached(URIRef("https://w3id.org/oc/meta/ra/06010"))
        )

        # Verifica che le entità correlate siano in cache
        self.assertTrue(
            editor.entity_cache.is_cached(id_0609.res)
        )
        self.assertTrue(
            editor.entity_cache.is_cached(id_0605.res)
        )


class TestEntityCache(unittest.TestCase):
    def setUp(self):
        self.cache = EntityCache()
        self.entity = URIRef("https://w3id.org/oc/meta/ra/0607")

    def test_add_and_is_cached(self):
        self.assertFalse(self.cache.is_cached(self.entity))
        self.cache.add(self.entity)
        self.assertTrue(self.cache.is_cached(self.entity))

    def test_clear(self):
        self.cache.add(self.entity)
        self.cache.clear()
        self.assertFalse(self.cache.is_cached(self.entity))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
