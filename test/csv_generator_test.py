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

import os
import unittest
from shutil import rmtree
from subprocess import call
from sys import executable

import redis
import yaml
from oc_meta.lib.file_manager import get_csv_data
from rdflib import URIRef
from SPARQLWrapper import POST, SPARQLWrapper

from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader

BASE = os.path.join("test", "csv_generator")
CONFIG = os.path.join(BASE, "meta_config.yaml")
OUTPUT = os.path.join(BASE, "csv_generated")

SERVER = "http://127.0.0.1:8805/sparql"

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 5  # For counters
REDIS_CACHE_DB = 2  # For cache, using same test DB as on_triplestore_test.py


def reset_server(server: str = SERVER) -> None:
    ts = SPARQLWrapper(server)
    for graph in {
        "https://w3id.org/oc/meta/br/",
        "https://w3id.org/oc/meta/ra/",
        "https://w3id.org/oc/meta/re/",
        "https://w3id.org/oc/meta/id/",
        "https://w3id.org/oc/meta/ar/",
    }:
        ts.setQuery(f"CLEAR GRAPH <{graph}>")
        ts.setMethod(POST)
        ts.query()


def reset_redis_counters():
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_cache_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_CACHE_DB
    )
    redis_client.flushdb()
    redis_cache_client.flushdb()


def get_counter_handler():
    return RedisCounterHandler(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


class TestCSVGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.counter_handler = get_counter_handler()

    def setUp(self):
        reset_server()
        reset_redis_counters()

        # Create temporary directory for cache files
        self.temp_dir = os.path.join("test", "temp_csv_generator_test")
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        os.makedirs(self.temp_dir)

        # Setup cache files
        self.cache_file = os.path.join(self.temp_dir, "ts_upload_cache.json")
        self.failed_file = os.path.join(self.temp_dir, "failed_queries.txt")
        self.stop_file = os.path.join(self.temp_dir, ".stop_upload")

        # Update config with Redis and cache settings
        with open(CONFIG, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        settings.update(
            {
                "redis_host": REDIS_HOST,
                "redis_port": REDIS_PORT,
                "redis_db": REDIS_DB,
                "redis_cache_db": REDIS_CACHE_DB,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )
        with open(CONFIG, "w", encoding="utf-8") as file:
            yaml.dump(settings, file)

    def tearDown(self):
        reset_redis_counters()
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)

    def test_generate_csv(self):
        call([executable, "-m", "oc_meta.run.meta_process", "-c", CONFIG])

        base_iri = "https://w3id.org/oc/meta/"
        g_set = GraphSet(
            base_iri,
            supplier_prefix="0620",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        endpoint = "http://127.0.0.1:8805/sparql"
        resp_agent = "https://orcid.org/0000-0002-8420-0696"
        rdf = os.path.join(BASE, "rdf") + os.sep
        reader = Reader()
        reader.import_entity_from_triplestore(
            g_set,
            endpoint,
            URIRef("https://w3id.org/oc/meta/ra/06105"),
            resp_agent,
            enable_validation=False,
        )
        ieee = g_set.get_entity(URIRef("https://w3id.org/oc/meta/ra/06105"))
        duplicated_id = g_set.add_id(resp_agent)
        duplicated_id.create_crossref("263")
        ieee.has_identifier(duplicated_id)
        provset = ProvSet(
            g_set,
            base_iri,
            wanted_label=False,
            supplier_prefix="0620",
            custom_counter_handler=self.counter_handler,
        )
        provset.generate_provenance()
        graph_storer = Storer(
            g_set, dir_split=10000, n_file_item=1000, zip_output=False
        )
        prov_storer = Storer(
            provset, dir_split=10000, n_file_item=1000, zip_output=False
        )
        graph_storer.store_all(rdf, base_iri)
        prov_storer.store_all(rdf, base_iri)
        graph_storer.upload_all(endpoint)
        call(
            [
                executable,
                "-m",
                "oc_meta.run.csv_generator",
                "-c",
                CONFIG,
                "-o",
                OUTPUT,
                "-t",
                "3000",
                "-m",
                "2",
                "-w",
                "doi",
                "issn",
            ]
        )
        output = get_csv_data(os.path.join(OUTPUT, "0.csv"))
        for stuff in os.listdir(BASE):
            if os.path.isdir(os.path.join(BASE, stuff)) and stuff not in {"input"}:
                rmtree(os.path.join(BASE, stuff))
            elif (
                os.path.isfile(os.path.join(BASE, stuff))
                and stuff != "meta_config.yaml"
            ):
                os.remove(os.path.join(BASE, stuff))
        expected_output = [
            {
                "id": "omid:br/06102 issn:0018-9464",
                "title": "IEEE Transactions On Magnetics",
                "author": "",
                "issue": "",
                "volume": "",
                "venue": "",
                "page": "",
                "pub_date": "",
                "type": "journal",
                "publisher": "",
                "editor": "",
            },
            {
                "id": "omid:br/06101 doi:10.1109/20.877674",
                "title": "An Investigation Of FEM-FCT Method For Streamer Corona Simulation",
                "author": "Woong-Gee Min, [omid:ra/06101]; Hyeong-Seok Kim, [omid:ra/06102]; Seok-Hyun Lee, [omid:ra/06103]; Song-Yop Hahn, [omid:ra/06104]",
                "issue": "4",
                "volume": "36",
                "venue": "IEEE Transactions On Magnetics [omid:br/06102 issn:0018-9464]",
                "page": "1280-1284",
                "pub_date": "2000-07",
                "type": "journal article",
                "publisher": "Institute Of Electrical And Electronics Engineers (Ieee) [omid:ra/06105 crossref:263 crossref:263]",
                "editor": "",
            },
        ]
        self.assertTrue(
            output == expected_output or output == list(reversed(expected_output))
        )

        # Verify Redis counters
        self.assertEqual(
            self.counter_handler.read_counter("br", supplier_prefix="0610"), 4
        )
        self.assertEqual(
            self.counter_handler.read_counter("ra", supplier_prefix="0610"), 5
        )
        self.assertEqual(
            self.counter_handler.read_counter("id", supplier_prefix="0610"), 3
        )
