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

import redis
import yaml
from oc_meta.plugins.editor import MetaEditor
from oc_meta.run.meta_process import run_meta_process
from rdflib import URIRef
from SPARQLWrapper import POST, SPARQLWrapper

from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader

BASE = os.path.join('test', 'editor')
OUTPUT = os.path.join(BASE, 'output')
META_CONFIG = os.path.join(BASE, 'meta_config.yaml')
SERVER = 'http://127.0.0.1:8805/sparql'

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 5

def reset_server(server:str=SERVER) -> None:
    ts = SPARQLWrapper(server)
    for graph in {'https://w3id.org/oc/meta/br/', 'https://w3id.org/oc/meta/ra/', 'https://w3id.org/oc/meta/re/', 'https://w3id.org/oc/meta/id/', 'https://w3id.org/oc/meta/ar/'}:
        ts.setQuery(f'CLEAR GRAPH <{graph}>')
        ts.setMethod(POST)
        ts.query()

def reset_redis_counters():
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_client.flushdb()

def get_counter_handler():
    return RedisCounterHandler(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

class TestEditor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.counter_handler = get_counter_handler()

    def setUp(self):
        reset_server()
        reset_redis_counters()
        if os.path.exists(OUTPUT):
            rmtree(OUTPUT)
        with open(META_CONFIG, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        # Update settings to use Redis
        settings['redis_host'] = REDIS_HOST
        settings['redis_port'] = REDIS_PORT
        settings['redis_db'] = REDIS_DB
        run_meta_process(settings=settings, meta_config_path=META_CONFIG)

    def tearDown(self):
        rmtree(OUTPUT)
        reset_redis_counters()
    
    def test_update_property(self):
        editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')
        editor.update_property(URIRef('https://w3id.org/oc/meta/ar/06101'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/06104'))
        editor.update_property(URIRef('https://w3id.org/oc/meta/ar/06104'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/06103'))
        editor.update_property(URIRef('https://w3id.org/oc/meta/ar/06103'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/06102'))
        editor.update_property(URIRef('https://w3id.org/oc/meta/ar/06102'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/06105'))
        with open(os.path.join(OUTPUT, 'rdf', 'ar', '0610', '10000', '1000.json'), 'r', encoding='utf8') as f:
            ar_data = json.load(f)
            for graph in ar_data:
                graph_data = graph['@graph']
                for ar in graph_data:
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/06101':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/06104')
                    elif ar['@id'] == 'https://w3id.org/oc/meta/ar/06103':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/06102')
                    elif ar['@id'] == 'https://w3id.org/oc/meta/ar/06104':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/06103')
                    elif ar['@id'] == 'https://w3id.org/oc/meta/ar/06102':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/06105')
        with open(os.path.join(OUTPUT, 'rdf', 'ar', '0610', '10000', '1000', 'prov', 'se.json'), 'r', encoding='utf8') as f:
            ar_prov = json.load(f)
            for graph in ar_prov:
                graph_prov = graph['@graph']
                for ar in graph_prov:
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/06101/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06101> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06102> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06101> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06104> . } }')
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/06103/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06103> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06104> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06103> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06102> . } }')
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/06104/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06104> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06105> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06104> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06103> . } }')
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/06102/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06102> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06103> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06102> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06105> . } }')

    def test_delete_property(self):
        editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')
        editor.delete(URIRef('https://w3id.org/oc/meta/br/06101'), 'has_title')
        with open(os.path.join(OUTPUT, 'rdf', 'br', '0610', '10000', '1000.json'), 'r', encoding='utf8') as f:
            br_data = json.load(f)
            for graph in br_data:
                graph_data = graph['@graph']
                for br in graph_data:
                    if br['@id'] == 'https://w3id.org/oc/meta/br/06101':
                        self.assertFalse('http://purl.org/dc/terms/title' in br)
        with open(os.path.join(OUTPUT, 'rdf', 'br', '0610', '10000', '1000', 'prov', 'se.json'), 'r', encoding='utf8') as f:
            br_prov = json.load(f)
            for graph in br_prov:
                graph_prov = graph['@graph'] 
                for br in graph_prov:
                    if br['@id'] == 'https://w3id.org/oc/meta/br/06101/prov/se/2':
                        self.assertEqual(br['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { <https://w3id.org/oc/meta/br/06101> <http://purl.org/dc/terms/title> "A Review Of Hemolytic Uremic Syndrome In Patients Treated With Gemcitabine Therapy" . } }')

    def test_delete_entity(self):
        editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')
        editor.delete(URIRef('https://w3id.org/oc/meta/id/06101'))
        with open(os.path.join(OUTPUT, 'rdf', 'id', '0610', '10000', '1000.json'), 'r', encoding='utf8') as f:
            id_data = json.load(f)
            for graph in id_data:
                graph_data = graph['@graph']
                for identifier in graph_data:
                    if identifier['@id'] == 'https://w3id.org/oc/meta/id/06101':
                        self.fail()
        with open(os.path.join(OUTPUT, 'rdf', 'id', '0610', '10000', '1000', 'prov', 'se.json'), 'r', encoding='utf8') as f:
            id_prov = json.load(f)
            for graph in id_prov:
                graph_prov = graph['@graph'] 
                for identifier in graph_prov:
                    if identifier['@id'] == 'https://w3id.org/oc/meta/id/06101/prov/se/2':
                        update_query = identifier['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'].replace('DELETE DATA { GRAPH <https://w3id.org/oc/meta/id/> { ', '').replace(' . } }', '').replace('\n', '').split(' .')
                        self.assertEqual(set(update_query), {'<https://w3id.org/oc/meta/id/06101> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/doi>', '<https://w3id.org/oc/meta/id/06101> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', '<https://w3id.org/oc/meta/id/06101> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1002/(sici)1097-0142(19990501)85:9<2023::aid-cncr21>3.0.co;2-2"'})
        with open(os.path.join(OUTPUT, 'rdf', 'br', '0610', '10000', '1000', 'prov', 'se.json'), 'r', encoding='utf8') as f:
            ra_prov = json.load(f)
            for graph in ra_prov:
                graph_prov = graph['@graph'] 
                for ra in graph_prov:
                    if ra['@id'] == 'https://w3id.org/oc/meta/br/06101/prov/se/2':
                        self.assertEqual(ra['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { <https://w3id.org/oc/meta/br/06101> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06101> . } }')

    def test_merge(self):
        base_iri = 'https://w3id.org/oc/meta/'
        resp_agent = 'https://orcid.org/0000-0002-8420-0696'
        g_set = GraphSet(base_iri, supplier_prefix='0620', wanted_label=False, custom_counter_handler=self.counter_handler)
        endpoint = 'http://127.0.0.1:8805/sparql'
        
        # Create entities  testing
        ra = g_set.add_ra(resp_agent=resp_agent, res=URIRef('https://w3id.org/oc/meta/ra/06205'))
        ra.has_name('Wiley')
        
        reader = Reader()
        id_06105 = reader.import_entity_from_triplestore(g_set, endpoint, URIRef('https://w3id.org/oc/meta/id/06105'), resp_agent, enable_validation=False)
        id_06203 = g_set.add_id(resp_agent=resp_agent)
        id_06203.create_crossref('313')
        
        ra.has_identifier(id_06105)
        ra.has_identifier(id_06203)
        
        # Generate provenance
        provset = ProvSet(g_set, base_iri, wanted_label=False, supplier_prefix='0620', custom_counter_handler=self.counter_handler)
        provset.generate_provenance()
        
        # Store and upload data
        rdf_dir = os.path.join(OUTPUT, 'rdf') + os.sep
        graph_storer = Storer(g_set, dir_split=10000, n_file_item=1000, zip_output=False)
        prov_storer = Storer(provset, dir_split=10000, n_file_item=1000, zip_output=False)
        
        graph_storer.store_all(rdf_dir, base_iri)
        prov_storer.store_all(rdf_dir, base_iri)
        graph_storer.upload_all(endpoint)
        
        # Perform merge
        editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')
        editor.merge(URIRef('https://w3id.org/oc/meta/ra/06107'), URIRef('https://w3id.org/oc/meta/ra/06205'))
        
        # Check Redis counters
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=1, supplier_prefix='0610'), 1)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=2, supplier_prefix='0610'), 1)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=3, supplier_prefix='0610'), 1)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=4, supplier_prefix='0610'), 1)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=5, supplier_prefix='0610'), 1)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=6, supplier_prefix='0610'), 1)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=7, supplier_prefix='0610'), 2)

        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=1, supplier_prefix='0620'), 0)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=2, supplier_prefix='0620'), 0)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=3, supplier_prefix='0620'), 0)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=4, supplier_prefix='0620'), 0)
        self.assertEqual(self.counter_handler.read_counter('ra', prov_short_name='se', identifier=5, supplier_prefix='0620'), 2)

        # Verify merged data
        for filepath in [
            os.path.join(OUTPUT, 'rdf', 'ra', '0610', '10000', '1000.json'),
            # os.path.join(OUTPUT, 'rdf', 'ar', '0620', '10000', '1000.json'),
            os.path.join(OUTPUT, 'rdf', 'ra', '0620', '10000', '1000', 'prov', 'se.json'),
            os.path.join(OUTPUT, 'rdf', 'ra', '0610', '10000', '1000', 'prov', 'se.json')
        ]:
            with open(filepath, 'r', encoding='utf8') as f:
                data = json.load(f)
                for graph in data:
                    graph_data = graph['@graph']
                    for entity in graph_data:
                        if entity['@id'] == 'https://w3id.org/oc/meta/ra/06107':
                            identifiers = {identifier['@id'] for identifier in entity['http://purl.org/spar/datacite/hasIdentifier']}
                            self.assertEqual(identifiers, {'https://w3id.org/oc/meta/id/06105', 'https://w3id.org/oc/meta/id/06201'})
                        elif entity['@id'] == 'https://w3id.org/oc/meta/ra/06205':
                            self.fail()
                        # elif entity['@id'] == 'https://w3id.org/oc/meta/ar/06205':
                        #     self.assertEqual(entity['http://purl.org/spar/pro/isHeldBy'][0]['@id'], 'https://w3id.org/oc/meta/ra/06107')
                        elif entity['@id'] in {'https://w3id.org/oc/meta/ra/06107/prov/se/1', 'https://w3id.org/oc/meta/ra/06205/prov/se/1'}:
                            self.assertTrue('http://www.w3.org/ns/prov#invalidatedAtTime' in entity)
                        elif entity['@id'] == 'https://w3id.org/oc/meta/ra/06107/prov/se/3':
                            self.assertEqual(entity['http://purl.org/dc/terms/description'][0]['@value'], "The entity 'https://w3id.org/oc/meta/ra/06107' has been merged with 'https://w3id.org/oc/meta/ra/06205'.")
                            self.assertEqual(entity['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06107> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06206> . } }')
                        elif entity['@id'] == 'https://w3id.org/oc/meta/ra/06205/prov/se/2':
                            update_query = entity['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'].replace('DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { ', '').replace(' . } }', '').replace('\n', '').split(' .')
                            self.assertEqual(set(update_query), {'<https://w3id.org/oc/meta/ra/06205> <http://xmlns.com/foaf/0.1/name> "Wiley"', '<https://w3id.org/oc/meta/ra/06205> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06201>', '<https://w3id.org/oc/meta/ra/06205> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06105>', '<https://w3id.org/oc/meta/ra/06205> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>'})

    def test_delete_entity_with_inferred_type(self):
        editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')
        endpoint = SPARQLWrapper(SERVER)

        # Remove the type from the entity
        delete_type_query = """
        DELETE {
            GRAPH <https://w3id.org/oc/meta/br/> {
                <https://w3id.org/oc/meta/br/06105> a <http://purl.org/spar/fabio/Expression> .
            }
        }
        WHERE {
            GRAPH <https://w3id.org/oc/meta/br/> {
                <https://w3id.org/oc/meta/br/06105> a <http://purl.org/spar/fabio/Expression> .
            }
        }
        """
        endpoint.setQuery(delete_type_query)
        endpoint.setMethod(POST)
        endpoint.query()

        # Ensure the entity exists before deletion
        select_query = """
        SELECT ?s WHERE {
            GRAPH <https://w3id.org/oc/meta/br/> {
                ?s <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date> .
            }
        }
        """
        endpoint.setQuery(select_query)
        endpoint.setReturnFormat('json')
        result = endpoint.query().convert()
        self.assertEqual(len(result['results']['bindings']), 1)

        # Perform deletion
        editor.delete(URIRef('https://w3id.org/oc/meta/br/06105'))

        # Ensure the entity is deleted
        result = endpoint.query().convert()
        self.assertEqual(len(result['results']['bindings']), 0)

        # Verify provenance information
        prov_path = os.path.join(OUTPUT, 'rdf', 'br', '0610', '10000', '1000', 'prov', 'se.json')
        with open(prov_path, 'r', encoding='utf8') as f:
            prov_data = json.load(f)
            br_06105_prov_se_2 = None
            br_06105_prov_se_1 = None
            for graph in prov_data:
                for entity in graph['@graph']:
                    if entity['@id'] == 'https://w3id.org/oc/meta/br/06105/prov/se/2':
                        br_06105_prov_se_2 = entity
                    if entity['@id'] == 'https://w3id.org/oc/meta/br/06105/prov/se/1':
                        br_06105_prov_se_1 = entity
            
            self.assertIsNotNone(br_06105_prov_se_2)
            self.assertEqual(br_06105_prov_se_2['http://purl.org/dc/terms/description'][0]['@value'], "The entity 'https://w3id.org/oc/meta/br/06105' has been deleted.")
            self.assertEqual(br_06105_prov_se_2['@type'][0], "http://www.w3.org/ns/prov#Entity")
            self.assertEqual(br_06105_prov_se_2['http://www.w3.org/ns/prov#specializationOf'][0]['@id'], "https://w3id.org/oc/meta/br/06105")
            self.assertEqual(br_06105_prov_se_2['http://www.w3.org/ns/prov#wasAttributedTo'][0]['@id'], "https://orcid.org/0000-0002-8420-0696")
            self.assertIn("http://www.w3.org/ns/prov#invalidatedAtTime", br_06105_prov_se_2)
            self.assertIn("http://www.w3.org/ns/prov#generatedAtTime", br_06105_prov_se_2)
            self.assertEqual(len(br_06105_prov_se_1['http://www.w3.org/ns/prov#generatedAtTime']), 1)
            self.assertIn("https://w3id.org/oc/ontology/hasUpdateQuery", br_06105_prov_se_2)
            update_query_value = br_06105_prov_se_2['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value']
            update_query_triples = update_query_value.replace('DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { ', '').replace(' } }', '').strip()
            actual_triples = set(triple.strip() for triple in update_query_triples.split(' .') if triple.strip())
            expected_triples = {
                '<https://w3id.org/oc/meta/br/06105> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06106>',
                '<https://w3id.org/oc/meta/br/06105> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date>'
            }
            self.assertEqual(actual_triples, expected_triples)
            
            self.assertIsNotNone(br_06105_prov_se_1)
            self.assertEqual(br_06105_prov_se_1['http://purl.org/dc/terms/description'][0]['@value'], "The entity 'https://w3id.org/oc/meta/br/06105' has been created.")
            self.assertEqual(br_06105_prov_se_1['@type'][0], "http://www.w3.org/ns/prov#Entity")
            self.assertEqual(br_06105_prov_se_1['http://www.w3.org/ns/prov#specializationOf'][0]['@id'], "https://w3id.org/oc/meta/br/06105")
            self.assertEqual(br_06105_prov_se_1['http://www.w3.org/ns/prov#wasAttributedTo'][0]['@id'], "https://w3id.org/oc/meta/prov/pa/1")
            self.assertIn("http://www.w3.org/ns/prov#generatedAtTime", br_06105_prov_se_1)
            self.assertEqual(len(br_06105_prov_se_1['http://www.w3.org/ns/prov#generatedAtTime']), 1)
            self.assertEqual(len(br_06105_prov_se_2['http://www.w3.org/ns/prov#invalidatedAtTime']), 1)
            self.assertIn("http://www.w3.org/ns/prov#hadPrimarySource", br_06105_prov_se_1)

        # Reinsert the publication date
        sparql_update_query = f"""
        INSERT DATA {{
            GRAPH <https://w3id.org/oc/meta/br/> {{
                <https://w3id.org/oc/meta/br/06105> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date> .
            }}
        }}
        """
        endpoint = SPARQLWrapper(SERVER)
        endpoint.setQuery(sparql_update_query)
        endpoint.setMethod(POST)
        endpoint.query()
    
        # Perform deletion again
        editor.delete(URIRef('https://w3id.org/oc/meta/br/06105'))

        # Verify and print the provenance graph for the entity
        prov_path = os.path.join(OUTPUT, 'rdf', 'br', '0610', '10000', '1000', 'prov', 'se.json')
        with open(prov_path, 'r', encoding='utf8') as f:
            prov_data = json.load(f)
            for graph in prov_data:
                for entity in graph['@graph']:
                    if 'https://w3id.org/oc/meta/br/06105' in entity['@id']:
                        if entity['@id'] == 'https://w3id.org/oc/meta/br/06105/prov/se/1':
                            self.assertEqual(len(entity['http://www.w3.org/ns/prov#generatedAtTime']), 1)
                            self.assertEqual(len(entity['http://www.w3.org/ns/prov#invalidatedAtTime']), 1)
                        elif entity['@id'] == 'https://w3id.org/oc/meta/br/06105/prov/se/2':
                            self.assertEqual(len(entity['http://www.w3.org/ns/prov#generatedAtTime']), 1)
                            # self.assertEqual(len(entity['http://www.w3.org/ns/prov#invalidatedAtTime']), 2)
                        elif entity['@id'] == 'https://w3id.org/oc/meta/br/06105/prov/se/3':
                            self.assertEqual(entity['http://purl.org/dc/terms/description'][0]['@value'], "The entity 'https://w3id.org/oc/meta/br/06105' has been deleted.")
                            self.assertIn("https://w3id.org/oc/ontology/hasUpdateQuery", entity)
                            update_query_value = entity['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value']
                            update_query_triples = update_query_value.replace('DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { ', '').replace(' } }', '').strip()
                            actual_triples = set(triple.strip() for triple in update_query_triples.split(' .') if triple.strip())
                            expected_triples = {
                                '<https://w3id.org/oc/meta/br/06105> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2024-04-14"^^<http://www.w3.org/2001/XMLSchema#date>'
                            }
                            self.assertEqual(actual_triples, expected_triples)
                            self.assertEqual(entity['@type'][0], "http://www.w3.org/ns/prov#Entity")
                            self.assertEqual(entity['http://www.w3.org/ns/prov#specializationOf'][0]['@id'], "https://w3id.org/oc/meta/br/06105")
                            self.assertEqual(entity['http://www.w3.org/ns/prov#wasAttributedTo'][0]['@id'], "https://orcid.org/0000-0002-8420-0696")
                            self.assertIn("http://www.w3.org/ns/prov#invalidatedAtTime", entity)
                            self.assertIn("http://www.w3.org/ns/prov#generatedAtTime", entity)
                            self.assertEqual(len(entity['http://www.w3.org/ns/prov#generatedAtTime']), 1)
                            self.assertEqual(len(entity['http://www.w3.org/ns/prov#invalidatedAtTime']), 1)
                            self.assertEqual(entity['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id'], "https://w3id.org/oc/meta/br/06105/prov/se/2")

if __name__ == '__main__': # pragma: no cover
    unittest.main()