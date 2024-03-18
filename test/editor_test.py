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
from test.curator_test import reset_server

import yaml
from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader
from rdflib import URIRef

from oc_meta.plugins.editor import MetaEditor
from oc_meta.run.meta_process import MetaProcess, run_meta_process

BASE = os.path.join('test', 'editor')
OUTPUT = os.path.join(BASE, 'output')
META_CONFIG = os.path.join(BASE, 'meta_config.yaml')

class TestEditor(unittest.TestCase):
    def setUp(self):
        reset_server()
        if os.path.exists(OUTPUT):
            rmtree(OUTPUT)
        with open(META_CONFIG, encoding='utf-8') as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=META_CONFIG)

    def tearDown(self):
        rmtree(OUTPUT)
    
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
        def read_and_normalize_file(filepath):
            with open(filepath, 'r', encoding='utf8') as f:
                lines = [line for line in f.readlines()]
            return lines
        base_iri = 'https://w3id.org/oc/meta/'
        info_dir = os.path.join(OUTPUT, 'info_dir', '0620', 'creator')
        resp_agent = 'https://orcid.org/0000-0002-8420-0696'
        g_set = GraphSet(base_iri, info_dir, supplier_prefix='0620', wanted_label=False)
        endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
        rdf = os.path.join(OUTPUT, 'rdf') + os.sep
        reader = Reader()
        ra = g_set.add_ra(resp_agent=resp_agent, res=URIRef('https://w3id.org/oc/meta/ra/06205'))
        ra.has_name('Wiley')
        id_06105 = reader.import_entity_from_triplestore(g_set, endpoint, URIRef('https://w3id.org/oc/meta/id/06105'), resp_agent, enable_validation=False)
        id_06203 = g_set.add_id(resp_agent=resp_agent)
        id_06203.create_crossref('313')
        ra.has_identifier(id_06105)
        ra.has_identifier(id_06203)
        provset = ProvSet(g_set, base_iri, info_dir, wanted_label=False, supplier_prefix='0620')
        provset.generate_provenance()
        graph_storer = Storer(g_set, dir_split=10000, n_file_item=1000, zip_output=False)
        prov_storer = Storer(provset, dir_split=10000, n_file_item=1000, zip_output=False)
        graph_storer.store_all(rdf, base_iri)
        prov_storer.store_all(rdf, base_iri)
        graph_storer.upload_all(endpoint)
        editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')
        editor.merge(URIRef('https://w3id.org/oc/meta/ra/06107'), URIRef('https://w3id.org/oc/meta/ra/06205'))
        expected_lines_0610 = ['1 \n', '1 \n', '1 \n', '1 \n', '1 \n', '1 \n', '2 \n']
        normalized_lines_0610 = read_and_normalize_file(os.path.join(OUTPUT, 'info_dir', '0610', 'creator', 'prov_file_ra.txt'))
        self.assertEqual(normalized_lines_0610, expected_lines_0610)
        expected_lines_0620 = ['  \n', '  \n', '  \n', '  \n', '2 \n']
        normalized_lines_0620 = read_and_normalize_file(os.path.join(OUTPUT, 'info_dir', '0620', 'creator', 'prov_file_ra.txt'))
        self.assertEqual(normalized_lines_0620, expected_lines_0620)
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
                            

if __name__ == '__main__': # pragma: no cover
    unittest.main()