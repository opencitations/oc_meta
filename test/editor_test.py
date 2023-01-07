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

from rdflib import URIRef

from oc_meta.plugins.editor import MetaEditor
from oc_meta.run.meta_process import MetaProcess, run_meta_process

BASE = os.path.join('test', 'editor')
OUTPUT = os.path.join(BASE, 'output')
META_CONFIG = os.path.join(BASE, 'meta_config.yaml')

class TestEditor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        reset_server()
        meta_process = MetaProcess(config=META_CONFIG)
        run_meta_process(meta_process)
        cls.editor = MetaEditor(META_CONFIG, 'https://orcid.org/0000-0002-8420-0696')

    @classmethod
    def tearDownClass(cls):
        rmtree(OUTPUT)

    def test_update_property(self):
        self.editor.update_property(URIRef('https://w3id.org/oc/meta/ar/0601'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/0604'))
        self.editor.update_property(URIRef('https://w3id.org/oc/meta/ar/0604'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/0603'))
        self.editor.update_property(URIRef('https://w3id.org/oc/meta/ar/0603'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/0602'))
        self.editor.update_property(URIRef('https://w3id.org/oc/meta/ar/0602'), 'has_next', URIRef('https://w3id.org/oc/meta/ar/0605'))
        self.editor.save()
        with open(os.path.join(OUTPUT, 'rdf', 'ar', '060', '10000', '1000.json'), 'r', encoding='utf8') as f:
            ar_data = json.load(f)
            for graph in ar_data:
                graph_data = graph['@graph']
                for ar in graph_data:
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/0601':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/0604')
                    elif ar['@id'] == 'https://w3id.org/oc/meta/ar/0603':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/0602')
                    elif ar['@id'] == 'https://w3id.org/oc/meta/ar/0604':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/0603')
                    elif ar['@id'] == 'https://w3id.org/oc/meta/ar/0602':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasNext'][0]['@id'], 'https://w3id.org/oc/meta/ar/0605')
        with open(os.path.join(OUTPUT, 'rdf', 'ar', '060', '10000', '1000', 'prov', 'se.json'), 'r', encoding='utf8') as f:
            ar_prov = json.load(f)
            for graph in ar_prov:
                graph_prov = graph['@graph']
                for ar in graph_prov:
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/0601/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0601> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0602> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0601> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0604> . } }')
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/0603/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0603> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0604> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0603> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0602> . } }')
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/0604/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0604> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0605> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0604> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0603> . } }')
                    if ar['@id'] == 'https://w3id.org/oc/meta/ar/0602/prov/se/2':
                        self.assertEqual(ar['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0602> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0603> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/0602> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/0605> . } }')

    def test_delete_property(self):
        self.editor.delete_property(URIRef('https://w3id.org/oc/meta/br/0601'), 'has_title')
        self.editor.save()
        with open(os.path.join(OUTPUT, 'rdf', 'br', '060', '10000', '1000.json'), 'r', encoding='utf8') as f:
            br_data = json.load(f)
            for graph in br_data:
                graph_data = graph['@graph']
                for br in graph_data:
                    if br['@id'] == 'https://w3id.org/oc/meta/br/0601':
                        self.assertFalse('http://purl.org/dc/terms/title' in br)
        with open(os.path.join(OUTPUT, 'rdf', 'br', '060', '10000', '1000', 'prov', 'se.json'), 'r', encoding='utf8') as f:
            br_prov = json.load(f)
            for graph in br_prov:
                graph_prov = graph['@graph'] 
                for br in graph_prov:
                    if br['@id'] == 'https://w3id.org/oc/meta/br/0601/prov/se/2':
                        self.assertEqual(br['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'], 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/br/> { <https://w3id.org/oc/meta/br/0601> <http://purl.org/dc/terms/title> "A Review Of Hemolytic Uremic Syndrome In Patients Treated With Gemcitabine Therapy"^^<http://www.w3.org/2001/XMLSchema#string> . } }')

if __name__ == '__main__': # pragma: no cover
    unittest.main()