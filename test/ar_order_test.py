#!python
# Copyright 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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

import json
import os
import unittest
from shutil import copy
from subprocess import call
from sys import executable
from test.curator_test import reset_server
from zipfile import ZipFile

from SPARQLWrapper import JSON, SPARQLWrapper

BASE = os.path.join('test', 'ar_order')

class test_ar_order(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
        reset_server(endpoint)
        cls.server = SPARQLWrapper(endpoint)
        cls.server.method = 'POST'
        for entity in ['br', 'ar']:
            cls.server.setQuery('LOAD <file:' + os.path.abspath(os.path.join(BASE, f'{entity}.nt')).replace('\\', '/') + '> INTO GRAPH <' + f'https://w3id.org/oc/meta/{entity}/' + '>')
            cls.server.query()

    @classmethod
    def tearDownClass(cls):
        os.remove(os.path.join(BASE, 'rdf', 'ar', '060', '10000', '1000.zip'))
        os.remove(os.path.join(BASE, 'rdf', 'ar', '06360', '310000', '301000.zip'))
        os.remove(os.path.join(BASE, 'rdf', 'ar', '060', '10000', '1000', 'prov', 'se.zip'))
        os.remove(os.path.join(BASE, 'rdf', 'ar', '06360', '310000', '301000', 'prov', 'se.zip'))
        os.remove(os.path.join(BASE, 'info_dir', '06360', 'creator', 'prov_file_ar.txt'))
        os.remove(os.path.join(BASE, 'info_dir', 'creator', 'prov_file_ar.txt'))
        copy(os.path.join(BASE, '1000.zip'), os.path.join(BASE, 'rdf', 'ar', '060', '10000'))
        copy(os.path.join(BASE, '301000.zip'), os.path.join(BASE, 'rdf', 'ar', '06360', '310000'))
        copy(os.path.join(BASE, '060', 'se.zip'), os.path.join(BASE, 'rdf', 'ar', '060', '10000', '1000', 'prov', 'se.zip'))
        copy(os.path.join(BASE, '06360', 'se.zip'), os.path.join(BASE, 'rdf', 'ar', '06360', '310000', '301000', 'prov', 'se.zip'))
        copy(os.path.join(BASE, '06360', 'prov_file_ar.txt'), os.path.join(BASE, 'info_dir', '06360', 'creator', 'prov_file_ar.txt'))
        copy(os.path.join(BASE, '060', 'prov_file_ar.txt'), os.path.join(BASE, 'info_dir', 'creator', 'prov_file_ar.txt'))

    def test_fix_broken_roles_two_last(self):
        call([executable, '-m', 'oc_meta.run.fixer', '-c', os.path.join(BASE, 'meta_config.yaml'), '-r', 'https://orcid.org/0000-0002-8420-0696', '-m', '2'])
        output = dict()
        for filepath in [os.path.join(BASE, 'rdf', 'ar', '060', '10000', '1000.zip'), os.path.join(BASE, 'rdf', 'ar', '06360', '310000', '301000.zip')]:
            with ZipFile(file=filepath, mode="r") as archive:
                for zf_name in archive.namelist():
                    with archive.open(zf_name) as f:
                        data = json.load(f)
                        for graph in data:
                            graph_data = graph['@graph']
                            for agent in graph_data:
                                if agent['@id'] in ['https://w3id.org/oc/meta/ar/06021', 'https://w3id.org/oc/meta/ar/06022', 'https://w3id.org/oc/meta/ar/06023', 'https://w3id.org/oc/meta/ar/06024', 'https://w3id.org/oc/meta/ar/06025', 'https://w3id.org/oc/meta/ar/06026', 'https://w3id.org/oc/meta/ar/06027', 'https://w3id.org/oc/meta/ar/06360300897', 'https://w3id.org/oc/meta/ar/06360300898', 'https://w3id.org/oc/meta/ar/06360300899', 'https://w3id.org/oc/meta/ar/06360300900']:
                                    if 'https://w3id.org/oc/ontology/hasNext' in agent:
                                        output[agent['@id']] = agent['https://w3id.org/oc/ontology/hasNext'][0]['@id']
                                    else:
                                        output[agent['@id']] = ''
        with ZipFile(file=os.path.join(BASE, 'rdf', 'ar', '060', '10000', '1000', 'prov', 'se.zip'), mode="r") as archive:
            for zf_name in archive.namelist():
                with archive.open(zf_name) as f:
                    data = json.load(f)
                    for graph in data:
                        graph_data = graph['@graph']
                        for agent in graph_data:
                            if agent['@id'] == ['https://w3id.org/oc/meta/ar/06026/prov/se/2']:
                                agent['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value'] = 'INSERT DATA { GRAPH <https://w3id.org/oc/meta/ar/> { <https://w3id.org/oc/meta/ar/06026> <https://w3id.org/oc/ontology/hasNext> <https://w3id.org/oc/meta/ar/06027> . } }'
        query = '''
        PREFIX oco: <https://w3id.org/oc/ontology/>
        SELECT ?ar ?next WHERE {
            VALUES ?ar {<https://w3id.org/oc/meta/ar/06021> <https://w3id.org/oc/meta/ar/06022> <https://w3id.org/oc/meta/ar/06023> <https://w3id.org/oc/meta/ar/06024> <https://w3id.org/oc/meta/ar/06025> <https://w3id.org/oc/meta/ar/06026> <https://w3id.org/oc/meta/ar/06027> <https://w3id.org/oc/meta/ar/06360300897> <https://w3id.org/oc/meta/ar/06360300898> <https://w3id.org/oc/meta/ar/06360300899> <https://w3id.org/oc/meta/ar/06360300900>}
            ?ar oco:hasNext ?next.
        }
        '''
        self.server.setQuery(query)
        self.server.setReturnFormat(JSON)
        result = self.server.queryAndConvert()
        expected_result = {'head': {'vars': ['ar', 'next']}, 'results': {'bindings': [
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06021'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06022'}}, 
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06022'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06023'}}, 
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06023'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06024'}}, 
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06024'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06025'}}, 
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06025'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06026'}},
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06360300897'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06360300898'}}, 
            {'ar': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06360300898'}, 'next': {'type': 'uri', 'value': 'https://w3id.org/oc/meta/ar/06360300899'}}]}}
        expected_output = {
            'https://w3id.org/oc/meta/ar/06021': 'https://w3id.org/oc/meta/ar/06022', 
            'https://w3id.org/oc/meta/ar/06022': 'https://w3id.org/oc/meta/ar/06023',
            'https://w3id.org/oc/meta/ar/06023': 'https://w3id.org/oc/meta/ar/06024',
            'https://w3id.org/oc/meta/ar/06024': 'https://w3id.org/oc/meta/ar/06025',
            'https://w3id.org/oc/meta/ar/06025': 'https://w3id.org/oc/meta/ar/06026',
            'https://w3id.org/oc/meta/ar/06026': '', 
            'https://w3id.org/oc/meta/ar/06027': '',
            'https://w3id.org/oc/meta/ar/06360300897': 'https://w3id.org/oc/meta/ar/06360300898', 
            'https://w3id.org/oc/meta/ar/06360300898': 'https://w3id.org/oc/meta/ar/06360300899',
            'https://w3id.org/oc/meta/ar/06360300899': '',
            'https://w3id.org/oc/meta/ar/06360300900': ''}
        self.assertEqual((output, result), (expected_output, expected_result))