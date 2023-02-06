#!python
# Copyright 2022-2023, Arcangelo Massari <arcangelo.massari@unibo.it>
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
from shutil import rmtree
from subprocess import call
from sys import executable
from test.curator_test import reset_server

from oc_ocdm import Storer
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader
from rdflib import URIRef

BASE = os.path.join('test', 'fixer', 'duplicated_ids')
CONFIG = os.path.join(BASE, 'meta_config.yaml')

class test_duplicated_ids(unittest.TestCase):
    def test_find_duplicated_ids_in_entity_type(self):
        reset_server()
        call([executable, '-m', 'oc_meta.run.meta_process', '-c', CONFIG])
        base_iri = 'https://w3id.org/oc/meta/'
        info_dir = os.path.join(BASE, 'info_dir', 'creator')
        g_set = GraphSet(base_iri, info_dir, supplier_prefix='060', wanted_label=False)
        endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
        resp_agent = 'https://orcid.org/0000-0002-8420-0696'
        rdf = os.path.join(BASE, 'rdf') + os.sep
        reader = Reader()
        reader.import_entity_from_triplestore(g_set, endpoint, URIRef('https://w3id.org/oc/meta/ra/0605'), resp_agent, enable_validation=False)
        ieee = g_set.get_entity(URIRef('https://w3id.org/oc/meta/ra/0605'))
        duplicated_id = g_set.add_id(resp_agent)
        duplicated_id.create_crossref('263')
        ieee.has_identifier(duplicated_id)
        provset = ProvSet(g_set, base_iri, info_dir, wanted_label=False)
        provset.generate_provenance()
        graph_storer = Storer(g_set, dir_split=10000, n_file_item=1000, zip_output=False)
        prov_storer = Storer(provset, dir_split=10000, n_file_item=1000, zip_output=False)
        graph_storer.store_all(rdf, base_iri)
        prov_storer.store_all(rdf, base_iri)
        graph_storer.upload_all(endpoint)
        call([executable, '-m', 'oc_meta.run.fixer.duplicated_ids', '-e', 'ra', '-c', os.path.join(BASE, 'meta_config.yaml'), '-r', 'https://orcid.org/0000-0002-8420-0696'])
        for filepath in [
            os.path.join(BASE, 'rdf', 'ra', '060', '10000', '1000.json'),
            os.path.join(BASE, 'rdf', 'ra', '060', '10000', '1000', 'prov', 'se.json')
        ]:
            with open(filepath, 'r', encoding='utf8') as f:
                data = json.load(f)
                for graph in data:
                    graph_data = graph['@graph']
                    for entity in graph_data:
                        if entity['@id'] == 'https://w3id.org/oc/meta/ra/0605':
                            identifiers = {identifier['@id'] for identifier in entity['http://purl.org/spar/datacite/hasIdentifier']}
                            self.assertTrue(identifiers == {'https://w3id.org/oc/meta/id/0604'})
                        elif entity['@id'] == 'https://w3id.org/oc/meta/ra/0605/prov/se/3':
                            update_query = entity['https://w3id.org/oc/ontology/hasUpdateQuery'][0]['@value']
                            self.assertTrue(update_query, 'DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/0605> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0603> . } }')
        for stuff in os.listdir(BASE):
            if os.path.isdir(os.path.join(BASE, stuff)) and stuff not in {'input'}:
                rmtree(os.path.join(BASE, stuff))
            elif os.path.isfile(os.path.join(BASE, stuff)) and stuff != 'meta_config.yaml':
                os.remove(os.path.join(BASE, stuff))