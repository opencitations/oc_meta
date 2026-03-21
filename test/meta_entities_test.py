#!/usr/bin/python

# Copyright 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import tempfile
import unittest

from rdflib import Graph
from sparqlite import SPARQLClient

from oc_meta.run.count.meta_entities import OCMetaStatistics, _count_venues_in_file
from test.test_utils import SERVER, reset_triplestore

TEST_DATA = """
@prefix fabio: <http://purl.org/spar/fabio/> .
@prefix pro: <http://purl.org/spar/pro/> .
@prefix oco: <https://w3id.org/oc/ontology/> .

# 3 bibliographic resources
<https://w3id.org/oc/meta/br/0601> a fabio:Expression, fabio:JournalArticle .
<https://w3id.org/oc/meta/br/0602> a fabio:Expression, fabio:JournalArticle .
<https://w3id.org/oc/meta/br/0603> a fabio:Expression, fabio:Journal .

# 4 authors
<https://w3id.org/oc/meta/ar/0601> a pro:RoleInTime ;
    pro:withRole pro:author ;
    oco:hasNext <https://w3id.org/oc/meta/ar/0602> .
<https://w3id.org/oc/meta/ar/0602> a pro:RoleInTime ;
    pro:withRole pro:author .
<https://w3id.org/oc/meta/ar/0603> a pro:RoleInTime ;
    pro:withRole pro:author .
<https://w3id.org/oc/meta/ar/0604> a pro:RoleInTime ;
    pro:withRole pro:author .

# 2 publishers
<https://w3id.org/oc/meta/ar/0605> a pro:RoleInTime ;
    pro:withRole pro:publisher .
<https://w3id.org/oc/meta/ar/0606> a pro:RoleInTime ;
    pro:withRole pro:publisher .

# 1 editor
<https://w3id.org/oc/meta/ar/0607> a pro:RoleInTime ;
    pro:withRole pro:editor .
"""


def reset_server(server: str = SERVER) -> None:
    reset_triplestore(server)


def load_test_data(server: str = SERVER) -> None:
    g = Graph()
    g.parse(data=TEST_DATA, format='ttl')

    with SPARQLClient(server, timeout=60) as client:
        for s, p, o in g:
            client.update(f'''
                INSERT DATA {{
                    GRAPH <https://w3id.org/oc/meta/> {{
                        {s.n3()} {p.n3()} {o.n3()} .
                    }}
                }}
            ''')


class TestOCMetaStatistics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        reset_server()

    def setUp(self):
        reset_server()
        load_test_data()

    def tearDown(self):
        reset_server()

    def test_count_expressions(self):
        with OCMetaStatistics(SERVER) as stats:
            count = stats.count_expressions()
        self.assertEqual(count, 3)

    def test_count_role_entities(self):
        with OCMetaStatistics(SERVER) as stats:
            counts = stats.count_role_entities()

        expected = {
            'pro:author': 4,
            'pro:publisher': 2,
            'pro:editor': 1
        }
        self.assertEqual(counts, expected)

    def test_count_venues_from_csv_requires_path(self):
        with OCMetaStatistics(SERVER) as stats:
            with self.assertRaises(ValueError) as context:
                stats.count_venues_from_csv()
            self.assertIn("CSV dump path is required", str(context.exception))


class TestVenueCountingFromCSV(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        for f in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, f))
        os.rmdir(self.temp_dir)

    def _write_csv(self, filename: str, content: str) -> str:
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, 'w') as f:
            f.write(content)
        return filepath

    def test_count_venues_in_file_with_external_ids(self):
        csv_content = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/1","Title1","Author1","2020","Nature [issn:0028-0836 omid:br/0601]","1","1","1-10","journal article","Publisher1",""
"doi:10.001/2","Title2","Author2","2020","Nature [issn:0028-0836 omid:br/0601]","1","2","11-20","journal article","Publisher1",""
"doi:10.001/3","Title3","Author3","2020","Science [issn:0036-8075 omid:br/0602]","1","1","1-10","journal article","Publisher2",""
'''
        filepath = self._write_csv('test1.csv', csv_content)
        venues = _count_venues_in_file(filepath)
        self.assertEqual(venues, {'omid:br/0601', 'omid:br/0602'})

    def test_count_venues_in_file_omid_only(self):
        csv_content = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/1","Title1","Author1","2020","Conference A [omid:br/0601]","","","1-10","conference paper","Publisher1",""
"doi:10.001/2","Title2","Author2","2020","conference a [omid:br/0602]","","","11-20","conference paper","Publisher1",""
'''
        filepath = self._write_csv('test2.csv', csv_content)
        venues = _count_venues_in_file(filepath)
        self.assertEqual(venues, {'conference a'})

    def test_count_venues_in_file_mixed(self):
        csv_content = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/1","Title1","Author1","2020","Nature [issn:0028-0836 omid:br/0601]","1","1","1-10","journal article","",""
"doi:10.001/2","Title2","Author2","2020","Conference A [omid:br/0602]","","","11-20","conference paper","",""
"doi:10.001/3","Title3","Author3","2020","Conference A [omid:br/0603]","","","21-30","conference paper","",""
'''
        filepath = self._write_csv('test3.csv', csv_content)
        venues = _count_venues_in_file(filepath)
        self.assertEqual(venues, {'omid:br/0601', 'conference a'})

    def test_count_venues_from_csv_integration(self):
        csv1 = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/1","Title1","Author1","2020","Nature [issn:0028-0836 omid:br/0601]","1","1","1-10","journal article","",""
"doi:10.001/2","Title2","Author2","2020","Science [issn:0036-8075 omid:br/0602]","1","1","1-10","journal article","",""
'''
        csv2 = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/3","Title3","Author3","2020","Nature [issn:0028-0836 omid:br/0601]","1","2","11-20","journal article","",""
"doi:10.001/4","Title4","Author4","2020","Cell [issn:0092-8674 omid:br/0603]","1","1","1-10","journal article","",""
'''
        self._write_csv('file1.csv', csv1)
        self._write_csv('file2.csv', csv2)

        with OCMetaStatistics(SERVER, csv_dump_path=self.temp_dir) as stats:
            count = stats.count_venues_from_csv()

        self.assertEqual(count, 3)

    def test_count_venues_empty_venue_field(self):
        csv_content = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/1","Title1","Author1","2020","","1","1","1-10","journal article","",""
"doi:10.001/2","Title2","Author2","2020","Nature [issn:0028-0836 omid:br/0601]","1","2","11-20","journal article","",""
'''
        filepath = self._write_csv('test4.csv', csv_content)
        venues = _count_venues_in_file(filepath)
        self.assertEqual(venues, {'omid:br/0601'})


class TestRunSelectedAnalyses(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        reset_server()

    def setUp(self):
        reset_server()
        load_test_data()
        self.temp_dir = tempfile.mkdtemp()
        csv_content = '''"id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
"doi:10.001/1","Title1","Author1","2020","Nature [issn:0028-0836 omid:br/0601]","1","1","1-10","journal article","",""
"doi:10.001/2","Title2","Author2","2020","Science [issn:0036-8075 omid:br/0602]","1","1","1-10","journal article","",""
'''
        with open(os.path.join(self.temp_dir, 'test.csv'), 'w') as f:
            f.write(csv_content)

    def tearDown(self):
        reset_server()
        for f in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, f))
        os.rmdir(self.temp_dir)

    def test_run_selected_analyses_br_only(self):
        with OCMetaStatistics(SERVER) as stats:
            results = stats.run_selected_analyses(analyze_br=True, analyze_ar=False, analyze_venues=False)

        self.assertEqual(results, {'fabio_expressions': 3})

    def test_run_selected_analyses_ar_only(self):
        with OCMetaStatistics(SERVER) as stats:
            results = stats.run_selected_analyses(analyze_br=False, analyze_ar=True, analyze_venues=False)

        expected = {
            'roles': {
                'pro:author': 4,
                'pro:publisher': 2,
                'pro:editor': 1
            }
        }
        self.assertEqual(results, expected)

    def test_run_selected_analyses_venues_without_csv_path(self):
        with OCMetaStatistics(SERVER) as stats:
            results = stats.run_selected_analyses(analyze_br=False, analyze_ar=False, analyze_venues=True)

        self.assertEqual(results, {'venues': None})

    def test_run_all_analyses(self):
        with OCMetaStatistics(SERVER, csv_dump_path=self.temp_dir) as stats:
            results = stats.run_all_analyses()

        expected = {
            'fabio_expressions': 3,
            'roles': {
                'pro:author': 4,
                'pro:publisher': 2,
                'pro:editor': 1
            },
            'venues': 2
        }
        self.assertEqual(results, expected)


if __name__ == '__main__':
    unittest.main()
