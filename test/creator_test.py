from meta.plugins.multiprocess.resp_agents_creator import RespAgentsCreator
from meta.scripts.creator import *
from test.curator_test import reset_server
from rdflib import XSD, compare, Graph
from rdflib.term import _toPythonMapping
import csv
import json
import os
import unittest


SERVER = 'http://127.0.0.1:9999/blazegraph/sparql'

# The following function has been added for handling gYear and gYearMonth correctly.
# Source: https://github.com/opencitations/script/blob/master/ocdm/storer.py
# More info at https://github.com/RDFLib/rdflib/issues/806.

def hack_dates():
    if XSD.gYear in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYear)
    if XSD.gYearMonth in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYearMonth)


def open_csv(path):
    path = os.path.abspath(path)
    with open(path, 'r', encoding="utf-8") as csvfile:
        reader = list(csv.DictReader(csvfile, delimiter=","))
        return reader


def open_json(path):
    path = os.path.abspath(path)
    with open(path) as json_file:
        data = json.load(json_file)
        return data


# creator executor
def prepare2test(name):
    reset_server()
    data = open_csv("test/testcases/testcase_data/testcase_" + name + "_data.csv")
    testcase_id_br = open_csv("test/testcases/testcase_data/indices/" + name + "/index_id_br_" + name + ".csv")
    testcase_id_ra = open_csv("test/testcases/testcase_data/indices/" + name + "/index_id_ra_" + name + ".csv")
    testcase_ar = open_csv("test/testcases/testcase_data/indices/" + name + "/index_ar_" + name + ".csv")
    testcase_re = open_csv("test/testcases/testcase_data/indices/" + name + "/index_re_" + name + ".csv")
    testcase_vi = open_json("test/testcases/testcase_data/indices/" + name + "/index_vi_" + name + ".json")
    testcase_ttl = "test/testcases/testcase_" + name + ".ttl"

    creator_info_dir = os.path.join("meta", "tdd", "creator_counter")
    creator = Creator(data, SERVER, "https://w3id.org/oc/meta/", creator_info_dir, "060", 'https://orcid.org/0000-0002-8420-0696', testcase_id_ra, testcase_id_br,
                      testcase_re, testcase_ar, testcase_vi, set())
    creator_setgraph = creator.creator()
    test_graph = Graph()
    hack_dates()
    test_graph = test_graph.parse(testcase_ttl, format="ttl")
    new_graph = Graph()
    for g in creator_setgraph.graphs():
        new_graph += g
    return test_graph, new_graph

class test_Creator(unittest.TestCase):
    def test_vvi_action(self):
        base_iri = 'https://w3id.org/oc/meta/'
        creator_info_dir = os.path.join("meta", "tdd", "creator_counter")
        vvi = {'0602': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        creator = Creator([], SERVER, base_iri, creator_info_dir, "060", 'https://orcid.org/0000-0002-8420-0696', [], [], [], [], vvi, set())
        creator.src = None
        creator.type = 'journal article'
        preexisting_graph = creator.finder.get_preexisting_graph(URIRef(f'{base_iri}br/0601'), dict())
        creator.br_graph = creator.setgraph.add_br('https://orcid.org/0000-0002-8420-0696', None, URIRef(f'{base_iri}br/0601'), preexisting_graph=preexisting_graph)
        creator.vvi_action('OECD [meta:br/0602]', '107', '1')
        output_graph = Graph()
        for g in creator.setgraph.graphs():
            output_graph += g
        expected_data = '''
            <https://w3id.org/oc/meta/br/0602> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
            <https://w3id.org/oc/meta/br/0602> <http://purl.org/dc/terms/title> "OECD".
            <https://w3id.org/oc/meta/br/0602> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal>.
            <https://w3id.org/oc/meta/br/4733> <http://purl.org/spar/fabio/hasSequenceIdentifier> "107".
            <https://w3id.org/oc/meta/br/4733> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
            <https://w3id.org/oc/meta/br/4733> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0602>.
            <https://w3id.org/oc/meta/br/4733> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume>.
            <https://w3id.org/oc/meta/br/4734> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalIssue>.
            <https://w3id.org/oc/meta/br/4734> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/4733>.
            <https://w3id.org/oc/meta/br/4734> <http://purl.org/spar/fabio/hasSequenceIdentifier> "1".
            <https://w3id.org/oc/meta/br/4734> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
            <https://w3id.org/oc/meta/br/0601> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/4734>.
            <https://w3id.org/oc/meta/br/0601> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
        '''
        expected_graph = Graph()
        expected_graph = expected_graph.parse(data=expected_data, format="nt")
        self.assertEqual(compare.isomorphic(output_graph, expected_graph), True)

class test_RespAgentsCreator(unittest.TestCase):
    def test_creator(self):
        reset_server()
        data = open_csv("test/testcases/testcase_data/resp_agents_curator_output.csv")
        creator_info_dir = os.path.join("meta", "tdd", "creator_counter")
        testcase_id_ra = open_csv("test/testcases/testcase_data/indices/resp_agents_curator_output/index_id_ra.csv")
        creator = RespAgentsCreator(data, SERVER, "https://w3id.org/oc/meta/", creator_info_dir, "060", 'https://orcid.org/0000-0002-8420-0696', testcase_id_ra, set())
        creator_graphset = creator.creator()
        output_graph = Graph()
        for g in creator_graphset.graphs():
            output_graph += g
        expected_data = '''
            <https://w3id.org/oc/meta/ra/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0601> .
            <https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/givenName> "Ron J." .
            <https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/familyName> "Deckert" .
            <https://w3id.org/oc/meta/ra/0601> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .
            <https://w3id.org/oc/meta/id/0601> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "0000-0003-2100-6412" .
            <https://w3id.org/oc/meta/id/0601> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .
            <https://w3id.org/oc/meta/id/0601> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid> .
            <https://w3id.org/oc/meta/ra/0602> <http://xmlns.com/foaf/0.1/givenName> "Juan M." .
            <https://w3id.org/oc/meta/ra/0602> <http://xmlns.com/foaf/0.1/familyName> "Ruso" .
            <https://w3id.org/oc/meta/ra/0602> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .
            <https://w3id.org/oc/meta/ra/0602> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0602> .
            <https://w3id.org/oc/meta/id/0602> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "0000-0001-5909-6754" .
            <https://w3id.org/oc/meta/id/0602> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .
            <https://w3id.org/oc/meta/id/0602> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid> .
            <https://w3id.org/oc/meta/ra/0603> <http://xmlns.com/foaf/0.1/familyName> "Sarmiento" .
            <https://w3id.org/oc/meta/ra/0603> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0603> .
            <https://w3id.org/oc/meta/ra/0603> <http://xmlns.com/foaf/0.1/givenName> "Félix" .
            <https://w3id.org/oc/meta/ra/0603> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .
            <https://w3id.org/oc/meta/id/0603> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .
            <https://w3id.org/oc/meta/id/0603> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "0000-0002-4487-6894" .
            <https://w3id.org/oc/meta/id/0603> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid> .
        '''
        expected_graph = Graph()
        expected_graph = expected_graph.parse(data=expected_data, format="nt")
        self.assertEqual(compare.isomorphic(output_graph, expected_graph), True)


class testcase_01(unittest.TestCase):
    def test(self):
        # testcase1: 2 different issues of the same venue (no volume)
        name = "01"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_02(unittest.TestCase):
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        name = "02"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_03(unittest.TestCase):
    def test(self):
        # testcase3: 2 different issues of the same volume
        name = "03"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_04(unittest.TestCase):
    def test(self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        name = "04"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_05(unittest.TestCase):
    def test(self):
        # testcase5: NO ID scenario
        name = "05"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)

class testcase_06(unittest.TestCase):
    def test(self):
        # testcase6: ALL types test
        name = "06"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_07(unittest.TestCase):
    def test(self):
        # testcase7: all journal related types with an editor
        name = "07"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_08(unittest.TestCase):
    def test(self):
        # testcase8: all book related types with an editor
        name = "08"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_09(unittest.TestCase):
    def test(self):
        # testcase9: all proceeding related types with an editor
        name = "09"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_10(unittest.TestCase):
    def test(self):
        # testcase10: a book inside a book series and a book inside a book set
        name = "10"
        test_graph, new_graph = prepare2test(name)
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


if __name__ == '__main__':
    unittest.main()
