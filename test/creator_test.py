# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os

import orjson
import pytest
from oc_meta.core.creator import Creator
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.lib.finder import ResourceFinder
from rdflib import XSD, Graph, URIRef, compare
from rdflib.term import _toPythonMapping
from test.test_utils import (
    SERVER,
    get_counter_handler,
    reset_redis_counters,
    reset_triplestore,
)


def reset_server(server: str = SERVER) -> None:
    reset_triplestore(server)

# The following function has been added for handling gYear and gYearMonth correctly.
# Source: https://github.com/opencitations/script/blob/master/ocdm/storer.py
# More info at https://github.com/RDFLib/rdflib/issues/806.

def hack_dates():
    if XSD.gYear in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYear)
    if XSD.gYearMonth in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYearMonth)

def open_json(path):
    path = os.path.abspath(path)
    with open(path, "rb") as json_file:
        return orjson.loads(json_file.read())

# creator executor
def prepare2test(name):
    reset_server()
    reset_redis_counters()
    data = get_csv_data("test/testcases/testcase_data/testcase_" + name + "_data.csv")
    testcase_id_br = get_csv_data("test/testcases/testcase_data/indices/" + name + "/index_id_br_" + name + ".csv")
    testcase_id_ra = get_csv_data("test/testcases/testcase_data/indices/" + name + "/index_id_ra_" + name + ".csv")
    testcase_ar = get_csv_data("test/testcases/testcase_data/indices/" + name + "/index_ar_" + name + ".csv")
    testcase_re = get_csv_data("test/testcases/testcase_data/indices/" + name + "/index_re_" + name + ".csv")
    testcase_vi = open_json("test/testcases/testcase_data/indices/" + name + "/index_vi_" + name + ".json")
    testcase_ttl = "test/testcases/testcase_" + name + ".ttl"

    counter_handler = get_counter_handler()
    finder = ResourceFinder(ts_url=SERVER, base_iri="https://w3id.org/oc/meta/", local_g=Graph())
    creator = Creator(data, finder, "https://w3id.org/oc/meta/", counter_handler, "060", 'https://orcid.org/0000-0002-8420-0696', testcase_id_ra, testcase_id_br,
                      testcase_re, testcase_ar, testcase_vi)
    creator_setgraph = creator.creator()
    test_graph = Graph()
    hack_dates()
    test_graph = test_graph.parse(testcase_ttl, format="ttl")
    new_graph = Graph()
    for g in creator_setgraph.graphs():
        new_graph += g
    return test_graph, new_graph


class TestCreator:
    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        reset_server()
        reset_redis_counters()
        self.counter_handler = get_counter_handler()
        yield
        reset_redis_counters()

    def test_vvi_action(self):
        base_iri = 'https://w3id.org/oc/meta/'
        vvi = {'0602': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        finder = ResourceFinder(ts_url=SERVER, base_iri=base_iri, local_g=Graph())
        creator = Creator([], finder, base_iri, self.counter_handler, "060", 'https://orcid.org/0000-0002-8420-0696', [], [], [], [], vvi)
        creator.src = None
        creator.type = 'journal article'
        creator.br_graph = creator.setgraph.add_br(
            resp_agent='https://orcid.org/0000-0002-8420-0696',
            res=URIRef(f'{base_iri}br/0601'),
        )
        creator.vvi_action('OECD [omid:br/0602]', '107', '1')
        output_graph = Graph()
        for g in creator.setgraph.graphs():
            output_graph += g
        expected_data = '''
            <https://w3id.org/oc/meta/br/0602> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
            <https://w3id.org/oc/meta/br/0602> <http://purl.org/dc/terms/title> "OECD"^^<http://www.w3.org/2001/XMLSchema#string>.
            <https://w3id.org/oc/meta/br/0602> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal>.
            <https://w3id.org/oc/meta/br/4733> <http://purl.org/spar/fabio/hasSequenceIdentifier> "107"^^<http://www.w3.org/2001/XMLSchema#string>.
            <https://w3id.org/oc/meta/br/4733> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
            <https://w3id.org/oc/meta/br/4733> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0602>.
            <https://w3id.org/oc/meta/br/4733> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume>.
            <https://w3id.org/oc/meta/br/4734> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalIssue>.
            <https://w3id.org/oc/meta/br/4734> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/4733>.
            <https://w3id.org/oc/meta/br/4734> <http://purl.org/spar/fabio/hasSequenceIdentifier> "1"^^<http://www.w3.org/2001/XMLSchema#string>.
            <https://w3id.org/oc/meta/br/4734> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
            <https://w3id.org/oc/meta/br/0601> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/4734>.
            <https://w3id.org/oc/meta/br/0601> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>.
        '''
        expected_graph = Graph()
        expected_graph = expected_graph.parse(data=expected_data, format="nt")
        assert compare.isomorphic(output_graph, expected_graph) == True


class TestCase01:
    def test(self):
        # testcase1: 2 different issues of the same venue (no volume)
        name = "01"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase02:
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        name = "02"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase03:
    def test(self):
        # testcase3: 2 different issues of the same volume
        name = "03"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase04:
    def test(self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        name = "04"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase05:
    def test(self):
        # testcase5: NO ID scenario
        name = "05"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase06:
    def test(self):
        # testcase6: ALL types test
        name = "06"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase07:
    def test(self):
        # testcase7: all journal related types with an editor
        name = "07"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase08:
    def test(self):
        # testcase8: all book related types with an editor
        name = "08"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase09:
    def test(self):
        # testcase9: all proceeding related types with an editor
        name = "09"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True


class TestCase10:
    def test(self):
        # testcase10: a book inside a book series and a book inside a book set
        name = "10"
        test_graph, new_graph = prepare2test(name)
        assert compare.isomorphic(new_graph, test_graph) == True
