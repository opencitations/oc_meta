import unittest
from migrator import *
import csv
from rdflib.term import _toPythonMapping
from rdflib import XSD, compare

def reset():
    with open("counter/re.txt", 'w') as br:
        br.write('0')
    with open("counter/ar.txt", 'w') as br:
        br.write('0')

# The following function has been added for handling gYear and gYearMonth correctly.
# Source: https://github.com/opencitations/script/blob/master/ocdm/storer.py
# More info at https://github.com/RDFLib/rdflib/issues/806.

def hack_dates():
    if XSD.gYear in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYear)
    if XSD.gYearMonth in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYearMonth)


#migrator executor
def prepare2test(test_data_csv,index_ra, index_br, testcase ):
    reset()
    with open(test_data_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    migrator = Migrator(data, index_ra,index_br)

    test_graph = Graph()
    hack_dates()
    test_graph = test_graph.parse(testcase, format="ttl")
    new_graph = migrator.final_graph
    return test_graph, new_graph



class testcase_X(unittest.TestCase):

    def test(self):
        #general test on example csv
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_X_data.csv",
                                             "testcases/testcase_data/indices/indexX_ra.csv",
                                             "testcases/testcase_data/indices/indexX_br.csv",
                                             "testcases/testcase_X.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_01 (unittest.TestCase):
    def test (self):
        # testcase1: 2 different issues of the same venue (no volume)
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_01_data.csv",
                                             "testcases/testcase_data/indices/index1_ra.csv",
                                             "testcases/testcase_data/indices/index1_br.csv",
                                             "testcases/testcase_01.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_02(unittest.TestCase):
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_02_data.csv",
                                             "testcases/testcase_data/indices/index2_ra.csv",
                                             "testcases/testcase_data/indices/index2_br.csv",
                                             "testcases/testcase_02.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)




class testcase_03(unittest.TestCase):
    def test(self):
        # testcase3: 2 different issues of the same volume
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_03_data.csv",
                                             "testcases/testcase_data/indices/index3_ra.csv",
                                             "testcases/testcase_data/indices/index3_br.csv",
                                             "testcases/testcase_03.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)





class testcase_04 (unittest.TestCase):
    def test (self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_04_data.csv",
                                             "testcases/testcase_data/indices/index4_ra.csv",
                                             "testcases/testcase_data/indices/index4_br.csv",
                                             "testcases/testcase_04.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)




class testcase_05 (unittest.TestCase):
    def test (self):
        # testcase5: NO ID scenario
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_05_data.csv",
                                             "testcases/testcase_data/indices/index5_ra.csv",
                                             "testcases/testcase_data/indices/index5_br.csv",
                                             "testcases/testcase_05.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)



class testcase_06 (unittest.TestCase):
    def test (self):
        # testcase6: ALL types test
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_06_data.csv",
                                             "testcases/testcase_data/indices/index6_ra.csv",
                                             "testcases/testcase_data/indices/index6_br.csv",
                                             "testcases/testcase_06.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)




class testcase_07 (unittest.TestCase):
    def test (self):
        # testcase7: archival document in an archival document set
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_07_data.csv",
                                             "testcases/testcase_data/indices/index7_ra.csv",
                                             "testcases/testcase_data/indices/index7_br.csv",
                                             "testcases/testcase_07.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)



class testcase_08 (unittest.TestCase):
    def test (self):
        # testcase8: all journal related types with an editor
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_08_data.csv",
                                             "testcases/testcase_data/indices/index8_ra.csv",
                                             "testcases/testcase_data/indices/index8_br.csv",
                                             "testcases/testcase_08.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)



class testcase_09 (unittest.TestCase):
    def test (self):
        # testcase9: all book related types with an editor
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_09_data.csv",
                                             "testcases/testcase_data/indices/index9_ra.csv",
                                             "testcases/testcase_data/indices/index9_br.csv",
                                             "testcases/testcase_09.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_10 (unittest.TestCase):
    def test (self):
        # testcase10: all proceeding related types with an editor
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_10_data.csv",
                                             "testcases/testcase_data/indices/index10_ra.csv",
                                             "testcases/testcase_data/indices/index10_br.csv",
                                             "testcases/testcase_10.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)

class testcase_11 (unittest.TestCase):
    def test (self):
        # testcase11: a book inside a book series and a book inside a book set
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_11_data.csv",
                                             "testcases/testcase_data/indices/index11_ra.csv",
                                             "testcases/testcase_data/indices/index11_br.csv",
                                             "testcases/testcase_11.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)

def suite(testobj):
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(testobj))
    return test_suite


TestSuit=suite(testcase_11)

runner=unittest.TextTestRunner()
runner.run(TestSuit)