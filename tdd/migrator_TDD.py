import unittest
from migrator import *
import csv
from rdflib.term import _toPythonMapping
from rdflib import XSD, compare



# The following function has been added for handling gYear and gYearMonth correctly.
# Source: https://github.com/opencitations/script/blob/master/ocdm/storer.py
# More info at https://github.com/RDFLib/rdflib/issues/806.

def hack_dates():
    if XSD.gYear in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYear)
    if XSD.gYearMonth in _toPythonMapping:
        _toPythonMapping.pop(XSD.gYearMonth)


#migrator executor
def prepare2test(test_data_csv,index_ra, index_br, index_re, index_ar, testcase ):
    with open(test_data_csv, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    migrator = Migrator(data, index_ra,index_br, index_re,index_ar)
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
                                             "testcases/testcase_data/indices/01/index_id_ra_01.csv",
                                             "testcases/testcase_data/indices/01/index_id_br_01.csv",
                                             "testcases/testcase_data/indices/01/index_re_01.csv",
                                             "testcases/testcase_data/indices/01/index_ar_01.csv",
                                             "testcases/testcase_01.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_02(unittest.TestCase):
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_02_data.csv",
                                             "testcases/testcase_data/indices/02/index_id_ra_02.csv",
                                             "testcases/testcase_data/indices/02/index_id_br_02.csv",
                                             "testcases/testcase_data/indices/02/index_re_02.csv",
                                             "testcases/testcase_data/indices/02/index_ar_02.csv",
                                             "testcases/testcase_02.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)




class testcase_03(unittest.TestCase):
    def test(self):
        # testcase3: 2 different issues of the same volume
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_03_data.csv",
                                             "testcases/testcase_data/indices/03/index_id_ra_03.csv",
                                             "testcases/testcase_data/indices/03/index_id_br_03.csv",
                                             "testcases/testcase_data/indices/03/index_re_03.csv",
                                             "testcases/testcase_data/indices/03/index_ar_03.csv",
                                             "testcases/testcase_03.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)





class testcase_04 (unittest.TestCase):
    def test (self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_04_data.csv",
                                             "testcases/testcase_data/indices/04/index_id_ra_04.csv",
                                             "testcases/testcase_data/indices/04/index_id_br_04.csv",
                                             "testcases/testcase_data/indices/04/index_re_04.csv",
                                             "testcases/testcase_data/indices/04/index_ar_04.csv",
                                             "testcases/testcase_04.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)




class testcase_05 (unittest.TestCase):
    def test (self):
        # testcase5: NO ID scenario
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_05_data.csv",
                                             "testcases/testcase_data/indices/05/index_id_ra_05.csv",
                                             "testcases/testcase_data/indices/05/index_id_br_05.csv",
                                             "testcases/testcase_data/indices/05/index_re_05.csv",
                                             "testcases/testcase_data/indices/05/index_ar_05.csv",
                                             "testcases/testcase_05.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)



class testcase_06 (unittest.TestCase):
    def test (self):
        # testcase6: ALL types test
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_06_data.csv",
                                             "testcases/testcase_data/indices/06/index_id_ra_06.csv",
                                             "testcases/testcase_data/indices/06/index_id_br_06.csv",
                                             "testcases/testcase_data/indices/06/index_re_06.csv",
                                             "testcases/testcase_data/indices/06/index_ar_06.csv",
                                             "testcases/testcase_06.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)




class testcase_07 (unittest.TestCase):
    def test (self):
        # testcase7: all journal related types with an editor
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_07_data.csv",
                                             "testcases/testcase_data/indices/07/index_id_ra_07.csv",
                                             "testcases/testcase_data/indices/07/index_id_br_07.csv",
                                             "testcases/testcase_data/indices/07/index_re_07.csv",
                                             "testcases/testcase_data/indices/07/index_ar_07.csv",
                                             "testcases/testcase_07.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)



class testcase_08 (unittest.TestCase):
    def test (self):
        # testcase8: all book related types with an editor
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_08_data.csv",
                                             "testcases/testcase_data/indices/08/index_id_ra_08.csv",
                                             "testcases/testcase_data/indices/08/index_id_br_08.csv",
                                             "testcases/testcase_data/indices/08/index_re_08.csv",
                                             "testcases/testcase_data/indices/08/index_ar_08.csv",
                                             "testcases/testcase_08.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)


class testcase_09 (unittest.TestCase):
    def test (self):
        # testcase9: all proceeding related types with an editor
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_09_data.csv",
                                             "testcases/testcase_data/indices/09/index_id_ra_09.csv",
                                             "testcases/testcase_data/indices/09/index_id_br_09.csv",
                                             "testcases/testcase_data/indices/09/index_re_09.csv",
                                             "testcases/testcase_data/indices/09/index_ar_09.csv",
                                             "testcases/testcase_09.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)

class testcase_10 (unittest.TestCase):
    def test (self):
        # testcase10: a book inside a book series and a book inside a book set
        test_graph, new_graph = prepare2test("testcases/testcase_data/testcase_10_data.csv",
                                             "testcases/testcase_data/indices/10/index_id_ra_10.csv",
                                             "testcases/testcase_data/indices/10/index_id_br_10.csv",
                                             "testcases/testcase_data/indices/10/index_re_10.csv",
                                             "testcases/testcase_data/indices/10/index_ar_10.csv",
                                             "testcases/testcase_10.ttl")
        self.assertEqual(compare.isomorphic(new_graph, test_graph), True)



def suite(testobj):
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(testobj))
    return test_suite


TestSuit=suite(testcase_10)

runner=unittest.TextTestRunner()
runner.run(TestSuit)