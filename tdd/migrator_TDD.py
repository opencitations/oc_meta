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


# !!!check if counter folder is empty before procede (Doing it automatically could be risky)!!!


class testcase_X(unittest.TestCase):

    def test(self):
        #general test on example csv

        with open("testcases/testcase_data/testcase_X_data.csv", 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            dataX = [dict(x) for x in reader]

        migrator_processedX = Migrator(dataX, "testcases/testcase_data/indices/indexX.txt")

        test_graphX = Graph()
        hack_dates()
        test_graphX = test_graphX.parse("testcases/testcase_X.ttl", format="ttl")

        new_graphX = migrator_processedX.final_graph

        self.assertEqual(compare.isomorphic(new_graphX, test_graphX), True)


class testcase_01 (unittest.TestCase):

    def test (self):
        # testcase1: 2 different issues of the same venue (no volume)
        with open("testcases/testcase_data/testcase_01_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data1 = [dict(x) for x in reader]

            migrator1 = Migrator(data1, "testcases/testcase_data/indices/index1.txt")

            test_graph1 = Graph()
            hack_dates()
            test_graph1 = test_graph1.parse("testcases/testcase_01.ttl", format="ttl")

            new_graph1 = migrator1.final_graph
            self.assertEqual(compare.isomorphic(new_graph1, test_graph1), True)


class testcase_02(unittest.TestCase):

    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        with open("testcases/testcase_data/testcase_02_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data2 = [dict(x) for x in reader]

            migrator2 = Migrator(data2, "testcases/testcase_data/indices/index2.txt")

            test_graph2 = Graph()
            hack_dates()
            test_graph2 = test_graph2.parse("testcases/testcase_02.ttl", format="ttl")

            new_graph2 = migrator2.final_graph
            self.assertEqual(compare.isomorphic(new_graph2, test_graph2), True)



class testcase_03(unittest.TestCase):

    def test(self):
        # testcase3: 2 different issues of the same volume
        with open("testcases/testcase_data/testcase_03_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data3 = [dict(x) for x in reader]

            migrator3 = Migrator(data3, "testcases/testcase_data/indices/index3.txt")

            test_graph3 = Graph()
            hack_dates()
            test_graph3 = test_graph3.parse("testcases/testcase_03.ttl", format="ttl")

            new_graph3 = migrator3.final_graph

            self.assertEqual(compare.isomorphic(new_graph3, test_graph3), True)





class testcase_04 (unittest.TestCase):

    def test (self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        with open("testcases/testcase_data/testcase_04_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data4 = [dict(x) for x in reader]

            migrator4 = Migrator(data4, "testcases/testcase_data/indices/index4.txt")

            test_graph4 = Graph()
            hack_dates()
            test_graph4 = test_graph4.parse("testcases/testcase_04.ttl", format="ttl")

            new_graph4 = migrator4.final_graph
            self.assertEqual(compare.isomorphic(new_graph4, test_graph4), True)




class testcase_05 (unittest.TestCase):

    def test (self):
        # testcase5: NO ID scenario
        with open("testcases/testcase_data/testcase_05_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data4 = [dict(x) for x in reader]

            migrator5 = Migrator(data4, "testcases/testcase_data/indices/index5.txt")

            test_graph5 = Graph()
            hack_dates()
            test_graph5 = test_graph5.parse("testcases/testcase_05.ttl", format="ttl")

            new_graph5 = migrator5.final_graph
            self.assertEqual(compare.isomorphic(new_graph5, test_graph5), True)




def suite(testobj):
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(testobj))
    return test_suite


TestSuit=suite(testcase_05)

runner=unittest.TextTestRunner()
runner.run(TestSuit)