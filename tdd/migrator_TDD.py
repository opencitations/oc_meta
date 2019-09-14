import unittest
from migrator import *
import csv


class testcase_X(unittest.TestCase):

    # check if counter folder is empty before procede (Doing it automatically could be risky)

    def test(self):
        #general test on example csv

        with open("testcases/testcase_data/testcase_X_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            dataX = [dict(x) for x in reader]

        migrator_processedX = Migrator(dataX)

        test_graphX = Graph()
        test_graphX = test_graphX.parse("testcases/testcase_X.ttl", format="ttl")

        new_graph = migrator_processedX.final_graph

        self.assertEqual(new_graph, test_graphX)


class testcase_01 (unittest.TestCase):

    def test (self):
        # testcase1: 2 different issues of the same venue (no volume)
        with open("testcases/testcase_data/testcase_01_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data1 = [dict(x) for x in reader]

            migrator1 = Migrator(data1)

            test_graph1 = Graph()
            test_graph1 = test_graph1.parse("testcases/testcase_01.ttl", format="ttl")

            new_graph1 = migrator1.final_graph
            self.assertEqual(new_graph1, test_graph1)


class testcase_02(unittest.TestCase):

    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        with open("testcases/testcase_data/testcase_02_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data2 = [dict(x) for x in reader]

            migrator2 = Migrator(data2)

            test_graph2 = Graph()
            test_graph2 = test_graph2.parse("testcases/testcase_02.ttl", format="ttl")

            new_graph2 = migrator2.final_graph
            self.assertEqual(new_graph2, test_graph2)



class testcase_03(unittest.TestCase):

    def test(self):
        # testcase3: 2 different issues of the same volume
        with open("testcases/testcase_data/testcase_03_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data3 = [dict(x) for x in reader]

            migrator3 = Migrator(data3)

            test_graph3 = Graph()
            test_graph3 = test_graph3.parse("testcases/testcase_03.ttl", format="ttl")
            
            new_graph3 = migrator3.final_graph
            self.assertEqual(new_graph3, test_graph3)




def suite(testobj):
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(testobj))
    return test_suite


mySuit=suite(testcase_03)

runner=unittest.TextTestRunner()
runner.run(mySuit)