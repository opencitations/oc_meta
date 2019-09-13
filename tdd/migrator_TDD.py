import unittest
from migrator import *
import csv


class TestMigrator(unittest.TestCase):

    # check if counter folder is empty before procede (Doing it automatically could be risky)
    def setUp(self):

        #data for testX
        with open("clean_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            dataX = [dict(x) for x in reader]
        self.migrator_processedX = Migrator(dataX)
        test_graphX = Graph()
        self.test_graphX = test_graphX.parse("example_graph.ttl", format="ttl")

        #data for testcase 1-2-3
        with open("new_test_clean_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            self.data123 = [dict(x) for x in reader]

        test_graph1 = Graph()
        self.test_graph1 = test_graph1.parse("testcase-01.ttl", format="ttl")

        test_graph2 = Graph()
        self.test_graph2 = test_graph2.parse("testcase-02.ttl", format="ttl")

        test_graph3 = Graph()
        self.test_graph3 = test_graph3.parse("testcase-03.ttl", format="ttl")



    def testX(self):
        #general test on example csv
        new_graph = self.migrator_processedX.final_graph

        self.assertEqual(new_graph, self.test_graphX)

    def testcase1(self):
        # testcase1: 2 different issues of the same venue (no volume)
        with open("new_clean_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data = [dict(x) for x in reader]

            partial_data1 = list()
            partial_data1 = list()
            partial_data1.append(data[0])
            partial_data1.append(data[5])

            migrator1 = Migrator(self.data123)
            new_graph = self.migrator_processed123.final_graph


if __name__ == '__main__':
    unittest.main()