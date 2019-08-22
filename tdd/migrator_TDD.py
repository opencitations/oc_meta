import unittest
from migrator import *
import csv


class TestConverterDemo(unittest.TestCase):

    # check if counter folder is empty before procede (Doing it automatically could be risky)
    def setUp(self):
        with open("clean_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data = [dict(x) for x in reader]

        self.migrator_processed = Migrator(data)

        test_graph = Graph()
        test_graph = test_graph.parse("example_graph.ttl", format="ttl")
        self.test_graph_set = set()
        for x, y, z in test_graph:
            self.test_graph_set.update([(x, y, z)])


    def test(self):
        new_graph = self.migrator_processed.final_graph
        new_graph_set = set()
        for x, y, z in new_graph:
            new_graph_set.update([(x, y, z)])
        self.assertEqual(new_graph_set == self.test_graph_set, True)

if __name__ == '__main__':
    unittest.main()