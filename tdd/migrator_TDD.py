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
        self.test_graph = test_graph.parse("example_graph.ttl", format="ttl")



    def test(self):
        new_graph = self.migrator_processed.final_graph

        #new_graph.serialize(destination='output.ttl', format='ttl')
        self.assertEqual(new_graph, self.test_graph)

if __name__ == '__main__':
    unittest.main()