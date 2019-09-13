import unittest
from converter import *
import csv


class TestConverter(unittest.TestCase):

    # check if counter folder is empty before procede (Doing it automatically could be risky)
    def setUp(self):
        with open("clean_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            self.data = [dict(x) for x in reader]



    def test(self):
        with open("test_data.csv", 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            newdata = [dict(x) for x in reader]
        newcleandata = converter(newdata).data

        self.assertEqual(self.data, newcleandata)

if __name__ == '__main__':
    unittest.main()