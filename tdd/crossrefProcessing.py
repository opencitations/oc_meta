import unittest
from crossref.crossrefProcessing import crossrefProcessing

class TestCrossrefProcessing(unittest.TestCase):

    def test_issn_worker(self):
        input = "ISSN 1050-124X"
        output = list()
        crossrefProcessing.issn_worker(input, output)
        expected_output = ["issn:1050-124X"]
        self.assertEqual(output, expected_output)

    def test_isbn_worker(self):
        input = "978-1-56619-909-4"
        output = list()
        crossrefProcessing.isbn_worker(input, output)
        expected_output = ["isbn:9781566199094"]
        self.assertEqual(output, expected_output)


if __name__ == '__main__':
    unittest.main()
