import unittest
from meta.orcid.index_orcid_doi import Index_orcid_doi
from csv import DictReader

CSV_PATH = 'meta/tdd/index_orcid_doi/output.csv'
CACHE_PATH = 'meta/tdd/cache.json'
SUMMARIES_PATH = 'meta/tdd/index_orcid_doi'

class test_Index_orcid_doi(unittest.TestCase):
    def test_explorer(self):
        iOd = Index_orcid_doi(csv_path=CSV_PATH, cache_path=CACHE_PATH)
        iOd.explorer(summaries_path=SUMMARIES_PATH, verbose=False)
        output = list(DictReader(open(CSV_PATH, 'r', encoding='utf-8')))
        expected_output = [
            {'id': '10.1016/j.indcrop.2020.112103', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2019/3213521', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.11.028', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.bioorg.2018.03.004', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1186/s13568-016-0300-2', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1016/j.toxicon.2014.04.010', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'},
            {'id': '10.1155/2014/691742', 'value': 'Gargouri, Ali [0000-0001-5009-9000]'}
        ]
        self.assertEqual(output, expected_output)


if __name__ == '__main__':
    unittest.main()
