from posixpath import split
import unittest
import shutil
import os
from csv import DictReader
from meta.plugins.multiprocess.prepare_multiprocess import prepare_relevant_venues, split_by_publisher


BASE = os.path.join('meta', 'tdd', 'prepare_multiprocess')
TMP_DIR = os.path.join(BASE, 'tmp')
CSV_DIR = os.path.join(BASE, 'input')


class TestPrepareMultiprocess(unittest.TestCase):
    def test_prepare_relevant_venues(self):
        prepare_relevant_venues(csv_dir=CSV_DIR, output_dir=TMP_DIR, wanted_dois=None, items_per_file=1, verbose=False)
        with open(os.path.join(TMP_DIR, '1.csv'), 'r', encoding='utf-8') as f:
            output = list(DictReader(f))
        expected_output = [{'id': 'issn:1225-4339', 'title': 'The Korean Journal of Food And Nutrition', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)
        shutil.rmtree(TMP_DIR)

    def test_split_by_publisher(self):
        split_by_publisher(csv_dir=CSV_DIR, output_dir=TMP_DIR, verbose=False)
        output = dict()
        for file in os.listdir(TMP_DIR):
            with open(os.path.join(TMP_DIR, file), 'r', encoding='utf-8') as f:
                output[file] = list(DictReader(f))
        expected_output = {
            'crossref_4768.csv': [
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}
            ], 
            'crossref_6623.csv': [
                {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'component', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': ''}
            ]
        }
        self.assertEqual(output, expected_output)
        shutil.rmtree(TMP_DIR)

if __name__ == '__main__':
    unittest.main()