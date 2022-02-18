from csv import DictReader
from meta.plugins.multiprocess.prepare_multiprocess import prepare_relevant_items, split_by_publisher
import os
import shutil
import unittest


BASE = os.path.join('meta', 'tdd', 'prepare_multiprocess')
TMP_DIR = os.path.join(BASE, 'tmp')
CSV_DIR = os.path.join(BASE, 'input')


class TestPrepareMultiprocess(unittest.TestCase):
    def test_prepare_relevant_venues(self):
        prepare_relevant_items(csv_dir=CSV_DIR, output_dir=TMP_DIR, items_per_file=3, verbose=False)
        output = list()
        for file in os.listdir(TMP_DIR):
            with open(os.path.join(TMP_DIR, file), 'r', encoding='utf-8') as f:
                output.extend(list(DictReader(f)))
        expected_output = [
            {'id': '', 'title': '', 'author': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:1225-4339', 'title': 'The Korean Journal of Food And Nutrition', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Kim, Young-Shik [orcid:0000-0001-5673-6314]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        ].sort(key=lambda x: x['id'])
        self.assertEqual(output.sort(key=lambda x: x['id']), expected_output)
        shutil.rmtree(TMP_DIR)

    def test_split_by_publisher(self):
        split_by_publisher(csv_dir=CSV_DIR, output_dir=TMP_DIR, verbose=False)
        output = dict()
        for file in os.listdir(TMP_DIR):
            with open(os.path.join(TMP_DIR, file), 'r', encoding='utf-8') as f:
                output[file] = list(DictReader(f))
        expected_output = {
            'crossref_4768.csv': [
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik [orcid:0000-0001-5673-6314]; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}], 
            'crossref_6623.csv': [{'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'component', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]'}]}
        self.assertEqual(output, expected_output)
        shutil.rmtree(TMP_DIR)

if __name__ == '__main__':
    unittest.main()