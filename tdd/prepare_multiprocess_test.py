from csv import DictReader
from meta.plugins.multiprocess.prepare_multiprocess import prepare_relevant_items, split_by_publisher, _do_collective_merge, _get_relevant_venues
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
        for root, _, files in os.walk(TMP_DIR):
            for file in files:
                if file.endswith('.csv'):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        output.extend(list(DictReader(f)))
        expected_output = [
            {'id': '', 'title': '', 'author': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Kim, Young-Shik [orcid:0000-0001-5673-6314]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '1', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:1225-4339', 'title': 'The Korean Journal of Food And Nutrition', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}
        ]
        shutil.rmtree(TMP_DIR)
        self.assertEqual(sorted(output, key=lambda x: x['id']+x['title']+x['author']+x['issue']+x['volume']+x['type']), sorted(expected_output, key=lambda x: x['id']+x['title']+x['author']+x['issue']+x['volume']+x['type']))

    def test_split_by_publisher(self):
        split_by_publisher(csv_dir=CSV_DIR, output_dir=TMP_DIR, verbose=False)
        output = dict()
        for root, _, files in os.walk(TMP_DIR):
            for file in files:
                if file.endswith('.csv'):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        output[file] = list(DictReader(f))
        expected_output = {
            'crossref_4768.csv': [
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik [orcid:0000-0001-5673-6314]; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '26', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}], 
            'crossref_6623.csv': [{'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'component', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]'}]}
        self.assertEqual(output, expected_output)
        shutil.rmtree(TMP_DIR)
        
    def test__get_relevant_venues(self):
        items_by_id = dict()
        item_1 = {'venue': 'Venue [id:a id:b id:c]', 'volume': '1', 'issue': 'a'}
        item_2 = {'venue': 'Venue [id:a id:d]', 'volume': '2', 'issue': 'b'}
        item_3 = {'venue': 'Venue [id:e id:d]', 'volume': '3', 'issue': 'c'}
        item_4 = {'venue': 'Venue [id:e id:f issn:0000-0000]', 'volume': '4', 'issue': 'd'}
        items = [item_1, item_2, item_3, item_4]
        _get_relevant_venues(data= items, items_by_id=items_by_id)
        expected_output = {
            'id:a': {'others': {'id:b', 'id:c', 'id:d'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1', '2'}, 'issue': {'a', 'b'}}, 
            'id:b': {'others': {'id:c', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1'}, 'issue': {'a'}}, 
            'id:c': {'others': {'id:b', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1'}, 'issue': {'a'}}, 
            'id:d': {'others': {'id:e', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'2', '3'}, 'issue': {'b', 'c'}}, 
            'id:e': {'others': {'id:d', 'id:f'}, 'name': 'Venue', 'type': 'journal', 'volume': {'3', '4'}, 'issue': {'c', 'd'}},
            'id:f': {'others': {'id:e'}, 'name': 'Venue', 'type': 'journal', 'volume': {'4'}, 'issue': {'d'}}}
        self.assertEqual(items_by_id, expected_output)

    def test__do_collective_merge(self):
        items = {
            'id:a': {'others': {'id:b', 'id:c', 'id:d'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1', '2'}, 'issue': {'a', 'b'}}, 
            'id:b': {'others': {'id:c', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1'}, 'issue': {'a'}}, 
            'id:c': {'others': {'id:b', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1'}, 'issue': {'a'}}, 
            'id:d': {'others': {'id:e', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'2', '3'}, 'issue': {'b', 'c'}}, 
            'id:e': {'others': {'id:d', 'id:f'}, 'name': 'Venue', 'type': 'journal', 'volume': {'3', '4'}, 'issue': {'c', 'd'}},
            'id:f': {'others': {'id:e'}, 'name': 'Venue', 'type': 'journal', 'volume': {'4'}, 'issue': {'d'}},
            'id:h': {'others': {}, 'name': 'Other venue', 'type': 'journal', 'volume': {'5'}, 'issue': {'e'}}}
        output = _do_collective_merge(items)
        expected_output = {
            'id:a': {'name': 'Venue', 'type': 'journal', 'others': {'id:d', 'id:b', 'id:e', 'id:c', 'id:f'}, 'issue': {'a', 'c', 'd', 'b'}, 'volume': {'1', '2', '4', '3'}},
            'id:h': {'others': set(), 'name': 'Other venue', 'type': 'journal', 'volume': {'5'}, 'issue': {'e'}}}
        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()