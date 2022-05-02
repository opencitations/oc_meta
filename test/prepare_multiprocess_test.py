from csv import DictReader
from oc_meta.lib.file_manager import get_data
from oc_meta.plugins.multiprocess.prepare_multiprocess import prepare_relevant_items, _do_collective_merge, _get_relevant_venues, _get_resp_agents, _get_publishers, _get_duplicated_ids, split_csvs_in_chunks
from pprint import pprint
from sys import platform
import os
import shutil
import unittest


BASE = os.path.join('test', 'prepare_multiprocess')
TMP_DIR = os.path.join(BASE, 'tmp')
CSV_DIR = os.path.join(BASE, 'input')


class TestPrepareMultiprocess(unittest.TestCase):
    def test_prepare_relevant_items(self):
        prepare_relevant_items(csv_dir=CSV_DIR, output_dir=TMP_DIR, items_per_file=3, verbose=False)
        output = list()
        for root, _, files in os.walk(TMP_DIR):
            for file in files:
                if file.endswith('.csv'):
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        output.extend(list(DictReader(f)))
        expected_output = [
            {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '69-76', 'type': 'journal article', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''}]
        shutil.rmtree(TMP_DIR)
        self.assertEqual(sorted(output, key=lambda x: x['id']+x['title']+x['author']+x['issue']+x['volume']+x['type']), sorted(expected_output, key=lambda x: x['id']+x['title']+x['author']+x['issue']+x['volume']+x['type']))
        
    def test__get_duplicated_ids(self):
        data = [
            {'id': 'issn:0098-7484 issn:0003-987X', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '50-55', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:0090-4295', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:2341-4022 issn:2341-4023', 'title': 'Acta urológica portuguesa', 'author': '', 'pub_date': '', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '', 'issue': '', 'page': '25', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:0098-7484', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '50-55', 'type': '', 'publisher': '', 'editor': ''}]
        ids_found = {'issn:2341-4022'}
        items_by_id = dict()
        _get_duplicated_ids(data, ids_found, items_by_id)
        expected_output = {
            'issn:2341-4022': {'others': {'issn:2341-4023'}, 'name': 'Acta urológica portuguesa', 'page': '25', 'type': 'journal'}, 
            'issn:2341-4023': {'others': {'issn:2341-4022'}, 'name': 'Acta urológica portuguesa', 'page': '25', 'type': 'journal'}}
        self.assertEqual(items_by_id, expected_output)
    
    def test__get_relevant_venues(self):
        items_by_id = dict()
        self.maxDiff = None
        item_1 = {'venue': 'Venue [issn:0098-7484 issn:0003-987X issn:0041-1345]', 'volume': '1', 'issue': 'a', 'type': 'journal article'}
        item_2 = {'venue': 'Venue [issn:0098-7484 issn:0040-6090]', 'volume': '2', 'issue': 'b', 'type': 'journal article'}
        item_3 = {'venue': 'Venue [issn:0090-4295 issn:0040-6090]', 'volume': '3', 'issue': 'c', 'type': 'journal article'}
        item_4 = {'venue': 'Venue [issn:0090-4295 issn:2341-4022 issn:0000-0000]', 'volume': '', 'issue': 'd', 'type': 'journal article'}
        item_5 = {'venue': 'Venue [issn:2341-4022]', 'volume': '', 'issue': 'e', 'type': 'journal article'}
        item_6 = {'id': 'isbn:9789089646491', 'title': 'Transit Migration in Europe', 'venue': '', 'volume': '', 'issue': '', 'type': 'book'}
        item_7 = {'id': 'isbn:9789089646491', 'title': 'Transit Migration in Europe', 'venue': 'An Introduction to Immigrant Incorporation Studies [issn:1750-743X]', 'volume': '', 'issue': '', 'type': 'book'}
        items = [item_1, item_2, item_3, item_4, item_5, item_6, item_7]
        _get_relevant_venues(data= items, ids_found={'issn:0098-7484': {'volumes': {'1': {'a'}}, 'issues': set()}, 'issn:2341-4022': {'volumes': dict(), 'issues': {'d', 'e'}}, 'isbn:9789089646491': {'volumes': dict(), 'issues': set()}, 'issn:1750-743X': {'volumes': dict(), 'issues': set()}}, items_by_id=items_by_id, overlapping_ids=dict())
        expected_output = {
            'issn:0098-7484': {'others': {'issn:0041-1345', 'issn:0040-6090', 'issn:0003-987X'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}}, 'issue': set()}, 
            'issn:0003-987X': {'others': {'issn:0041-1345', 'issn:0098-7484'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}}, 'issue': set()}, 
            'issn:0041-1345': {'others': {'issn:0098-7484', 'issn:0003-987X'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}}, 'issue': set()}, 
            'issn:0040-6090': {'others': {'issn:0090-4295', 'issn:0098-7484'}, 'name': 'Venue', 'type': 'journal', 'volume': {}, 'issue': set()}, 
            'issn:0090-4295': {'others': {'issn:2341-4022', 'issn:0040-6090'}, 'name': 'Venue', 'type': 'journal', 'volume': {}, 'issue': {'d'}}, 
            'issn:2341-4022': {'others': {'issn:0090-4295'}, 'name': 'Venue', 'type': 'journal', 'volume': {}, 'issue': {'d', 'e'}}, 
            'isbn:9789089646491': {'others': set(), 'name': 'Transit Migration in Europe', 'type': 'book', 'volume': {}, 'issue': set()}, 
            'issn:1750-743X': {'others': set(), 'name': 'An Introduction to Immigrant Incorporation Studies', 'type': 'book series', 'volume': {}, 'issue': set()}}
        self.assertEqual(items_by_id, expected_output)

    def test__get_publishers(self):
        items_by_id = dict()
        self.maxDiff = None
        items = [
            {'id': '', 'title': '', 'author': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'American Medical Association (AMA) [crossref:10 crossref:9999]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Elsevier BV [crossref:78]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Wiley [crossref:311]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': 'Kim, Young-Shik [orcid:0000-0001-5673-6314]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Wiley [crossref:311]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''}]
        _get_publishers(data=items, ids_found={'crossref:10'}, items_by_id=items_by_id)
        expected_output = {
            'crossref:10': {'others': {'crossref:9999'}, 'name': 'American Medical Association (AMA)', 'type': 'publisher'}, 
            'crossref:9999': {'others': {'crossref:10'}, 'name': 'American Medical Association (AMA)', 'type': 'publisher'}}
        self.assertEqual(items_by_id, expected_output)

    def test__get_resp_agents(self):
        items_by_id = dict()
        item_1 = {'author': 'Massari, Arcangelo [orcid:0000-0002-8420-0696]', 'editor': ''}
        item_2 = {'author': '', 'editor': 'Massari, A. [orcid:0000-0002-8420-0696 viaf:1]'}
        item_3 = {'author': 'Massari, A [viaf:1]', 'editor': ''}
        item_4 = {'author': 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'editor': ''}
        items = [item_1, item_2, item_3, item_4]
        _get_resp_agents(data=items, ids_found={'orcid:0000-0002-8420-0696', 'orcid:0000-0003-0530-4305'}, items_by_id=items_by_id)
        expected_output = {
            'orcid:0000-0002-8420-0696': {'others': {'viaf:1'}, 'name': 'Massari, Arcangelo', 'type': 'author'}, 
            'viaf:1': {'others': {'orcid:0000-0002-8420-0696'}, 'name': 'Massari, A.', 'type': 'author'}, 
            'orcid:0000-0003-0530-4305': {'others': set(), 'name': 'Peroni, Silvio', 'type': 'author'}}
        self.assertEqual(items_by_id, expected_output)
    
    def test__merge_publishers(self):
        publishers_by_id = {
            'crossref:10': {'others': {'crossref:9999'}, 'name': 'American Medical Association (AMA)', 'type': 'publisher'}, 
            'crossref:9999': {'others': {'crossref:10'}, 'name': 'American Medical Association (AMA)', 'type': 'publisher'},
            'crossref:78': {'others': set(), 'name': 'Elsevier BV', 'type': 'publisher'}, 
            'crossref:311': {'others': set(), 'name': 'Wiley', 'type': 'publisher'}}
        output = _do_collective_merge(publishers_by_id)
        expected_output = {
            'crossref:10': {'name': 'American Medical Association (AMA)', 'type': 'publisher', 'others': {'crossref:9999'}}, 
            'crossref:78': {'name': 'Elsevier BV', 'type': 'publisher', 'others': set()}, 
            'crossref:311': {'name': 'Wiley', 'type': 'publisher', 'others': set()}}
        self.assertEqual(output, expected_output)

    def test__do_collective_merge(self):
        items = {
            'id:a': {'others': {'id:c', 'id:d', 'id:b'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}, '2': {'b'}}, 'issue': set()}, 
            'id:b': {'others': {'id:c', 'id:a'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}}, 'issue': set()}, 
            'id:c': {'others': {'id:a', 'id:b'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}}, 'issue': set()}, 
            'id:d': {'others': {'id:a', 'id:e'}, 'name': 'Venue', 'type': 'journal', 'volume': {'2': {'c'}, '3': {'c'}}, 'issue': set()}, 
            'id:e': {'others': {'id:f', 'id:d'}, 'name': 'Venue', 'type': 'journal', 'volume': {'3': {'c'}}, 'issue': {'d'}}, 
            'id:f': {'others': {'id:e'}, 'name': 'Venue', 'type': 'journal', 'volume': dict(), 'issue': {'vol. 17, n° 2', 'e'}}}
        output = _do_collective_merge(items)
        vi_number = 0
        for _, data in output.items():
            for _, issues in data['volume'].items():
                if issues:
                    vi_number += len(issues)
                elif not issues:
                    vi_number += 1
            vi_number += len(data['issue'])
        expected_output = {'id:a': {'name': 'Venue', 'type': 'journal', 'others': {'id:c', 'id:f', 'id:e', 'id:b', 'id:d'}, 'volume': {'1': {'a'}, '2': {'b', 'c'}, '3': {'c'}}, 'issue': {'vol. 17, n° 2', 'e', 'd'}}}
        self.assertEqual(output, expected_output)

    def test_split_csvs_in_chunk(self):
        CHUNK_SIZE = 4
        split_csvs_in_chunks(csv_dir=CSV_DIR, output_dir=TMP_DIR, chunk_size=CHUNK_SIZE, verbose=False)
        output = dict()
        for file in os.listdir(TMP_DIR):
            output[file] = get_data(os.path.join(TMP_DIR, file))
        is_unix = platform in {'linux', 'linux2', 'darwin'}
        expected_output_unix = {
            '0.csv': [
                {'id': 'issn:1524-4539 issn:0009-7322', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
                {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik [orcid:0000-0001-5673-6314]; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}]}
        expected_output_windows = {
            '0.csv': [
                {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik [orcid:0000-0001-5673-6314]; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}],
            '1.csv': [
                {'id': 'issn:1524-4539 issn:0009-7322', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}
            ]}
        expected_output = expected_output_unix if is_unix else expected_output_windows
        shutil.rmtree(TMP_DIR)
        self.assertEqual(output, expected_output)
        

if __name__ == '__main__':
    unittest.main()