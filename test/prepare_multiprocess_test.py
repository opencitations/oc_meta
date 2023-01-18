import os
import shutil
import unittest
from csv import DictReader

from oc_meta.plugins.multiprocess.prepare_multiprocess import (
    _do_collective_merge, _find_all_names, _get_duplicated_ids,
    _get_relevant_venues, _get_resp_agents, prepare_relevant_items)

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
            {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
            {'id': '', 'title': '', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'American Society for Microbiology [crossref:235]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'Journal of Bacteriology [issn:1098-5530 issn:0021-9193]', 'volume': '197', 'issue': '6', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''}]
        shutil.rmtree(TMP_DIR)
        self.assertEqual(sorted(output, key=lambda x: x['id']+x['title']+x['author']+x['editor']+x['venue']+x['issue']+x['volume']+x['type']), sorted(expected_output, key=lambda x: x['id']+x['title']+x['author']+x['editor']+x['venue']+x['issue']+x['volume']+x['type']))
        
    def test__get_duplicated_ids(self):
        data = [
            {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'book chapter', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'},
            {'id': 'doi:10.9799/uirca.2012.25.1.069', 'title': '', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Massari, Arcangelo'},
            {'id': 'issn:0098-7484 issn:0003-987X', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '50-55', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:0090-4295', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:2341-4022 issn:2341-4023', 'title': 'Acta urológica portuguesa', 'author': '', 'pub_date': '', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '', 'issue': '', 'page': '25', 'type': 'journal', 'publisher': '', 'editor': ''}, 
            {'id': 'issn:0098-7484', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '50-55', 'type': '', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': '', 'author': 'Peroni, Silvio', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': 'Massari, Arcangelo'},
            {'id': 'doi:10.9799/ksfan.2012.25.1.078', 'title': '', 'author': 'Peroni, Silvio', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': 'Peroni, Silvio'},
            {'id': 'doi:10.1128/jb.01991-14', 'title': 'Pseudomonas aeruginosa LysR PA4203 Regulator NmoR Acts as a Repressor of the PA4202            nmoA            Gene, Encoding a Nitronate Monooxygenase', 'author': 'Vercammen, Ken; Wei, Qing; Charlier, Daniel [orcid:0000-0002-6844-376X]; Dötsch, Andreas [orcid:0000-0001-9086-2584]; Haüssler, Susanne; Schulz, Sebastian; Salvi, Francesca [orcid:0000-0001-5294-1310]; Gadda, Giovanni; Spain, Jim; Rybtke, Morten Levin; Tolker-Nielsen, Tim [orcid:0000-0002-9751-474X]; Dingemans, Jozef [orcid:0000-0001-8079-3087]; Ye, Lumeng; Cornelis, Pierre', 'pub_date': '', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '1026-1039', 'type': 'journal article', 'publisher': 'American Society for Microbiology [crossref:235]', 'editor': "O'Toole, G. A."}
        ]
        ids_found = {'doi:10.9799/ksfan.2012.25.1.077'}
        items_by_id = dict()
        _get_duplicated_ids(data, ids_found, {'issn:1225-4339'}, items_by_id)
        expected_output = {
            'doi:10.9799/ksfan.2012.25.1.069': {'others': set(), 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'book chapter', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
            'doi:10.9799/ksfan.2012.25.1.077': {'others': set(), 'title': '', 'author': 'Peroni, Silvio', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': 'Massari, Arcangelo'}}
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
        _get_relevant_venues(data= items, ids_found={'issn:0098-7484': {'volumes': {'1': {'a'}}, 'issues': set()}, 'issn:2341-4022': {'volumes': dict(), 'issues': {'d', 'e'}}, 'isbn:9789089646491': {'volumes': dict(), 'issues': set()}, 'issn:1750-743X': {'volumes': dict(), 'issues': set()}}, items_by_id=items_by_id, duplicated_items=items_by_id)
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

    def test__get_resp_agents(self):
        items_by_id = dict()
        duplicated_items = dict()
        items = [
            {'author': 'Massari, [orcid:0000-0002-8420-0696]', 'editor': '', 'publisher': 'American Medical Association (AMA) [crossref:10 crossref:9999]'},
            {'author': '', 'editor': 'Massari, A. [orcid:0000-0002-8420-0696 viaf:1]', 'publisher': 'Elsevier BV [crossref:78]'},
            {'author': 'Massari, Arcangelo [viaf:1]', 'editor': '', 'publisher': 'Wiley [crossref:311]'},
            {'author': 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'editor': '', 'publisher': 'Wiley [crossref:311]'}
        ]
        _get_resp_agents(data=items, ids_found={'orcid:0000-0002-8420-0696', 'orcid:0000-0003-0530-4305', 'crossref:10'}, items_by_id=items_by_id, duplicated_items=duplicated_items)
        expected_output = {
            'orcid:0000-0002-8420-0696': {'others': {'viaf:1'}, 'name': 'Massari, A.', 'type': 'author'}, 
            'viaf:1': {'others': {'orcid:0000-0002-8420-0696'}, 'name': 'Massari, Arcangelo', 'type': 'editor'}, 
            'orcid:0000-0003-0530-4305': {'others': set(), 'name': 'Peroni, Silvio', 'type': 'author'},
            'crossref:10': {'others': {'crossref:9999'}, 'type': 'publisher', 'name': 'American Medical Association (AMA)'},
            'crossref:9999': {'others': {'crossref:10'}, 'type': 'publisher', 'name': 'American Medical Association (AMA)'}}
        self.assertEqual(duplicated_items, expected_output)
    
    def test__merge_publishers(self):
        publishers_by_id = {
            'crossref:10': {'others': {'crossref:9999'}, 'name': 'American Medical Association (AMA)', 'type': 'publisher'}, 
            'crossref:9999': {'others': {'crossref:10'}, 'name': 'American Medical Association (AMA)', 'type': 'publisher'},
            'crossref:78': {'others': set(), 'name': 'Elsevier BV', 'type': 'publisher'}, 
            'crossref:311': {'others': set(), 'name': 'Wiley', 'type': 'publisher'}}
        output = _do_collective_merge(publishers_by_id, publishers_by_id)
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
            'id:f': {'others': {'id:e'}, 'name': 'Venue', 'type': 'journal', 'volume': dict(), 'issue': {'vol. 17, n° 2', 'e'}},
            'orcid:0000-0002-8420-0696': {'others': {'viaf:1'}, 'name': 'Massari, A.', 'type': 'author'}, 
            'viaf:1': {'others': {'orcid:0000-0002-8420-0696'}, 'name': 'Massari, Arcangelo', 'type': 'author'}, 
            'orcid:0000-0003-0530-4305': {'others': set(), 'name': 'Peroni, Silvio', 'type': 'author'}}
        output = _do_collective_merge(items, items)
        expected_output = {
            'id:a': {'others': {'id:c', 'id:b', 'id:f', 'id:d', 'id:e'}, 'name': 'Venue', 'type': 'journal', 'volume': {'1': {'a'}, '2': {'b', 'c'}, '3': {'c'}}, 'issue': {'vol. 17, n° 2', 'd', 'e'}}, 
            'orcid:0000-0002-8420-0696': {'others': {'viaf:1'}, 'name': 'Massari, Arcangelo', 'type': 'author'}, 
            'orcid:0000-0003-0530-4305': {'others': set(), 'name': 'Peroni, Silvio', 'type': 'author'}}
        self.assertEqual(output, expected_output)
    
    def test___find_all_names(self):
        items_by_id = {
            'orcid:0000-0002-8420-0696': {'others': {'viaf:1', 'viaf:2'}, 'name': 'Arcangelo Massari', 'type': 'author'}, 
            'viaf:1': {'others': {'orcid:0000-0002-8420-0696', 'viaf:2'}, 'name': 'Massari, Arcangelo', 'type': 'author'}, 
            'viaf:2': {'others': {'viaf:1', 'orcid:0000-0002-8420-0696'}, 'name': 'Massari, A.', 'type': 'author'},
            'orcid:0000-0002-8420-0695': {'others': set(), 'name': 'Silvio Peroni', 'type': 'author'}}
        longest_name_1 = _find_all_names(items_by_id, ids_list = ['orcid:0000-0002-8420-0696', 'viaf:1', 'viaf:2'], cur_name='Arcangelo Massari')
        longest_name_2 = _find_all_names(items_by_id, ids_list = ['orcid:0000-0002-8420-0696', 'viaf:1', 'viaf:2'], cur_name='Massari, A.')
        longest_name_3 = _find_all_names(items_by_id, ids_list = ['orcid:0000-0002-8420-0696', 'viaf:1', 'viaf:2'], cur_name='Massari, Arcangelo')
        longest_name_4 = _find_all_names(items_by_id, ids_list = ['orcid:0000-0002-8420-0696', 'viaf:1', 'viaf:2'], cur_name='Massari, A')
        longest_name_5 = _find_all_names(items_by_id, ids_list = ['orcid:0000-0002-8420-0696', 'viaf:1', 'viaf:2'], cur_name='Massari,')
        longest_name_6 = _find_all_names(items_by_id, ids_list = ['orcorcid:0000-0002-8420-0695'], cur_name='Silvio Peroni')
        self.assertEqual((longest_name_1, longest_name_2, longest_name_3, longest_name_4, longest_name_5, longest_name_6), ('Massari, Arcangelo', 'Massari, Arcangelo', 'Massari, Arcangelo', 'Massari, Arcangelo', 'Massari, Arcangelo', 'Silvio Peroni'))

    # def test_split_csvs_in_chunk(self):
    #     CHUNK_SIZE = 4
    #     split_csvs_in_chunks(csv_dir=CSV_DIR, output_dir=TMP_DIR, chunk_size=CHUNK_SIZE, verbose=False)
    #     output = dict()
    #     for file in os.listdir(TMP_DIR):
    #         output[file] = get_csv_data(os.path.join(TMP_DIR, file))
    #     expected_outputs = [{
    #         '0.csv': [
    #             {'id': 'issn:1524-4539 issn:0009-7322', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
    #             {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]'}, 
    #             {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik [orcid:0000-0001-5673-6314]; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Massari,Arcangelo'}], 
    #         '1.csv': [
    #             {'id': 'doi:10.1128/jb.00727-18', 'title': 'Effect of the MotA(M206I) Mutation on Torque Generation and Stator Assembly in the            Salmonella            H            +            -Driven Flagellar Motor', 'author': 'Suzuki, Yuya', 'pub_date': '2019-3-15', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '', 'type': 'journal article', 'publisher': 'American Society for Microbiology [crossref:235]', 'editor': 'Mullineaux, Conrad W.'}, 
    #             {'id': 'doi:10.1128/jb.01991-14', 'title': 'Pseudomonas aeruginosa LysR PA4203 Regulator NmoR Acts as a Repressor of the PA4202            nmoA            Gene, Encoding a Nitronate Monooxygenase', 'author': 'Vercammen, Ken; Wei, Qing; Charlier, Daniel [orcid:0000-0002-6844-376X]', 'pub_date': '2015-3-15', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '1026-1039', 'type': 'journal article', 'publisher': 'American Society for Microbiology [crossref:235]', 'editor': "O'Toole, G. A."}, 
    #             {'id': 'doi:10.1128/jb.01991-15', 'title': '', 'author': 'Vercammen, Ken', 'pub_date': '', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': 'Peroni, Silvio'}, 
    #             {'id': 'doi:10.1093/ije/31.3.555', 'title': 'Teen pregnancy is not a public health crisis in the United States. It is time we made it one', 'author': 'Rich-Edwards, Janet', 'pub_date': '2002-6', 'venue': 'International Journal of Epidemiology [issn:1464-3685 issn:0300-5771]', 'volume': '31', 'issue': '3', 'page': '555-556', 'type': 'journal article', 'publisher': 'Oxford University Press (OUP) [crossref:286]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'doi:10.1073/pnas.152186199', 'title': 'Y586F mutation in murine leukemia virus reverse transcriptase decreases fidelity of DNA synthesis in regions associated with adenine-thymine tracts', 'author': 'Zhang, W.-H.; Svarovskaia, E. S.; Barr, R.; Pathak, V. K. [orcid:0000-0003-2441-8412]', 'pub_date': '2002-7-15', 'venue': 'International Journal of Epidemiology [issn:1464-3685 issn:0300-5771]', 'volume': '31', 'issue': '3', 'page': '10090-10095', 'type': 'journal article', 'publisher': 'Proceedings of the National Academy of Sciences [crossref:341]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}]},
    #         {'0.csv': [
    #             {'id': 'doi:10.1093/ije/31.3.555', 'title': 'Teen pregnancy is not a public health crisis in the United States. It is time we made it one', 'author': 'Rich-Edwards, Janet', 'pub_date': '2002-6', 'venue': 'International Journal of Epidemiology [issn:1464-3685 issn:0300-5771]', 'volume': '31', 'issue': '3', 'page': '555-556', 'type': 'journal article', 'publisher': 'Oxford University Press (OUP) [crossref:286]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'doi:10.1073/pnas.152186199', 'title': 'Y586F mutation in murine leukemia virus reverse transcriptase decreases fidelity of DNA synthesis in regions associated with adenine-thymine tracts', 'author': 'Zhang, W.-H.; Svarovskaia, E. S.; Barr, R.; Pathak, V. K. [orcid:0000-0003-2441-8412]', 'pub_date': '2002-7-15', 'venue': 'International Journal of Epidemiology [issn:1464-3685 issn:0300-5771]', 'volume': '31', 'issue': '3', 'page': '10090-10095', 'type': 'journal article', 'publisher': 'Proceedings of the National Academy of Sciences [crossref:341]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'issn:1524-4539 issn:0009-7322', 'title': 'Circulation', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}, 
    #             {'id': 'doi:10.1128/jb.00727-18', 'title': 'Effect of the MotA(M206I) Mutation on Torque Generation and Stator Assembly in the            Salmonella            H            +            -Driven Flagellar Motor', 'author': 'Suzuki, Yuya', 'pub_date': '2019-3-15', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '', 'type': 'journal article', 'publisher': 'American Society for Microbiology [crossref:235]', 'editor': 'Mullineaux, Conrad W.'}, 
    #             {'id': 'doi:10.1128/jb.01991-14', 'title': 'Pseudomonas aeruginosa LysR PA4203 Regulator NmoR Acts as a Repressor of the PA4202            nmoA            Gene, Encoding a Nitronate Monooxygenase', 'author': 'Vercammen, Ken; Wei, Qing; Charlier, Daniel [orcid:0000-0002-6844-376X]', 'pub_date': '2015-3-15', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '1026-1039', 'type': 'journal article', 'publisher': 'American Society for Microbiology [crossref:235]', 'editor': "O'Toole, G. A."}, 
    #             {'id': 'doi:10.1128/jb.01991-15', 'title': '', 'author': 'Vercammen, Ken', 'pub_date': '', 'venue': 'Journal of Bacteriology [issn:0021-9193 issn:1098-5530]', 'volume': '197', 'issue': '6', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': 'Peroni, Silvio'}], 
    #         '1.csv': [
    #             {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '26', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'NAIMI, ELMEHDI [orcid:0000-0002-4126-8519]'}, 
    #             {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #             {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik [orcid:0000-0001-5673-6314]; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '', 'issue': '2', 'page': '77-82', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Massari,Arcangelo'}]}]      
    #     output = {k:v.sort(key=lambda x: x['id']+x['title']+x['author']+x['venue']+x['issue']+x['volume']+x['type']) for k,v in output.items()}
    #     expected_outputs = [{k:v.sort(key=lambda x: x['id']+x['title']+x['author']+x['venue']+x['issue']+x['volume']+x['type']) for k,v in expected_output.items()} for expected_output in expected_outputs]
    #     shutil.rmtree(TMP_DIR)
    #     self.assertIn(output, expected_outputs)
    
    # def test__enrich_duplicated_ids_found(self):
    #     data = [{'id': 'issn:2341-4022', 'title': 'Acta urológica portuguesa 1', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '2012-3-31', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '25', 'issue': '1', 'page': '25', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}]
    #     items_by_id = {
    #         'issn:2341-4022': {'others': {'issn:2341-4023'}, 'title': 'Acta urológica portuguesa', 'author': 'Cheigh, Chan-Ick', 'pub_date': '2012-3-31', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '', 'issue': '', 'page': '25', 'type': 'journal article', 'publisher': '', 'editor': 'Chung, Myong-Soo'}, 
    #         'issn:2341-4023': {'others': {'issn:2341-4022'}, 'title': 'Acta urológica portuguesa', 'author': 'Cheigh, Chan-Ick', 'pub_date': '2012-3-31', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '', 'issue': '', 'page': '25', 'type': 'journal article', 'publisher': '', 'editor': 'Chung, Myong-Soo'}}
    #     _enrich_duplicated_ids_found(data, items_by_id)
    #     expected_output = {
    #         'issn:2341-4022': {'others': {'issn:2341-4023'}, 'title': 'Acta urológica portuguesa 1', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '2012-3-31', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '25', 'issue': '1', 'page': '25', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}, 
    #         'issn:2341-4023': {'others': {'issn:2341-4022'}, 'title': 'Acta urológica portuguesa 1', 'author': 'Cheigh, Chan-Ick [orcid:0000-0003-2542-5788]', 'pub_date': '2012-3-31', 'venue': 'Transit Migration in Europe [issn:0003-987X]', 'volume': '25', 'issue': '1', 'page': '25', 'type': 'journal article', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': 'Chung, Myong-Soo [orcid:0000-0002-9666-2513]'}}
    #     self.assertEqual(items_by_id, expected_output)
        

if __name__ == '__main__': # pragma: no cover
    unittest.main()