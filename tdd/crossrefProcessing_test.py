import unittest, os, csv, shutil
from plugins.crossref.crossrefProcessing import crossrefProcessing
from meta.lib.jsonmanager import *
from run.crossref_process import preprocess
from pprint import pprint

BASE = 'meta\\tdd\\crossrefProcessing'
IOD = f'{BASE}\\iod'
WANTED_DOIS = BASE
DATA = f'{BASE}\\40228.json'
DATA_DIR = BASE
OUTPUT = f'{BASE}\\meta_input'
MULTIPROCESS_OUTPUT = f'{BASE}\\multi_process_test'
GZIP_INPUT = f'{BASE}\\gzip_test'

class TestCrossrefProcessing(unittest.TestCase):

    def test_csv_creator(self):
        crossref_processor = crossrefProcessing(IOD, WANTED_DOIS)
        data = load_json(DATA, None)
        csv_created = crossref_processor.csv_creator(data)
        expected_output = [
            {'id': 'doi:10.47886/9789251092637.ch7', 'title': 'Freshwater, Fish and the Future: Proceedings of the Global Cross-Sectoral Conference', 'author': '', 'pub_date': '2016', 'venue': 'Freshwater, Fish and the Future: Proceedings of the Global Cross-Sectoral Conference', 'volume': '', 'issue': '', 'page': '', 'type': 'book chapter', 'publisher': 'American Fisheries Society [crossref:460]', 'editor': 'Lymer, David; Marttin, Felix; Marmulla, Gerd; Bartley, Devin M.'},
            {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.105', 'title': 'A Study on Dietary Habit and Eating Snack Behaviors of Middle School Students with Different Obesity Indexes in Chungnam Area', 'author': 'Kim, Myung-Hee; Seo, Jin-Seon; Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]; Kim, Eun-Young', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '105-115', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.123', 'title': 'The Protective Effects of Chrysanthemum cornarium L. var. spatiosum Extract on HIT-T15 Pancreatic β-Cells against Alloxan-induced Oxidative Stress', 'author': 'Kim, In-Hye; Cho, Kang-Jin; Ko, Jeong-Sook; Kim, Jae-Hyun; Om, Ae-Son', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '123-131', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}
        ]
        self.assertEqual(csv_created, expected_output)

    def test_orcid_finder(self):
        crossref_processor = crossrefProcessing(IOD, WANTED_DOIS)
        orcid_found = crossref_processor.orcid_finder('10.9799/ksfan.2012.25.1.105')
        expected_output = {'0000-0002-6227-4053': 'choi, mi-kyeong'}
        self.assertEqual(orcid_found, expected_output)

    def test_get_agents_strings_list(self):
        authors_list = [
            {
                'given': 'Myung-Hee',
                'family': 'Kim',
                'affiliation': []
            },
            {
                'given': 'Jin-Seon',
                'family': 'Seo',
                'affiliation': []
            },
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': []
            },
            {
                'given': 'Eun-Young',
                'family': 'Kim',
                'affiliation': []
            }
        ]
        crossref_processor = crossrefProcessing(IOD, WANTED_DOIS)
        authors_strings_list = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Kim, Myung-Hee', 'Seo, Jin-Seon', 'Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]', 'Kim, Eun-Young']
        self.assertEqual(authors_strings_list, expected_authors_list)
    
    def test_id_worker(self):
        field_issn = 'ISSN 1050-124X'
        field_isbn = ['978-1-56619-909-4']
        issn_list = list()
        isbn_list = list()
        crossrefProcessing.id_worker(field_issn, issn_list, crossrefProcessing.issn_worker)
        crossrefProcessing.id_worker(field_isbn, isbn_list, crossrefProcessing.isbn_worker)
        expected_issn_list = ['issn:1050-124X']
        expected_isbn_list = ['isbn:9781566199094']
        self.assertEqual((issn_list, isbn_list), (expected_issn_list, expected_isbn_list))

    def test_issn_worker(self):
        input = 'ISSN 1050-124X'
        output = list()
        crossrefProcessing.issn_worker(input, output)
        expected_output = ['issn:1050-124X']
        self.assertEqual(output, expected_output)

    def test_isbn_worker(self):
        input = '978-1-56619-909-4'
        output = list()
        crossrefProcessing.isbn_worker(input, output)
        expected_output = ['isbn:9781566199094']
        self.assertEqual(output, expected_output)
    
    def test_preprocess(self):
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        preprocess(crossref_json_dir=MULTIPROCESS_OUTPUT, orcid_doi_filepath=IOD, csv_dir=OUTPUT, wanted_doi_filepath=None)
        output = list()
        for file in os.listdir(OUTPUT):
            with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
                output.append(list(csv.DictReader(f)))
        expected_output = [
            [
                {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': 'None', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'component', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': ''}
            ],
            [
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''},
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}
            ]
        ]
        self.assertEqual(output, expected_output)

    def test_gzip_input(self):
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        preprocess(crossref_json_dir=GZIP_INPUT, orcid_doi_filepath=IOD, csv_dir=OUTPUT, wanted_doi_filepath=BASE)
        output = list()
        for file in os.listdir(OUTPUT):
            with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
                output.append(list(csv.DictReader(f)))
        expected_output = [
            [
                {'id': 'doi:10.1001/.389', 'title': 'Decision Making at the Fringe of Evidence: Take What You Can Get', 'author': 'Col, N. F.', 'pub_date': '2006-2-27', 'venue': 'Archives of Internal Medicine [issn:0003-9926]', 'volume': '166', 'issue': '4', 'page': '389-390', 'type': 'journal article', 'publisher': 'American Medical Association (AMA) [crossref:10]', 'editor': ''}
            ], 
            [
                {'id': 'doi:10.1001/archderm.108.4.583b', 'title': 'Letter: Bleaching of hair after use of benzoyl peroxide acne lotions', 'author': 'Bleiberg, J.', 'pub_date': '1973-10-1', 'venue': 'Archives of Dermatology [issn:0003-987X]', 'volume': '108', 'issue': '4', 'page': '583b-583', 'type': 'journal article', 'publisher': 'American Medical Association (AMA) [crossref:10]', 'editor': ''}
            ]
        ]
        self.assertEqual(output, expected_output)

    def test_tar_gz_file(self):
        tar_gz_file_path = f'{BASE}/tar_gz_test/40228.tar.gz'
        result, targz_fd = get_all_files(tar_gz_file_path)
        for file in result:
            data = load_json(file, targz_fd)
            output = data['items'][0]['DOI']
        expected_output = '10.9799/ksfan.2012.25.1.069'
        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()
