import unittest, os, csv, shutil
from crossref.crossrefProcessing import crossrefProcessing
from run_preprocess import preprocess
from pprint import pprint

IOD = 'meta/tdd/crossrefProcessing/iod'
WANTED_DOIS = 'meta/tdd/crossrefProcessing'
DATA = 'meta/tdd/crossrefProcessing/40228.json'
DATA_DIR = 'meta/tdd/crossrefProcessing'
OUTPUT = 'meta/tdd/crossrefProcessing/meta_input'
MULTIPROCESS_OUTPUT = 'meta/tdd/crossrefProcessing/multi_process_test'

class TestCrossrefProcessing(unittest.TestCase):

    def test_csv_creator(self):
        crossref_processor = crossrefProcessing(IOD, WANTED_DOIS)
        csv_created = crossref_processor.csv_creator(DATA)
        expected_output = [
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
                "given": "Myung-Hee",
                "family": "Kim",
                "affiliation": []
            },
            {
                "given": "Jin-Seon",
                "family": "Seo",
                "affiliation": []
            },
            {
                "given": "Mi-Kyeong",
                "family": "Choi",
                "affiliation": []
            },
            {
                "given": "Eun-Young",
                "family": "Kim",
                "affiliation": []
            }
        ]
        crossref_processor = crossrefProcessing(IOD, WANTED_DOIS)
        authors_strings_list = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Kim, Myung-Hee', 'Seo, Jin-Seon', 'Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]', 'Kim, Eun-Young']
        self.assertEqual(authors_strings_list, expected_authors_list)
    
    def test_id_worker(self):
        field_issn = "ISSN 1050-124X"
        field_isbn = ["978-1-56619-909-4"]
        issn_list = list()
        isbn_list = list()
        crossrefProcessing.id_worker(field_issn, issn_list, crossrefProcessing.issn_worker)
        crossrefProcessing.id_worker(field_isbn, isbn_list, crossrefProcessing.isbn_worker)
        expected_issn_list = ['issn:1050-124X']
        expected_isbn_list = ['isbn:9781566199094']
        self.assertEqual((issn_list, isbn_list), (expected_issn_list, expected_isbn_list))

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


if __name__ == '__main__':
    unittest.main()
