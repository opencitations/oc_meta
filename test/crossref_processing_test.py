import unittest, os, csv, shutil
from oc_meta.plugins.crossref.crossref_processing import CrossrefProcessing
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.jsonmanager import *
from oc_meta.run.crossref_process import preprocess
from pprint import pprint

BASE = os.path.join('test', 'crossref_processing')
IOD = os.path.join(BASE, 'iod')
WANTED_DOIS = os.path.join(BASE, 'wanted_dois.csv')
WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois')
DATA = os.path.join(BASE, '40228.json')
DATA_DIR = BASE
OUTPUT = os.path.join(BASE, 'meta_input')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
GZIP_INPUT = os.path.join(BASE, 'gzip_test')
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')

class TestCrossrefProcessing(unittest.TestCase):

    def test_csv_creator(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, publishers_filepath=None)
        data = load_json(DATA, None)
        output = list()
        for item in data['items']:
            tabular_data = crossref_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)
        expected_output = [
            {'id': 'doi:10.47886/9789251092637.ch7', 'title': 'Freshwater, Fish and the Future: Proceedings of the Global Cross-Sectoral Conference', 'author': '', 'pub_date': '2016', 'venue': 'Freshwater, Fish and the Future: Proceedings of the Global Cross-Sectoral Conference', 'volume': '', 'issue': '', 'page': '', 'type': 'book chapter', 'publisher': 'American Fisheries Society [crossref:460]', 'editor': 'Lymer, David; Food and Agriculture Organization of the United Nations Fisheries and Aquaculture Department Viale delle Terme di Caracalla Rome 00153 Italy; Marttin, Felix; Marmulla, Gerd; Bartley, Devin M.'},
            {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick [orcid:0000-0002-6227-4053]; Mun, Ji-Hye [orcid:0000-0002-6227-4053]; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.105', 'title': 'A Study on Dietary Habit and Eating Snack Behaviors of Middle School Students with Different Obesity Indexes in Chungnam Area', 'author': 'Kim, Myung-Hee; Seo, Jin-Seon; Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]; Kim, Eun-Young', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '105-115', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}, 
            {'id': 'doi:10.9799/ksfan.2012.25.1.123', 'title': 'The Protective Effects of Chrysanthemum cornarium L. var. spatiosum Extract on HIT-T15 Pancreatic β-Cells against Alloxan-induced Oxidative Stress', 'author': 'Kim, In-Hye; Cho, Kang-Jin; Ko, Jeong-Sook; Kim, Jae-Hyun; Om, Ae-Son', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '123-131', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}
        ]
        self.assertEqual(output, expected_output)

    def test_orcid_finder(self):
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS)
        orcid_found = crossref_processor.orcid_finder('10.9799/ksfan.2012.25.1.105')
        expected_output = {'0000-0002-6227-4053': 'choi, mi-kyeong'}
        self.assertEqual(orcid_found, expected_output)

    def test_get_agents_strings_list_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        authors_list = [
            {
                "given": "Puvaneswari",
                "family": "Paravamsivam",
                "sequence": "first",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Chua Kek",
                "family": "Heng",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Sri Nurestri Abdul",
                "family": "Malek",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Vikineswary",
                "family": "Sabaratnam",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Ravishankar Ram",
                "family": "M",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            },
            {
                "given": "Sri Nurestri Abdul",
                "family": "Malek",
                "sequence": "additional",
                "affiliation": [],
                "role": "editor"
            },
            {
                "given": "Umah Rani",
                "family": "Kuppusamy",
                "sequence": "additional",
                "affiliation": [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(None, None)
        csv_manager = CSVManager()
        csv_manager.data = {'10.9799/ksfan.2012.25.1.105': {'Malek, Sri Nurestri Abdul [0000-0001-6278-8559]'}}
        crossref_processor.orcid_index = csv_manager
        authors_strings_list, editors_strings_list = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Paravamsivam, Puvaneswari', 'Heng, Chua Kek', 'Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]', 'Sabaratnam, Vikineswary', 'M, Ravishankar Ram', 'Kuppusamy, Umah Rani']
        expected_editors_list = ['Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]']
        self.assertEqual((authors_strings_list, editors_strings_list), (expected_authors_list, expected_editors_list))

    def test_get_agents_strings_list(self):
        authors_list = [
            {
                'given': 'Myung-Hee',
                'family': 'Kim',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Jin-Seon',
                'family': 'Seo',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Eun-Young',
                'family': 'Kim',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Kim, Myung-Hee', 'Seo, Jin-Seon', 'Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]', 'Kim, Eun-Young']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_same_family(self):
        # Two authors have the same family name and the same given name initials
        authors_list = [
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Hong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Choi, Mi-Kyeong [orcid:0000-0002-6227-4053]', 'Choi, Mi-Hong']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_homonyms(self):
        # Two authors have the same family name and the same given name
        authors_list = [
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Kyeong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Choi, Mi-Kyeong', 'Choi, Mi-Kyeong']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_inverted_names(self):
        # One author with an ORCID has as a name the surname of another
        authors_list = [
            {
                'given': 'Choi',
                'family': 'Mi-Kyeong',
                'affiliation': [],
                "role": "author"
            },
            {
                'given': 'Mi-Hong',
                'family': 'Choi',
                'affiliation': [],
                "role": "author"
            }
        ]
        crossref_processor = CrossrefProcessing(IOD, WANTED_DOIS)
        authors_strings_list, _ = crossref_processor.get_agents_strings_list('10.9799/ksfan.2012.25.1.105', authors_list)
        expected_authors_list = ['Mi-Kyeong, Choi', 'Choi, Mi-Hong']
        self.assertEqual(authors_strings_list, expected_authors_list)
    
    def test_id_worker(self):
        field_issn = 'ISSN 1050-124X'
        field_isbn = ['978-1-56619-909-4']
        issn_list = list()
        isbn_list = list()
        CrossrefProcessing.id_worker(field_issn, issn_list, CrossrefProcessing.issn_worker)
        CrossrefProcessing.id_worker(field_isbn, isbn_list, CrossrefProcessing.isbn_worker)
        expected_issn_list = ['issn:1050-124X']
        expected_isbn_list = ['isbn:9781566199094']
        self.assertEqual((issn_list, isbn_list), (expected_issn_list, expected_isbn_list))

    def test_issn_worker(self):
        input = 'ISSN 1050-124X'
        output = list()
        CrossrefProcessing.issn_worker(input, output)
        expected_output = ['issn:1050-124X']
        self.assertEqual(output, expected_output)

    def test_isbn_worker(self):
        input = '978-1-56619-909-4'
        output = list()
        CrossrefProcessing.isbn_worker(input, output)
        expected_output = ['isbn:9781566199094']
        self.assertEqual(output, expected_output)
    
    def test_preprocess(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        preprocess(crossref_json_dir=MULTIPROCESS_OUTPUT, publishers_filepath=None, orcid_doi_filepath=IOD, csv_dir=OUTPUT, wanted_doi_filepath=None)
        output = dict()
        for file in os.listdir(OUTPUT):
            with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output = {
            '40228.csv': [
                {'id': 'doi:10.9799/ksfan.2012.25.1.069', 'title': 'Nonthermal Sterilization and Shelf-life Extension of Seafood Products by Intense Pulsed Light Treatment', 'author': 'Cheigh, Chan-Ick; Mun, Ji-Hye; Chung, Myong-Soo', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '69-76', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''},
                {'id': 'doi:10.9799/ksfan.2012.25.1.077', 'title': 'Properties of Immature Green Cherry Tomato Pickles', 'author': 'Koh, Jong-Ho; Shin, Hae-Hun; Kim, Young-Shik; Kook, Moo-Chang', 'pub_date': '2012-3-31', 'venue': 'The Korean Journal of Food And Nutrition [issn:1225-4339]', 'volume': '25', 'issue': '1', 'page': '77-82', 'type': 'journal article', 'publisher': 'The Korean Society of Food and Nutrition [crossref:4768]', 'editor': ''}
            ],
            '30719.csv': [
                {'id': 'doi:10.17117/na.2015.08.1067', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'component', 'publisher': 'Consulting Company Ucom [crossref:6623]', 'editor': ''}
            ]
        }
        self.assertEqual(output, expected_output)

    def test_gzip_input(self):
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        preprocess(crossref_json_dir=GZIP_INPUT, publishers_filepath=None, orcid_doi_filepath=IOD, csv_dir=OUTPUT, wanted_doi_filepath=WANTED_DOIS)
        output = dict()
        for file in os.listdir(OUTPUT):
            with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output = {
            '0.csv': [{'id': 'doi:10.1001/.389', 'title': 'Decision Making at the Fringe of Evidence: Take What You Can Get', 'author': 'Col, N. F.', 'pub_date': '2006-2-27', 'venue': 'Archives of Internal Medicine [issn:0003-9926]', 'volume': '166', 'issue': '4', 'page': '389-390', 'type': 'journal article', 'publisher': 'American Medical Association (AMA) [crossref:10]', 'editor': ''}],
            '1.csv': [{'id': 'doi:10.1001/archderm.108.4.583b', 'title': 'Letter: Bleaching of hair after use of benzoyl peroxide acne lotions', 'author': 'Bleiberg, J.', 'pub_date': '1973-10-1', 'venue': 'Archives of Dermatology [issn:0003-987X]', 'volume': '108', 'issue': '4', 'page': '583-583', 'type': 'journal article', 'publisher': 'American Medical Association (AMA) [crossref:10]', 'editor': ''}]
        }
        self.assertEqual(output, expected_output)

    def test_tar_gz_file(self):
        tar_gz_file_path = f'{BASE}/tar_gz_test/40228.tar.gz'
        result, targz_fd = get_all_files(tar_gz_file_path)
        for file in result:
            data = load_json(file, targz_fd)
            output = data['items'][0]['DOI']
        expected_output = '10.9799/ksfan.2012.25.1.069'
        self.assertEqual(output, expected_output)
    
    def test_load_publishers_mapping(self):
        output = CrossrefProcessing.load_publishers_mapping(publishers_filepath=PUBLISHERS_MAPPING)
        expected_output = {
            '1': {'name': 'Annals of Family Medicine', 'prefixes': {'10.1370'}},
            '2': {'name': 'American Association of Petroleum Geologists AAPG/Datapages', 'prefixes': {'10.15530', '10.1306'}},
            '3': {'name': 'American Association of Physics Teachers (AAPT)','prefixes': {'10.1119'}},
            '6': {'name': 'American College of Medical Physics (ACMP)','prefixes': {'10.1120'}},
            '9': {'name': 'Allen Press', 'prefixes': {'10.1043'}},
            '10': {'name': 'American Medical Association (AMA)', 'prefixes': {'10.1001'}},
            '11': {'name': 'American Economic Association', 'prefixes': {'10.1257'}},
            '460': {'name': 'American Fisheries Society', 'prefixes': {'10.1577', '10.47886'}}
        }
        self.assertEqual(output, expected_output)
    
    def test_get_publisher_name(self):
        # The item's member is in the publishers' mapping
        item = {
            'publisher': 'American Fisheries Society',
            'DOI': '10.47886\/9789251092637.ch7',
            'prefix': '10.47886',
            'member': '460'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')

    def test_get_publisher_name_no_member(self):
        # The item has no member, but the DOI prefix is the publishers' mapping
        item = {
            'publisher': 'American Fisheries Society',
            'DOI': '10.47886/9789251092637.ch7',
            'prefix': '10.47886'
        }
        doi = '10.47886/9789251092637.ch7'
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = crossref_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'American Fisheries Society [crossref:460]')
    
    def test_get_venue_name(self):
        item = {
            'container-title': ['Cerebrospinal Fluid [Working Title]'],
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = crossref_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'Cerebrospinal Fluid [Working Title]')

    def test_get_venue_name_with_ISSN(self):
        item = {
            'container-title': ["troitel'stvo: nauka i obrazovanie [Construction: Science and Education]"],
            'ISSN': '2305-5502'
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = crossref_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, "troitel'stvo: nauka i obrazovanie [Construction: Science and Education] [issn:2305-5502]")
    
    def test_get_pages(self):
        item = {
            'page': '469-476'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_pages(item)
        self.assertEqual(pages, '469-476')

    def test_get_pages_right_letter(self):
        item = {
            'page': 'G22'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_pages(item)
        self.assertEqual(pages, 'G22')

    def test_get_pages_wrong_letter(self):
        item = {
            'page': '583b-584'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_pages(item)
        self.assertEqual(pages, '583-584')

    def test_get_pages_roman_letters(self):
        item = {
            'page': 'iv-l'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_pages(item)
        self.assertEqual(pages, 'iv-l')

    def test_get_pages_non_roman_letters(self):
        item = {
            'page': 'kj-hh'
        }
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = crossref_processor.get_pages(item)
        self.assertEqual(pages, '')
    
    def test_report_series_venue_id(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS, publishers_filepath=None)
        items = {'items': [{
            'DOI': '10.1007/978-3-030-00668-6_8',
            'container-title': ["troitel'stvo: nauka i obrazovanie [Construction: Science and Education]"],
            'ISSN': '2305-5502',
            'type': 'report-series'
        }]}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['items']:
            output.append(crossref_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1007/978-3-030-00668-6_8', 'title': '', 'author': '', 'pub_date': '', 'venue': "troitel'stvo: nauka i obrazovanie [Construction: Science and Education] [issn:2305-5502]", 'volume': '', 'issue': '', 'page': '', 'type': 'report series', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_report_series_br_id(self):
        crossref_processor = CrossrefProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS, publishers_filepath=None)
        items = {'items': [{
            'DOI': '10.1007/978-3-030-00668-6_8',
            'container-title': [],
            'ISSN': '2305-5502',
            'type': 'report-series'
        }]}
        crossref_processor = CrossrefProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['items']:
            output.append(crossref_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1007/978-3-030-00668-6_8 issn:2305-5502', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': 'report series', 'publisher': '', 'editor': ''}]
        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()
