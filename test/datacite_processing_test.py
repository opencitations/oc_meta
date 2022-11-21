import csv
import os
import shutil
import unittest
from pprint import pprint

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.jsonmanager import *
from oc_meta.plugins.datacite.datacite_processing import DataciteProcessing

# from oc_meta.run.datacite_process import preprocess

BASE = os.path.join('test', 'datacite_processing')
IOD = os.path.join(BASE, 'iod')
WANTED_DOIS = os.path.join(BASE, 'wanted_dois.csv')
WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois')
DATA = os.path.join(BASE, 'jSonFile_1.json')
DATA_DIR = BASE
OUTPUT = os.path.join(BASE, 'meta_input')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
ZST_INPUT = os.path.join(BASE, 'zst_test', "40228.json.zst")
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')


class TestCrossrefProcessing(unittest.TestCase):

    def test_csv_creator(self):
        datacite_processor = DataciteProcessing(orcid_index=IOD, doi_csv=WANTED_DOIS_FOLDER, publishers_filepath=None)
        data = load_json(DATA, None)
        output = list()
        for item in data['data']:
            tabular_data = datacite_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)

        expected_output = [
            {'id': 'doi:10.1002/2014jc009965',
             'title': 'On the physical and biogeochemical processes driving the high frequency variability of CO fugacity at 6°S, 10°W: Potential role of the internal waves',
             'author': 'Parard, Gaëlle; Boutin, J.; Cuypers, Y.; Bouruet-Aubertot, P.; Caniaux, G.',
             'pub_date': '2014-12',
             'venue': 'journal of geophysical research: oceans [issn:2169-9275]',
             'volume': '119',
             'issue': '12',
             'page': '8357-8374',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1002/2014jd022411',
             'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
             'author': 'Ott, Lesley E.; Pawson, Steven; Collatz, George J.; Gregg, Watson W.; Menemenlis, Dimitris [orcid:0000-0001-9940-8409]; Brix, Holger; Rousseaux, Cecile S.; Bowman, Kevin W.; Liu, Junjie; Eldering, Annmarie; Gunson, Michael R.; Kawa, Stephan R.',
             'pub_date': '2015-01-27',
             'venue': 'journal of geophysical research: atmospheres [issn:2169-897X]',
             'volume': '120',
             'issue': '2',
             'page': '734-765',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1002/2015gb005314',
             'title': 'Satellite estimates of net community production based on O /Ar observations and comparison to other estimates',
             'author': 'Li, Zuchuan; Cassar, Nicolas',
             'pub_date': '2016-05',
             'venue': 'global biogeochemical cycles [issn:0886-6236]',
             'volume': '30',
             'issue': '5',
             'page': '735-752',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1002/2015gl065259',
             'title': 'Observed eastward progression of the Fukushima Cs signal across the North Pacific',
             'author': 'Yoshida, Sachiko; Macdonald, Alison M.; Jayne, Steven R.; Rypina, Irina I.; Buesseler, Ken O.',
             'pub_date': '2015-09-16',
             'venue': 'geophysical research letters [issn:0094-8276]',
             'volume': '42',
             'issue': '17',
             'page': '7139-7147',
             'type': 'journal article',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1594/pangaea.231378',
             'title': 'Phosphate, fluoride and cell abundance of bacteria Thiomargarita namibiensis in porewater of sediment profile M57/3_203 from Walvis Ridge',
             'author': 'Schulz, Heide N [orcid:0000-0003-1445-0291]; Schulz, Horst D',
             'pub_date': '2005',
             'venue': '',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'dataset',
             'publisher': 'PANGAEA - Data Publisher for Earth & Environmental Science', 'editor': ''}

        ]

        self.assertEqual(output, expected_output)

    def test_orcid_finder(self):
        datacite_processor = DataciteProcessing(IOD, WANTED_DOIS)
        orcid_found = datacite_processor.orcid_finder('10.1002/2014jd022411')
        expected_output = {'0000-0001-9940-8409': 'menemenlis, dimitris'}
        self.assertEqual(orcid_found, expected_output)

    def test_get_agents_strings_list_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        entity_attr_dict = {
            "creators": [
                {"name": "Olivarez Lyle, Annette",
                 "givenName": "Annette",
                 "familyName": "Olivarez Lyle",
                 "affiliation": [],
                 "nameIdentifiers": []
                 },
                {"name": "Lyle, Mitchell W",
                 "givenName": "Mitchell W",
                 "familyName": "Lyle",
                 "nameIdentifiers": [
                     {"schemeUri": "https://orcid.org",
                      "nameIdentifier": "https://orcid.org/0000-0002-0861-0511",
                      "nameIdentifierScheme": "ORCID"}
                 ],
                 "affiliation": []
                 }
            ],
            "contributors": []
        }

        datacite_processor = DataciteProcessing(None, None)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        csv_manager = CSVManager()
        csv_manager.data = {'10.1594/pangaea.777220': {'Lyle, Mitchell W [0000-0002-0861-0511]'}}
        datacite_processor.orcid_index = csv_manager
        authors_strings_list, editors_strings_list = datacite_processor.get_agents_strings_list(
            '10.1594/pangaea.777220', agents_list)
        expected_authors_list = ['Olivarez Lyle, Annette',
                                 'Lyle, Mitchell W [orcid:0000-0002-0861-0511]']
        expected_editors_list = []
        self.assertEqual((authors_strings_list, editors_strings_list), (expected_authors_list, expected_editors_list))

    def test_get_agents_strings_list(self):
        entity_attr_dict = {
            "doi": "10.1002/2014jd022411",
            "creators": [{"name": "Ott, Lesley E.", "nameType": "Personal", "givenName": "Lesley E.", "familyName": "Ott", "affiliation": [], "nameIdentifiers": []}, {"name": "Pawson, Steven", "nameType": "Personal", "givenName": "Steven", "familyName": "Pawson", "affiliation": [], "nameIdentifiers": []}, {"name": "Collatz, George J.", "nameType": "Personal", "givenName": "George J.", "familyName": "Collatz", "affiliation": [], "nameIdentifiers": []}, {"name": "Gregg, Watson W.", "nameType": "Personal", "givenName": "Watson W.", "familyName": "Gregg", "affiliation": [], "nameIdentifiers": []}, {"name": "Menemenlis, Dimitris", "nameType": "Personal", "givenName": "Dimitris", "familyName": "Menemenlis", "affiliation": [], "nameIdentifiers": [{"schemeUri": "https://orcid.org", "nameIdentifier": "https://orcid.org/0000-0001-9940-8409", "nameIdentifierScheme": "ORCID"}]}, {"name": "Brix, Holger", "nameType": "Personal", "givenName": "Holger", "familyName": "Brix", "affiliation": [], "nameIdentifiers": []}, {"name": "Rousseaux, Cecile S.", "nameType": "Personal", "givenName": "Cecile S.", "familyName": "Rousseaux", "affiliation": [], "nameIdentifiers": []}, {"name": "Bowman, Kevin W.", "nameType": "Personal", "givenName": "Kevin W.", "familyName": "Bowman", "affiliation": [], "nameIdentifiers": []}, {"name": "Liu, Junjie", "nameType": "Personal", "givenName": "Junjie", "familyName": "Liu", "affiliation": [], "nameIdentifiers": []}, {"name": "Eldering, Annmarie", "nameType": "Personal", "givenName": "Annmarie", "familyName": "Eldering", "affiliation": [], "nameIdentifiers": []}, {"name": "Gunson, Michael R.", "nameType": "Personal", "givenName": "Michael R.", "familyName": "Gunson", "affiliation": [], "nameIdentifiers": []}, {"name": "Kawa, Stephan R.", "nameType": "Personal", "givenName": "Stephan R.", "familyName": "Kawa", "affiliation": [], "nameIdentifiers": []}],
            "contributors": []
        }

        datacite_processor = DataciteProcessing(IOD, WANTED_DOIS)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list


        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.1002/2014jd022411',
                                                                             agents_list)
        expected_authors_list = ['Ott, Lesley E.', 'Pawson, Steven', 'Collatz, George J.', 'Gregg, Watson W.', 'Menemenlis, Dimitris [orcid:0000-0001-9940-8409]', 'Brix, Holger', 'Rousseaux, Cecile S.', 'Bowman, Kevin W.', 'Liu, Junjie', 'Eldering, Annmarie', 'Gunson, Michael R.', 'Kawa, Stephan R.']
        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_get_agents_strings_list_same_family(self):
        # Two authors have the same family name and the same given name initials
        entity_attr_dict = {
            "creators": [
                {"name": "Schulz, Heide N",
                 "nameType": "Personal",
                 "givenName": "Heide N",
                 "familyName": "Schulz",
                 "nameIdentifiers":
                     [
                         {"schemeUri": "https://orcid.org", "nameIdentifier": "https://orcid.org/0000-0003-1445-0291", "nameIdentifierScheme": "ORCID"}
                     ],
                 "affiliation": []},
                {"name": "Schulz, Horst D",
                 "nameType": "Personal",
                 "givenName": "Horst D",
                 "familyName": "Schulz",
                 "affiliation": [],
                 "nameIdentifiers": []}],
            "contributors": []
        }
        datacite_processor = DataciteProcessing(IOD, WANTED_DOIS)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.1594/pangaea.231378',
                                                                             agents_list)
        expected_authors_list = ['Schulz, Heide N [orcid:0000-0003-1445-0291]', 'Schulz, Horst D']
        self.assertEqual(authors_strings_list, expected_authors_list)

    def test_get_agents_strings_list_homonyms(self):
        # Two authors have the same family name and the same given name
        entity_attr_dict = {
            "creators":
                [
                    {"name":  "Viorel, Cojocaru",
                     "nameType": "Personal",
                     "givenName": "Cojocaru",
                     "familyName": "Viorel",
                     "affiliation": [],
                     "nameIdentifiers": []},
                    {"name": "Viorel, Cojocaru",
                     "nameType": "Personal",
                     "givenName": "Cojocaru",
                     "familyName": "Viorel",
                     "affiliation": [],
                     "nameIdentifiers": []
                     },
                    {"name": "Ciprian, Panait",
                     "nameType": "Personal",
                     "givenName": "Panait",
                     "familyName": "Ciprian",
                     "affiliation": [],
                     "nameIdentifiers": []}
                ],
            "contributors": []
        }
        datacite_processor = DataciteProcessing(None, None)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.12753/2066-026x-14-246',
                                                                             agents_list)
        expected_authors_list = ['Viorel, Cojocaru', 'Viorel, Cojocaru', 'Ciprian, Panait']
        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_get_agents_strings_list_inverted_names(self):
        # One author with an ORCID has as a name the surname of another
        entity_attr_dict = {
            "creators":
                [
                    {"name":  "Viorel, Cojocaru",
                     "nameType": "Personal",
                     "givenName": "Cojocaru",
                     "familyName": "Viorel",
                     "affiliation": [],
                     "nameIdentifiers": []},

                    {"name": "Cojocaru, John",
                     "nameType": "Personal",
                     "givenName": "John",
                     "familyName": "Cojocaru",
                     "affiliation": [],
                     "nameIdentifiers": []
                     },
                    {"name": "Ciprian, Panait",
                     "nameType": "Personal",
                     "givenName": "Panait",
                     "familyName": "Ciprian",
                     "affiliation": [],
                     "nameIdentifiers": []}
                ],
            "contributors": []
        }
        # Note : 'Cojocaru, John' is not one of the authors of the item, the name was made up for testing purposes
        datacite_processor = DataciteProcessing(None, None)
        authors_list = datacite_processor.add_authors_to_agent_list(entity_attr_dict, [])
        editors_list = datacite_processor.add_editors_to_agent_list(entity_attr_dict, [])
        agents_list = authors_list + editors_list
        authors_strings_list, _ = datacite_processor.get_agents_strings_list('10.12753/2066-026x-14-246',
                                                                             agents_list)
        expected_authors_list = ['Viorel, Cojocaru', 'Cojocaru, John', 'Ciprian, Panait']
        self.assertEqual(authors_strings_list, expected_authors_list)


    def test_id_worker(self):
        field_issn = '2169897X'
        field_isbn = ['978-3-905673-82-1']
        issn_list = list()
        isbn_list = list()
        DataciteProcessing.id_worker(field_issn, issn_list, DataciteProcessing.issn_worker)
        DataciteProcessing.id_worker(field_isbn, isbn_list, DataciteProcessing.isbn_worker)
        expected_issn_list = ['issn:2169-897X']
        expected_isbn_list = ['isbn:9783905673821']
        self.assertEqual((issn_list, isbn_list), (expected_issn_list, expected_isbn_list))

    def test_issn_worker(self):
        input = 'ISSN 2169-897X'
        output = list()
        DataciteProcessing.issn_worker(input, output)
        expected_output = ['issn:2169-897X']
        self.assertEqual(output, expected_output)

    def test_isbn_worker(self):
        input = '978-3-905673-82-1'
        output = list()
        DataciteProcessing.isbn_worker(input, output)
        expected_output = ['isbn:9783905673821']
        self.assertEqual(output, expected_output)

    # def test_preprocess(self):
    #     self.maxDiff = None
    #     if os.path.exists(OUTPUT):
    #         shutil.rmtree(OUTPUT)
    #     preprocess(datacite_json_dir=MULTIPROCESS_OUTPUT, publishers_filepath=None, orcid_doi_filepath=IOD,
    #                csv_dir=OUTPUT, wanted_doi_filepath=None)
    #     output = dict()
    #     for file in os.listdir(OUTPUT):
    #         with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
    #             output[file] = list(csv.DictReader(f))
    #     expected_output = {
    #         '40228.csv': [{'id': 'doi:10.1002/2014jd022411',
    #                         'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
    #                         'author': 'Ott, Lesley E.; Pawson, Steven; Collatz, George J.; Gregg, Watson W.; Menemenlis, Dimitris [orcid:0000-0001-9940-8409]; Brix, Holger; Rousseaux, Cecile S.; Bowman, Kevin W.; Liu, Junjie; Eldering, Annmarie; Gunson, Michael R.; Kawa, Stephan R.',
    #                         'pub_date': '2015-01-27',
    #                         'venue': 'journal of geophysical research: atmospheres [issn:2169-897X]',
    #                         'volume': '120',
    #                         'issue': '2',
    #                         'page': '734-765',
    #                         'type': 'journal article',
    #                         'publisher': '',
    #                         'editor': ''},
    #                        {'id': 'doi:10.1594/pangaea.231378',
    #                         'title': 'Phosphate, fluoride and cell abundance of bacteria Thiomargarita namibiensis in porewater of sediment profile M57/3_203 from Walvis Ridge',
    #                         'author': 'Schulz, Heide N [orcid:0000-0003-1445-0291]; Schulz, Horst D',
    #                         'pub_date': '2005',
    #                         'venue': '',
    #                         'volume': '',
    #                         'issue': '',
    #                         'page': '',
    #                         'type': 'dataset',
    #                         'publisher': 'PANGAEA - Data Publisher for Earth & Environmental Science',
    #                         'editor': ''}
    #                        ],
    #          '30719.csv': [{'id': 'doi:10.1002/2014gb004975',
    #                         'title': 'Biological and physical controls on N , O , and CO distributions in contrasting Southern Ocean surface waters',
    #                         'author': 'Tortell, Philippe D.; Bittig, Henry C.; Körtzinger, Arne; Jones, Elizabeth M.; Hoppema, Mario',
    #                         'pub_date': '2015-07',
    #                         'venue': 'global biogeochemical cycles [issn:0886-6236]',
    #                         'volume': '29',
    #                         'issue': '7',
    #                         'page': '994-1013',
    #                         'type': 'journal article',
    #                         'publisher': '',
    #                         'editor': ''}
    #                        ]
    #          }

    #     self.assertEqual(output, expected_output)

    # def test_zst_input(self):
    #     if os.path.exists(OUTPUT):
    #         shutil.rmtree(OUTPUT)
    #     preprocess(datacite_json_dir=ZST_INPUT, publishers_filepath=None, orcid_doi_filepath=IOD, csv_dir=OUTPUT,
    #                wanted_doi_filepath=WANTED_DOIS)
    #     output = dict()
    #     for file in os.listdir(OUTPUT):
    #         with open(os.path.join(OUTPUT, file), 'r', encoding='utf-8') as f:
    #             output[file] = list(csv.DictReader(f))
    #     expected_output =  {
    #         '40228.csv':
    #             [
    #                 {
    #                     'id': 'doi:10.1002/2014jd022411',
    #                     'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project",
    #                     'author': 'Ott, Lesley E.; Pawson, Steven; Collatz, George J.; Gregg, Watson W.; Menemenlis, Dimitris [orcid:0000-0001-9940-8409]; Brix, Holger; Rousseaux, Cecile S.; Bowman, Kevin W.; Liu, Junjie; Eldering, Annmarie; Gunson, Michael R.; Kawa, Stephan R.',
    #                     'pub_date': '2015-01-27',
    #                     'venue': 'journal of geophysical research: atmospheres [issn:2169-897X]',
    #                     'volume': '120',
    #                     'issue': '2',
    #                     'page': '734-765',
    #                     'type': 'journal article',
    #                     'publisher': '',
    #                     'editor': ''},
    #                 {
    #                     'id': 'doi:10.1594/pangaea.231378',
    #                     'title': 'Phosphate, fluoride and cell abundance of bacteria Thiomargarita namibiensis in porewater of sediment profile M57/3_203 from Walvis Ridge',
    #                     'author': 'Schulz, Heide N [orcid:0000-0003-1445-0291]; Schulz, Horst D',
    #                     'pub_date': '2005',
    #                     'venue': '',
    #                     'volume': '',
    #                     'issue': '',
    #                     'page': '',
    #                     'type': 'dataset',
    #                     'publisher': 'PANGAEA - Data Publisher for Earth & Environmental Science',
    #                     'editor': ''
    #                 }
    #             ]
    #     }

    #     self.assertEqual(output, expected_output)

    def test_load_publishers_mapping(self):
        output = DataciteProcessing.load_publishers_mapping(publishers_filepath=PUBLISHERS_MAPPING)
        expected_output = {
            '1': {'name': 'Wiley', 'prefixes': {'10.1002'}},
            '2': {'name': 'PANGAEA - Data Publisher for Earth & Environmental Science', 'prefixes': {'10.1594'}},
            '3': {'name': 'ADLRO', 'prefixes': {'10.12753'}}}
        self.assertEqual(output, expected_output)

    def test_get_publisher_name(self):
        # The item's member is in the publishers' mapping
        item = {
            "doi": "10.1594/pangaea.777220",
            "publisher": "PANGAEA - Data Publisher for Earth & Environmental Science"
        }
        doi = '10.1594/pangaea.777220'
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = datacite_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'PANGAEA - Data Publisher for Earth & Environmental Science [datacite:2]')

    def test_get_publisher_name_from_prefix(self):
        # The item has no declared publisher, but the DOI prefix is in the publishers' mapping
        item = {
            'publisher': '',
            'doi': '10.12753/sample_test_doi_with_known_prefix',
        }
        doi = '10.12753/sample_test_doi_with_known_prefix'
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        publisher_name = datacite_processor.get_publisher_name(doi, item)
        self.assertEqual(publisher_name, 'ADLRO [datacite:3]')

    def test_get_venue_name(self):
        item = {
             "container": {"type": "Journal", "issue": "2", "title": "Journal of Geophysical Research: Atmospheres", "volume": "120", "lastPage": "765", "firstPage": "734"}
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, 'journal of geophysical research: atmospheres')

    def test_get_venue_name_with_ISSN(self):
        item = {
            "container": {"type": "Journal", "issue": "18", "title": "Geophysical Research Letters", "volume": "41",
                          "lastPage": "6451", "firstPage": "6443", "identifier": "00948276", "identifierType": "ISSN"}
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name,
                         "geophysical research letters [issn:0094-8276]")

    def test_get_venue_ISSN_from_rel_id(self):
        item = {"relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "00948276", "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
        }
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '',
               'type': 'journal article', 'publisher': '', 'editor': ''}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        venue_name = datacite_processor.get_venue_name(item, row)
        self.assertEqual(venue_name, "[issn:0094-8276]")

    def test_get_pages(self):
        item = {
            "container": {"type": "Journal", "issue": "7", "title": "Global Biogeochemical Cycles", "volume": "29",
                          "lastPage": "1013", "firstPage": "994", "identifier": "08866236", "identifierType": "ISSN"}
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '994-1013')

    def test_get_pages_right_letter(self):
        item = {
            "container": {"type": "Journal", "issue": "4", "title": "Ecosphere", "volume": "10", "firstPage": "e02701", "identifier": "2150-8925", "identifierType": "ISSN"}
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, 'e02701')

    def test_get_pages_wrong_letter(self):
        item = {
            "relatedIdentifiers": [
                {"relationType": "IsPartOf",
                 "relatedIdentifier": "0094-2405",
                 "relatedIdentifierType": "ISSN",
                 "firstPage": "583b",
                 "lastPage": "584"},
                {"relationType": "References",
                 "relatedIdentifier": "10.1016/j.ecl.2014.08.007",
                 "relatedIdentifierType": "DOI"}
            ]
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '583-584')

    def test_get_pages_roman_letters(self):
        item = {
            "relatedIdentifiers": [
                {"relationType": "IsPartOf",
                 "relatedIdentifier": "0094-2405",
                 "relatedIdentifierType": "ISSN",
                 "firstPage": "iv",
                 "lastPage": "l"},
                {"relationType": "References",
                 "relatedIdentifier": "10.1016/j.ecl.2014.08.007",
                 "relatedIdentifierType": "DOI"}
            ]
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, 'iv-l')

    def test_get_pages_non_roman_letters(self):
        item = {
            "relatedIdentifiers": [
                {"relationType": "IsPartOf",
                 "relatedIdentifier": "0094-2405",
                 "relatedIdentifierType": "ISSN",
                 "firstPage": "kj",
                 "lastPage": "hh"},
                {"relationType": "References",
                 "relatedIdentifier": "10.1016/j.ecl.2014.08.007",
                 "relatedIdentifierType": "DOI"}
            ]
        }
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        pages = datacite_processor.get_datacite_pages(item)
        self.assertEqual(pages, '')

    def test_venue_id_cont_and_rel_id(self):
        items = {'data': [
        {
            "id": "10.1002/2014jd022411",
            "type": "dois",
            "attributes": {
                "doi": "10.1002/2014jd022411",
                "identifiers": [],
                "titles": [{"title": "\n              Assessing the magnitude of CO\n              \n              flux uncertainty in atmospheric CO\n              \n              records using products from NASA's Carbon Monitoring Flux Pilot Project\n            "}],
                "publisher": "(:unav)",
                "container": {"type": "Journal", "issue": "2", "title": "Journal of Geophysical Research: Atmospheres", "volume": "120", "lastPage": "765", "firstPage": "734", "identifier": "2169897X", "identifierType": "ISSN"},
                "types": {"ris": "JOUR", "bibtex": "article", "citeproc": "article-journal",
                          "schemaOrg": "ScholarlyArticle", "resourceType": "JournalArticle",
                          "resourceTypeGeneral": "Text"},
                "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X", "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
            }
        }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['data']:
            output.append(datacite_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1002/2014jd022411', 'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project", 'author': '', 'pub_date': '', 'venue': 'journal of geophysical research: atmospheres [issn:2169-897X]', 'volume': '120', 'issue': '2', 'page': '734-765', 'type': 'journal article', 'publisher': 'Wiley [datacite:1]', 'editor': ''}]
        self.assertEqual(output, expected_output)

    def test_venue_id_cont_and_rel_id_no_types(self):
        # the absence of publication types specified excludes the possibility
        # to assert whether the container can have an ISSN or not
        items = {'data': [
        {
            "id": "10.1002/2014jd022411",
            "type": "dois",
            "attributes": {
                "doi": "10.1002/2014jd022411",
                "identifiers": [],
                "titles": [{"title": "\n              Assessing the magnitude of CO\n              \n              flux uncertainty in atmospheric CO\n              \n              records using products from NASA's Carbon Monitoring Flux Pilot Project\n            "}],
                "publisher": "(:unav)",
                "container": {"type": "Journal", "issue": "2", "title": "Journal of Geophysical Research: Atmospheres", "volume": "120", "lastPage": "765", "firstPage": "734", "identifier": "2169897X", "identifierType": "ISSN"},
                "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X", "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
            }
        }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['data']:
            output.append(datacite_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1002/2014jd022411', 'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project", 'author': '', 'pub_date': '', 'venue': 'journal of geophysical research: atmospheres', 'volume': '120', 'issue': '2', 'page': '734-765', 'type': '', 'publisher': 'Wiley [datacite:1]', 'editor': ''}]
        self.assertEqual(output, expected_output)


    def test_venue_id_rel_id_only(self):
        # the absence of publication types specified excludes the possibility
        # to assert whether the container can have an ISSN or not
        items = {'data': [
        {
            "id": "10.1002/2014jd022411",
            "type": "dois",
            "attributes": {
                "doi": "10.1002/2014jd022411",
                "identifiers": [],
                "titles": [{"title": "\n              Assessing the magnitude of CO\n              \n              flux uncertainty in atmospheric CO\n              \n              records using products from NASA's Carbon Monitoring Flux Pilot Project\n            "}],
                "publisher": "(:unav)",
                "container": {},
                "types": {"ris": "JOUR", "bibtex": "article", "citeproc": "article-journal",
                          "schemaOrg": "ScholarlyArticle", "resourceType": "JournalArticle",
                          "resourceTypeGeneral": "Text"},
                "relatedIdentifiers": [{"relationType": "IsPartOf", "relatedIdentifier": "2169897X", "resourceTypeGeneral": "Collection", "relatedIdentifierType": "ISSN"}]
            }
        }
        ]}
        datacite_processor = DataciteProcessing(orcid_index=None, doi_csv=None, publishers_filepath=PUBLISHERS_MAPPING)
        output = list()
        for item in items['data']:
            output.append(datacite_processor.csv_creator(item))
        expected_output = [{'id': 'doi:10.1002/2014jd022411', 'title': "Assessing the magnitude of CO flux uncertainty in atmospheric CO records using products from NASA's Carbon Monitoring Flux Pilot Project", 'author': '', 'pub_date': '', 'venue': '[issn:2169-897X]', 'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Wiley [datacite:1]', 'editor': ''}]
        self.assertEqual(output, expected_output)

if __name__ == '__main__':  # pragma: no cover
    unittest.main()