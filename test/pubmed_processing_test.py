import csv
import unittest
from oc_meta.lib.jsonmanager import *
from oc_meta.plugins.pubmed.pubmed_processing import PubmedProcessing

BASE = os.path.join('test', 'pubmed_processing')
IOD = os.path.join(BASE, 'iod')
ALL_CASES_IOD = os.path.join(BASE, 'iod_all_cases')
WANTED_PMIDS = os.path.join(BASE, 'wanted_pmids.csv')
WANTED_PMIDS_FOLDER = os.path.join(BASE, 'wanted_pmids')
JOURNALS_DICT = os.path.join(BASE, 'journals.json')
DATA = os.path.join(BASE, '40228.csv')
DATA_TEST_NAMES = os.path.join(BASE, 'CSVFile_iod_test.csv')
DATA_DIR = BASE
OUTPUT = os.path.join(BASE, 'meta_input')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
ZIP_INPUT = os.path.join(BASE, 'zip_test')
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')


class TestPubmedProcessing(unittest.TestCase):
    maxDiff = None

    def test_csv_creator(self):
        pubmed_processor = PubmedProcessing(orcid_index=IOD, doi_csv=WANTED_PMIDS_FOLDER, publishers_filepath_pubmed=None, journals_filepath=None)
        data = open(DATA, newline="")
        reader = csv.DictReader(data)
        output = list()
        for item in reader:
            tabular_data = pubmed_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)

        issnstr_1 = "[issn:0006-291X issn:1090-2104]"
        issnstr_2 = "[issn:1090-2104 issn:0006-291X]"
        expected_output = [
            {'id': 'pmid:1 doi:10.1016/0006-2944(75)90147-7',
             'title': 'Formate assay in body fluids: application in methanol poisoning.',
             'author': 'A B Makar; K E McMartin; M Palese; T R Tephly',
             'pub_date': '1975',
             'venue': 'Biochemical medicine [issn:0006-2944]',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:2 doi:10.1016/0006-291x(75)90482-9',
             'title': 'Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution.',
             'author': 'K S Bose; Sarma, R H [orcid:0000-0000-0000-0000]',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_1}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:3 doi:10.1016/0006-291x(75)90498-2',
             'title': 'Metal substitutions incarbonic anhydrase: a halide ion probe study.',
             'author': 'R J Smith; R G Bryant',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_1}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:5 doi:10.1016/0006-291x(75)90508-2',
             'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'author': 'W A Hendrickson; K B Ward',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_1}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''}
        ]
        expected_output_alt= [
            {'id': 'pmid:1 doi:10.1016/0006-2944(75)90147-7',
             'title': 'Formate assay in body fluids: application in methanol poisoning.',
             'author': 'A B Makar; K E McMartin; M Palese; T R Tephly',
             'pub_date': '1975',
             'venue': 'Biochemical medicine [issn:0006-2944]',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:2 doi:10.1016/0006-291x(75)90482-9',
             'title': 'Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution.',
             'author': 'K S Bose; Sarma, R H [orcid:0000-0000-0000-0000]',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_2}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:3 doi:10.1016/0006-291x(75)90498-2',
             'title': 'Metal substitutions incarbonic anhydrase: a halide ion probe study.',
             'author': 'R J Smith; R G Bryant',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_2}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:5 doi:10.1016/0006-291x(75)90508-2',
             'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'author': 'W A Hendrickson; K B Ward',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_2}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''}
        ]
        data.close()
        self.assertTrue(output == expected_output or output == expected_output_alt)

    def test_csv_creator_w_journal_dict(self):
        pubmed_processor = PubmedProcessing(orcid_index=IOD, doi_csv=WANTED_PMIDS_FOLDER, journals_filepath=JOURNALS_DICT)
        data = open(DATA, newline="")
        reader = csv.DictReader(data)
        output = list()
        for item in reader:
            tabular_data = pubmed_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)
        issnstr_1 = "[issn:0006-291X issn:1090-2104]"
        issnstr_2 = "[issn:1090-2104 issn:0006-291X]"
        expected_output = [
            {'id': 'pmid:1 doi:10.1016/0006-2944(75)90147-7',
             'title': 'Formate assay in body fluids: application in methanol poisoning.',
             'author': 'A B Makar; K E McMartin; M Palese; T R Tephly',
             'pub_date': '1975',
             'venue': 'Biochemical medicine [issn:0006-2944]',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:2 doi:10.1016/0006-291x(75)90482-9',
             'title': 'Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution.',
             'author': 'K S Bose; Sarma, R H [orcid:0000-0000-0000-0000]',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_2}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:3 doi:10.1016/0006-291x(75)90498-2',
             'title': 'Metal substitutions incarbonic anhydrase: a halide ion probe study.',
             'author': 'R J Smith; R G Bryant',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_2}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:5 doi:10.1016/0006-291x(75)90508-2',
             'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'author': 'W A Hendrickson; K B Ward',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_2}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''
             }
        ]
        expected_output_alt = [
            {'id': 'pmid:1 doi:10.1016/0006-2944(75)90147-7',
             'title': 'Formate assay in body fluids: application in methanol poisoning.',
             'author': 'A B Makar; K E McMartin; M Palese; T R Tephly',
             'pub_date': '1975',
             'venue': 'Biochemical medicine [issn:0006-2944]',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:2 doi:10.1016/0006-291x(75)90482-9',
             'title': 'Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution.',
             'author': 'K S Bose; Sarma, R H [orcid:0000-0000-0000-0000]',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_1}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:3 doi:10.1016/0006-291x(75)90498-2',
             'title': 'Metal substitutions incarbonic anhydrase: a halide ion probe study.',
             'author': 'R J Smith; R G Bryant',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_1}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''},
            {'id': 'pmid:5 doi:10.1016/0006-291x(75)90508-2',
             'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'author': 'W A Hendrickson; K B Ward',
             'pub_date': '1975',
             'venue': f'Biochemical and biophysical research communications {issnstr_1}',
             'volume': '',
             'issue': '',
             'page': '',
             'type': 'journal article',
             'publisher': 'Elsevier BV',
             'editor': ''
             }
        ]

        data.close()
        self.assertTrue(output == expected_output or output == expected_output_alt)

    def test_orcid_finder(self):
        pubmed_processor = PubmedProcessing(IOD, WANTED_PMIDS)
        orcid_found = pubmed_processor.orcid_finder('10.1016/0006-291x(75)90482-9')
        expected_output = {'0000-0000-0000-0000': 'sarma, r h'}
        self.assertEqual(orcid_found, expected_output)

    def test_get_agents_strings_list(self):
        pubmed_processor = PubmedProcessing(orcid_index=ALL_CASES_IOD, doi_csv=WANTED_PMIDS)
        agents_list = [{'role': 'author', 'name': 'Arianna Moretti', 'family': '', 'given': ''}, {'role': 'author', 'name': 'S Peroni', 'family': '', 'given': ''}, {'role': 'author', 'name': 'Chiara D Giambattista', 'family': '', 'given': ''}]
        ag = pubmed_processor.get_agents_strings_list("10.3000/1000000001", agents_list)
        self.assertEqual(ag[0], ['Moretti, Arianna [orcid:0000-0001-5486-7070]', 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'Di Giambattista, Chiara [orcid:0000-0001-8665-095X]'] )

    def test_get_venue_name_with_extended_map(self):
        item = {
             "journal": "Biochim Biophys Acta"}
        pubmed_processor = PubmedProcessing(orcid_index=None, doi_csv=None, journals_filepath=JOURNALS_DICT)
        venue_name = pubmed_processor.get_venue_name(item, "59")
        self.assertEqual(venue_name, "Biochimica et biophysica acta [issn:0006-3002]")

    def test_get_venue_name_with_extended_map_ISSN(self):
        item = {
             "journal": "Biochem Biophys Res Commun"}
        pubmed_processor = PubmedProcessing(orcid_index=None, doi_csv=None, journals_filepath=JOURNALS_DICT)
        venue_name = pubmed_processor.get_venue_name(item, "2")
        alt_1 = "Biochemical and biophysical research communications [issn:0006-291X issn:1090-2104]"
        alt_2 = "Biochemical and biophysical research communications [issn:1090-2104 issn:0006-291X]"
        self.assertTrue(venue_name == alt_1 or venue_name == alt_2)

    def test_get_venue_name_no_extended_map(self):
        item = {
             "journal": "Biochim Biophys Acta"}
        pubmed_processor = PubmedProcessing(orcid_index=None, doi_csv=None, journals_filepath=None)
        venue_name = pubmed_processor.get_venue_name(item, "59")
        self.assertEqual(venue_name, "Biochimica et biophysica acta [issn:0006-3002]")

    def test_issn_worker(self):
        input = 'ISSN 0006-291X'
        output = list()
        PubmedProcessing.issn_worker(input, output)
        expected_output = ['issn:0006-291X']
        self.assertEqual(output, expected_output)

    def test_id_worker(self):
        field_issn = '00062952'
        field_isbn = ['978-3-905673-82-1']
        issn_list = list()
        isbn_list = list()
        PubmedProcessing.id_worker(field_issn, issn_list, PubmedProcessing.issn_worker)
        PubmedProcessing.id_worker(field_isbn, isbn_list, PubmedProcessing.isbn_worker)
        expected_issn_list = ['issn:0006-2952']
        expected_isbn_list = ['isbn:9783905673821']
        self.assertEqual((issn_list, isbn_list), (expected_issn_list, expected_isbn_list))

    def test_find_homonyms(self):
        ra_list_1 = [{'role': 'author', 'name': 'K S Bose', 'family': '', 'given': ''},
         {'role': 'author', 'name': 'R H Sarma', 'family': '', 'given': ''}]
        ra_list_2 = [{'role': 'author', 'name': 'K S Bose', 'family': '', 'given': ''},
         {'role': 'author', 'name': 'R H Bose', 'family': '', 'given': ''}]
        ra_list_3 = [{'role': 'author', 'name': 'Anna Maria Rossi', 'family': '', 'given': ''},
                     {'role': 'author', 'name': 'Katia Rossi Bianchi', 'family': '', 'given': ''},
                     {'role': 'author', 'name': 'K Rossi B', 'family': '', 'given': ''}]
        ra_list_4 = [{'role': 'author', 'name': 'R J Smith', 'family': '', 'given': ''},
                      {'role': 'author', 'name': 'R J Smith Bryant', 'family': '', 'given': ''},
                      {'role': 'author', 'name': 'Ronald Bryant', 'family': '', 'given': ''}]
        pubmed_processor_ra = PubmedProcessing(orcid_index=IOD, doi_csv=WANTED_PMIDS_FOLDER, publishers_filepath_pubmed=None, journals_filepath=None)
        homonyms_l1 = pubmed_processor_ra.find_homonyms(ra_list_1)
        homonyms_l2 = pubmed_processor_ra.find_homonyms(ra_list_2)
        homonyms_l3 = pubmed_processor_ra.find_homonyms(ra_list_3)
        homonyms_l4 = pubmed_processor_ra.find_homonyms(ra_list_4)

        self.assertEqual(homonyms_l1, {})
        self.assertCountEqual(homonyms_l2, {'K S Bose': ['R H Bose'], 'R H Bose': ['K S Bose']})
        self.assertCountEqual(homonyms_l3, {'Anna Maria Rossi': ['Katia Rossi Bianchi', 'K Rossi B'], 'Katia Rossi Bianchi': ['Anna Maria Rossi', 'K Rossi B'], 'K Rossi B': ['Anna Maria Rossi', 'Katia Rossi Bianchi']})
        self.assertCountEqual(homonyms_l4, {'R J Smith': ['R J Smith Bryant'], 'R J Smith Bryant': ['Ronald Bryant', 'R J Smith'], 'Ronald Bryant': ['R J Smith Bryant']})

    def test_compute_affinity(self):
        pubmed_processor_ra_ca = PubmedProcessing(orcid_index=IOD, doi_csv=WANTED_PMIDS_FOLDER, publishers_filepath_pubmed=None, journals_filepath=None)
        target_full_names = "Anna Cristiana Cardinali Santelli"
        ra_list_1 = ["Anna Cardinali", "Anna C. Santelli", "A.Cristiana Santelli Cardinali", "Anna Cristiana Santelli Cardinali", "Anna C S C", "Anna cristiana Santelli   CARDINALI"]
        ra_list_2 = ["Anna Cardinali", "Anna C. Santelli", "A.Cristiana Santelli Cardinali", "Anna C S C", "Anna cristiana Santelli   CARDINALI"]
        ra_list_3 = ["Anna Cardinali", "Anna C. Santelli", "A.Cristiana Santelli Cardinali", "Anna C S C"]
        ra_list_4 = ["Anna Cardinali", "Anna C. Santelli", "Anna C S C"]
        ra_list_5 = ["Anna Cardinali", "Anna C S C"]
        ra_list_6 = ["Filippo C. Moroni", "Silvia C."]
        ra_list_7 = ["Olga Santelli", " Vincenzo Cardinali"]
        ra_list_8 = ["Olga Santelli", " Vincenzo Cardinali", "Carla Anna Cardinali Santelli"]

        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_1), "Anna Cristiana Santelli Cardinali")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_2), "Anna cristiana Santelli   CARDINALI")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_3), "A.Cristiana Santelli Cardinali")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_4), "Anna C. Santelli")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_5), "Anna Cardinali")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_6), "")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_7), "")
        self.assertEqual(pubmed_processor_ra_ca.compute_affinity(target_full_names, ra_list_8), "Carla Anna Cardinali Santelli")


    def test_redis_db(self):
        pubmed_processor = PubmedProcessing(orcid_index=IOD, doi_csv=WANTED_PMIDS_FOLDER, journals_filepath=JOURNALS_DICT)
        inp_ent = {'pmid': '5', 'doi': '10.1016/0006-291x(75)90508-2',
             'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'authors': 'W A Hendrickson, K B Ward', 'year': '1975', 'journal': 'Biochem Biophys Res Commun',
             'cited_by': '7118409 6768892 2619971 2190210 3380793 20577584 8372226 7012375 856811 678527 33255345 33973855 402092 7012894 1257769 861288 1061139 3681996', 'references': '4882249 5059118 14834145 1056020 5509841'}
        exp_res = {'id': 'pmid:5 doi:10.1016/0006-291x(75)90508-2', 'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'author': 'W A Hendrickson; K B Ward', 'pub_date': '1975', 'venue': 'Biochemical and biophysical research communications [issn:0006-291X issn:1090-2104]',
             'volume': '', 'issue': '', 'page': '', 'type': 'journal article', 'publisher': 'Elsevier BV', 'editor': ''}

        tabular_data = pubmed_processor.csv_creator(inp_ent)
        self.assertEqual(exp_res, tabular_data)

        inp_ent_fake_doi = {'pmid': '5', 'doi': '10.1016/a_fake_doi',
             'title': 'Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.',
             'authors': 'W A Hendrickson, K B Ward', 'year': '1975', 'journal': 'Biochem Biophys Res Commun',
             'cited_by': '7118409 6768892 2619971 2190210 3380793 20577584 8372226 7012375 856811 678527 33255345 33973855 402092 7012894 1257769 861288 1061139 3681996', 'references': '4882249 5059118 14834145 1056020 5509841'}

        tabular_data_no_redis_data = pubmed_processor.csv_creator(inp_ent_fake_doi)
        self.assertEqual(tabular_data_no_redis_data['id'], 'pmid:5')

        pubmed_processor.BR_redis.set('doi:10.1016/a_fake_doi', 'meta:000101')

        tabular_data_w_redis_data = pubmed_processor.csv_creator(inp_ent_fake_doi)
        self.assertEqual(tabular_data_w_redis_data['id'], 'pmid:5 doi:10.1016/a_fake_doi')

if __name__ == '__main__':
    unittest.main()