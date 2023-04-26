import csv
import os
import shutil
import unittest

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.plugins.jalc.jalc_processing import JalcProcessing
from oc_meta.run.jalc_process import load_ndjson
import oc_meta.run.jalc_process

BASE = os.path.join('test', 'jalc_processing')
IOD = os.path.join(BASE, 'iod')
WANTED_DOIS = os.path.join(BASE, 'wanted_dois.csv')
WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois')
DATA = os.path.join(BASE, 'filtered_66.ndjson')
DATA_DIR = BASE
OUTPUT1 = os.path.join(BASE, 'meta_input_without_citing')
OUTPUT2 = os.path.join(BASE, 'meta_input_with_citing')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')
CITING_ENTITIES = os.path.join(BASE, 'citing_entities.zip')


class TestJalcProcessing(unittest.TestCase):
    def test_csv_creator(self):
        jalc_processor = JalcProcessing()
        data = load_ndjson(DATA)
        output = list()
        for item in data:
            tabular_data = jalc_processor.csv_creator(item)
            if tabular_data:
                output.append(tabular_data)
            for citation in item['citation_list']:
                tabular_data_cited = jalc_processor.csv_creator(citation)
                if tabular_data_cited:
                    output.append(tabular_data_cited)
        expected_output=[
            {'id': 'doi:10.11230/jsts.17.1_11',
             'title': 'Development of an Airtight Recirculating Zooplankton Culture Device for Closed Ecological Recirculating Aquaculture System (CERAS)',
             'author': 'OMORI, Katsunori; WATANABE, Shigeki; ENDO, Masato; TAKEUCHI, Toshio; OGUCHI, Mitsuo',
             'issue': '1',
             'volume': '17',
             'venue': 'The Journal of Space Technology and Science [issn:0911-551X issn:2186-4772 jid:jsts]',
             'pub_date': '2001',
             'page': '111-117',
             'type': 'journal article',
             'publisher': '特定非営利活動法人 日本ロケット協会',
             'editor': ''},
            {'id': 'doi:10.11450/seitaikogaku1989.10.1',
             'title': 'Study on the Development of Closed Ecological Recirculating Aquaculture System(CERAS). I. Development of Fish-rearing Closed Tank.',
             'author': '',
             'issue': '1',
             'volume': '10',
             'venue': '',
             'pub_date': '1997',
             'page': '1-4',
             'type': '',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.11230/jsts.17.2_1',
             'title': 'Diamond Synthesis with Completely Closed Chamber from Solid or Liquid Carbon Sources, In-Situ Analysis of Gaseous Species and the Possible Reaction Model',
             'author': 'TAKAGI, Yoshiki',
             'issue': '2',
             'volume': '17',
             'venue': 'The Journal of Space Technology and Science [issn:0911-551X issn:2186-4772 jid:jsts]',
             'pub_date': '2001',
             'page': '21-27',
             'type': 'journal article',
             'publisher': '特定非営利活動法人 日本ロケット協会',
             'editor': ''},
            {'id': 'doi:10.1063/1.1656693',
             'title': '',
             'author': '',
             'issue': '',
             'volume': '39',
             'venue': '',
             'pub_date': '1968',
             'page': '2915-2915',
             'type': '',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1016/0022-0248(83)90411-6',
             'title': '',
             'author': '',
             'issue': '',
             'volume': '62',
             'venue': 'Journal of Crystal Growth',
             'pub_date': '1983',
             'page': '642-642',
             'type': '',
             'publisher': '',
             'editor': ''},
            {'id': 'doi:10.1016/0038-1098(88)91128-3',
             'title': 'Raman spectra of diamondlike amorphous carbon films.',
             'author': '',
             'issue': '11',
             'volume': '66',
             'venue': '',
             'pub_date': '1988',
             'page': '1177-1180',
             'type': '',
             'publisher': '',
             'editor': ''}]

        self.assertEqual(output, expected_output)

    def test_orcid_finder(self):
        datacite_processor = JalcProcessing(IOD)
        orcid_found = datacite_processor.orcid_finder('doi:10.11185/imt.8.380')
        expected_output = {'0000-0002-2149-4113': 'dobashi, yoshinori'}
        self.assertEqual(orcid_found, expected_output)

    def test_get_agents_strings_list_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        authors_list = [
            {'role': 'author',
             'name': '井崎 豊田, 理理子',
             'family': '井崎 豊田',
             'given': '理理子'},
            {'role': 'author',
             'name': '豊田, 純一朗',
             'family': '豊田',
             'given': '純一朗'}
            ]
        jalc_processor = JalcProcessing()
        csv_manager = CSVManager()
        csv_manager.data = {'10.11224/cleftpalate1976.23.2_83': {'豊田, 純一朗 [0000-0002-8210-7076]'}}
        jalc_processor.orcid_index = csv_manager
        authors_strings_list, editors_strings_list = jalc_processor.get_agents_strings_list('10.11224/cleftpalate1976.23.2_83', authors_list)
        expected_authors_list = ['井崎 豊田, 理理子', '豊田, 純一朗 [orcid:0000-0002-8210-7076]']
        expected_editors_list = []
        self.assertEqual((authors_strings_list, editors_strings_list), (expected_authors_list, expected_editors_list))

    def test_preprocess_without_citing_entities_filepath(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT1):
            shutil.rmtree(OUTPUT1)
        oc_meta.run.jalc_process.preprocess(jalc_json_dir=MULTIPROCESS_OUTPUT, csv_dir=OUTPUT1, citing_entities_filepath=None, publishers_filepath=None, orcid_doi_filepath=None)
        output = dict()
        for file in os.listdir(OUTPUT1):
            with open(os.path.join(OUTPUT1, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output = {
            'filtered_46.csv': [
                {'id': 'doi:10.11209/jim.20.35', 'title': '腸内フローラの構造解析―定量的PCRを用いたヒト糞便中の菌叢解析―', 'author': '渡辺, 幸一', 'issue': '1', 'volume': '20', 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2006', 'page':'35-42', 'type':'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''},
                {'id': 'doi:10.1073/pnas.87.7.2725', 'title': 'Analysis of cytokine mRNA and DNA: Detection and quantitation by competitive polymerase chain reaction.', 'author': '', 'issue': '7', 'volume': '87', 'venue': '', 'pub_date': '1990', 'page': '2725-2729', 'type': '', 'publisher': '', 'editor': ''},
                {'id': 'doi:10.11209/jim.22.253', 'title': '16S rRNA配列を指標としたヒト腸内フローラ最優勢菌の系統関係', 'author': '松木, 隆広', 'issue': '4', 'volume': '22', 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2008', 'page': '253-261', 'type': 'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''},
                {'id': 'doi:10.11209/jim.20.35', 'title': '', 'author': '', 'issue': '', 'volume': '', 'venue': '', 'pub_date': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
            ],
            'filtered_47.csv': [
                {'id': 'doi:10.11224/cleftpalate1976.17.4_340', 'title': '合併奇形を有する口唇裂口蓋裂児の臨床統計的観察', 'author': '小野, 和宏; 大橋, 靖; 中野, 久; 飯田, 明彦; 神成, 庸二; 磯野, 信策', 'issue': '4', 'volume': '17', 'venue': 'Journal of Japanese Cleft Palate Association [issn:0386-5185]', 'pub_date': '1992', 'page': '340-355', 'type': 'journal article', 'publisher': 'Japanese Cleft Palate Association', 'editor': ''},
                {'id': 'doi:10.14930/jsma1939.45.339', 'title': 'The associated anomlies wite cleft lip and/or cleft palate.', 'author': '伊藤, 芳憲', 'issue': '3', 'volume': '45', 'venue': '', 'pub_date': '1985', 'page': '339-351', 'type': '', 'publisher': '', 'editor': ''}
            ]
        }
        self.assertEqual(output, expected_output)

    def test_preprocess_with_citing_entities_filepath(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT2):
            shutil.rmtree(OUTPUT2)
        oc_meta.run.jalc_process.preprocess(jalc_json_dir=MULTIPROCESS_OUTPUT, csv_dir=OUTPUT2, citing_entities_filepath=CITING_ENTITIES, publishers_filepath=None, orcid_doi_filepath=None)
        output = dict()
        for file in os.listdir(OUTPUT2):
            with open(os.path.join(OUTPUT2, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output = {
            'filtered_46.csv': [
                {'id': 'doi:10.11209/jim.20.35', 'title': '腸内フローラの構造解析―定量的PCRを用いたヒト糞便中の菌叢解析―', 'author': '渡辺, 幸一', 'issue': '1', 'volume': '20', 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2006', 'page':'35-42', 'type':'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''},
                {'id': 'doi:10.1073/pnas.87.7.2725', 'title': 'Analysis of cytokine mRNA and DNA: Detection and quantitation by competitive polymerase chain reaction.', 'author': '', 'issue': '7', 'volume': '87', 'venue': '', 'pub_date': '1990', 'page': '2725-2729', 'type': '', 'publisher': '', 'editor': ''},
                {'id': 'doi:10.11209/jim.22.253', 'title': '16S rRNA配列を指標としたヒト腸内フローラ最優勢菌の系統関係', 'author': '松木, 隆広', 'issue': '4', 'volume': '22', 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2008', 'page': '253-261', 'type': 'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''}
            ],
            'filtered_47.csv': [
                {'id': 'doi:10.11224/cleftpalate1976.17.4_340', 'title': '合併奇形を有する口唇裂口蓋裂児の臨床統計的観察', 'author': '小野, 和宏; 大橋, 靖; 中野, 久; 飯田, 明彦; 神成, 庸二; 磯野, 信策', 'issue': '4', 'volume': '17', 'venue': 'Journal of Japanese Cleft Palate Association [issn:0386-5185]', 'pub_date': '1992', 'page': '340-355', 'type': 'journal article', 'publisher': 'Japanese Cleft Palate Association', 'editor': ''},
                {'id': 'doi:10.14930/jsma1939.45.339', 'title': 'The associated anomlies wite cleft lip and/or cleft palate.', 'author': '伊藤, 芳憲', 'issue': '3', 'volume': '45', 'venue': '', 'pub_date': '1985', 'page': '339-351', 'type': '', 'publisher': '', 'editor': ''}
            ]
        }
        self.assertEqual(output, expected_output)
        
#python -m unittest discover -s test -p "jalc_processing_test.py"



