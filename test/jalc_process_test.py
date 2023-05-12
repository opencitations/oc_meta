import csv
import os
import shutil
import unittest

from oc_meta.run.jalc_process import *
from oc_meta.lib.file_manager import get_csv_data

BASE = os.path.join('test', 'jalc_process')
OUTPUT1 = os.path.join(BASE, 'meta_input_without_citing')
OUTPUT2 = os.path.join(BASE, 'meta_input_with_citing')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
CITING_ENTITIES = os.path.join(BASE, 'cit_map_dir')
OUTPUT = os.path.join(BASE, 'output')
SUPPORT_MATERIAL = os.path.join(BASE, 'support_material')
IOD_SUPPORT = os.path.join(SUPPORT_MATERIAL, 'iod')
INPUT_SUPPORT = os.path.join(SUPPORT_MATERIAL, 'input')
PUBLISHERS_SUPPORT = os.path.join(SUPPORT_MATERIAL, 'publishers.csv')




class TestJalcProcess(unittest.TestCase):

    def test_preprocess_without_citing_entities_filepath(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT1):
            shutil.rmtree(OUTPUT1)
        preprocess(jalc_json_dir=MULTIPROCESS_OUTPUT, csv_dir=OUTPUT1, citing_entities_filepath=None, publishers_filepath=None, orcid_doi_filepath=None)
        output = dict()
        for file in os.listdir(OUTPUT1):
            with open(os.path.join(OUTPUT1, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output = {
            'filtered_46.csv': [
                {'id': 'doi:10.11209/jim.20.35', 'title': '腸内フローラの構造解析―定量的PCRを用いたヒト糞便中の菌叢解析―',
                 'author': '渡辺, 幸一', 'issue': '1', 'volume': '20',
                 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2006',
                 'page': '35-42', 'type': 'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''},
                {'id': 'doi:10.1073/pnas.87.7.2725',
                 'title': 'Analysis of cytokine mRNA and DNA: Detection and quantitation by competitive polymerase chain reaction.',
                 'author': '', 'issue': '7', 'volume': '87', 'venue': '', 'pub_date': '1990', 'page': '2725-2729',
                 'type': '', 'publisher': '', 'editor': ''},
                {'id': 'doi:10.11209/jim.22.253', 'title': '16S rRNA配列を指標としたヒト腸内フローラ最優勢菌の系統関係',
                 'author': '松木, 隆広', 'issue': '4', 'volume': '22',
                 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2008',
                 'page': '253-261', 'type': 'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''},
                {'id': 'doi:10.11209/jim.20.35', 'title': '', 'author': '', 'issue': '', 'volume': '', 'venue': '',
                 'pub_date': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
            ],
            'filtered_47.csv': [
                {'id': 'doi:10.11224/cleftpalate1976.17.4_340', 'title': '合併奇形を有する口唇裂口蓋裂児の臨床統計的観察',
                 'author': '小野, 和宏; 大橋, 靖; 中野, 久; 飯田, 明彦; 神成, 庸二; 磯野, 信策', 'issue': '4',
                 'volume': '17', 'venue': 'Journal of Japanese Cleft Palate Association [issn:0386-5185]',
                 'pub_date': '1992', 'page': '340-355', 'type': 'journal article',
                 'publisher': 'Japanese Cleft Palate Association', 'editor': ''},
                {'id': 'doi:10.14930/jsma1939.45.339',
                 'title': 'The associated anomlies wite cleft lip and/or cleft palate.', 'author': '伊藤, 芳憲',
                 'issue': '3', 'volume': '45', 'venue': '', 'pub_date': '1985', 'page': '339-351', 'type': '',
                 'publisher': '', 'editor': ''}
            ]
        }
        self.assertEqual(output, expected_output)


    def test_preprocess_with_citing_entities_filepath(self):
        self.maxDiff = None
        if os.path.exists(OUTPUT2):
            shutil.rmtree(OUTPUT2)
        preprocess(jalc_json_dir=MULTIPROCESS_OUTPUT, csv_dir=OUTPUT2, citing_entities_filepath=CITING_ENTITIES, publishers_filepath=None, orcid_doi_filepath=None)
        output = dict()
        for file in os.listdir(OUTPUT2):
            with open(os.path.join(OUTPUT2, file), 'r', encoding='utf-8') as f:
                output[file] = list(csv.DictReader(f))
        expected_output = {
            'filtered_46.csv': [
                {'id': 'doi:10.11209/jim.20.35', 'title': '腸内フローラの構造解析―定量的PCRを用いたヒト糞便中の菌叢解析―',
                 'author': '渡辺, 幸一', 'issue': '1', 'volume': '20',
                 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2006',
                 'page': '35-42', 'type': 'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''},
                {'id': 'doi:10.1073/pnas.87.7.2725',
                 'title': 'Analysis of cytokine mRNA and DNA: Detection and quantitation by competitive polymerase chain reaction.',
                 'author': '', 'issue': '7', 'volume': '87', 'venue': '', 'pub_date': '1990', 'page': '2725-2729',
                 'type': '', 'publisher': '', 'editor': ''},
                {'id': 'doi:10.11209/jim.22.253', 'title': '16S rRNA配列を指標としたヒト腸内フローラ最優勢菌の系統関係',
                 'author': '松木, 隆広', 'issue': '4', 'volume': '22',
                 'venue': '腸内細菌学雑誌 [issn:1343-0882 issn:1343-0882 issn:1349-8363 jid:jim]', 'pub_date': '2008',
                 'page': '253-261', 'type': 'journal article', 'publisher': '公益財団法人 腸内細菌学会', 'editor': ''}
            ],
            'filtered_47.csv': [
                {'id': 'doi:10.11224/cleftpalate1976.17.4_340', 'title': '合併奇形を有する口唇裂口蓋裂児の臨床統計的観察',
                 'author': '小野, 和宏; 大橋, 靖; 中野, 久; 飯田, 明彦; 神成, 庸二; 磯野, 信策', 'issue': '4',
                 'volume': '17', 'venue': 'Journal of Japanese Cleft Palate Association [issn:0386-5185]',
                 'pub_date': '1992', 'page': '340-355', 'type': 'journal article',
                 'publisher': 'Japanese Cleft Palate Association', 'editor': ''},
                {'id': 'doi:10.14930/jsma1939.45.339',
                 'title': 'The associated anomlies wite cleft lip and/or cleft palate.', 'author': '伊藤, 芳憲',
                 'issue': '3', 'volume': '45', 'venue': '', 'pub_date': '1985', 'page': '339-351', 'type': '',
                 'publisher': '', 'editor': ''}
            ]
        }
        self.assertEqual(output, expected_output)

    def test_preprocess_support_data(self):
        """Test that the support material is correctly used, if provided"""
        if os.path.exists(OUTPUT):
            shutil.rmtree(OUTPUT)
        preprocess(jalc_json_dir=INPUT_SUPPORT, publishers_filepath=PUBLISHERS_SUPPORT, citing_entities_filepath=None, csv_dir=OUTPUT, orcid_doi_filepath=IOD_SUPPORT)
        for file in os.listdir(OUTPUT):
            ent_list = get_csv_data(os.path.join(OUTPUT, file))
            for e in ent_list:
                if "doi:10.11178/jdsa.10.19" in e.get("id"):
                    self.assertEqual(e.get("publisher"), "筑波大学農林技術センター")
                if "doi:10.11178/jdsa.12.52" in e.get("id"):
                    self.assertEqual(e.get("author"), "Hirao, Akira S. [orcid:0000-0002-1115-8079]")
        for file in os.listdir(OUTPUT):
            os.remove(os.path.join(OUTPUT, file))

if __name__ == '__main__':
    unittest.main()
#python -m unittest discover -s test -p "jalc_process_test.py"