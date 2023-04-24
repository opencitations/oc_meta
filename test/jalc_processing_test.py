import os
import unittest

from oc_meta.lib.csvmanager import CSVManager
from oc_meta.plugins.jalc.jalc_processing import JalcProcessing
from oc_meta.run.jalc_process import load_ndjson

BASE = os.path.join('test', 'jalc_processing')
IOD = os.path.join(BASE, 'iod')
WANTED_DOIS = os.path.join(BASE, 'wanted_dois.csv')
WANTED_DOIS_FOLDER = os.path.join(BASE, 'wanted_dois')
DATA = os.path.join(BASE, 'filtered_66.ndjson')
DATA_DIR = BASE
OUTPUT = os.path.join(BASE, 'meta_input')
MULTIPROCESS_OUTPUT = os.path.join(BASE, 'multi_process_test')
PUBLISHERS_MAPPING = os.path.join(BASE, 'publishers.csv')


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
        
#python -m unittest discover -s test -p "jalc_processing_test.py"



