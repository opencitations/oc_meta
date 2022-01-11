import unittest
from meta.scripts.curator import *
from meta.scripts.creator import Creator
import csv
from SPARQLWrapper import SPARQLWrapper
from pprint import pprint
from oc_ocdm import Storer
from datetime import datetime


SERVER = 'http://127.0.0.1:9999/blazegraph/sparql'
BASE_DIR = 'meta/tdd'
MANUAL_DATA_CSV = f'{BASE_DIR}/manual_data.csv'
MANUAL_DATA_RDF = f'{BASE_DIR}/testcases/ts/testcase_ts-13.ttl'
REAL_DATA_CSV = f'{BASE_DIR}/real_data.csv'
REAL_DATA_RDF = f'{BASE_DIR}/testcases/ts/real_data.nt'
BASE_IRI = 'https://w3id.org/oc/meta/'
CURATOR_COUNTER_DIR = f'{BASE_DIR}/curator_counter'
OUTPUT_DIR = f'{BASE_DIR}/output'


def get_path(path:str) -> str:
    # absolute_path:str = os.path.abspath(path)
    universal_path = path.replace('\\', '/')
    return universal_path

def reset():
    with open(get_path(f'{CURATOR_COUNTER_DIR}/br.txt'), 'w') as br:
        br.write('0')
    with open(get_path(f'{CURATOR_COUNTER_DIR}/id.txt'), 'w') as br:
        br.write('0')
    with open(get_path(f'{CURATOR_COUNTER_DIR}/ra.txt'), 'w') as br:
        br.write('0')
    with open(get_path(f'{CURATOR_COUNTER_DIR}/ar.txt'), 'w') as br:
        br.write('0')
    with open(get_path(f'{CURATOR_COUNTER_DIR}/re.txt'), 'w') as br:
        br.write('0')

def reset_server(server:str=SERVER) -> None:
    ts = sparql.SPARQLServer(server)
    ts.update('delete{?x ?y ?z} where{?x ?y ?z}')

def add_data_ts(server:str=SERVER, data_path:str=REAL_DATA_RDF):
    reset_server(server)
    ts = SPARQLWrapper(server)
    ts.method = 'POST'
    # f_path = os.path.abspath('meta/tdd/testcases/ts/testcase_ts-13.ttl').replace('\\', '/')
    f_path = get_path(data_path)
    ts.setQuery(f'LOAD <file:{f_path}>')
    ts.query()

def store_curated_data(curator_obj:Curator, server:str) -> None:
    creator_obj = Creator(curator_obj.data, BASE_IRI, None, None,
                            curator_obj.index_id_ra, curator_obj.index_id_br, curator_obj.re_index,
                            curator_obj.ar_index, curator_obj.VolIss)
    creator = creator_obj.creator(source=None)
    res_storer = Storer(creator)
    res_storer.upload_all(server, base_dir=None, batch_size=100)

def data_collect(csv_path:str) -> List[dict]:
    with open(get_path(csv_path), 'r', encoding='utf-8') as csvfile:
        data = list(csv.DictReader(csvfile, delimiter=','))
    return data

def prepare_to_test(data, name):
    reset()
    
    reset_server(SERVER)
    if float(name) > 12:
        add_data_ts(SERVER, MANUAL_DATA_RDF)

    testcase_csv = get_path('meta/tdd/testcases/testcase_data/testcase_' + name + '_data.csv')
    testcase_id_br = get_path('meta/tdd/testcases/testcase_data/indices/' + name + '/index_id_br_' + name + '.csv')
    testcase_id_ra = get_path('meta/tdd/testcases/testcase_data/indices/' + name + '/index_id_ra_' + name + '.csv')
    testcase_ar = get_path('meta/tdd/testcases/testcase_data/indices/' + name + '/index_ar_' + name + '.csv')
    testcase_re = get_path('meta/tdd/testcases/testcase_data/indices/' + name + '/index_re_' + name + '.csv')
    testcase_vi = get_path('meta/tdd/testcases/testcase_data/indices/' + name + '/index_vi_' + name + '.json')

    curator_obj = Curator(data, SERVER, info_dir=get_path(f'{CURATOR_COUNTER_DIR}/'))
    curator_obj.curator()
    with open(testcase_csv, 'r', encoding='utf-8') as csvfile:
        testcase_csv = list(csv.DictReader(csvfile, delimiter=','))

    with open(testcase_id_br, 'r', encoding='utf-8') as csvfile:
        testcase_id_br = list(csv.DictReader(csvfile, delimiter=','))

    with open(testcase_id_ra, 'r', encoding='utf-8') as csvfile:
        testcase_id_ra = list(csv.DictReader(csvfile, delimiter=','))

    with open(testcase_ar, 'r', encoding='utf-8') as csvfile:
        testcase_ar = list(csv.DictReader(csvfile, delimiter=','))

    with open(testcase_re, 'r', encoding='utf-8') as csvfile:
        testcase_re = list(csv.DictReader(csvfile, delimiter=','))

    with open(testcase_vi) as json_file:
        testcase_vi = json.load(json_file)
    
    testcase = [testcase_csv, testcase_id_br, testcase_id_ra, testcase_ar, testcase_re, testcase_vi]
    data_curated = [curator_obj.data, curator_obj.index_id_br, curator_obj.index_id_ra, curator_obj.ar_index,
                    curator_obj.re_index, curator_obj.VolIss]
    return data_curated, testcase

def prepareCurator(data:list, server:str=SERVER) -> Curator:
    reset()
    return Curator(data, server, info_dir=get_path(f'{CURATOR_COUNTER_DIR}/'))


class test_Curator(unittest.TestCase):
    def test_merge_entities_in_csv(self):
        curator = prepareCurator(list())
        with open(f'{CURATOR_COUNTER_DIR}/id.txt', "w") as f:
            f.writelines('4\n')
        entity_dict = {'0601': {'ids': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': []}}
        id_dict = dict()
        curator.merge_entities_in_csv(['doi:10.1787/eco_outlook-v2011-2-graph138-en'], '0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', entity_dict, id_dict)
        expected_output = (
            {'0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': []}},
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0605'}
        )
        self.assertEqual((entity_dict, id_dict), expected_output)

    def test_clean_id_list(self):
        input = ['doi:10.001/B-1', 'wikidata:B1111111', 'META:br/060101']
        output = Curator.clean_id_list(input, br=True)
        expected_output = (['doi:10.001/B-1', 'wikidata:B1111111'], '060101')
        self.assertEqual(output, expected_output)
    
    def test__add_number(self):
        reset()
        input = f'{CURATOR_COUNTER_DIR}/br.txt'
        output = Curator._add_number(input)
        expected_output = 1
        self.assertEqual(output, expected_output)
    
    def test_equalizer(self):
        add_data_ts()
        curator = prepareCurator(list())
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '1972-12-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator.log[0] = {'id': {}}
        curator.equalizer(row, '4125')
        output = (curator.log, row)
        expected_output = (
            {0: {'id': {'status': 'ENTITY ALREADY EXISTS'}}}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '1972-12-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '106', 'issue': '6', 'page': '837-838', 'type': 'journal article', 'publisher': '', 'editor': ''}
        )
        self.assertEqual(output, expected_output)
    
    def test_clean_id(self):
        curator = prepareCurator(list())
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Blasto', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator.log[0] = {'id': {}}
        curator.clean_id(row)
        expected_output = {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        self.assertEqual(row, expected_output)
    
    def test_check_equality(self):
        data = [
            {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
            {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-02', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
            {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-03', 'venue': 'Archives Of Blast [meta:br/4416]', 'volume': '105', 'issue': '2', 'page': '106-108', 'type': 'journal volume', 'publisher': '', 'editor': ''},
        ]
        curator = prepareCurator(list())
        curator.data = data
        for i in range(3):
            curator.log[i] = {
                'id': {},
                'author': {},
                'venue': {},
                'editor': {},
                'publisher': {},
                'page': {},
                'volume': {},
                'issue': {},
                'pub_date': {},
                'type': {}
            }
        curator.brdict = {'3757': {'ids': ['doi:10.1001/archderm.104.1.106', 'pmid:29098884'], 'title': 'Multiple Keloids', 'others': ['wannabe_0']}}
        curator.check_equality()
        expected_output = (
            [
                {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}, 
                {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
                {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
            ],
            {
                0: {'id': {}, 'author': {}, 'venue': {}, 'editor': {}, 'publisher': {}, 'page': {}, 'volume': {}, 'issue': {}, 'pub_date': {}, 'type': {}}, 
                1: {'id': {'status': 'ENTITY ALREADY EXISTS'}, 'author': {}, 'venue': {}, 'editor': {}, 'publisher': {}, 'page': {}, 'volume': {}, 'issue': {}, 'pub_date': {}, 'type': {}}, 
                2: {'id': {'status': 'ENTITY ALREADY EXISTS'}, 'author': {}, 'venue': {}, 'editor': {}, 'publisher': {}, 'page': {}, 'volume': {}, 'issue': {}, 'pub_date': {}, 'type': {}}
            }
        )
        self.assertEqual((curator.data, curator.log), expected_output)

    def test_clean_vvi_all_data_on_ts(self):
        # All data are already on the triplestore. They need to be retrieved and organized correctly
        add_data_ts()
        row = {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {
            '4416': {
                'issue': {}, 
                'volume': {
                    '107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, 
                    '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, 
                    '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, 
                    '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, 
                    '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, 
                    '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, 
                    '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, 
                    '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, 
                    '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, 
                    '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}
                }
            }
        }
        self.assertEqual(curator.vvi, expected_output)

    def test_clean_vvi_new_venue(self):
        # It is a new venue
        add_data_ts()
        row = {'id': 'wannabe_1', 'title': 'Money growth, interest rates, inflation and raw materials prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'wannabe_0': {'volume': {'2011': {'id': 'wannabe_1', 'issue': {'2': {'id': 'wannabe_2'}}}}, 'issue': {}}}
        self.assertEqual(curator.vvi, expected_output)

    def test_clean_vvi_new_volume_and_issue(self):
        # There are a new volume and a new issue
        add_data_ts()
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'Archives Of Surgery [meta:br/4480]', 'volume': '99', 'issue': '1', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {
            '4480': {
                'issue': {}, 
                'volume': {
                    '147': {'id': '4481', 'issue': {'11': {'id': '4482'}, '12': {'id': '4487'}}}, 
                    '99': {'id': 'wannabe_0', 'issue': {'1': {'id': 'wannabe_1'}}}
                }
            }
        }
        self.assertEqual(curator.vvi, expected_output)

    # def test_volume_issue(self):
    #     curator = prepareCurator(list())
    #     meta = 'wannabe_0' 
    #     path = {'1': {'id': '060310', 'issue': {'5-6': {'id': '060300'}}}} 
    #     value = '1' 
    #     row = {'id': 'wannabe_0', 'title': 'Title1', 'author': 'Surname1, Name1 [orcid:0000-0001]', 'pub_date': '', 'venue': '060301', 'volume': '', 'issue': '', 'page': '266-278', 'type': 'journal volume', 'publisher': 'pub1 [crossref:1111]', 'editor': ''}
    #     curator.volume_issue(meta, path, value, row)

    def test_clean_ra_with_br_metaid(self):
        add_data_ts()
        # One author is in the triplestore, the other is not. 
        # br_metaval is a MetaID
        # There are two ids for one author
        row = {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W.; McSorley, J. [orcid:0000-0002-8420-0698 schema:12345]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'3757': {'ids': ['doi:10.1001/archderm.104.1.106'], 'title': 'Multiple Keloids', 'others': []}}
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}}, 
            {'6033': {'ids': [], 'others': [], 'title': 'Curth, W.'}, 'wannabe_0': {'ids': ['orcid:0000-0002-8420-0698', 'schema:12345'], 'others': [], 'title': 'Mcsorley, J.'}}, 
            {'orcid:0000-0002-8420-0698': '0601', 'schema:12345': '0602'}
        )
        self.assertEqual(output, expected_output)

    def test_clean_ra_with_br_wannabe(self):
        add_data_ts()
        # Authors not on the triplestore. 
        # br_metaval is a wannabe
        row = {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': 'Curth, W. [orcid:0000-0002-8420-0697] ; McSorley, J. [orcid:0000-0002-8420-0698]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'wannabe_0': {'ids': ['doi:10.1001/archderm.104.1.106'], 'title': 'Multiple Keloids', 'others': []}}
        curator.wnb_cnt = 1
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'wannabe_0': {'author': [('0601', 'wannabe_1'), ('0602', 'wannabe_2')], 'editor': [], 'publisher': []}}, 
            {'wannabe_1': {'ids': ['orcid:0000-0002-8420-0697'], 'others': [], 'title': 'Curth, W.'}, 'wannabe_2': {'ids': ['orcid:0000-0002-8420-0698'], 'others': [], 'title': 'Mcsorley, J.'}}, 
            {'orcid:0000-0002-8420-0697': '0601', 'orcid:0000-0002-8420-0698': '0602'}
        )
        self.assertEqual(output, expected_output)
    
    def test_meta_maker(self):
        curator = prepareCurator(list())
        curator.brdict = {'3757': {'ids': ['doi:10.1001/archderm.104.1.106', 'pmid:29098884'], 'title': 'Multiple Keloids', 'others': []}, '4416': {'ids': ['issn:0003-987X'], 'title': 'Archives Of Dermatology', 'others': []}}
        curator.radict = {'6033': {'ids': [], 'others': [], 'title': 'Curth, W.'}, 'wannabe_0': {'ids': ['orcid:0000-0002-8420-0698', 'schema:12345'], 'others': [], 'title': 'Mcsorley, J.'}}
        curator.ardict = {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}}
        curator.vvi = {'4416': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        curator.meta_maker()
        output = (curator.brmeta, curator.rameta, curator.armeta)
        expected_output = (
            {'3757': {'ids': ['doi:10.1001/archderm.104.1.106', 'pmid:29098884', 'meta:br/3757'], 'title': 'Multiple Keloids', 'others': []}, '4416': {'ids': ['issn:0003-987X', 'meta:br/4416'], 'title': 'Archives Of Dermatology', 'others': []}},
            {'6033': {'ids': ['meta:ra/6033'], 'others': [], 'title': 'Curth, W.'}, '0601': {'ids': ['orcid:0000-0002-8420-0698', 'schema:12345', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Mcsorley, J.'}},
            {'3757': {'author': [('9445', '6033'), ('0601', '0601')], 'editor': [], 'publisher': []}}
        )
        self.assertEqual(output, expected_output)

    def test_enricher(self):
        curator = prepareCurator(list())
        curator.data = [{'id': 'wannabe_0', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'wannabe_1', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'OECD [crossref:1963]', 'editor': ''}]
        curator.brmeta = {
            '0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'meta:br/0601'], 'others': ['wannabe_0'], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            '0602': {'ids': ['meta:br/0604'], 'others': ['wannabe_1'], 'title': 'OECD Economic Outlook'}
        }
        curator.armeta = {'0601': {'author': [], 'editor': [], 'publisher': [('0601', '0601')]}}
        curator.rameta = {'0601': {'ids': ['crossref:1963', 'meta:ra/0601'], 'others': ['wannabe_2'], 'title': 'Oecd'}}
        curator.remeta = dict()
        curator.enrich()
        output = curator.data
        expected_output = [{'id': 'doi:10.1787/eco_outlook-v2011-2-graph138-en meta:br/0601', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook [meta:br/0604]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Oecd [crossref:1963 meta:ra/0601]', 'editor': ''}]
        self.assertEqual(output, expected_output)


class test_id_worker(unittest.TestCase):
    def test_id_worker_1(self):
        # 1 EntityA is a new one
        add_data_ts()
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = ['doi:10.1163/2214-8655_lgo_lgo_02_0074_ger']
        wannabe_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': ['doi:10.1163/2214-8655_lgo_lgo_02_0074_ger'], 'others': [], 'title': 'βέβαιος, α, ον'}},
            {},
            {'doi:10.1163/2214-8655_lgo_lgo_02_0074_ger': '0601'},
            {},
            {},
            {},
            {}
        )
        self.assertEqual(output, expected_output)

    def test_id_worker_1_no_id(self):
        # 1 EntityA is a new one and has no ids
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = []
        wannabe_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': [], 'others': [], 'title': 'βέβαιος, α, ον'}},
            {},
            {},
            {},
            {},
            {},
            {}
        )
        self.assertEqual(output, expected_output)

    def test_id_worker_2_id_ts(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        idslist = ['crossref:10']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {}, {}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_2_metaid_ts(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # MetaID only
        idslist = ['meta:ra/3309']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {}, {}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_2_id_metaid_ts(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # ID and MetaID
        idslist = ['crossref:10', 'meta:id/4274']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {}, {}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_3(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV. MetaID on ts has precedence
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # ID and MetaID, but it's meta:id/4274 on ts
        idslist = ['crossref:10', 'meta:id/4275']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {}, {}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_2_meta_in_entity_dict(self):
        # MetaID exists among data.
        # MetaID already in entity_dict (no care about conflicts, we have a MetaID specified)
        # 2 Retrieve EntityA data to update EntityA inside CSV
        data = data_collect(REAL_DATA_CSV)
        curator = prepareCurator(data)
        curator.curator()
        store_curated_data(curator, SERVER)
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['meta:br/0601']
        curator_empty = prepareCurator(list())
        # put metaval in entity_dict
        meta_id = curator_empty.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        # metaval is in entity_dict
        meta_id = curator_empty.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator_empty.brdict, curator_empty.radict, curator_empty.idbr, curator_empty.idra, curator_empty.conflict_br, curator_empty.conflict_ra, curator_empty.log)
        expected_output = ('0601', {'0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': []}}, {}, {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, {}, {}, {}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_conflict(self):
        # there's no meta or there was one but it didn't exist
        # There are other ids that already exist, but refer to multiple entities on ts.
        # Conflict!
        add_data_ts()
        idslist = ['doi:10.1001/2013.jamasurg.270']
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        curator = prepareCurator(list())
        curator.log[0] = {'id': {}}
        id_dict = dict()
        metaval = curator.conflict(idslist, name, id_dict, 'id') # Only the conflict function is tested here, not id_worker
        output = (metaval, curator.conflict_br, curator.log, id_dict)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': ['doi:10.1001/2013.jamasurg.270'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}},
            {0: {'id': {'Conflict Entity': 'wannabe_0'}}}, 
            {'doi:10.1001/2013.jamasurg.270': '2585'}
        )
        self.assertEqual(output, expected_output)

    def test_conflict_br(self):
        # No MetaId, an identifier to which two separate br point: there is a conflict, and a new entity must be created
        add_data_ts()
        curator = prepareCurator(list())
        curator.log[0] = {'id': {}}
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1001/2013.jamasurg.270']
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0', 
            {'doi:10.1001/2013.jamasurg.270': '2585'}, 
            {}, 
            {'wannabe_0': {'ids': ['doi:10.1001/2013.jamasurg.270'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}}, 
            {}, 
            {0: {'id': {'Conflict Entity': 'wannabe_0'}}}
        )
        self.assertEqual(output, expected_output)

    def test_conflict_ra(self):
        # No MetaId, an identifier to which two separate ra point: there is a conflict, and a new entity must be created
        add_data_ts()
        idslist = ['orcid:0000-0001-6994-8412']
        name = 'Alarcon, Louis H.'
        curator = prepareCurator(list())
        curator.log[0] = {'author': {}}
        meta_id = curator.id_worker('author', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0', 
            {}, 
            {'orcid:0000-0001-6994-8412': '4475'}, 
            {}, 
            {'wannabe_0': {'ids': ['orcid:0000-0001-6994-8412'], 'others': [], 'title': 'Alarcon, Louis H.'}}, 
            {0: {'author': {'Conflict Entity': 'wannabe_0'}}}
        )
        self.assertEqual(output, expected_output)
    
    def test_conflict_existing(self):
        # ID already exist in entity_dict but refer to multiple entities having a MetaID
        reset_server()
        br_dict = {
            'meta:br/0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            'meta:br/0602': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'], 'others': [], 'title': 'Contributions To GDP Growth And Inflation: South Africa'}, 
            'meta:br/0603': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'}, 
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator = prepareCurator(list())
        curator.log[0] = {'id': {}}
        curator.brdict = br_dict
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0', 
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, 
            {}, 
            {'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}}, 
            {}, 
            {0: {'id': {'Conflict Entity': 'wannabe_0'}}}
        )
        self.assertEqual(output, expected_output)

    def test_no_conflict_existing(self):
        # ID already exist in entity_dict and refer to one entity
        reset_server()
        br_dict = {
            'meta:br/0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            'meta:br/0602': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'], 'others': [], 'title': 'Contributions To GDP Growth And Inflation: South Africa'}, 
            'meta:br/0603': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph18-en'], 'others': [], 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'}, 
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan' # The first title must have precedence (China, not Japan)
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator = prepareCurator(list())
        curator.log[0] = {'id': {}}
        curator.brdict = br_dict
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'meta:br/0601', 
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, 
            {}, 
            {}, 
            {}, 
            {0: {'id': {}}}
        )
        self.assertEqual(output, expected_output)

    def test_conflict_suspect_id_among_existing(self):
        # ID already exist in entity_dict and refer to one entity having a MetaID, but there is another ID not in entity_dict that highlights a conflict on ts
        add_data_ts()
        br_dict = {
            'meta:br/0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            'meta:br/0602': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'], 'others': [], 'title': 'Contributions To GDP Growth And Inflation: South Africa'}, 
            'meta:br/0603': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph18-en'], 'others': [], 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'}, 
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan' # The first title must have precedence (China, not Japan)
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270']
        curator = prepareCurator(data_collect(REAL_DATA_CSV))
        curator.log[0] = {'id': {}}
        curator.brdict = br_dict
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0', 
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601', 
                'doi:10.1001/2013.jamasurg.270': '2585'
            }, 
            {}, 
            {'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan'}}, 
            {}, 
            {0: {'id': {'Conflict Entity': 'wannabe_0'}}}
        )
        self.assertEqual(output, expected_output)

    def test_conflict_suspect_id_among_wannabe(self):
        # ID already exist in entity_dict and refer to one temporary, but there is another ID not in entity_dict that highlights a conflict on ts
        add_data_ts()
        br_dict = {
            'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            'wannabe_2': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'], 'others': [], 'title': 'Contributions To GDP Growth And Inflation: South Africa'}, 
            'wannabe_3': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph18-en'], 'others': [], 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'}, 
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan' # The first title must have precedence (China, not Japan)
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270']
        curator = prepareCurator(data_collect(REAL_DATA_CSV))
        curator.log[0] = {'id': {}}
        curator.brdict = br_dict
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0', 
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601', 
                'doi:10.1001/2013.jamasurg.270': '2585'
            }, 
            {}, 
            {'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan'}}, 
            {}, 
            {0: {'id': {'Conflict Entity': 'wannabe_0'}}}
        )
        self.assertEqual(output, expected_output)

    def test_id_worker_4(self):
        # 4 Merge data from EntityA (CSV) with data from EntityX (CSV), update both with data from EntityA (RDF)
        add_data_ts()
        br_dict = {
            'wannabe_0': {'ids': ['doi:10.1001/archderm.104.1.106'], 'others': [], 'title': 'Multiple eloids'}, 
            'wannabe_1': {'ids': ['doi:10.1001/archderm.104.1.106'], 'others': [], 'title': 'Multiple Blastoids'}, 
        }
        name = 'Multiple Palloids'
        idslist = ['doi:10.1001/archderm.104.1.106', 'pmid:29098884']
        curator = prepareCurator(list())
        curator.brdict = br_dict
        curator.wnb_cnt = 2
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            '3757', 
            {'doi:10.1001/archderm.104.1.106': '3624', 'pmid:29098884': '2000000'}, 
            {}, 
            {}, 
            {}, 
            {}
        )
        self.assertEqual(output, expected_output)

    def test_id_worker_5(self):
        # ID already exist in entity_dict and refer to one or more temporary entities -> collective merge
        reset_server()
        br_dict = {
            'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            'wannabe_1': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'], 'others': [], 'title': 'Contributions To GDP Growth And Inflation: South Africa'}, 
            'wannabe_2': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'others': [], 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'}, 
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator = prepareCurator(list())
        curator.brdict = br_dict
        curator.wnb_cnt = 2
        meta_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.conflict_br, curator.conflict_ra, curator.log)
        expected_output = (
            'wannabe_0', 
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, 
            {}, 
            {}, 
            {}, 
            {}
        )
        self.assertEqual(output, expected_output)


class testcase_01(unittest.TestCase):
    def test(self):
        # testcase1: 2 different issues of the same venue (no volume)
        name = '01'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[0])
        partial_data.append(data[5])
        data_curated, testcase = prepare_to_test(partial_data, name)
        for pos, element in enumerate(data_curated):
            self.assertEqual(element, testcase[pos])


class testcase_02(unittest.TestCase):
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        name = '02'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[1])
        partial_data.append(data[3])
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_03(unittest.TestCase):
    def test(self):
        # testcase3: 2 different issues of the same volume
        name = '03'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[2])
        partial_data.append(data[4])
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_04(unittest.TestCase):
    def test(self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        name = '04'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[6])
        partial_data.append(data[7])
        data_curated, testcase = prepare_to_test(partial_data, name)
        for pos, element in enumerate(data_curated):
            self.assertEqual(element, testcase[pos])


class testcase_05(unittest.TestCase):
    def test(self):
        # testcase5: NO ID scenario
        name = '05'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[8])
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_06(unittest.TestCase):
    def test(self):
        # testcase6: ALL types test
        name = '06'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[9:33]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_07(unittest.TestCase):
    def test(self):
        # testcase7: all journal related types with an editor
        name = '07'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[34:40]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_08(unittest.TestCase):
    def test(self):
        # testcase8: all book related types with an editor
        name = '08'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[40:43]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_09(unittest.TestCase):
    def test(self):
        # testcase09: all proceeding related types with an editor
        name = '09'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[43:45]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_10(unittest.TestCase):
    def test(self):
        # testcase10: a book inside a book series and a book inside a book set
        name = '10'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[45:49]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_11(unittest.TestCase):
    def test(self):
        # testcase11: real time entity update
        name = '11'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[49:52]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_12(unittest.TestCase):
    def test(self):
        # testcase12: clean name, title, ids
        name = '12'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[52:53]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_13(unittest.TestCase):
    # testcase13: ID_clean massive test

    def test1(self):
        # 1--- meta specified br in a row, wannabe with a new id in a row, meta specified with an id related to wannabe
        # in a row
        name = '13.1'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[53:56]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test2(self):
        # 2---Conflict with META precedence: a br has a meta_id and an id related to another meta_id, the first
        # specified meta has precedence
        data = data_collect(MANUAL_DATA_CSV)
        name = '13.2'
        partial_data = data[56:57]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test3(self):
        # 3--- conflict: br with id shared with 2 meta
        data = data_collect(MANUAL_DATA_CSV)
        name = '13.3'
        partial_data = data[57:58]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_14(unittest.TestCase):

    def test1(self):
        # update existing sequence, in particular, a new author and an existing author without an existing id (matched
        # thanks to surname,name(BAD WRITTEN!)
        name = '14.1'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[58:59]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test2(self):
        # same sequence different order, with new ids
        name = '14.2'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[59:60]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test3(self):
        # RA
        # Author with two different ids
        name = '14.3'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[60:61]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test4(self):
        # meta specified ra in a row, wannabe ra with a new id in a row, meta specified with an id related to wannabe
        # in a ra
        name = '14.4'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[61:64]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_15(unittest.TestCase):

    def test1(self):
        # venue volume issue  already exists in ts
        name = '15.1'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[64:65]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test2(self):
        # venue conflict
        name = '15.2'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[65:66]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test3(self):
        # venue in ts is now the br
        name = '15.3'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[66:67]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test4(self):
        # br in ts is now the venue
        name = '15.4'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[67:68]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test5(self):
        # volume in ts is now the br
        name = '15.5'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[71:72]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test6(self):
        # br is a volume
        name = '15.6'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[72:73]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test7(self):
        # issue in ts is now the br
        name = '15.7'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[73:74]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test8(self):
        # br is a issue
        name = '15.8'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[74:75]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


class testcase_16(unittest.TestCase):

    def test1(self):
        # Date cleaning 2019-02-29
        name = '16.1'
        # add_data_ts('http://127.0.0.1:9999/blazegraph/sparql')
        # wrong date (2019/02/29)
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[75:76]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test2(self):
        # existing re
        name = '16.2'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[76:77]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)

    def test3(self):
        # given name for an RA with only a family name in TS
        name = '16.3'
        data = data_collect(MANUAL_DATA_CSV)
        partial_data = data[77:78]
        data_curated, testcase = prepare_to_test(partial_data, name)
        self.assertEqual(data_curated, testcase)


if __name__ == '__main__':
    unittest.main()

