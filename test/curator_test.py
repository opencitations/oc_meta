from oc_meta.plugins.multiprocess.resp_agents_curator import RespAgentsCurator
from oc_meta.scripts.creator import Creator
from oc_meta.scripts.curator import *
from oc_ocdm import Storer
from pprint import pprint
from SPARQLWrapper import SPARQLWrapper, POST
import csv
import shutil
import unittest


SERVER = 'http://127.0.0.1:9999/blazegraph/sparql'
BASE_DIR = os.path.join('test')
MANUAL_DATA_CSV = f'{BASE_DIR}/manual_data.csv'
MANUAL_DATA_RDF = f'{BASE_DIR}/testcases/ts/testcase_ts-13.ttl'
REAL_DATA_CSV = os.path.join(BASE_DIR, 'real_data.csv')
REAL_DATA_RDF = f'{BASE_DIR}/testcases/ts/real_data.nt'
REAL_DATA_RDF_WITH_PROV = f'{BASE_DIR}/testcases/ts/real_data_with_prov.nq'
BASE_IRI = 'https://w3id.org/oc/meta/'
CURATOR_COUNTER_DIR = f'{BASE_DIR}/curator_counter'
OUTPUT_DIR = f'{BASE_DIR}/output'
PROV_CONFIG = f'{BASE_DIR}/prov_config.json'


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
    ts = SPARQLWrapper(server)
    ts.setQuery('delete{?x ?y ?z} where{?x ?y ?z}')
    ts.setMethod(POST)
    ts.query()

def add_data_ts(server:str=SERVER, data_path:str=REAL_DATA_RDF):
    reset_server(server)
    ts = SPARQLWrapper(server)
    ts.method = 'POST'
    f_path = get_path(data_path)
    ts.setQuery(f'LOAD <file:{f_path}>')
    ts.query()

def store_curated_data(curator_obj:Curator, server:str) -> None:
    creator_obj = Creator(curator_obj.data, SERVER, BASE_IRI, None, None, 'https://orcid.org/0000-0002-8420-0696',
                            curator_obj.index_id_ra, curator_obj.index_id_br, curator_obj.re_index,
                            curator_obj.ar_index, curator_obj.VolIss, preexisting_entities=set())
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

    testcase_csv = get_path('test/testcases/testcase_data/testcase_' + name + '_data.csv')
    testcase_id_br = get_path('test/testcases/testcase_data/indices/' + name + '/index_id_br_' + name + '.csv')
    testcase_id_ra = get_path('test/testcases/testcase_data/indices/' + name + '/index_id_ra_' + name + '.csv')
    testcase_ar = get_path('test/testcases/testcase_data/indices/' + name + '/index_ar_' + name + '.csv')
    testcase_re = get_path('test/testcases/testcase_data/indices/' + name + '/index_re_' + name + '.csv')
    testcase_vi = get_path('test/testcases/testcase_data/indices/' + name + '/index_vi_' + name + '.json')

    curator_obj = Curator(data, SERVER, prov_config=PROV_CONFIG, info_dir=get_path(f'{CURATOR_COUNTER_DIR}/'))
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

def prepareCurator(data:list, server:str=SERVER, resp_agents_only:bool=False) -> Curator:
    reset()
    if resp_agents_only:
        curator = RespAgentsCurator(data, server, prov_config=PROV_CONFIG, info_dir=get_path(f'{CURATOR_COUNTER_DIR}/'))
    else:
        curator = Curator(data, server, prov_config=PROV_CONFIG, info_dir=get_path(f'{CURATOR_COUNTER_DIR}/'))
    return curator


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
        expected_output = (['doi:10.001/b-1', 'wikidata:B1111111'], '060101')
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
            {0: {'id': {'status': 'Entity already exists'}}}, 
            {'id': '', 'title': '', 'author': 'Katz, R. [meta:ra/6376]', 'pub_date': '1972-12-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '106', 'issue': '6', 'page': '837-838', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [meta:ra/3309 crossref:10]', 'editor': ''}
        )
        self.assertEqual(output, expected_output)
    
    def test_clean_id_metaid_not_in_ts(self):
        # A MetaId was specified, but it is not on ts. Therefore, it is invalid
        add_data_ts()
        curator = prepareCurator(list())
        row = {'id': 'meta:br/131313', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator.log[0] = {'id': {}}
        curator.clean_id(row)
        expected_output = {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        self.assertEqual(row, expected_output)

    def test_clean_id(self):
        add_data_ts()
        curator = prepareCurator(list())
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Blasto', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator.log[0] = {'id': {}}
        curator.clean_id(row)
        expected_output = {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [meta:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [meta:ra/3309 crossref:10]', 'editor': ''}
        self.assertEqual(row, expected_output)
    
    def test_merge_duplicate_entities(self):
        add_data_ts()
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
        curator.merge_duplicate_entities()
        expected_output = (
            [
                {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}, 
                {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [meta:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [meta:ra/3309 crossref:10]', 'editor': ''},
                {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [meta:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [meta:ra/3309 crossref:10]', 'editor': ''}
            ],
            {
                0: {'id': {}, 'author': {}, 'venue': {}, 'editor': {}, 'publisher': {}, 'page': {}, 'volume': {}, 'issue': {}, 'pub_date': {}, 'type': {}}, 
                1: {'id': {'status': 'Entity already exists'}, 'author': {}, 'venue': {}, 'editor': {}, 'publisher': {}, 'page': {}, 'volume': {}, 'issue': {}, 'pub_date': {}, 'type': {}}, 
                2: {'id': {'status': 'Entity already exists'}, 'author': {}, 'venue': {}, 'editor': {}, 'publisher': {}, 'page': {}, 'volume': {}, 'issue': {}, 'pub_date': {}, 'type': {}}
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

    def test_clean_vvi_volume_with_title(self):
        # A journal volume having a title
        row = [{'id': '', 'title': 'The volume title', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}]
        curator = prepareCurator(row)
        curator.curator()
        expected_output = [{'id': 'meta:br/0601', 'title': 'The Volume Title', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook [meta:br/0602]', 'volume': '', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}]
        self.assertEqual(curator.data, expected_output)

    def test_clean_vvi_invalid_volume(self):
        # The data must be invalidated, because the resource is journal volume but an issue has also been specified
        add_data_ts()
        row = {'id': 'wannabe_1', 'title': '', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'wannabe_0': {'volume': {}, 'issue': {}}}
        self.assertEqual(curator.vvi, expected_output)

    def test_clean_vvi_invalid_venue(self):
        # The data must be invalidated, because the resource is journal but a volume has also been specified
        add_data_ts()
        row = {'id': 'wannabe_1', 'title': '', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'wannabe_0': {'volume': {}, 'issue': {}}}
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

    def test_clean_ra_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        row = {'id': 'wannabe_0', 'title': 'Giant Oyster Mushroom Pleurotus giganteus (Agaricomycetes) Enhances Adipocyte Differentiation and Glucose Uptake via Activation of PPARγ and Glucose Transporters 1 and 4 in 3T3-L1 Cells', 'author': 'Paravamsivam, Puvaneswari; Heng, Chua Kek; Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]; Sabaratnam, Vikineswary; M, Ravishankar Ram; Kuppusamy, Umah Rani', 'pub_date': '2016', 'venue': 'International Journal of Medicinal Mushrooms [issn:1521-9437]', 'volume': '18', 'issue': '9', 'page': '821-831', 'type': 'journal article', 'publisher': 'Begell House [crossref:613]', 'editor': ''}
        curator = prepareCurator(list())

        curator.brdict = {'wannabe_0': {'ids': ['doi:10.1615/intjmedmushrooms.v18.i9.60'], 'title': 'Giant Oyster Mushroom Pleurotus giganteus (Agaricomycetes) Enhances Adipocyte Differentiation and Glucose Uptake via Activation of PPARγ and Glucose Transporters 1 and 4 in 3T3-L1 Cells', 'others': []}}
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'wannabe_0': {'author': [('0601', 'wannabe_0'), ('0602', 'wannabe_1'), ('0603', 'wannabe_2'), ('0604', 'wannabe_3'), ('0605', 'wannabe_4'), ('0606', 'wannabe_5')], 'editor': [], 'publisher': []}}, 
            {'wannabe_0': {'ids': [], 'others': [], 'title': 'Paravamsivam, Puvaneswari'}, 'wannabe_1': {'ids': [], 'others': [], 'title': 'Heng, Chua Kek'}, 'wannabe_2': {'ids': ['orcid:0000-0001-6278-8559'], 'others': [], 'title': 'Malek, Sri Nurestri Abdul'}, 'wannabe_3': {'ids': [], 'others': [], 'title': 'Sabaratnam, Vikineswary'}, 'wannabe_4': {'ids': [], 'others': [], 'title': 'M, Ravishankar Ram'}, 'wannabe_5': {'ids': [], 'others': [], 'title': 'Kuppusamy, Umah Rani'}},
            {'orcid:0000-0001-6278-8559': '0601'}
        )
        self.assertEqual(output, expected_output)

    def test_clean_ra_with_br_metaid(self):
        add_data_ts()
        # One author is in the triplestore, the other is not. 
        # br_metaval is a MetaID
        # There are two ids for one author
        row = {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W.; McSorley, J. [orcid:0000-0003-0530-4305 schema:12345]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'3757': {'ids': ['doi:10.1001/archderm.104.1.106'], 'title': 'Multiple Keloids', 'others': []}}
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}}, 
            {'6033': {'ids': [], 'others': [], 'title': 'Curth, W.'}, 'wannabe_0': {'ids': ['orcid:0000-0003-0530-4305', 'schema:12345'], 'others': [], 'title': 'McSorley, J.'}}, 
            {'orcid:0000-0003-0530-4305': '0601', 'schema:12345': '0602'}
        )
        self.assertEqual(output, expected_output)

    def test_clean_ra_with_br_wannabe(self):
        add_data_ts()
        # Authors not on the triplestore. 
        # br_metaval is a wannabe
        row = {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': 'Curth, W. [orcid:0000-0002-8420-0696] ; McSorley, J. [orcid:0000-0003-0530-4305]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'wannabe_0': {'ids': ['doi:10.1001/archderm.104.1.106'], 'title': 'Multiple Keloids', 'others': []}}
        curator.wnb_cnt = 1
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'wannabe_0': {'author': [('0601', 'wannabe_1'), ('0602', 'wannabe_2')], 'editor': [], 'publisher': []}}, 
            {'wannabe_1': {'ids': ['orcid:0000-0002-8420-0696'], 'others': [], 'title': 'Curth, W.'}, 'wannabe_2': {'ids': ['orcid:0000-0003-0530-4305'], 'others': [], 'title': 'McSorley, J.'}}, 
            {'orcid:0000-0002-8420-0696': '0601', 'orcid:0000-0003-0530-4305': '0602'}
        )
        self.assertEqual(output, expected_output)

    def test_clean_ra_with_empty_square_brackets(self):
        add_data_ts()
        # One author's name contains a closed square bracket.
        row = {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Bernacki, Edward J. [    ]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [meta:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'3757': {'ids': ['doi:10.1001/archderm.104.1.106'], 'title': 'Multiple Keloids', 'others': []}}
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}},
            {'6033': {'ids': [], 'others': [], 'title': 'Curth, W.'}, 'wannabe_0': {'ids': [], 'others': [], 'title': 'Bernacki, Edward J.'}},
            {}
        )
        self.assertEqual(output, expected_output)
    
    def test_meta_maker(self):
        curator = prepareCurator(list())
        curator.brdict = {'3757': {'ids': ['doi:10.1001/archderm.104.1.106', 'pmid:29098884'], 'title': 'Multiple Keloids', 'others': []}, '4416': {'ids': ['issn:0003-987X'], 'title': 'Archives Of Dermatology', 'others': []}}
        curator.radict = {'6033': {'ids': [], 'others': [], 'title': 'Curth, W.'}, 'wannabe_0': {'ids': ['orcid:0000-0003-0530-4305', 'schema:12345'], 'others': [], 'title': 'Mcsorley, J.'}}
        curator.ardict = {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}}
        curator.vvi = {'4416': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        curator.meta_maker()
        output = (curator.brmeta, curator.rameta, curator.armeta)
        expected_output = (
            {'3757': {'ids': ['doi:10.1001/archderm.104.1.106', 'pmid:29098884', 'meta:br/3757'], 'title': 'Multiple Keloids', 'others': []}, '4416': {'ids': ['issn:0003-987X', 'meta:br/4416'], 'title': 'Archives Of Dermatology', 'others': []}},
            {'6033': {'ids': ['meta:ra/6033'], 'others': [], 'title': 'Curth, W.'}, '0601': {'ids': ['orcid:0000-0003-0530-4305', 'schema:12345', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Mcsorley, J.'}},
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
        curator.meta_maker()
        curator.enrich()
        output = curator.data
        expected_output = [{'id': 'doi:10.1787/eco_outlook-v2011-2-graph138-en meta:br/0601', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook [meta:br/0604]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Oecd [crossref:1963 meta:ra/0601]', 'editor': ''}]
        self.assertEqual(output, expected_output)
    
    def test_indexer(self):
        path_index = f'{OUTPUT_DIR}/index'
        path_csv = f'{OUTPUT_DIR}'
        curator = prepareCurator(list())
        curator.filename = '0.csv'
        curator.idra = {'orcid:0000-0003-0530-4305': '0601', 'schema:12345': '0602'}
        curator.idbr = {'doi:10.1001/2013.jamasurg.270': '2585'}
        curator.armeta = {'2585': {'author': [('9445', '0602'), ('0601', '0601')], 'editor': [], 'publisher': []}}
        curator.remeta = dict()
        curator.brmeta = {
            '0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'meta:br/0601'], 'others': ['wannabe_0'], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}, 
            '0602': {'ids': ['meta:br/0602'], 'others': ['wannabe_1'], 'title': 'OECD Economic Outlook'}
        }
        curator.vvi = {
            'wannabe_1': {
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
        curator.meta_maker()
        curator.indexer(path_index, path_csv)
        with open(os.path.join(path_index, 'index_ar.csv'), 'r', encoding='utf-8') as f:
            index_ar = list(csv.DictReader(f))
        with open(os.path.join(path_index, 'index_id_br.csv'), 'r', encoding='utf-8') as f:
            index_id_br = list(csv.DictReader(f))
        with open(os.path.join(path_index, 'index_id_ra.csv'), 'r', encoding='utf-8') as f:
            index_id_ra = list(csv.DictReader(f))
        with open(os.path.join(path_index, 'index_vi.json'), 'r', encoding='utf-8') as f:
            index_vi = json.load(f)
        with open(os.path.join(path_index, 'index_re.csv'), 'r', encoding='utf-8') as f:
            index_re = list(csv.DictReader(f))
        expected_index_ar = [{'meta': '2585', 'author': '9445, 0602; 0601, 0601', 'editor': '', 'publisher': ''}]
        expected_index_id_br = [{'id': 'doi:10.1001/2013.jamasurg.270', 'meta': '2585'}]
        expected_index_id_ra = [{'id': 'orcid:0000-0003-0530-4305', 'meta': '0601'}, {'id': 'schema:12345', 'meta': '0602'}]
        expected_index_re = [{'br': '', 're': ''}]
        expected_index_vi = {'0602': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        output = (index_ar, index_id_br, index_id_ra, index_re, index_vi)
        expected_output = (expected_index_ar, expected_index_id_br, expected_index_id_ra, expected_index_re, expected_index_vi)
        shutil.rmtree(OUTPUT_DIR)
        self.assertEqual(output, expected_output)
    
    def test_is_a_valid_row(self):
        rows = [
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '1', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '1', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''},
            {'id': 'id:1234', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412]', 'pub_date': '03-01-2020', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': 'book'}
        ]
        output = []
        curator = prepareCurator(list())
        for row in rows:
            output.append(curator.is_a_valid_row(row))
        expected_output = [False, False, False, True, False, True]
        self.assertEqual(output, expected_output)
    
    def test_get_preexisting_entities(self):
        add_data_ts()
        row = {'id': 'meta:br/2715', 'title': 'Image Of The Year For 2012', 'author': '', 'pub_date': '', 'venue': 'Archives Of Surgery [meta:br/4480]', 'volume': '99', 'issue': '1', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(data=[row])
        curator.curator()
        expected_output = (
            {'ra/3309', 'br/4487', 'br/2715', 'id/2581', 'ar/7240', 'id/4270', 'id/4274', 'br/4481', 'br/4480', 'br/4482', 're/2350'},
            [{'id': 'doi:10.1001/2013.jamasurg.202 meta:br/2715', 'title': 'Image Of The Year For 2012', 'author': '', 'pub_date': '2012-12-01', 'venue': 'Archives Of Surgery [issn:0004-0010 meta:br/4480]', 'volume': '147', 'issue': '12', 'page': '1140-1140', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [crossref:10 meta:ra/3309]', 'editor': ''}]
        )
        self.assertEqual((curator.preexisting_entities, curator.data), expected_output)
        


class test_RespAgentsCurator(unittest.TestCase):
    def test_curator_publishers(self):
        reset()
        data = [
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'American Medical Association (AMA) [crossref:10 crossref:9999]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Elsevier BV [crossref:78]', 'editor': ''}, 
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Wiley [crossref:311]', 'editor': ''}]
        resp_agents_curator = prepareCurator(data=data, server=SERVER, resp_agents_only=True)
        resp_agents_curator.curator(filename=None, path_csv=None, path_index=None)
        output = (resp_agents_curator.data, resp_agents_curator.radict, resp_agents_curator.idra, resp_agents_curator.rameta)
        expected_output = (
            [
                {'id': '', 'title': '', 'author': '', 'venue': '', 'editor': '', 'publisher': 'American Medical Association (ama) [crossref:10 crossref:9999 meta:ra/3309]', 'page': '', 'volume': '', 'issue': '', 'pub_date': '', 'type': ''}, 
                {'id': '', 'title': '', 'author': '', 'venue': '', 'editor': '', 'publisher': 'Elsevier Bv [crossref:78 meta:ra/0601]', 'page': '', 'volume': '', 'issue': '', 'pub_date': '', 'type': ''}, 
                {'id': '', 'title': '', 'author': '', 'venue': '', 'editor': '', 'publisher': 'Wiley [crossref:311 meta:ra/0602]', 'page': '', 'volume': '', 'issue': '', 'pub_date': '', 'type': ''}],
            {
                '3309': {'ids': ['crossref:10', 'crossref:9999', 'meta:ra/3309'], 'others': [], 'title': 'American Medical Association (ama)'}, 
                'wannabe_0': {'ids': ['crossref:78', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Elsevier Bv'}, 
                'wannabe_1': {'ids': ['crossref:311', 'meta:ra/0602'], 'others': ['wannabe_1'], 'title': 'Wiley'}},
            {'crossref:10': '4274', 'crossref:9999': '0601', 'crossref:78': '0602', 'crossref:311': '0603'},
            {
                '3309': {'ids': ['crossref:10', 'crossref:9999', 'meta:ra/3309'], 'others': [], 'title': 'American Medical Association (ama)'}, 
                '0601': {'ids': ['crossref:78', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Elsevier Bv'}, 
                '0602': {'ids': ['crossref:311', 'meta:ra/0602'], 'others': ['wannabe_1'], 'title': 'Wiley'}}        
        )
        self.assertEqual(output, expected_output)

    def test_curator(self):
        reset()
        data = [
            {'id': '', 'title': '', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': 'Ruso, Juan M. [orcid:0000-0001-5909-6754]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': 'Sarmiento, Félix [orcid:0000-0002-4487-6894]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        ]
        resp_agents_curator = prepareCurator(data=data, server=SERVER, resp_agents_only=True)
        resp_agents_curator.curator(filename='resp_agents_curator_output', path_csv='test/testcases/testcase_data', path_index='test/testcases/testcase_data/indices')
        output = (resp_agents_curator.data, resp_agents_curator.radict, resp_agents_curator.idra, resp_agents_curator.rameta)
        expected_output = (
            [
                {'id': '', 'title': '', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412 meta:ra/0601]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
                {'id': '', 'title': '', 'author': 'Ruso, Juan M. [orcid:0000-0001-5909-6754 meta:ra/0602]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
                {'id': '', 'title': '', 'author': 'Sarmiento, Félix [orcid:0000-0002-4487-6894 meta:ra/0603]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}],
                {
                    'wannabe_0': {'ids': ['orcid:0000-0003-2100-6412', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Deckert, Ron J.'}, 
                    'wannabe_1': {'ids': ['orcid:0000-0001-5909-6754', 'meta:ra/0602'], 'others': ['wannabe_1'], 'title': 'Ruso, Juan M.'}, 
                    'wannabe_2': {'ids': ['orcid:0000-0002-4487-6894', 'meta:ra/0603'], 'others': ['wannabe_2'], 'title': 'Sarmiento, Félix'}},
                {'orcid:0000-0003-2100-6412': '0601', 'orcid:0000-0001-5909-6754': '0602', 'orcid:0000-0002-4487-6894': '0603'},
                {
                    '0601': {'ids': ['orcid:0000-0003-2100-6412', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Deckert, Ron J.'}, 
                    '0602': {'ids': ['orcid:0000-0001-5909-6754', 'meta:ra/0602'], 'others': ['wannabe_1'], 'title': 'Ruso, Juan M.'}, 
                    '0603': {'ids': ['orcid:0000-0002-4487-6894', 'meta:ra/0603'], 'others': ['wannabe_2'], 'title': 'Sarmiento, Félix'}}
        )
        self.assertEqual(output, expected_output)

    def test_curator_ra_on_ts(self):
        # A responsible agent is already on the triplestore
        add_data_ts(server=SERVER, data_path=REAL_DATA_RDF)
        self.maxDiff = None
        data = [
            {'id': '', 'title': '', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': 'Mehrotra, Ateev [orcid:0000-0003-2223-1582]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': 'Sarmiento, Félix [orcid:0000-0002-4487-6894]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        ]
        resp_agents_curator = prepareCurator(data=data, server=SERVER, resp_agents_only=True)
        resp_agents_curator.curator()
        output = (resp_agents_curator.data, resp_agents_curator.radict, resp_agents_curator.idra, resp_agents_curator.rameta)
        expected_output = (
            [
                {'id': '', 'title': '', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412 meta:ra/0601]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
                {'id': '', 'title': '', 'author': 'Mehrotra, Ateev [orcid:0000-0003-2223-1582 meta:ra/3976]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}, 
                {'id': '', 'title': '', 'author': 'Sarmiento, Félix [orcid:0000-0002-4487-6894 meta:ra/0602]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}],
                {
                    'wannabe_0': {'ids': ['orcid:0000-0003-2100-6412', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Deckert, Ron J.'}, 
                    '3976': {'ids': ['orcid:0000-0003-2223-1582', 'meta:ra/3976'], 'others': [], 'title': 'Mehrotra, Ateev'}, 
                    'wannabe_1': {'ids': ['orcid:0000-0002-4487-6894', 'meta:ra/0602'], 'others': ['wannabe_1'], 'title': 'Sarmiento, Félix'}},
                {'orcid:0000-0003-2100-6412': '0601', 'orcid:0000-0003-2223-1582': '4351', 'orcid:0000-0002-4487-6894': '0602'},
                {
                    '0601': {'ids': ['orcid:0000-0003-2100-6412', 'meta:ra/0601'], 'others': ['wannabe_0'], 'title': 'Deckert, Ron J.'}, 
                    '3976': {'ids': ['orcid:0000-0003-2223-1582', 'meta:ra/3976'], 'others': [], 'title': 'Mehrotra, Ateev'}, 
                    '0602': {'ids': ['orcid:0000-0002-4487-6894', 'meta:ra/0602'], 'others': ['wannabe_1'], 'title': 'Sarmiento, Félix'}}
        )
        self.assertEqual(output, expected_output)


class test_id_worker(unittest.TestCase):
    def test_id_worker_1(self):
        # 1 EntityA is a new one
        add_data_ts()
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = ['doi:10.1163/2214-8655_lgo_lgo_02_0074_ger']
        wannabe_id = curator.id_worker('id', name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.log)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': ['doi:10.1163/2214-8655_lgo_lgo_02_0074_ger'], 'others': [], 'title': 'βέβαιος, α, ον'}},
            {},
            {'doi:10.1163/2214-8655_lgo_lgo_02_0074_ger': '0601'},
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
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.log)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': [], 'others': [], 'title': 'βέβαιος, α, ον'}},
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
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_2_metaid_ts(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # MetaID only
        idslist = ['meta:ra/3309']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_2_id_metaid_ts(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # ID and MetaID
        idslist = ['crossref:10', 'meta:id/4274']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_3(self):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV. MetaID on ts has precedence
        curator = prepareCurator(list())
        add_data_ts(SERVER, REAL_DATA_RDF)
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # ID and MetaID, but it's meta:id/4274 on ts
        idslist = ['crossref:10', 'meta:id/4275']
        wannabe_id = curator.id_worker('editor', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra, curator.log)
        expected_output = ('3309', {}, {'3309': {'ids': ['crossref:10'], 'others': [], 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'}, {})
        self.assertEqual(output, expected_output)

    def test_id_worker_2_meta_in_entity_dict(self):
        # MetaID exists among data.
        # MetaID already in entity_dict (no care about conflicts, we have a MetaID specified)
        # 2 Retrieve EntityA data to update EntityA inside CSV
        reset_server()
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
        output = (meta_id, curator_empty.brdict, curator_empty.radict, curator_empty.idbr, curator_empty.idra, curator_empty.log)
        expected_output = ('0601', {'0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': []}}, {}, {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, {}, {})
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
        output = (metaval, curator.brdict, curator.log, id_dict)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': ['doi:10.1001/2013.jamasurg.270'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}},
            {0: {'id': {'Conflict entity': 'wannabe_0'}}}, 
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
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.log)
        expected_output = (
            'wannabe_0', 
            {'doi:10.1001/2013.jamasurg.270': '2585'}, 
            {}, 
            {'wannabe_0': {'ids': ['doi:10.1001/2013.jamasurg.270'], 'others': [], 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}}, 
            {0: {'id': {'Conflict entity': 'wannabe_0'}}}
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
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict, curator.log)
        expected_output = (
            'wannabe_0', 
            {}, 
            {'orcid:0000-0001-6994-8412': '4475'}, 
            {}, 
            {'wannabe_0': {'ids': ['orcid:0000-0001-6994-8412'], 'others': [], 'title': 'Alarcon, Louis H.'}}, 
            {0: {'author': {'Conflict entity': 'wannabe_0'}}}
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
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict, curator.log)
        expected_output = (
            'wannabe_0', 
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, 
            {}, 
            {'meta:br/0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'],
                            'others': [],
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                        'Materials Prices: China'},
            'meta:br/0602': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'],
                            'others': [],
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                        'Africa'},
            'meta:br/0603': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'],
                            'others': [],
                            'title': 'Official Loans To The Governments Of Greece, '
                                        'Ireland And Portugal'},
            'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'],
                            'others': [],
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                    'Materials Prices: China'}},
            {}, 
            {0: {'id': {'Conflict entity': 'wannabe_0'}}}
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
        output = (meta_id, curator.idbr, curator.idra, curator.log)
        expected_output = (
            'meta:br/0601', 
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, 
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
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict, curator.log)
        expected_output = (
            'wannabe_0', 
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601', 
                'doi:10.1001/2013.jamasurg.270': '2585'
            }, 
            {}, 
            {'meta:br/0601': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en'],
                            'others': [],
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                        'Materials Prices: China'},
            'meta:br/0602': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'],
                            'others': [],
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                        'Africa'},
            'meta:br/0603': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph18-en'],
                            'others': [],
                            'title': 'Official Loans To The Governments Of Greece, '
                                        'Ireland And Portugal'},
            'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en',
                                    'doi:10.1001/2013.jamasurg.270'],
                            'others': [],
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                    'Materials Prices: Japan'}},
            {}, 
            {0: {'id': {'Conflict entity': 'wannabe_0'}}}
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
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict, curator.log)
        expected_output = (
            'wannabe_0', 
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601', 
                'doi:10.1001/2013.jamasurg.270': '2585'
            }, 
            {}, 
            {'wannabe_0': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph138-en',
                                    'doi:10.1001/2013.jamasurg.270'],
                            'others': [],
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                    'Materials Prices: Japan'},
            'wannabe_2': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph150-en'],
                            'others': [],
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                    'Africa'},
            'wannabe_3': {'ids': ['doi:10.1787/eco_outlook-v2011-2-graph18-en'],
                            'others': [],
                            'title': 'Official Loans To The Governments Of Greece, Ireland '
                                    'And Portugal'}},
            {}, 
            {0: {'id': {'Conflict entity': 'wannabe_0'}}}
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
        output = (meta_id, curator.idbr, curator.idra, curator.log)
        expected_output = (
            '3757', 
            {'doi:10.1001/archderm.104.1.106': '3624', 'pmid:29098884': '2000000'}, 
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
        output = (meta_id, curator.idbr, curator.idra, curator.log)
        expected_output = (
            'wannabe_0', 
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, 
            {}, 
            {}
        )
        self.assertEqual(output, expected_output)

    def test_metaid_in_prov(self):
        # MetaID not found in data, but found in the provenance metadata.
        reset_server()
        add_data_ts(server=SERVER, data_path=REAL_DATA_RDF_WITH_PROV)
        name = ''
        idslist = ['meta:ra/4321']
        curator = prepareCurator(list())
        meta_id = curator.id_worker('id', name, idslist, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
        self.assertEqual(meta_id, '38013')


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

