import unittest
from meta.scripts.curator import *
from meta.scripts.creator import Creator
import csv
from SPARQLWrapper import SPARQLWrapper
from pprint import pprint
from oc_ocdm import Storer


SERVER = 'http://127.0.0.1:9999/blazegraph/sparql'
MANUAL_DATA_CSV = 'meta/tdd/manual_data.csv'
MANUAL_DATA_RDF = 'meta/tdd/testcases/ts/testcase_ts-13.ttl'
REAL_DATA_CSV = 'meta/tdd/real_data.csv'
REAL_DATA_RDF = 'meta/tdd/testcases/ts/real_data.nt'
BASE_IRI = 'https://w3id.org/oc/meta/'
CURATOR_COUNTER_DIR = 'meta/tdd/curator_counter'


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

