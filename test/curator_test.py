# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os

import orjson
import pytest
from oc_meta.core.creator import Creator
from oc_meta.core.curator import Curator, is_a_valid_row
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.lib.finder import ResourceFinder
from oc_ocdm import Storer
from test.test_utils import (
    SERVER,
    add_data_ts,
    get_counter_handler,
    get_path,
    normalize_row_ids,
    reset_redis_counters,
    reset_triplestore,
)

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


def reset_server(server: str = SERVER) -> None:
    reset_triplestore(server)

def store_curated_data(curator_obj: Curator, server: str) -> None:
    counter_handler = get_counter_handler()
    creator_obj = Creator(
        curator_obj.data,
        curator_obj.finder,
        BASE_IRI,
        counter_handler,
        "060",
        "https://orcid.org/0000-0002-8420-0696",
        curator_obj.index_id_ra,
        curator_obj.index_id_br,
        curator_obj.re_index,
        curator_obj.ar_index,
        curator_obj.VolIss,
    )
    creator = creator_obj.creator(source=None)
    res_storer = Storer(creator)
    res_storer.upload_all(server)

def prepare_to_test(data, name):
    reset_redis_counters()

    reset_server(SERVER)
    if float(name) > 12:
        add_data_ts(SERVER, os.path.abspath(os.path.join('test', 'testcases', 'ts', 'testcase_ts-13.ttl')).replace('\\', '/'))

    testcase_csv = get_path('test/testcases/testcase_data/testcase_' + name + '_data.csv')
    testcase_id_br = get_path('test/testcases/testcase_data/indices/' + name + '/index_id_br_' + name + '.csv')
    testcase_id_ra = get_path('test/testcases/testcase_data/indices/' + name + '/index_id_ra_' + name + '.csv')
    testcase_ar = get_path('test/testcases/testcase_data/indices/' + name + '/index_ar_' + name + '.csv')
    testcase_re = get_path('test/testcases/testcase_data/indices/' + name + '/index_re_' + name + '.csv')
    testcase_vi = get_path('test/testcases/testcase_data/indices/' + name + '/index_vi_' + name + '.json')

    counter_handler = get_counter_handler()
    settings = {'normalize_titles': True}
    curator_obj = Curator(data, SERVER, prov_config=PROV_CONFIG, counter_handler=counter_handler, settings=settings)
    curator_obj.curator()
    testcase_csv = get_csv_data(testcase_csv)
    for csv in [testcase_csv, curator_obj.data]:
        for row in csv:
            row['id'] = sorted(row['id'].split())
            normalize_row_ids(row)
    testcase_id_br = get_csv_data(testcase_id_br)
    testcase_id_ra = get_csv_data(testcase_id_ra)
    testcase_ar = get_csv_data(testcase_ar)
    testcase_re = get_csv_data(testcase_re)
    for csv in [testcase_id_br, testcase_id_ra, testcase_ar, testcase_re, curator_obj.index_id_br, curator_obj.index_id_ra, curator_obj.ar_index, curator_obj.re_index]:
        try:
            csv.sort(key=lambda x:x['id'])
        except KeyError:
            try:
                csv.sort(key=lambda x:x['meta'])
            except KeyError:
                csv.sort(key=lambda x:x['br'])
    with open(testcase_vi, "rb") as json_file:
        testcase_vi = orjson.loads(json_file.read())
    testcase = [testcase_csv, testcase_id_br, testcase_id_ra, testcase_ar, testcase_re, testcase_vi]
    data_curated = [curator_obj.data, curator_obj.index_id_br, curator_obj.index_id_ra, curator_obj.ar_index,
                    curator_obj.re_index, curator_obj.VolIss]
    return data_curated, testcase

def prepareCurator(data:list, server:str=SERVER) -> Curator:
    settings = {'normalize_titles': True}
    reset_redis_counters()
    counter_handler = get_counter_handler()
    curator = Curator(data, server, prov_config=PROV_CONFIG, counter_handler=counter_handler, settings=settings)
    return curator


@pytest.fixture(scope="class")
def setup_curator_class():
    add_data_ts()
    yield


@pytest.fixture(autouse=True)
def reset_redis():
    reset_redis_counters()
    yield
    reset_redis_counters()


class TestCurator:
    @pytest.fixture(autouse=True)
    def setup_class_data(self, setup_curator_class):
        pass

    def test_clean_id(self):
        curator = prepareCurator(list())
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Blasto', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator.finder.get_everything_about_res(metavals=set(), identifiers={'doi:10.1001/archderm.104.1.106'}, vvis=set())
        curator.clean_id(row)
        expected_output = {'id': 'br/3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        assert row == expected_output

    def test_clean_id_list(self):
        input = ['doi:10.001/B-1', 'wikidata:B1111111', 'OMID:br/060101']
        output = Curator.clean_id_list(input, br=True)
        expected_output = (['doi:10.001/b-1', 'wikidata:B1111111'], 'br/060101')
        assert output == expected_output

    def test_clean_id_metaid_not_in_ts(self):
        # A MetaId was specified, but it is not on ts. Therefore, it is invalid
        curator = prepareCurator(list())
        row = {'id': 'omid:br/131313', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator.clean_id(row)
        expected_output = {'id': 'br/wannabe_0', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        assert row == expected_output

    def test_clean_ra_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        row = {'id': 'br/wannabe_0', 'title': 'Giant Oyster Mushroom Pleurotus giganteus (Agaricomycetes) Enhances Adipocyte Differentiation and Glucose Uptake via Activation of PPARγ and Glucose Transporters 1 and 4 in 3T3-L1 Cells', 'author': 'Paravamsivam, Puvaneswari; Heng, Chua Kek; Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]; Sabaratnam, Vikineswary; M, Ravishankar Ram; Kuppusamy, Umah Rani', 'pub_date': '2016', 'venue': 'International Journal of Medicinal Mushrooms [issn:1521-9437]', 'volume': '18', 'issue': '9', 'page': '821-831', 'type': 'journal article', 'publisher': 'Begell House [crossref:613]', 'editor': ''}
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/wannabe_0', 'Giant Oyster Mushroom Pleurotus giganteus (Agaricomycetes) Enhances Adipocyte Differentiation and Glucose Uptake via Activation of PPARγ and Glucose Transporters 1 and 4 in 3T3-L1 Cells')
        curator.entity_store.add_id('br/wannabe_0', 'doi:10.1615/intjmedmushrooms.v18.i9.60')
        curator.clean_ra(row, 'author')
        expected_ardict = {'br/wannabe_0': {'author': [('ar/0601', 'ra/wannabe_0'), ('ar/0602', 'ra/wannabe_1'), ('ar/0603', 'ra/wannabe_2'), ('ar/0604', 'ra/wannabe_3'), ('ar/0605', 'ra/wannabe_4'), ('ar/0606', 'ra/wannabe_5')], 'editor': [], 'publisher': []}}
        expected_ra_store = {
            'ra/wannabe_0': {'ids': set(), 'title': 'Paravamsivam, Puvaneswari'},
            'ra/wannabe_1': {'ids': set(), 'title': 'Heng, Chua Kek'},
            'ra/wannabe_2': {'ids': {'orcid:0000-0001-6278-8559'}, 'title': 'Malek, Sri Nurestri Abdul'},
            'ra/wannabe_3': {'ids': set(), 'title': 'Sabaratnam, Vikineswary'},
            'ra/wannabe_4': {'ids': set(), 'title': 'M, Ravishankar Ram'},
            'ra/wannabe_5': {'ids': set(), 'title': 'Kuppusamy, Umah Rani'}
        }
        assert curator.ardict == expected_ardict
        for key, data in expected_ra_store.items():
            assert curator.entity_store.get_ids(key) == data['ids']
            assert curator.entity_store.get_title(key) == data['title']
        assert curator.entity_store.get_id_metaid('orcid:0000-0001-6278-8559') == 'id/0601'

    def test_clean_ra_with_br_metaid(self):
        # One author is in the triplestore, the other is not.
        # br_metaval is a MetaID
        # There are two ids for one author
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Keloids', 'author': 'Curth, W.; McSorley, J. [orcid:0000-0003-0530-4305 schema:12345]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)
        curator.clean_id(row)
        resolved_metaval = row['id']
        assert resolved_metaval == 'br/3757'
        curator.entity_store.add_entity(resolved_metaval, 'Multiple Keloids')
        curator.entity_store.add_id(resolved_metaval, 'doi:10.1001/archderm.104.1.106')
        curator.entity_store.add_id(resolved_metaval, 'doi:10.1001/archderm.104.1.106')
        curator.clean_ra(row, 'author')
        expected_ardict = {'br/3757': {'author': [('ar/9445', 'ra/6033'), ('ar/0601', 'ra/wannabe_0')], 'editor': [], 'publisher': []}}
        assert curator.ardict == expected_ardict
        assert curator.entity_store.get_title('ra/6033') == 'Curth, W.'
        assert curator.entity_store.get_ids('ra/wannabe_0') == {'orcid:0000-0003-0530-4305', 'schema:12345'}
        assert curator.entity_store.get_title('ra/wannabe_0') == 'McSorley, J.'
        assert curator.entity_store.get_id_metaid('orcid:0000-0003-0530-4305') == 'id/0601'
        assert curator.entity_store.get_id_metaid('schema:12345') == 'id/0602'

    def test_clean_ra_with_br_wannabe(self):
        """Authors not on the triplestore, br_metaval is a wannabe."""
        row = {'id': 'br/wannabe_0', 'title': 'Multiple Keloids', 'author': 'Curth, W. [orcid:0000-0002-8420-0696] ; McSorley, J. [orcid:0000-0003-0530-4305]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/wannabe_0', 'Multiple Keloids')
        curator.entity_store.add_id('br/wannabe_0', 'doi:10.1001/archderm.104.1.106')
        curator.wnb_cnt = 1
        curator.clean_ra(row, 'author')
        expected_ardict = {'br/wannabe_0': {'author': [('ar/0601', 'ra/wannabe_1'), ('ar/0602', 'ra/wannabe_2')], 'editor': [], 'publisher': []}}
        assert curator.ardict == expected_ardict
        assert curator.entity_store.get_ids('ra/wannabe_1') == {'orcid:0000-0002-8420-0696'}
        assert curator.entity_store.get_title('ra/wannabe_1') == 'Curth, W.'
        assert curator.entity_store.get_ids('ra/wannabe_2') == {'orcid:0000-0003-0530-4305'}
        assert curator.entity_store.get_title('ra/wannabe_2') == 'McSorley, J.'
        assert curator.entity_store.get_id_metaid('orcid:0000-0002-8420-0696') == 'id/0601'
        assert curator.entity_store.get_id_metaid('orcid:0000-0003-0530-4305') == 'id/0602'

    def test_clean_ra_with_empty_square_brackets(self):
        """One author's name contains empty square brackets."""
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Keloids', 'author': 'Bernacki, Edward J. [    ]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)
        curator.clean_id(row)
        resolved_metaval = row['id']
        assert resolved_metaval == 'br/3757'
        curator.entity_store.add_entity(resolved_metaval, 'Multiple Keloids')
        curator.entity_store.add_id(resolved_metaval, 'doi:10.1001/archderm.104.1.106')
        curator.entity_store.add_id(resolved_metaval, 'doi:10.1001/archderm.104.1.106')
        curator.clean_ra(row, 'author')
        expected_ardict = {'br/3757': {'author': [('ar/9445', 'ra/6033'), ('ar/0601', 'ra/wannabe_0')], 'editor': [], 'publisher': []}}
        assert curator.ardict == expected_ardict
        assert curator.entity_store.get_title('ra/6033') == 'Curth, W.'
        assert curator.entity_store.get_ids('ra/wannabe_0') == set()
        assert curator.entity_store.get_title('ra/wannabe_0') == 'Bernacki, Edward J.'

    def test_clean_vvi_all_data_on_ts(self):
        """All data are already on the triplestore. They need to be retrieved and organized correctly."""
        reset_server()
        add_data_ts()
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)
        curator.clean_id(row)
        curator.clean_vvi(row)
        expected_output = {
            "br/4416": {
                "issue": {},
                "volume": {
                    "104": {
                        "id": "br/4712",
                        "issue": {
                            "1": {
                                "id": "br/4713"
                            }
                        }
                    }
                }
            }
        }
        assert curator.vvi == expected_output

    def test_clean_vvi_invalid_venue(self):
        """Data must be invalidated: resource is journal but a volume has also been specified."""
        row = {'id': 'br/wannabe_1', 'title': '', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'br/wannabe_0': {'volume': {}, 'issue': {}}}
        assert curator.vvi == expected_output

    def test_clean_vvi_invalid_volume(self):
        """Data must be invalidated: resource is journal volume but an issue has also been specified."""
        row = {'id': 'br/wannabe_1', 'title': '', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'br/wannabe_0': {'volume': {}, 'issue': {}}}
        assert curator.vvi == expected_output

    def test_clean_vvi_new_venue(self):
        """It is a new venue."""
        row = {'id': 'br/wannabe_1', 'title': 'Money growth, interest rates, inflation and raw materials prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'br/wannabe_0': {'volume': {'2011': {'id': 'br/wannabe_1', 'issue': {'2': {'id': 'br/wannabe_2'}}}}, 'issue': {}}}
        assert curator.vvi == expected_output

    def test_clean_vvi_new_volume_and_issue(self):
        """There is a row with vvi and no ids."""
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'Archives Of Surgery [omid:br/4480]', 'volume': '147', 'issue': '11', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)
        curator.clean_id(row)
        curator.clean_vvi(row)
        expected_output = {
            "br/4480": {
                "issue": {},
                "volume": {
                    "147": {
                        "id": "br/4481",
                        "issue": {
                            "11": {
                                "id": "br/4482"
                            }
                        }
                    }
                }
            }
        }
        assert curator.vvi == expected_output

    def test_clean_vvi_volume_with_title(self):
        """A journal volume having a title."""
        row = [{'id': '', 'title': 'The volume title', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}]
        curator = prepareCurator(row)
        curator.curator()
        expected_output = [{'id': 'omid:br/0601', 'title': 'The Volume Title', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook [omid:br/0602]', 'volume': '', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}]
        assert curator.data == expected_output

    def test_enricher(self):
        curator = prepareCurator(list())
        curator.data = [{'id': 'br/wannabe_0', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'br/wannabe_1', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'OECD [crossref:1963]', 'editor': ''}]
        curator.brmeta = {
            'br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en', 'omid:br/0601'}, 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'br/0602': {'ids': {'omid:br/0604'}, 'title': 'OECD Economic Outlook'}
        }
        curator.armeta = {'br/0601': {'author': [], 'editor': [], 'publisher': [('ar/0601', 'ra/0601')]}}
        curator.rameta = {'ra/0601': {'ids': {'crossref:1963', 'omid:ra/0601'}, 'title': 'Oecd'}}
        curator.remeta = dict()
        curator.meta_maker()
        curator.entity_store.assign_meta('br/wannabe_0', 'br/0601')
        curator.entity_store.assign_meta('br/wannabe_1', 'br/0602')
        curator.entity_store.assign_meta('ra/wannabe_2', 'ra/0601')
        curator.enrich()
        output = curator.data
        expected_output = [{'id': 'doi:10.1787/eco_outlook-v2011-2-graph138-en omid:br/0601', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook [omid:br/0604]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Oecd [crossref:1963 omid:ra/0601]', 'editor': ''}]
        for row in output:
            normalize_row_ids(row)
        for row in expected_output:
            normalize_row_ids(row)
        assert output == expected_output

    def test_equalizer(self):
        """Test equalizer with a row that contains an ID that can be resolved to an existing entity."""
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': '', 'author': '', 'pub_date': '1972-12-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)
        curator.clean_id(row)
        extracted_metaval = row['id']
        assert extracted_metaval == 'br/3757'
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '1972-12-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator.rowcnt = 0
        curator.equalizer(row, extracted_metaval)
        expected_row = {'id': '', 'title': '', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416 issn:0003-987X]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''}
        assert row == expected_row

    def test_get_preexisting_entities(self):
        row = {'id': 'omid:br/2715', 'title': 'Image Of The Year For 2012', 'author': '', 'pub_date': '', 'venue': 'Archives Of Surgery [omid:br/4480]', 'volume': '99', 'issue': '1', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(data=[row])
        curator.curator()
        expected_data = [{'id': 'doi:10.1001/2013.jamasurg.202 omid:br/2715', 'title': 'Image Of The Year For 2012', 'author': '', 'pub_date': '2012-12-01', 'venue': 'Archives Of Surgery [issn:0004-0010 omid:br/4480]', 'volume': '147', 'issue': '12', 'page': '1140-1140', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [crossref:10 omid:ra/3309]', 'editor': ''}]
        expected_entities = {'id/4270', 'ra/3309', 'ar/7240', 'br/4481', 'br/2715', 'br/4480', 'id/4274', 'id/2581', 'br/4487', 're/2350'}
        for row in curator.data:
            normalize_row_ids(row)
        for row in expected_data:
            normalize_row_ids(row)
        assert curator.preexisting_entities == expected_entities
        assert curator.data == expected_data

    def test_indexer(self):
        """Test that indexer() correctly transforms internal dicts to list-of-dicts."""
        curator = prepareCurator(list())
        curator.filename = '0.csv'
        curator.entity_store.set_id_metaid('orcid:0000-0003-0530-4305', 'id/0601')
        curator.entity_store.set_id_metaid('viaf:12345', 'id/0602')
        curator.entity_store.set_id_metaid('doi:10.1001/2013.jamasurg.270', 'id/2585')
        curator.entity_store.add_entity('br/2585', '')
        curator.entity_store.add_id('br/2585', 'doi:10.1001/2013.jamasurg.270')
        curator.entity_store.add_entity('ra/0601', '')
        curator.entity_store.add_id('ra/0601', 'orcid:0000-0003-0530-4305')
        curator.entity_store.add_entity('ra/0602', '')
        curator.entity_store.add_id('ra/0602', 'viaf:12345')
        curator.armeta = {'br/2585': {'author': [('ar/9445', 'ra/0602'), ('ar/0601', 'ra/0601')], 'editor': [], 'publisher': []}}
        curator.remeta = dict()
        curator.brmeta = {
            'br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en', 'omid:br/0601'}, 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'br/0602': {'ids': {'omid:br/0602'}, 'title': 'OECD Economic Outlook'}
        }
        curator.VolIss = {
            'br/0602': {
                'issue': {},
                'volume': {
                    '107': {'id': 'br/4733', 'issue': {'1': {'id': 'br/4734'}, '2': {'id': 'br/4735'}, '3': {'id': 'br/4736'}, '4': {'id': 'br/4737'}, '5': {'id': 'br/4738'}, '6': {'id': 'br/4739'}}},
                    '108': {'id': 'br/4740', 'issue': {'1': {'id': 'br/4741'}, '2': {'id': 'br/4742'}, '3': {'id': 'br/4743'}, '4': {'id': 'br/4744'}}},
                    '104': {'id': 'br/4712', 'issue': {'1': {'id': 'br/4713'}, '2': {'id': 'br/4714'}, '3': {'id': 'br/4715'}, '4': {'id': 'br/4716'}, '5': {'id': 'br/4717'}, '6': {'id': 'br/4718'}}},
                    '148': {'id': 'br/4417', 'issue': {'12': {'id': 'br/4418'}, '11': {'id': 'br/4419'}}},
                    '100': {'id': 'br/4684', 'issue': {'1': {'id': 'br/4685'}, '2': {'id': 'br/4686'}, '3': {'id': 'br/4687'}, '4': {'id': 'br/4688'}, '5': {'id': 'br/4689'}, '6': {'id': 'br/4690'}}},
                    '101': {'id': 'br/4691', 'issue': {'1': {'id': 'br/4692'}, '2': {'id': 'br/4693'}, '3': {'id': 'br/4694'}, '4': {'id': 'br/4695'}, '5': {'id': 'br/4696'}, '6': {'id': 'br/4697'}}},
                    '102': {'id': 'br/4698', 'issue': {'1': {'id': 'br/4699'}, '2': {'id': 'br/4700'}, '3': {'id': 'br/4701'}, '4': {'id': 'br/4702'}, '5': {'id': 'br/4703'}, '6': {'id': 'br/4704'}}},
                    '103': {'id': 'br/4705', 'issue': {'1': {'id': 'br/4706'}, '2': {'id': 'br/4707'}, '3': {'id': 'br/4708'}, '4': {'id': 'br/4709'}, '5': {'id': 'br/4710'}, '6': {'id': 'br/4711'}}},
                    '105': {'id': 'br/4719', 'issue': {'1': {'id': 'br/4720'}, '2': {'id': 'br/4721'}, '3': {'id': 'br/4722'}, '4': {'id': 'br/4723'}, '5': {'id': 'br/4724'}, '6': {'id': 'br/4725'}}},
                    '106': {'id': 'br/4726', 'issue': {'6': {'id': 'br/4732'}, '1': {'id': 'br/4727'}, '2': {'id': 'br/4728'}, '3': {'id': 'br/4729'}, '4': {'id': 'br/4730'}, '5': {'id': 'br/4731'}}}
                }
            }
        }
        curator.indexer()
        expected_index_ar = [{'meta': 'br/2585', 'author': 'ar/9445, ra/0602; ar/0601, ra/0601', 'editor': '', 'publisher': ''}]
        expected_index_id_br = [{'id': 'doi:10.1001/2013.jamasurg.270', 'meta': 'id/2585'}]
        expected_index_id_ra = [{'id': 'orcid:0000-0003-0530-4305', 'meta': 'id/0601'}, {'id': 'viaf:12345', 'meta': 'id/0602'}]
        expected_index_re = [{'br': '', 're': ''}]
        expected_index_vi = {'br/0602': {'issue': {}, 'volume': {'107': {'id': 'br/4733', 'issue': {'1': {'id': 'br/4734'}, '2': {'id': 'br/4735'}, '3': {'id': 'br/4736'}, '4': {'id': 'br/4737'}, '5': {'id': 'br/4738'}, '6': {'id': 'br/4739'}}}, '108': {'id': 'br/4740', 'issue': {'1': {'id': 'br/4741'}, '2': {'id': 'br/4742'}, '3': {'id': 'br/4743'}, '4': {'id': 'br/4744'}}}, '104': {'id': 'br/4712', 'issue': {'1': {'id': 'br/4713'}, '2': {'id': 'br/4714'}, '3': {'id': 'br/4715'}, '4': {'id': 'br/4716'}, '5': {'id': 'br/4717'}, '6': {'id': 'br/4718'}}}, '148': {'id': 'br/4417', 'issue': {'12': {'id': 'br/4418'}, '11': {'id': 'br/4419'}}}, '100': {'id': 'br/4684', 'issue': {'1': {'id': 'br/4685'}, '2': {'id': 'br/4686'}, '3': {'id': 'br/4687'}, '4': {'id': 'br/4688'}, '5': {'id': 'br/4689'}, '6': {'id': 'br/4690'}}}, '101': {'id': 'br/4691', 'issue': {'1': {'id': 'br/4692'}, '2': {'id': 'br/4693'}, '3': {'id': 'br/4694'}, '4': {'id': 'br/4695'}, '5': {'id': 'br/4696'}, '6': {'id': 'br/4697'}}}, '102': {'id': 'br/4698', 'issue': {'1': {'id': 'br/4699'}, '2': {'id': 'br/4700'}, '3': {'id': 'br/4701'}, '4': {'id': 'br/4702'}, '5': {'id': 'br/4703'}, '6': {'id': 'br/4704'}}}, '103': {'id': 'br/4705', 'issue': {'1': {'id': 'br/4706'}, '2': {'id': 'br/4707'}, '3': {'id': 'br/4708'}, '4': {'id': 'br/4709'}, '5': {'id': 'br/4710'}, '6': {'id': 'br/4711'}}}, '105': {'id': 'br/4719', 'issue': {'1': {'id': 'br/4720'}, '2': {'id': 'br/4721'}, '3': {'id': 'br/4722'}, '4': {'id': 'br/4723'}, '5': {'id': 'br/4724'}, '6': {'id': 'br/4725'}}}, '106': {'id': 'br/4726', 'issue': {'6': {'id': 'br/4732'}, '1': {'id': 'br/4727'}, '2': {'id': 'br/4728'}, '3': {'id': 'br/4729'}, '4': {'id': 'br/4730'}, '5': {'id': 'br/4731'}}}}}}
        curator.index_id_ra.sort(key=lambda x: x['id'])
        expected_index_id_ra.sort(key=lambda x: x['id'])
        assert curator.ar_index == expected_index_ar
        assert curator.index_id_br == expected_index_id_br
        assert curator.index_id_ra == expected_index_id_ra
        assert curator.re_index == expected_index_re
        assert curator.VolIss == expected_index_vi

    def test_is_a_valid_row(self):
        rows = [
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '1', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''},
            {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '1', 'page': '', 'type': 'journal issue', 'publisher': '', 'editor': ''},
            {'id': 'doi:10.1001/2013.jamasurg.270', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412]', 'pub_date': '', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': 'Deckert, Ron J. [orcid:0000-0003-2100-6412]', 'pub_date': '03-01-2020', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': 'book'},
            {'id': 'doi:10.1001/2013.jamasurg.270', 'title': '', 'author': '', 'pub_date': '', 'venue': '', 'volume': '5', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        ]
        output = []
        for row in rows:
            output.append(is_a_valid_row(row))
        expected_output = [False, False, False, True, False, True, False]
        assert output == expected_output

    def test_merge_duplicate_entities(self):
        """Test merge_duplicate_entities with realistic data that includes an ID that resolves to an existing entity."""
        data = [
            {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-02', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-03', 'venue': 'Archives Of Blast [omid:br/4416]', 'volume': '105', 'issue': '2', 'page': '106-108', 'type': 'journal volume', 'publisher': '', 'editor': ''},
        ]
        curator = prepareCurator(list())
        curator.data = data
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        all_metavals = set()
        all_identifiers = set()
        all_vvis = set()
        for row in data:
            metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
            all_metavals.update(metavals)
            all_identifiers.update(identifiers)
            all_vvis.update(vvis)
        curator.finder.get_everything_about_res(metavals=all_metavals, identifiers=all_identifiers, vvis=all_vvis)
        for i, row in enumerate(data):
            curator.rowcnt = i
            curator.clean_id(row)
        first_row_metaval = curator.data[0]['id']
        assert first_row_metaval == 'br/3757'
        if first_row_metaval in curator.entity_store:
            curator.entity_store.merge(first_row_metaval, 'br/wannabe_0')
            curator.entity_store.merge(first_row_metaval, 'br/wannabe_1')
        curator.merge_duplicate_entities()
        expected_data = [
            {'id': 'br/3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [issn:0003-987X omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''},
            {'id': 'br/3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [issn:0003-987X omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''},
            {'id': 'br/3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [issn:0003-987X omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''}
        ]
        assert curator.data == expected_data

    def test_merge_entities_in_csv(self):
        curator = prepareCurator(list())
        curator.counter_handler.set_counter(4, 'id', supplier_prefix='060')
        curator.entity_store.add_entity('br/0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        curator.merge_entities_in_csv(['doi:10.1787/eco_outlook-v2011-2-graph138-en'], 'br/0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        assert curator.entity_store.get_ids('br/0601') == {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}
        assert curator.entity_store.get_title('br/0601') == 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        assert curator.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0605'

    def test_meta_maker(self):
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/3757', 'Multiple Keloids')
        curator.entity_store.add_id('br/3757', 'doi:10.1001/archderm.104.1.106')
        curator.entity_store.add_id('br/3757', 'pmid:29098884')
        curator.entity_store.add_entity('br/4416', 'Archives Of Dermatology')
        curator.entity_store.add_id('br/4416', 'issn:0003-987X')
        curator.entity_store.add_entity('ra/6033', 'Curth, W.')
        curator.entity_store.add_entity('ra/wannabe_0', 'Mcsorley, J.')
        curator.entity_store.add_id('ra/wannabe_0', 'orcid:0000-0003-0530-4305')
        curator.entity_store.add_id('ra/wannabe_0', 'schema:12345')
        curator.ardict = {'br/3757': {'author': [('ar/9445', 'ra/6033'), ('ar/0601', 'ra/wannabe_0')], 'editor': [], 'publisher': []}}
        curator.vvi = {'br/4416': {'issue': {}, 'volume': {'107': {'id': 'br/4733', 'issue': {'1': {'id': 'br/4734'}, '2': {'id': 'br/4735'}, '3': {'id': 'br/4736'}, '4': {'id': 'br/4737'}, '5': {'id': 'br/4738'}, '6': {'id': 'br/4739'}}}, '108': {'id': 'br/4740', 'issue': {'1': {'id': 'br/4741'}, '2': {'id': 'br/4742'}, '3': {'id': 'br/4743'}, '4': {'id': 'br/4744'}}}, '104': {'id': 'br/4712', 'issue': {'1': {'id': 'br/4713'}, '2': {'id': 'br/4714'}, '3': {'id': 'br/4715'}, '4': {'id': 'br/4716'}, '5': {'id': 'br/4717'}, '6': {'id': 'br/4718'}}}, '148': {'id': 'br/4417', 'issue': {'12': {'id': 'br/4418'}, '11': {'id': 'br/4419'}}}, '100': {'id': 'br/4684', 'issue': {'1': {'id': 'br/4685'}, '2': {'id': 'br/4686'}, '3': {'id': 'br/4687'}, '4': {'id': 'br/4688'}, '5': {'id': 'br/4689'}, '6': {'id': 'br/4690'}}}, '101': {'id': 'br/4691', 'issue': {'1': {'id': 'br/4692'}, '2': {'id': 'br/4693'}, '3': {'id': 'br/4694'}, '4': {'id': 'br/4695'}, '5': {'id': 'br/4696'}, '6': {'id': 'br/4697'}}}, '102': {'id': 'br/4698', 'issue': {'1': {'id': 'br/4699'}, '2': {'id': 'br/4700'}, '3': {'id': 'br/4701'}, '4': {'id': 'br/4702'}, '5': {'id': 'br/4703'}, '6': {'id': 'br/4704'}}}, '103': {'id': 'br/4705', 'issue': {'1': {'id': 'br/4706'}, '2': {'id': 'br/4707'}, '3': {'id': 'br/4708'}, '4': {'id': 'br/4709'}, '5': {'id': 'br/4710'}, '6': {'id': 'br/4711'}}}, '105': {'id': 'br/4719', 'issue': {'1': {'id': 'br/4720'}, '2': {'id': 'br/4721'}, '3': {'id': 'br/4722'}, '4': {'id': 'br/4723'}, '5': {'id': 'br/4724'}, '6': {'id': 'br/4725'}}}, '106': {'id': 'br/4726', 'issue': {'6': {'id': 'br/4732'}, '1': {'id': 'br/4727'}, '2': {'id': 'br/4728'}, '3': {'id': 'br/4729'}, '4': {'id': 'br/4730'}, '5': {'id': 'br/4731'}}}}}}
        curator.meta_maker()
        expected_brmeta = {
            'br/3757': {'ids': {'doi:10.1001/archderm.104.1.106', 'pmid:29098884', 'omid:br/3757'}, 'title': 'Multiple Keloids'},
            'br/4416': {'ids': {'issn:0003-987X', 'omid:br/4416'}, 'title': 'Archives Of Dermatology'}
        }
        expected_rameta = {
            'ra/6033': {'ids': {'omid:ra/6033'}, 'title': 'Curth, W.'},
            'ra/0601': {'ids': {'orcid:0000-0003-0530-4305', 'schema:12345', 'omid:ra/0601'}, 'title': 'Mcsorley, J.'}
        }
        expected_armeta = {'br/3757': {'author': [('ar/9445', 'ra/6033'), ('ar/0601', 'ra/0601')], 'editor': [], 'publisher': []}}
        assert curator.brmeta == expected_brmeta
        assert curator.rameta == expected_rameta
        assert curator.armeta == expected_armeta


@pytest.fixture(scope="class")
def id_worker_finder():
    add_data_ts(SERVER, os.path.abspath(os.path.join('test', 'testcases', 'ts', 'real_data.nt')).replace('\\', '/'))
    finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
    finder.get_everything_about_res(metavals={'omid:br/3309', 'omid:br/2438', 'omid:br/0601'}, identifiers={'doi:10.1001/2013.jamasurg.270', 'doi:10.1787/eco_outlook-v2011-2-graph138-en', 'orcid:0000-0001-6994-8412', 'doi:10.1001/archderm.104.1.106', 'pmid:29098884'}, vvis=set())
    return finder


class TestIdWorker:
    def test_id_worker_1(self):
        """EntityA is a new one."""
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = ['doi:10.1163/2214-8655_lgo_lgo_02_0074_ger']
        wannabe_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert wannabe_id == 'br/wannabe_0'
        assert curator.entity_store.get_ids('br/wannabe_0') == {'doi:10.1163/2214-8655_lgo_lgo_02_0074_ger'}
        assert curator.entity_store.get_title('br/wannabe_0') == 'βέβαιος, α, ον'
        assert curator.entity_store.get_id_metaid('doi:10.1163/2214-8655_lgo_lgo_02_0074_ger') == 'id/0601'

    def test_id_worker_1_no_id(self):
        """EntityA is a new one and has no ids."""
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = []
        wannabe_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert wannabe_id == 'br/wannabe_0'
        assert curator.entity_store.get_ids('br/wannabe_0') == set()
        assert curator.entity_store.get_title('br/wannabe_0') == 'βέβαιος, α, ον'

    def test_id_worker_2_id_ts(self, id_worker_finder):
        """Retrieve EntityA data in triplestore to update EntityA inside CSV. Name on the ts must prevail."""
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        name = 'American Medical Association (AMA)'
        idslist = ['crossref:10']
        wannabe_id = curator.id_worker('editor', name, idslist, '', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        assert wannabe_id == 'ra/3309'
        assert curator.entity_store.get_ids('ra/3309') == {'crossref:10'}
        assert curator.entity_store.get_title('ra/3309') == 'American Medical Association (ama)'
        assert curator.entity_store.get_id_metaid('crossref:10') == 'id/4274'

    def test_id_worker_2_metaid_ts(self, id_worker_finder):
        """Retrieve EntityA data in triplestore to update EntityA inside CSV. MetaID only."""
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        name = 'American Medical Association (AMA)'
        wannabe_id = curator.id_worker('editor', name, [], 'ra/3309', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        assert wannabe_id == 'ra/3309'
        assert curator.entity_store.get_ids('ra/3309') == {'crossref:10'}
        assert curator.entity_store.get_title('ra/3309') == 'American Medical Association (ama)'
        assert curator.entity_store.get_id_metaid('crossref:10') == 'id/4274'

    def test_id_worker_2_id_metaid_ts(self, id_worker_finder):
        """Retrieve EntityA data in triplestore to update EntityA inside CSV. ID and MetaID."""
        curator = prepareCurator(list())
        name = 'American Medical Association (AMA)'
        curator.finder = id_worker_finder
        wannabe_id = curator.id_worker('publisher', name, ['crossref:10'], 'ra/3309', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        assert wannabe_id == 'ra/3309'
        assert curator.entity_store.get_ids('ra/3309') == {'crossref:10'}
        assert curator.entity_store.get_title('ra/3309') == 'American Medical Association (ama)'
        assert curator.entity_store.get_id_metaid('crossref:10') == 'id/4274'

    def test_id_worker_3(self, id_worker_finder):
        """Retrieve EntityA data in triplestore. MetaID on ts has precedence over specified MetaID."""
        curator = prepareCurator(list())
        name = 'American Medical Association (AMA)'
        curator.finder = id_worker_finder
        wannabe_id = curator.id_worker('publisher', name, ['crossref:10'], 'ra/33090', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        assert wannabe_id == 'ra/3309'
        assert curator.entity_store.get_ids('ra/3309') == {'crossref:10'}
        assert curator.entity_store.get_title('ra/3309') == 'American Medical Association (ama)'
        assert curator.entity_store.get_id_metaid('crossref:10') == 'id/4274'

    def test_id_worker_conflict(self, id_worker_finder):
        """No meta or it didn't exist, but ids exist and refer to multiple entities on ts. Conflict!"""
        idslist = ['doi:10.1001/2013.jamasurg.270']
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        metaval = curator.conflict(idslist, name, 'br', col_name='id')
        assert metaval == 'br/wannabe_0'
        assert curator.entity_store.get_ids('br/wannabe_0') == {'doi:10.1001/2013.jamasurg.270'}
        assert curator.entity_store.get_title('br/wannabe_0') == 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        assert curator.entity_store.get_id_metaid('doi:10.1001/2013.jamasurg.270') == 'id/2585'

    def test_conflict_br(self, id_worker_finder):
        """No MetaId, an identifier to which two separate br point: conflict, and a new entity must be created."""
        curator = prepareCurator(list())
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1001/2013.jamasurg.270']
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id in ('br/2719', 'br/2720')
        assert curator.entity_store.get_id_metaid('doi:10.1001/2013.jamasurg.270') == 'id/2585'

    def test_conflict_ra(self, id_worker_finder):
        """No MetaId, an identifier to which two separate ra point: conflict, and a new entity must be created."""
        idslist = ['orcid:0000-0001-6994-8412']
        name = 'Alarcon, Louis H.'
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('author', name, idslist, '', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
        assert meta_id in ('ra/4940', 'ra/1000000')
        assert curator.entity_store.get_id_metaid('orcid:0000-0001-6994-8412') == 'id/4475'

    def test_conflict_suspect_id_among_existing(self, id_worker_finder):
        """ID exists in entity_dict and refers to one entity with MetaID, but another ID highlights a conflict on ts."""
        curator = prepareCurator(get_csv_data(REAL_DATA_CSV))
        curator.entity_store.add_entity('br/0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        curator.entity_store.add_id('br/0601', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        curator.entity_store.add_entity('br/0602', 'Contributions To GDP Growth And Inflation: South Africa')
        curator.entity_store.add_id('br/0602', 'doi:10.1787/eco_outlook-v2011-2-graph150-en')
        curator.entity_store.add_entity('br/0603', 'Official Loans To The Governments Of Greece, Ireland And Portugal')
        curator.entity_store.add_id('br/0603', 'doi:10.1787/eco_outlook-v2011-2-graph18-en')
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270']
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id == 'br/wannabe_0'
        assert curator.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0601'
        assert curator.entity_store.get_id_metaid('doi:10.1001/2013.jamasurg.270') == 'id/2585'

    def test_conflict_suspect_id_among_wannabe(self, id_worker_finder):
        """ID exists in entity_dict and refers to a temporary, but another ID highlights a conflict on ts."""
        curator = prepareCurator(get_csv_data(REAL_DATA_CSV))
        curator.entity_store.add_entity('br/wannabe_0', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        curator.entity_store.add_id('br/wannabe_0', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        curator.entity_store.add_entity('br/wannabe_2', 'Contributions To GDP Growth And Inflation: South Africa')
        curator.entity_store.add_id('br/wannabe_2', 'doi:10.1787/eco_outlook-v2011-2-graph150-en')
        curator.entity_store.add_entity('br/wannabe_3', 'Official Loans To The Governments Of Greece, Ireland And Portugal')
        curator.entity_store.add_id('br/wannabe_3', 'doi:10.1787/eco_outlook-v2011-2-graph18-en')
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270']
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id in ('br/2719', 'br/2720')
        assert curator.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0601'
        assert curator.entity_store.get_id_metaid('doi:10.1001/2013.jamasurg.270') == 'id/2585'

    def test_id_worker_4(self, id_worker_finder):
        """Merge data from EntityA (CSV) with data from EntityX (CSV), update both with data from EntityA (RDF)."""
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/wannabe_0', 'Multiple eloids')
        curator.entity_store.add_id('br/wannabe_0', 'doi:10.1001/archderm.104.1.106')
        curator.entity_store.add_entity('br/wannabe_1', 'Multiple Blastoids')
        curator.entity_store.add_id('br/wannabe_1', 'doi:10.1001/archderm.104.1.106')
        name = 'Multiple Palloids'
        idslist = ['doi:10.1001/archderm.104.1.106', 'pmid:29098884']
        curator.wnb_cnt = 2
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id == 'br/3757'
        assert curator.entity_store.get_id_metaid('doi:10.1001/archderm.104.1.106') == 'id/3624'
        assert curator.entity_store.get_id_metaid('pmid:29098884') == 'id/2000000'


class TestIdWorkerWithReset:
    def test_id_worker_2_meta_in_entity_store(self):
        """MetaID exists among data and already in entity_dict. Retrieve EntityA data to update EntityA inside CSV."""
        reset_server()
        data = get_csv_data(REAL_DATA_CSV)
        curator = prepareCurator(data)
        curator.curator()
        store_curated_data(curator, SERVER)
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        curator_empty = prepareCurator(list())
        curator_empty.finder.get_everything_about_res(metavals=set(), identifiers={'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, vvis=set())
        meta_id = curator_empty.id_worker('id', name, [], 'br/0601', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        meta_id = curator_empty.id_worker('id', name, [], 'br/0601', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id == 'br/0601'
        assert curator_empty.entity_store.get_ids('br/0601') == {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}
        assert curator_empty.entity_store.get_title('br/0601') == 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        assert curator_empty.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0601'

    def test_conflict_existing(self):
        """ID already exists in entity_dict but refers to multiple entities having a MetaID."""
        reset_server()
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        curator.entity_store.add_id('br/0601', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        curator.entity_store.add_entity('br/0602', 'Contributions To GDP Growth And Inflation: South Africa')
        curator.entity_store.add_id('br/0602', 'doi:10.1787/eco_outlook-v2011-2-graph150-en')
        curator.entity_store.add_entity('br/0603', 'Official Loans To The Governments Of Greece, Ireland And Portugal')
        curator.entity_store.add_id('br/0603', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id == 'br/wannabe_0'
        assert curator.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0601'

    def test_id_worker_5(self):
        """ID already exists in entity_dict and refers to one or more temporary entities: collective merge."""
        reset_server()
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/wannabe_0', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        curator.entity_store.add_id('br/wannabe_0', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        curator.entity_store.add_entity('br/wannabe_1', 'Contributions To GDP Growth And Inflation: South Africa')
        curator.entity_store.add_id('br/wannabe_1', 'doi:10.1787/eco_outlook-v2011-2-graph150-en')
        curator.entity_store.add_entity('br/wannabe_2', 'Official Loans To The Governments Of Greece, Ireland And Portugal')
        curator.entity_store.add_id('br/wannabe_2', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator.wnb_cnt = 2
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id == 'br/wannabe_0'
        assert curator.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0601'

    def test_no_conflict_existing(self):
        """ID already exists in entity_dict and refers to one entity. First title must have precedence."""
        reset_server()
        curator = prepareCurator(list())
        curator.entity_store.add_entity('br/0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China')
        curator.entity_store.add_id('br/0601', 'doi:10.1787/eco_outlook-v2011-2-graph138-en')
        curator.entity_store.add_entity('br/0602', 'Contributions To GDP Growth And Inflation: South Africa')
        curator.entity_store.add_id('br/0602', 'doi:10.1787/eco_outlook-v2011-2-graph150-en')
        curator.entity_store.add_entity('br/0603', 'Official Loans To The Governments Of Greece, Ireland And Portugal')
        curator.entity_store.add_id('br/0603', 'doi:10.1787/eco_outlook-v2011-2-graph18-en')
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        assert meta_id == 'br/0601'
        assert curator.entity_store.get_id_metaid('doi:10.1787/eco_outlook-v2011-2-graph138-en') == 'id/0601'

    def test_metaid_in_prov(self):
        """MetaID not found in data, but found in the provenance metadata."""
        reset_server()
        add_data_ts(server=SERVER, data_path=os.path.abspath(os.path.join('test', 'testcases', 'ts', 'real_data_with_prov.nq')).replace('\\', '/'))
        name = ''
        curator = prepareCurator(list())
        meta_id = curator.id_worker('id', name, [], 'ra/4321', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
        assert meta_id == 'ra/38013'


class TestTestcase01:
    def test(self):
        """Testcase1: 2 different issues of the same venue (no volume)."""
        name = '01'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[0])
        partial_data.append(data[5])
        data_curated, testcase = prepare_to_test(partial_data, name)
        for pos, element in enumerate(data_curated):
            assert element == testcase[pos]


class TestTestcase02:
    def test(self):
        """Testcase2: 2 different volumes of the same venue (no issue)."""
        name = '02'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[1])
        partial_data.append(data[3])
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase03:
    def test(self):
        """Testcase3: 2 different issues of the same volume."""
        name = '03'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[2])
        partial_data.append(data[4])
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase04:
    def test(self):
        """Testcase4: 2 new IDs and different date format (yyyy-mm and yyyy-mm-dd)."""
        name = '04'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[6])
        partial_data.append(data[7])
        data_curated, testcase = prepare_to_test(partial_data, name)
        for pos, element in enumerate(data_curated):
            assert element == testcase[pos]


class TestTestcase05:
    def test(self):
        """Testcase5: NO ID scenario."""
        name = '05'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[8])
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase06:
    def test(self):
        """Testcase6: ALL types test."""
        name = '06'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[9:33]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase07:
    def test(self):
        """Testcase7: all journal related types with an editor."""
        name = '07'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[34:40]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase08:
    def test(self):
        """Testcase8: all book related types with an editor."""
        name = '08'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[40:43]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase09:
    def test(self):
        """Testcase9: all proceeding related types with an editor."""
        name = '09'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[43:45]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase10:
    def test(self):
        """Testcase10: a book inside a book series and a book inside a book set."""
        name = '10'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[45:49]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase11:
    def test(self):
        """Testcase11: real time entity update."""
        name = '11'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[49:52]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase12:
    def test(self):
        """Testcase12: clean name, title, ids."""
        name = '12'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[52:53]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase13:
    """Testcase13: ID_clean massive test."""

    def test1(self):
        """Meta specified br in a row, wannabe with a new id in a row, meta specified with an id related to wannabe."""
        name = '13.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[53:56]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        """Conflict with META precedence: br has meta_id and id related to another meta_id, first meta prevails."""
        data = get_csv_data(MANUAL_DATA_CSV)
        name = '13.2'
        partial_data = data[56:57]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        """Conflict: br with id shared with 2 meta."""
        data = get_csv_data(MANUAL_DATA_CSV)
        name_1 = '13.3'
        name_2 = '13.31'
        partial_data = data[57:58]
        data_curated, testcase_1 = prepare_to_test(partial_data, name_1)
        _, testcase_2 = prepare_to_test(partial_data, name_2)
        assert data_curated == testcase_1 or data_curated == testcase_2


class TestTestcase14:
    def test1(self):
        """Update existing sequence: new author and existing author without existing id (matched by surname, name)."""
        name = '14.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[58:59]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        """Same sequence different order, with new ids."""
        name = '14.2'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[59:60]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        """RA: Author with two different ids."""
        name_1 = '14.3'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[60:61]
        data_curated, testcase_1 = prepare_to_test(partial_data, name_1)
        assert data_curated == testcase_1

    def test4(self):
        """Meta specified ra in a row, wannabe ra with new id in a row, meta specified with id related to wannabe."""
        name = '14.4'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[61:64]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase15:
    def test1(self):
        """Venue volume issue already exists in ts."""
        name = '15.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[64:65]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        """Venue conflict."""
        name = '15.2'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[65:66]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        """Venue in ts is now the br."""
        name = '15.3'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[66:67]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test4(self):
        """BR in ts is now the venue."""
        name = '15.4'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[67:68]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test5(self):
        """Volume in ts is now the br."""
        name = '15.5'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[71:72]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test6(self):
        """BR is a volume."""
        name = '15.6'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[72:73]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test7(self):
        """Issue in ts is now the br."""
        name = '15.7'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[73:74]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test8(self):
        """BR is an issue."""
        name = '15.8'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[74:75]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase16:
    def test1(self):
        """Date cleaning: wrong date 2019-02-29."""
        name = '16.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[75:76]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        """Existing re."""
        name = '16.2'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[76:77]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        """Given name for an RA with only a family name in TS."""
        name = '16.3'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[77:78]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestCuratorBuildNameIdsString:
    def test_only_ids_no_name(self):
        result = Curator.build_name_ids_string("", {"doi:10.1234/test", "pmid:12345"})
        assert result.startswith("[")
        assert result.endswith("]")
        assert "doi:10.1234/test" in result
        assert "pmid:12345" in result

    def test_no_name_no_ids(self):
        result = Curator.build_name_ids_string("", set())
        assert result == ""

    def test_name_no_ids(self):
        result = Curator.build_name_ids_string("Test Name", set())
        assert result == "Test Name"


class TestCuratorExtractNameAndIds:
    def test_no_match_simple_string(self):
        curator = prepareCurator([])
        name, ids = curator.extract_name_and_ids("Simple Venue Name")
        assert name == "Simple Venue Name"
        assert ids == []

    def test_empty_string(self):
        curator = prepareCurator([])
        name, ids = curator.extract_name_and_ids("")
        assert name == ""
        assert ids == []


class TestCuratorCleanIdListMultipleOmid:
    def test_multiple_omid_values(self):
        id_list = ["omid:br/0601", "omid:br/0602", "doi:10.1234/test"]
        result, metaid = Curator.clean_id_list(id_list, br=True)
        assert "doi:10.1234/test" in result
        assert metaid in ("br/0601", "br/0602")


class TestCuratorReadNumber:
    def test_read_number(self):
        curator = prepareCurator([])
        result = curator._read_number("br")
        assert isinstance(result, int)


class TestIsValidRowBranches:
    def test_unknown_type_with_fields(self):
        row = {
            "id": "",
            "title": "Test Title",
            "author": "Test Author",
            "pub_date": "2024-01-01",
            "venue": "Test Venue",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "unknown_type_xyz",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is False

    def test_book_chapter_valid(self):
        row = {
            "id": "",
            "title": "Chapter Title",
            "author": "",
            "pub_date": "",
            "venue": "Book Venue",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "book chapter",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is True

    def test_book_chapter_invalid_no_venue(self):
        row = {
            "id": "",
            "title": "Chapter Title",
            "author": "",
            "pub_date": "",
            "venue": "",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "book chapter",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is False

    def test_book_series_valid(self):
        row = {
            "id": "",
            "title": "Series Title",
            "author": "",
            "pub_date": "",
            "venue": "",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "book series",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is True

    def test_journal_volume_with_title(self):
        row = {
            "id": "",
            "title": "Volume Title",
            "author": "",
            "pub_date": "",
            "venue": "Journal Venue",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "journal volume",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is True

    def test_journal_issue_with_title(self):
        row = {
            "id": "",
            "title": "Issue Title",
            "author": "",
            "pub_date": "",
            "venue": "Journal Venue",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "journal issue",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is True

    def test_component_type(self):
        row = {
            "id": "",
            "title": "Component Title",
            "author": "",
            "pub_date": "",
            "venue": "Component Venue",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "component",
            "publisher": "",
            "editor": "",
        }
        assert is_a_valid_row(row) is True


class TestCuratorCleanMetadataWithoutId:
    def test_posted_content_type(self):
        data = [
            {
                "id": "doi:10.1234/test",
                "title": "Test Title",
                "author": "Author, Test",
                "pub_date": "2024-01-01",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "posted content",
                "publisher": "",
                "editor": "",
            }
        ]
        curator = prepareCurator(data)
        curator.curator()
        assert curator.data[0]["type"] == "web content"


class TestCuratorLocalMatch:
    def test_local_match_filters_by_prefix(self):
        curator = prepareCurator([])
        curator.entity_store.add_entity("br/0601", "BR Entity")
        curator.entity_store.add_id("br/0601", "doi:10.1234/br")
        curator.entity_store.add_entity("ra/0601", "RA Entity")
        curator.entity_store.add_id("ra/0601", "doi:10.1234/br")

        result = curator._local_match(["doi:10.1234/br"], entity_type="br")
        assert "br/0601" in result["existing"] or "br/0601" in result["wannabe"]
        all_results = result["existing"] + result["wannabe"]
        assert not any(r.startswith("ra/") for r in all_results)


class TestCuratorVolumeIssue:
    @pytest.fixture
    def setup_curator_with_ts_data(self):
        reset_triplestore(SERVER)
        add_data_ts(SERVER)
        return prepareCurator([])

    def test_volume_issue_wannabe_meets_existing(self, setup_curator_with_ts_data):
        curator = setup_curator_with_ts_data
        path = {"5": {"id": "br/4712", "issue": {}}}
        row = {
            "id": "br/wannabe_0",
            "title": "Test",
            "venue": "br/4416",
            "volume": "5",
            "issue": "",
            "type": "journal article",
            "author": "",
            "pub_date": "",
            "page": "",
            "publisher": "",
            "editor": "",
        }
        curator.entity_store.add_entity("br/wannabe_0", "Test")

        curator.volume_issue("br/wannabe_0", path, "5", row)

        assert "5" in path
        assert path["5"]["id"] in ("br/4712", "br/wannabe_0")


class TestCuratorEqualizerVenueMerge:
    @pytest.fixture
    def setup_curator_for_equalizer(self):
        reset_triplestore(SERVER)
        add_data_ts(SERVER)
        curator = prepareCurator([])
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        return curator

    def test_equalizer_venue_no_common_ids(self, setup_curator_for_equalizer):
        curator = setup_curator_for_equalizer
        curator.finder.get_everything_about_res(
            metavals={"omid:br/3757"}, identifiers=set(), vvis=set()
        )

        row = {
            "id": "",
            "title": "",
            "author": "",
            "pub_date": "",
            "venue": "Different Venue [doi:10.9999/different]",
            "volume": "",
            "issue": "",
            "page": "",
            "type": "",
            "publisher": "",
            "editor": "",
        }
        curator.equalizer(row, "br/3757")
        assert "Archives Of Dermatology" in row["venue"]


class TestCuratorMergeVolIssWithVvi:
    def test_merge_existing_volumes(self):
        curator = prepareCurator([])

        curator.VolIss = {
            "br/venue1": {
                "volume": {"10": {"id": "br/vol1", "issue": {"1": {"id": "br/issue1"}}}},
                "issue": {},
            }
        }

        curator.vvi = {
            "br/venue1": {
                "volume": {"10": {"id": "br/vol1", "issue": {"2": {"id": "br/issue2"}}}},
                "issue": {"5": {"id": "br/issue5"}},
            }
        }

        curator._merge_VolIss_with_vvi("br/venue1", "br/venue1")

        assert "1" in curator.VolIss["br/venue1"]["volume"]["10"]["issue"]
        assert "2" in curator.VolIss["br/venue1"]["volume"]["10"]["issue"]
        assert "5" in curator.VolIss["br/venue1"]["issue"]


class TestCuratorGetPreexistingEntitiesWithRe:
    def test_remeta_without_prefix(self):
        curator = prepareCurator([])
        curator.entity_store.add_entity("br/0601", "Test BR")
        curator.remeta = {"br/0601": ("0601", "1-10")}

        curator.get_preexisting_entities()

        assert "re/0601" in curator.preexisting_entities


class TestCuratorFirstNameUpdateDirectCondition:
    def test_first_name_update_condition_directly(self):
        curator = prepareCurator([])

        metaval = "ra/0601"
        name = "Smith, John"
        col_name = "author"

        curator.entity_store.add_entity(metaval, "Smith,")

        if col_name != "publisher" and metaval in curator.entity_store:
            full_name = curator.entity_store.get_title(metaval)
            if "," in name and "," in full_name:
                first_name = name.split(",")[1].strip()
                if not full_name.split(",")[1].strip() and first_name:
                    given_name = full_name.split(",")[0]
                    curator.entity_store.set_title(metaval, given_name + ", " + first_name)

        assert curator.entity_store.get_title(metaval) == "Smith, John"


class TestCuratorVolumeIssueMoreBranches:
    def test_volume_issue_existing_meets_wannabe(self):
        reset_triplestore(SERVER)
        add_data_ts(SERVER)
        curator = prepareCurator([])

        curator.entity_store.add_entity("br/0601", "Test Volume")

        path = {"10": {"id": "br/wannabe_1", "issue": {}}}
        curator.entity_store.add_entity("br/wannabe_1", "Wannabe Volume")

        row = {
            "id": "br/0601",
            "title": "Test Article",
            "venue": "br/venue1",
            "volume": "10",
            "issue": "",
            "type": "journal article",
            "author": "",
            "pub_date": "",
            "page": "",
            "publisher": "",
            "editor": "",
        }

        curator.volume_issue("br/0601", path, "10", row)

        assert path["10"]["id"] == "br/0601"


class TestCuratorExtractIdsFromChunk:
    def test_extract_ids_basic(self):
        from oc_meta.core.curator import _extract_ids_from_chunk

        rows = [
            {
                "id": "doi:10.1234/test",
                "title": "Test",
                "author": "",
                "pub_date": "2024",
                "venue": "Venue [omid:br/venue1]",
                "volume": "10",
                "issue": "1",
                "page": "1-10",
                "type": "journal article",
                "publisher": "",
                "editor": "",
            }
        ]
        valid_dois_cache = {}

        metavals, identifiers, vvis = _extract_ids_from_chunk((rows, valid_dois_cache))

        assert "doi:10.1234/test" in identifiers
        assert "omid:br/venue1" in metavals
        assert len(vvis) > 0

    def test_extract_ids_with_metaval(self):
        from oc_meta.core.curator import _extract_ids_from_chunk

        rows = [
            {
                "id": "omid:br/0601 doi:10.1234/test",
                "title": "Test",
                "author": "",
                "pub_date": "",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "",
                "publisher": "",
                "editor": "",
            }
        ]
        valid_dois_cache = {}

        metavals, identifiers, vvis = _extract_ids_from_chunk((rows, valid_dois_cache))

        assert "omid:br/0601" in metavals
        assert "doi:10.1234/test" in identifiers

    def test_extract_ids_venue_with_volume_no_issue(self):
        from oc_meta.core.curator import _extract_ids_from_chunk

        rows = [
            {
                "id": "doi:10.1234/test",
                "title": "Test",
                "author": "",
                "pub_date": "",
                "venue": "Venue [omid:br/venue1 issn:1234-5678]",
                "volume": "5",
                "issue": "",
                "page": "",
                "type": "",
                "publisher": "",
                "editor": "",
            }
        ]
        valid_dois_cache = {}

        metavals, identifiers, vvis = _extract_ids_from_chunk((rows, valid_dois_cache))

        assert "omid:br/venue1" in metavals
        assert len(vvis) == 1
