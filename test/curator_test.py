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
        expected_output = {'id': '3757', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        assert row == expected_output

    def test_clean_id_list(self):
        input = ['doi:10.001/B-1', 'wikidata:B1111111', 'OMID:br/060101']
        output = Curator.clean_id_list(input, br=True)
        expected_output = (['doi:10.001/b-1', 'wikidata:B1111111'], '060101')
        assert output == expected_output

    def test_clean_id_metaid_not_in_ts(self):
        # A MetaId was specified, but it is not on ts. Therefore, it is invalid
        curator = prepareCurator(list())
        row = {'id': 'omid:br/131313', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator.clean_id(row)
        expected_output = {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        assert row == expected_output

    def test_clean_ra_overlapping_surnames(self):
        # The surname of one author is included in the surname of another.
        row = {'id': 'wannabe_0', 'title': 'Giant Oyster Mushroom Pleurotus giganteus (Agaricomycetes) Enhances Adipocyte Differentiation and Glucose Uptake via Activation of PPARγ and Glucose Transporters 1 and 4 in 3T3-L1 Cells', 'author': 'Paravamsivam, Puvaneswari; Heng, Chua Kek; Malek, Sri Nurestri Abdul [orcid:0000-0001-6278-8559]; Sabaratnam, Vikineswary; M, Ravishankar Ram; Kuppusamy, Umah Rani', 'pub_date': '2016', 'venue': 'International Journal of Medicinal Mushrooms [issn:1521-9437]', 'volume': '18', 'issue': '9', 'page': '821-831', 'type': 'journal article', 'publisher': 'Begell House [crossref:613]', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'wannabe_0': {'ids': {'doi:10.1615/intjmedmushrooms.v18.i9.60'}, 'title': 'Giant Oyster Mushroom Pleurotus giganteus (Agaricomycetes) Enhances Adipocyte Differentiation and Glucose Uptake via Activation of PPARγ and Glucose Transporters 1 and 4 in 3T3-L1 Cells', 'others': set()}}
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'wannabe_0': {'author': [('0601', 'wannabe_0'), ('0602', 'wannabe_1'), ('0603', 'wannabe_2'), ('0604', 'wannabe_3'), ('0605', 'wannabe_4'), ('0606', 'wannabe_5')], 'editor': [], 'publisher': []}},
            {'wannabe_0': {'ids': set(), 'others': set(), 'title': 'Paravamsivam, Puvaneswari'}, 'wannabe_1': {'ids': set(), 'others': set(), 'title': 'Heng, Chua Kek'}, 'wannabe_2': {'ids': {'orcid:0000-0001-6278-8559'}, 'others': set(), 'title': 'Malek, Sri Nurestri Abdul'}, 'wannabe_3': {'ids': set(), 'others': set(), 'title': 'Sabaratnam, Vikineswary'}, 'wannabe_4': {'ids': set(), 'others': set(), 'title': 'M, Ravishankar Ram'}, 'wannabe_5': {'ids': set(), 'others': set(), 'title': 'Kuppusamy, Umah Rani'}},
            {'orcid:0000-0001-6278-8559': '0601'}
        )
        assert output == expected_output

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
        assert resolved_metaval == '3757'
        curator.brdict = {resolved_metaval: {'ids': {'doi:10.1001/archderm.104.1.106'}, 'title': 'Multiple Keloids', 'others': set()}}

        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}},
            {'6033': {'ids': set(), 'others': set(), 'title': 'Curth, W.'}, 'wannabe_0': {'ids': {'orcid:0000-0003-0530-4305', 'schema:12345'}, 'others': set(), 'title': 'McSorley, J.'}},
            {'orcid:0000-0003-0530-4305': '0601', 'schema:12345': '0602'}
        )
        assert output == expected_output

    def test_clean_ra_with_br_wannabe(self):
        # Authors not on the triplestore.
        # br_metaval is a wannabe
        row = {'id': 'wannabe_0', 'title': 'Multiple Keloids', 'author': 'Curth, W. [orcid:0000-0002-8420-0696] ; McSorley, J. [orcid:0000-0003-0530-4305]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.brdict = {'wannabe_0': {'ids': {'doi:10.1001/archderm.104.1.106'}, 'title': 'Multiple Keloids', 'others': set()}}
        curator.wnb_cnt = 1
        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'wannabe_0': {'author': [('0601', 'wannabe_1'), ('0602', 'wannabe_2')], 'editor': [], 'publisher': []}},
            {'wannabe_1': {'ids': {'orcid:0000-0002-8420-0696'}, 'others': set(), 'title': 'Curth, W.'}, 'wannabe_2': {'ids': {'orcid:0000-0003-0530-4305'}, 'others': set(), 'title': 'McSorley, J.'}},
            {'orcid:0000-0002-8420-0696': '0601', 'orcid:0000-0003-0530-4305': '0602'}
        )
        assert output == expected_output

    def test_clean_ra_with_empty_square_brackets(self):
        # One author's name contains a closed square bracket.
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Keloids', 'author': 'Bernacki, Edward J. [    ]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)

        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)

        curator.clean_id(row)

        resolved_metaval = row['id']
        assert resolved_metaval == '3757'
        curator.brdict = {resolved_metaval: {'ids': {'doi:10.1001/archderm.104.1.106'}, 'title': 'Multiple Keloids', 'others': set()}}

        curator.clean_ra(row, 'author')
        output = (curator.ardict, curator.radict, curator.idra)
        expected_output = (
            {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}},
            {'6033': {'ids': set(), 'others': set(), 'title': 'Curth, W.'}, 'wannabe_0': {'ids': set(), 'others': set(), 'title': 'Bernacki, Edward J.'}},
            {}
        )
        assert output == expected_output

    def test_clean_vvi_all_data_on_ts(self):
        # All data are already on the triplestore. They need to be retrieved and organized correctly
        # Reset server to ensure clean state (test_merge_duplicate_entities may load extra vvi data)
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
            "4416": {
                "issue": {},
                "volume": {
                    "104": {
                        "id": "4712",
                        "issue": {
                            "1": {
                                "id": "4713"
                            }
                        }
                    }
                }
            }
        }
        assert curator.vvi == expected_output

    def test_clean_vvi_invalid_venue(self):
        # The data must be invalidated, because the resource is journal but a volume has also been specified
        row = {'id': 'wannabe_1', 'title': '', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '', 'page': '', 'type': 'journal', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'wannabe_0': {'volume': {}, 'issue': {}}}
        assert curator.vvi == expected_output

    def test_clean_vvi_invalid_volume(self):
        # The data must be invalidated, because the resource is journal volume but an issue has also been specified
        row = {'id': 'wannabe_1', 'title': '', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'wannabe_0': {'volume': {}, 'issue': {}}}
        assert curator.vvi == expected_output

    def test_clean_vvi_new_venue(self):
        # It is a new venue
        row = {'id': 'wannabe_1', 'title': 'Money growth, interest rates, inflation and raw materials prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.clean_vvi(row)
        expected_output = {'wannabe_0': {'volume': {'2011': {'id': 'wannabe_1', 'issue': {'2': {'id': 'wannabe_2'}}}}, 'issue': {}}}
        assert curator.vvi == expected_output

    def test_clean_vvi_new_volume_and_issue(self):
        # There is a row with vvi and no ids
        row = {'id': '', 'title': '', 'author': '', 'pub_date': '', 'venue': 'Archives Of Surgery [omid:br/4480]', 'volume': '147', 'issue': '11', 'page': '', 'type': 'journal article', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)

        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)
        curator.clean_id(row)
        curator.clean_vvi(row)
        expected_output = {
            "4480": {
                "issue": {},
                "volume": {
                    "147": {
                        "id": "4481",
                        "issue": {
                            "11": {
                                "id": "4482"
                            }
                        }
                    }
                }
            }
        }
        assert curator.vvi == expected_output

    def test_clean_vvi_volume_with_title(self):
        # A journal volume having a title
        row = [{'id': '', 'title': 'The volume title', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook', 'volume': '2011', 'issue': '2', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}]
        curator = prepareCurator(row)
        curator.curator()
        expected_output = [{'id': 'omid:br/0601', 'title': 'The Volume Title', 'author': '', 'pub_date': '', 'venue': 'OECD Economic Outlook [omid:br/0602]', 'volume': '', 'issue': '', 'page': '', 'type': 'journal volume', 'publisher': '', 'editor': ''}]
        assert curator.data == expected_output

    def test_enricher(self):
        curator = prepareCurator(list())
        curator.data = [{'id': 'wannabe_0', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'wannabe_1', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'OECD [crossref:1963]', 'editor': ''}]
        curator.brmeta = {
            '0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en', 'omid:br/0601'}, 'others': {'wannabe_0'}, 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            '0602': {'ids': {'omid:br/0604'}, 'others': {'wannabe_1'}, 'title': 'OECD Economic Outlook'}
        }
        curator.armeta = {'0601': {'author': [], 'editor': [], 'publisher': [('0601', '0601')]}}
        curator.rameta = {'0601': {'ids': {'crossref:1963', 'omid:ra/0601'}, 'others': {'wannabe_2'}, 'title': 'Oecd'}}
        curator.remeta = dict()
        curator.meta_maker()
        # Set inverse indexes after meta_maker() since it resets them
        curator._br_wannabe_to_meta = {'wannabe_0': '0601', 'wannabe_1': '0602'}
        curator._ra_wannabe_to_meta = {'wannabe_2': '0601'}
        curator.enrich()
        output = curator.data
        expected_output = [{'id': 'doi:10.1787/eco_outlook-v2011-2-graph138-en omid:br/0601', 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'author': '', 'pub_date': '2011-11-28', 'venue': 'OECD Economic Outlook [omid:br/0604]', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': 'Oecd [crossref:1963 omid:ra/0601]', 'editor': ''}]
        for row in output:
            normalize_row_ids(row)
        for row in expected_output:
            normalize_row_ids(row)
        assert output == expected_output

    def test_equalizer(self):
        # Test equalizer with a row that contains an ID that can be resolved to an existing entity
        row = {'id': 'doi:10.1001/archderm.104.1.106', 'title': '', 'author': '', 'pub_date': '1972-12-01', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'type': '', 'publisher': '', 'editor': ''}
        curator = prepareCurator(list())
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)

        metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
        curator.finder.get_everything_about_res(metavals=metavals, identifiers=identifiers, vvis=vvis)

        curator.clean_id(row)
        extracted_metaval = row['id']
        assert extracted_metaval == '3757'

        # Reset the row to test equalizer
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
        curator.idra = {'orcid:0000-0003-0530-4305': '0601', 'schema:12345': '0602'}
        curator.idbr = {'doi:10.1001/2013.jamasurg.270': '2585'}
        curator.armeta = {'2585': {'author': [('9445', '0602'), ('0601', '0601')], 'editor': [], 'publisher': []}}
        curator.remeta = dict()
        curator.brmeta = {
            '0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en', 'omid:br/0601'}, 'others': {'wannabe_0'}, 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            '0602': {'ids': {'omid:br/0602'}, 'others': {'wannabe_1'}, 'title': 'OECD Economic Outlook'}
        }
        # VolIss is set directly with resolved venue ID (0602 instead of wannabe_1)
        # since we're not calling meta_maker() which would do the resolution
        curator.VolIss = {
            '0602': {
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
        curator.indexer()
        # Test in-memory data structures
        expected_index_ar = [{'meta': '2585', 'author': '9445, 0602; 0601, 0601', 'editor': '', 'publisher': ''}]
        expected_index_id_br = [{'id': 'doi:10.1001/2013.jamasurg.270', 'meta': '2585'}]
        expected_index_id_ra = [{'id': 'orcid:0000-0003-0530-4305', 'meta': '0601'}, {'id': 'schema:12345', 'meta': '0602'}]
        expected_index_re = [{'br': '', 're': ''}]
        expected_index_vi = {'0602': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        # Sort for comparison (order may vary due to dict iteration)
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
        # Test merge_duplicate_entities with realistic data that includes an ID that resolves to an existing entity
        data = [
            {'id': 'doi:10.1001/archderm.104.1.106', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-02', 'venue': 'Archives Of Dermatology [omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': '', 'editor': ''},
            {'id': '', 'title': 'Multiple Keloids', 'author': '', 'pub_date': '1971-07-03', 'venue': 'Archives Of Blast [omid:br/4416]', 'volume': '105', 'issue': '2', 'page': '106-108', 'type': 'journal volume', 'publisher': '', 'editor': ''},
        ]
        curator = prepareCurator(list())
        curator.data = data
        curator.finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)

        # Extract metavals and identifiers from each row
        all_metavals = set()
        all_identifiers = set()
        all_vvis = set()

        for row in data:
            metavals, identifiers, vvis = curator.extract_identifiers_and_metavals(row, valid_dois_cache=set())
            all_metavals.update(metavals)
            all_identifiers.update(identifiers)
            all_vvis.update(vvis)

        curator.finder.get_everything_about_res(metavals=all_metavals, identifiers=all_identifiers, vvis=all_vvis)

        # Process each row with clean_id to get the actual metavals
        for i, row in enumerate(data):
            curator.rowcnt = i
            curator.clean_id(row)

        # The brdict should be populated by clean_id, but we need to set up the "others" relationship
        # The first row should have resolved to '3757', and the other rows should be wannabes
        first_row_metaval = curator.data[0]['id']  # Should be '3757'
        assert first_row_metaval == '3757'

        # Set up the relationship between the existing entity and the wannabes
        if first_row_metaval in curator.brdict:
            curator.brdict[first_row_metaval]['others'].update({'wannabe_0', 'wannabe_1'})

        curator.merge_duplicate_entities()

        expected_data = [
            {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [issn:0003-987X omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''},
            {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [issn:0003-987X omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''},
            {'id': '3757', 'title': 'Multiple Keloids', 'author': 'Curth, W. [omid:ra/6033]', 'pub_date': '1971-07-01', 'venue': 'Archives Of Dermatology [issn:0003-987X omid:br/4416]', 'volume': '104', 'issue': '1', 'page': '106-107', 'type': 'journal article', 'publisher': 'American Medical Association (ama) [omid:ra/3309 crossref:10]', 'editor': ''}
        ]
        assert curator.data == expected_data

    def test_merge_entities_in_csv(self):
        curator = prepareCurator(list())
        curator.counter_handler.set_counter(4, 'id', supplier_prefix='060')
        entity_dict = {'0601': {'ids': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': set()}}
        id_dict = dict()
        curator.merge_entities_in_csv(['doi:10.1787/eco_outlook-v2011-2-graph138-en'], '0601', 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', entity_dict, id_dict)
        expected_output = (
            {'0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': set()}},
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0605'}
        )
        assert (entity_dict, id_dict) == expected_output

    def test_meta_maker(self):
        curator = prepareCurator(list())
        curator.brdict = {'3757': {'ids': {'doi:10.1001/archderm.104.1.106', 'pmid:29098884'}, 'title': 'Multiple Keloids', 'others': set()}, '4416': {'ids': {'issn:0003-987X'}, 'title': 'Archives Of Dermatology', 'others': set()}}
        curator.radict = {'6033': {'ids': set(), 'others': set(), 'title': 'Curth, W.'}, 'wannabe_0': {'ids': {'orcid:0000-0003-0530-4305', 'schema:12345'}, 'others': set(), 'title': 'Mcsorley, J.'}}
        curator.ardict = {'3757': {'author': [('9445', '6033'), ('0601', 'wannabe_0')], 'editor': [], 'publisher': []}}
        curator.vvi = {'4416': {'issue': {}, 'volume': {'107': {'id': '4733', 'issue': {'1': {'id': '4734'}, '2': {'id': '4735'}, '3': {'id': '4736'}, '4': {'id': '4737'}, '5': {'id': '4738'}, '6': {'id': '4739'}}}, '108': {'id': '4740', 'issue': {'1': {'id': '4741'}, '2': {'id': '4742'}, '3': {'id': '4743'}, '4': {'id': '4744'}}}, '104': {'id': '4712', 'issue': {'1': {'id': '4713'}, '2': {'id': '4714'}, '3': {'id': '4715'}, '4': {'id': '4716'}, '5': {'id': '4717'}, '6': {'id': '4718'}}}, '148': {'id': '4417', 'issue': {'12': {'id': '4418'}, '11': {'id': '4419'}}}, '100': {'id': '4684', 'issue': {'1': {'id': '4685'}, '2': {'id': '4686'}, '3': {'id': '4687'}, '4': {'id': '4688'}, '5': {'id': '4689'}, '6': {'id': '4690'}}}, '101': {'id': '4691', 'issue': {'1': {'id': '4692'}, '2': {'id': '4693'}, '3': {'id': '4694'}, '4': {'id': '4695'}, '5': {'id': '4696'}, '6': {'id': '4697'}}}, '102': {'id': '4698', 'issue': {'1': {'id': '4699'}, '2': {'id': '4700'}, '3': {'id': '4701'}, '4': {'id': '4702'}, '5': {'id': '4703'}, '6': {'id': '4704'}}}, '103': {'id': '4705', 'issue': {'1': {'id': '4706'}, '2': {'id': '4707'}, '3': {'id': '4708'}, '4': {'id': '4709'}, '5': {'id': '4710'}, '6': {'id': '4711'}}}, '105': {'id': '4719', 'issue': {'1': {'id': '4720'}, '2': {'id': '4721'}, '3': {'id': '4722'}, '4': {'id': '4723'}, '5': {'id': '4724'}, '6': {'id': '4725'}}}, '106': {'id': '4726', 'issue': {'6': {'id': '4732'}, '1': {'id': '4727'}, '2': {'id': '4728'}, '3': {'id': '4729'}, '4': {'id': '4730'}, '5': {'id': '4731'}}}}}}
        curator.meta_maker()
        output = (curator.brmeta, curator.rameta, curator.armeta)
        expected_output = (
            {'3757': {'ids': {'doi:10.1001/archderm.104.1.106', 'pmid:29098884', 'omid:br/3757'}, 'title': 'Multiple Keloids', 'others': set()}, '4416': {'ids': {'issn:0003-987X', 'omid:br/4416'}, 'title': 'Archives Of Dermatology', 'others': set()}},
            {'6033': {'ids': {'omid:ra/6033'}, 'others': set(), 'title': 'Curth, W.'}, '0601': {'ids': {'orcid:0000-0003-0530-4305', 'schema:12345', 'omid:ra/0601'}, 'others': {'wannabe_0'}, 'title': 'Mcsorley, J.'}},
            {'3757': {'author': [('9445', '6033'), ('0601', '0601')], 'editor': [], 'publisher': []}}
        )
        assert output == expected_output


@pytest.fixture(scope="class")
def id_worker_finder():
    add_data_ts(SERVER, os.path.abspath(os.path.join('test', 'testcases', 'ts', 'real_data.nt')).replace('\\', '/'))
    finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
    finder.get_everything_about_res(metavals={'omid:br/3309', 'omid:br/2438', 'omid:br/0601'}, identifiers={'doi:10.1001/2013.jamasurg.270', 'doi:10.1787/eco_outlook-v2011-2-graph138-en', 'orcid:0000-0001-6994-8412', 'doi:10.1001/archderm.104.1.106', 'pmid:29098884'}, vvis=set())
    return finder


class TestIdWorker:
    def test_id_worker_1(self):
        # 1 EntityA is a new one
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = ['doi:10.1163/2214-8655_lgo_lgo_02_0074_ger']
        wannabe_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': {'doi:10.1163/2214-8655_lgo_lgo_02_0074_ger'}, 'others': set(), 'title': 'βέβαιος, α, ον'}},
            {},
            {'doi:10.1163/2214-8655_lgo_lgo_02_0074_ger': '0601'},
            {}
        )
        assert output == expected_output

    def test_id_worker_1_no_id(self):
        # 1 EntityA is a new one and has no ids
        curator = prepareCurator(list())
        name = 'βέβαιος, α, ον'
        idslist = []
        wannabe_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': set(), 'others': set(), 'title': 'βέβαιος, α, ον'}},
            {},
            {},
            {}
        )
        assert output == expected_output

    def test_id_worker_2_id_ts(self, id_worker_finder):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        idslist = ['crossref:10']
        wannabe_id = curator.id_worker('editor', name, idslist, '', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra)
        expected_output = ('3309', {}, {'3309': {'ids': {'crossref:10'}, 'others': set(), 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'})
        assert output == expected_output

    def test_id_worker_2_metaid_ts(self, id_worker_finder):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        # MetaID only
        wannabe_id = curator.id_worker('editor', name, [], '3309', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra)
        expected_output = ('3309', {}, {'3309': {'ids': {'crossref:10'}, 'others': set(), 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'})
        assert output == expected_output

    def test_id_worker_2_id_metaid_ts(self, id_worker_finder):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV
        curator = prepareCurator(list())
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        curator.finder = id_worker_finder
        # ID and MetaID
        wannabe_id = curator.id_worker('publisher', name, ['crossref:10'], '3309', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra)
        expected_output = ('3309', {}, {'3309': {'ids': {'crossref:10'}, 'others': set(), 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'})
        assert output == expected_output

    def test_id_worker_3(self, id_worker_finder):
        # 2 Retrieve EntityA data in triplestore to update EntityA inside CSV. MetaID on ts has precedence
        curator = prepareCurator(list())
        name = 'American Medical Association (AMA)' # *(ama) on the ts. The name on the ts must prevail
        curator.finder = id_worker_finder
        # ID and MetaID, but it's omid:ra/3309 on ts
        wannabe_id = curator.id_worker('publisher', name, ['crossref:10'], '33090', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
        output = (wannabe_id, curator.brdict, curator.radict, curator.idbr, curator.idra)
        expected_output = ('3309', {}, {'3309': {'ids': {'crossref:10'}, 'others': set(), 'title': 'American Medical Association (ama)'}}, {}, {'crossref:10': '4274'})
        assert output == expected_output

    def test_id_worker_conflict(self, id_worker_finder):
        # there's no meta or there was one but it didn't exist
        # There are other ids that already exist, but refer to multiple entities on ts.
        # Conflict!
        idslist = ['doi:10.1001/2013.jamasurg.270']
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        id_dict = dict()
        metaval = curator.conflict(idslist, name, id_dict, 'id') # Only the conflict function is tested here, not id_worker
        output = (metaval, curator.brdict, id_dict)
        expected_output = (
            'wannabe_0',
            {'wannabe_0': {'ids': {'doi:10.1001/2013.jamasurg.270'}, 'others': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'}},
            {'doi:10.1001/2013.jamasurg.270': '2585'}
        )
        assert output == expected_output

    def test_conflict_br(self, id_worker_finder):
        # No MetaId, an identifier to which two separate br point: there is a conflict, and a new entity must be created
        curator = prepareCurator(list())
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1001/2013.jamasurg.270']
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.brdict)
        expected_output_1 = (
            '2719',
            {'doi:10.1001/2013.jamasurg.270': '2585'},
            {},
            {'2719': {'ids': {'doi:10.1001/2013.jamasurg.270'}, 'others': set(), 'title': 'Patient Satisfaction As A Possible Indicator Of Quality Surgical Care'}}
        )
        expected_output_2 = ('2720',
            {'doi:10.1001/2013.jamasurg.270': '2585'},
            {},
            {'2720': {'ids': {'doi:10.1001/2013.jamasurg.270'},
                    'others': set(),
                    'title': 'Pediatric Injury Outcomes In Racial/Ethnic Minorities In '
                                'California'}}
        )
        assert output == expected_output_1 or output == expected_output_2

    def test_conflict_ra(self, id_worker_finder):
        # No MetaId, an identifier to which two separate ra point: there is a conflict, and a new entity must be created
        idslist = ['orcid:0000-0001-6994-8412']
        name = 'Alarcon, Louis H.'
        curator = prepareCurator(list())
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('author', name, idslist, '', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict)
        expected_output_1 = (
            '4940',
            {},
            {'orcid:0000-0001-6994-8412': '4475'},
            {},
            {'4940': {'ids': {'orcid:0000-0001-6994-8412'}, 'others': set(), 'title': 'Alarcon, Louis H.'}}
        )
        expected_output_2 = ('1000000',
            {},
            {'orcid:0000-0001-6994-8412': '4475'},
            {},
            {'1000000': {'ids': {'orcid:0000-0001-6994-8412'},
                        'others': set(),
                        'title': 'Alarcon, Louis H.'}})
        assert output == expected_output_1 or output == expected_output_2

    def test_conflict_suspect_id_among_existing(self, id_worker_finder):
        # ID already exist in entity_dict and refer to one entity having a MetaID, but there is another ID not in entity_dict that highlights a conflict on ts
        br_dict = {
            'omid:br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'omid:br/0602': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'}, 'others': set(), 'title': 'Contributions To GDP Growth And Inflation: South Africa'},
            'omid:br/0603': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph18-en'}, 'others': set(), 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'},
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan' # The first title must have precedence (China, not Japan)
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270']
        curator = prepareCurator(get_csv_data(REAL_DATA_CSV))
        curator.brdict = br_dict
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict)
        expected_output = (
            'wannabe_0',
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601',
                'doi:10.1001/2013.jamasurg.270': '2585'
            },
            {},
            {'omid:br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'},
                            'others': set(),
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                        'Materials Prices: China'},
            'omid:br/0602': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'},
                            'others': set(),
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                        'Africa'},
            'omid:br/0603': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph18-en'},
                            'others': set(),
                            'title': 'Official Loans To The Governments Of Greece, '
                                        'Ireland And Portugal'},
            'wannabe_0': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en',
                                    'doi:10.1001/2013.jamasurg.270'},
                            'others': set(),
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                    'Materials Prices: Japan'}},
            {}
        )
        assert output == expected_output

    def test_conflict_suspect_id_among_wannabe(self, id_worker_finder):
        # ID already exist in entity_dict and refer to one temporary, but there is another ID not in entity_dict that highlights a conflict on ts
        br_dict = {
            'wannabe_0': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'wannabe_2': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'}, 'others': set(), 'title': 'Contributions To GDP Growth And Inflation: South Africa'},
            'wannabe_3': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph18-en'}, 'others': set(), 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'},
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan' # The first title must have precedence (China, not Japan)
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en', 'doi:10.1001/2013.jamasurg.270']
        curator = prepareCurator(get_csv_data(REAL_DATA_CSV))
        curator.brdict = br_dict
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict)
        expected_output_1 = (
            '2720',
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601',
                'doi:10.1001/2013.jamasurg.270': '2585'
            },
            {},
            {'2720': {'ids': {'doi:10.1001/2013.jamasurg.270', 'doi:10.1787/eco_outlook-v2011-2-graph138-en'},
                            'others': {'wannabe_0'},
                            'title': 'Pediatric Injury Outcomes In Racial/Ethnic Minorities In '
                                        'California'},
            'wannabe_2': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'},
                            'others': set(),
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                    'Africa'},
            'wannabe_3': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph18-en'},
                            'others': set(),
                            'title': 'Official Loans To The Governments Of Greece, Ireland '
                                    'And Portugal'}},
            {}
        )
        expected_output_2 = (
            '2719',
            {
                'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601',
                'doi:10.1001/2013.jamasurg.270': '2585'
            },
            {},
            {'2719': {'ids': {'doi:10.1001/2013.jamasurg.270', 'doi:10.1787/eco_outlook-v2011-2-graph138-en'},
                            'others': {'wannabe_0'},
                            'title': 'Patient Satisfaction As A Possible Indicator Of Quality '
                                        'Surgical Care'},
            'wannabe_2': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'},
                            'others': set(),
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                    'Africa'},
            'wannabe_3': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph18-en'},
                            'others': set(),
                            'title': 'Official Loans To The Governments Of Greece, Ireland '
                                    'And Portugal'}},
            {}
        )
        assert output == expected_output_1 or output == expected_output_2

    def test_id_worker_4(self, id_worker_finder):
        # 4 Merge data from EntityA (CSV) with data from EntityX (CSV), update both with data from EntityA (RDF)
        br_dict = {
            'wannabe_0': {'ids': {'doi:10.1001/archderm.104.1.106'}, 'others': set(), 'title': 'Multiple eloids'},
            'wannabe_1': {'ids': {'doi:10.1001/archderm.104.1.106'}, 'others': set(), 'title': 'Multiple Blastoids'},
        }
        name = 'Multiple Palloids'
        idslist = ['doi:10.1001/archderm.104.1.106', 'pmid:29098884']
        curator = prepareCurator(list())
        curator.brdict = br_dict
        curator.wnb_cnt = 2
        curator.finder = id_worker_finder
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra)
        expected_output = (
            '3757',
            {'doi:10.1001/archderm.104.1.106': '3624', 'pmid:29098884': '2000000'},
            {}
        )
        assert output == expected_output


class TestIdWorkerWithReset:
    def test_id_worker_2_meta_in_entity_dict(self):
        # MetaID exists among data.
        # MetaID already in entity_dict (no care about conflicts, we have a MetaID specified)
        # 2 Retrieve EntityA data to update EntityA inside CSV
        reset_server()
        data = get_csv_data(REAL_DATA_CSV)
        curator = prepareCurator(data)
        curator.curator()
        store_curated_data(curator, SERVER)
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        curator_empty = prepareCurator(list())
        curator_empty.finder.get_everything_about_res(metavals=set(), identifiers={'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, vvis=set())
        # put metaval in entity_dict
        meta_id = curator_empty.id_worker('id', name, [], '0601', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        # metaval is in entity_dict
        meta_id = curator_empty.id_worker('id', name, [], '0601', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator_empty.brdict, curator_empty.radict, curator_empty.idbr, curator_empty.idra)
        expected_output = ('0601', {'0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China', 'others': set()}}, {}, {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'}, {})
        assert output == expected_output

    def test_conflict_existing(self):
        # ID already exist in entity_dict but refer to multiple entities having a MetaID
        reset_server()
        br_dict = {
            'omid:br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'omid:br/0602': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'}, 'others': set(), 'title': 'Contributions To GDP Growth And Inflation: South Africa'},
            'omid:br/0603': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'},
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator = prepareCurator(list())
        curator.brdict = br_dict
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra, curator.brdict, curator.radict)
        expected_output = (
            'wannabe_0',
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'},
            {},
            {'omid:br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'},
                            'others': set(),
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                        'Materials Prices: China'},
            'omid:br/0602': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'},
                            'others': set(),
                            'title': 'Contributions To GDP Growth And Inflation: South '
                                        'Africa'},
            'omid:br/0603': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'},
                            'others': set(),
                            'title': 'Official Loans To The Governments Of Greece, '
                                        'Ireland And Portugal'},
            'wannabe_0': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'},
                            'others': set(),
                            'title': 'Money Growth, Interest Rates, Inflation And Raw '
                                    'Materials Prices: China'}},
            {}
        )
        assert output == expected_output

    def test_id_worker_5(self):
        # ID already exist in entity_dict and refer to one or more temporary entities -> collective merge
        reset_server()
        br_dict = {
            'wannabe_0': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'wannabe_1': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'}, 'others': set(), 'title': 'Contributions To GDP Growth And Inflation: South Africa'},
            'wannabe_2': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'},
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator = prepareCurator(list())
        curator.brdict = br_dict
        curator.wnb_cnt = 2
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra)
        expected_output = (
            'wannabe_0',
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'},
            {}
        )
        assert output == expected_output

    def test_no_conflict_existing(self):
        # ID already exist in entity_dict and refer to one entity
        reset_server()
        br_dict = {
            'omid:br/0601': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph138-en'}, 'others': set(), 'title': 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: China'},
            'omid:br/0602': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph150-en'}, 'others': set(), 'title': 'Contributions To GDP Growth And Inflation: South Africa'},
            'omid:br/0603': {'ids': {'doi:10.1787/eco_outlook-v2011-2-graph18-en'}, 'others': set(), 'title': 'Official Loans To The Governments Of Greece, Ireland And Portugal'},
        }
        name = 'Money Growth, Interest Rates, Inflation And Raw Materials Prices: Japan' # The first title must have precedence (China, not Japan)
        idslist = ['doi:10.1787/eco_outlook-v2011-2-graph138-en']
        curator = prepareCurator(list())
        curator.brdict = br_dict
        meta_id = curator.id_worker('id', name, idslist, '', ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        output = (meta_id, curator.idbr, curator.idra)
        expected_output = (
            'omid:br/0601',
            {'doi:10.1787/eco_outlook-v2011-2-graph138-en': '0601'},
            {}
        )
        assert output == expected_output

    def test_metaid_in_prov(self):
        # MetaID not found in data, but found in the provenance metadata.
        reset_server()
        add_data_ts(server=SERVER, data_path=os.path.abspath(os.path.join('test', 'testcases', 'ts', 'real_data_with_prov.nq')).replace('\\', '/'))
        name = ''
        curator = prepareCurator(list())
        meta_id = curator.id_worker('id', name, [], '4321', ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
        assert meta_id == '38013'


class TestTestcase01:
    def test(self):
        # testcase1: 2 different issues of the same venue (no volume)
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
        # testcase2: 2 different volumes of the same venue (no issue)
        name = '02'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[1])
        partial_data.append(data[3])
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase03:
    def test(self):
        # testcase3: 2 different issues of the same volume
        name = '03'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[2])
        partial_data.append(data[4])
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase04:
    def test(self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
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
        # testcase5: NO ID scenario
        name = '05'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = list()
        partial_data.append(data[8])
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase06:
    def test(self):
        # testcase6: ALL types test
        name = '06'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[9:33]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase07:
    def test(self):
        # testcase7: all journal related types with an editor
        name = '07'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[34:40]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase08:
    def test(self):
        # testcase8: all book related types with an editor
        name = '08'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[40:43]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase09:
    def test(self):
        # testcase09: all proceeding related types with an editor
        name = '09'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[43:45]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase10:
    def test(self):
        # testcase10: a book inside a book series and a book inside a book set
        name = '10'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[45:49]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase11:
    def test(self):
        # testcase11: real time entity update
        name = '11'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[49:52]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase12:
    def test(self):
        # testcase12: clean name, title, ids
        name = '12'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[52:53]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase13:
    # testcase13: ID_clean massive test

    def test1(self):
        # 1--- meta specified br in a row, wannabe with a new id in a row, meta specified with an id related to wannabe
        # in a row
        name = '13.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[53:56]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        # 2---Conflict with META precedence: a br has a meta_id and an id related to another meta_id, the first
        # specified meta has precedence
        data = get_csv_data(MANUAL_DATA_CSV)
        name = '13.2'
        partial_data = data[56:57]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        # 3--- conflict: br with id shared with 2 meta
        data = get_csv_data(MANUAL_DATA_CSV)
        name_1 = '13.3'
        name_2 = '13.31'
        partial_data = data[57:58]
        data_curated, testcase_1 = prepare_to_test(partial_data, name_1)
        _, testcase_2 = prepare_to_test(partial_data, name_2)
        assert data_curated == testcase_1 or data_curated == testcase_2


class TestTestcase14:

    def test1(self):
        # update existing sequence, in particular, a new author and an existing author without an existing id (matched
        # thanks to surname,name(BAD WRITTEN!)
        name = '14.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[58:59]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        # same sequence different order, with new ids
        name = '14.2'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[59:60]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        # RA
        # Author with two different ids
        name_1 = '14.3'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[60:61]
        data_curated, testcase_1 = prepare_to_test(partial_data, name_1)
        assert data_curated == testcase_1

    def test4(self):
        # meta specified ra in a row, wannabe ra with a new id in a row, meta specified with an id related to wannabe
        # in a ra
        name = '14.4'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[61:64]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase15:

    def test1(self):
        # venue volume issue already exists in ts
        name = '15.1'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[64:65]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        # venue conflict
        name = '15.2'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[65:66]
        data_curated, testcase = prepare_to_test(partial_data, name)
        # _, testcase_2 = prepare_to_test(partial_data, name_2)
        assert data_curated == testcase

    def test3(self):
        # venue in ts is now the br
        name = '15.3'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[66:67]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test4(self):
        # br in ts is now the venue
        name = '15.4'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[67:68]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test5(self):
        # volume in ts is now the br
        name = '15.5'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[71:72]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test6(self):
        # br is a volume
        name = '15.6'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[72:73]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test7(self):
        # issue in ts is now the br
        name = '15.7'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[73:74]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test8(self):
        # br is a issue
        name = '15.8'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[74:75]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase


class TestTestcase16:

    def test1(self):
        # Date cleaning 2019-02-29
        name = '16.1'
        # add_data_ts('http://127.0.0.1:8805/sparql')
        # wrong date (2019/02/29)
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[75:76]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test2(self):
        # existing re
        name = '16.2'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[76:77]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase

    def test3(self):
        # given name for an RA with only a family name in TS
        name = '16.3'
        data = get_csv_data(MANUAL_DATA_CSV)
        partial_data = data[77:78]
        data_curated, testcase = prepare_to_test(partial_data, name)
        assert data_curated == testcase
