import unittest
from meta.lib.finder import ResourceFinder
from SPARQLWrapper import SPARQLWrapper, POST
from pprint import pprint


ENDPOINT = 'http://localhost:9999/blazegraph/sparql'
REAL_DATA_FILE = 'meta/tdd/testcases/ts/real_data.nt'
finder = ResourceFinder(ENDPOINT)
# Clear ts
ts = SPARQLWrapper(ENDPOINT)
ts.setMethod(POST)
ts.setQuery('DELETE {?s ?p ?o} WHERE {?s ?p ?o}')
ts.query()
# Upload data
ts.setQuery(f'LOAD <file:meta/tdd/testcases/ts/real_data.nt>')
ts.query()

class test_ResourceFinder(unittest.TestCase):
    def test_retrieve_br_from_id(self):
        value = '10.1001/.391'
        schema = 'doi'
        output = finder.retrieve_br_from_id(schema, value)
        expected_output = [(
            '2373', 
            'Treatment Of Excessive Anticoagulation With Phytonadione (Vitamin K): A Meta-analysis', 
            [('2239', 'doi:10.1001/.391')]
        )]
        self.assertEqual(output, expected_output)

    def test_retrieve_br_from_id_multiple_ids(self):
        value = '10.1001/.405'
        schema = 'doi'
        output = finder.retrieve_br_from_id(schema, value)
        expected_output = [(
            '2374', 
            "Neutropenia In Human Immunodeficiency Virus Infection: Data From The Women's Interagency HIV Study", 
            [('2240', 'doi:10.1001/.405'), ('5000', 'doi:10.1001/.406')]
        )]
        self.assertEqual(output, expected_output)
    
    def test_retrieve_br_from_meta(self):
        metaid = '2373'
        output = finder.retrieve_br_from_meta(metaid)
        expected_output = ('Treatment Of Excessive Anticoagulation With Phytonadione (Vitamin K): A Meta-analysis', [('2239', 'doi:10.1001/.391')])
        self.assertEqual(output, expected_output)

    def test_retrieve_br_from_meta_multiple_ids(self):
        metaid = '2374'
        output = finder.retrieve_br_from_meta(metaid)
        expected_output = ("Neutropenia In Human Immunodeficiency Virus Infection: Data From The Women's Interagency HIV Study", [('2240', 'doi:10.1001/.405'), ('5000', 'doi:10.1001/.406')])
        self.assertEqual(output, expected_output)

    def test_retrieve_metaid_from_id(self):
        schema = 'doi'
        value = '10.1001/.391'
        output = finder.retrieve_metaid_from_id(schema, value)
        expected_output = '2239'
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_meta(self):
        metaid = '3308'
        output = finder.retrieve_ra_from_meta(metaid, publisher=False)
        expected_output = ('Dezee, K. J.', [])
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_meta_with_orcid(self):
        metaid = '4940'
        output = finder.retrieve_ra_from_meta(metaid, publisher=False)
        expected_output = ('Alarcon, Louis H.', [('4475', 'orcid:0000-0001-6994-8412')])
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_meta_if_publisher(self):
        metaid = '3309'
        output = finder.retrieve_ra_from_meta(metaid, publisher=True)
        expected_output = ('American Medical Association (ama)', [('4274', 'crossref:10')])
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_id(self):
        schema = 'orcid'
        value = '0000-0001-6994-8412'
        output = finder.retrieve_ra_from_id(schema, value, publisher=False)
        expected_output = [('4940', 'Alarcon, Louis H.', [('4475', 'orcid:0000-0001-6994-8412')])]
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_id_if_publisher(self):
        schema = 'crossref'
        value = '10'
        output = finder.retrieve_ra_from_id(schema, value, publisher=True)
        expected_output = [('3309', 'American Medical Association (ama)', [('4274', 'crossref:10')])]
        self.assertEqual(output, expected_output)

    def test_retrieve_vvi_by_venue(self):
        venue_meta = '4387'
        output = finder.retrieve_venue_from_meta(venue_meta)
        expected_output = {
            'issue': {}, 
            'volume': {
                '166': {'id': '4388', 'issue': {'4': {'id': '4389'}}}, 
                '172': {'id': '4434', 
                    'issue': {
                        '22': {'id': '4435'}, 
                        '20': {'id': '4436'}, 
                        '21': {'id': '4437'}, 
                        '19': {'id': '4438'}
                    }
                }
            }
        }        
        self.assertEqual(output, expected_output)


if __name__ == '__main__':
    unittest.main()