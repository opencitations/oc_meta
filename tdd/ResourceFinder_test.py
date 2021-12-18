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
    def test_retrieve_vvi_by_venue(self):
        venue_meta = '4387'
        content = {'issue': dict(), 'volume': dict()}
        output = finder.retrieve_vvi(venue_meta, content)
        expected_output = {
            'issue': {'4': '4389', '22': '4435', '20': '4436', '21': '4437', '19': '4438'}, 
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

    def test_retrieve_vvi_by_volume(self):
        volume_meta = '4434'
        content = {'issue': dict(), 'volume': dict()}
        output = finder.retrieve_vvi(volume_meta, content)
        expected_output = {'issue': {'22': '4435', '20': '4436', '21': '4437', '19': '4438'}, 'volume': {}}    
        self.assertEqual(output, expected_output)        

if __name__ == '__main__':
    unittest.main()