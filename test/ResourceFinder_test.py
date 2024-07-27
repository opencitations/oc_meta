import os
import unittest
from pprint import pprint

from rdflib import Graph
from SPARQLWrapper import POST, SPARQLWrapper

from oc_meta.lib.finder import ResourceFinder
from rdflib import URIRef, ConjunctiveGraph, Graph


def get_path(path:str) -> str:
    # absolute_path:str = os.path.abspath(path)
    universal_path = path.replace('\\', '/')
    return universal_path

def add_data_ts(server, data_path, batch_size:int=100, default_graph_uri=URIRef("http://default.graph/")):
    f_path = get_path(data_path)
    
    # Determina il formato del file
    file_extension = os.path.splitext(f_path)[1].lower()
    if file_extension == '.nt':
        g = Graph()
        g.parse(location=f_path, format='nt')
    elif file_extension == '.nq':
        g = ConjunctiveGraph()
        g.parse(location=f_path, format='nquads')
    elif file_extension == '.ttl':
        g = Graph()
        g.parse(location=f_path, format='turtle')
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")
    
    triples_list = []
    if file_extension == '.nt':
        for subj, pred, obj in g:
            triples_list.append((subj, pred, obj, default_graph_uri))
    elif file_extension == '.nq':
        for subj, pred, obj, ctx in g.quads((None, None, None, None)):
            triples_list.append((subj, pred, obj, ctx))
    
    for i in range(0, len(triples_list), batch_size):
        batch_triples = triples_list[i:i + batch_size]
        
        triples_str = ""
        for subj, pred, obj, ctx in batch_triples:
            if ctx:
                triples_str += f"GRAPH {ctx.n3().replace('[', '').replace(']', '')} {{ {subj.n3()} {pred.n3()} {obj.n3()} }} "
            else: 
                triples_str += f"{subj.n3()} {pred.n3()} {obj.n3()} . "
        
        query = f"INSERT DATA {{ {triples_str} }}"
                
        ts = SPARQLWrapper(server)
        ts.setQuery(query)
        ts.setMethod(POST)
        ts.query()

def reset_server(server) -> None:
    ts = SPARQLWrapper(server)
    for graph in {'https://w3id.org/oc/meta/br/', 'https://w3id.org/oc/meta/ra/', 'https://w3id.org/oc/meta/re/', 'https://w3id.org/oc/meta/id/', 'https://w3id.org/oc/meta/ar/'}:
        ts.setQuery(f'CLEAR GRAPH <{graph}>')
        ts.setMethod(POST)
        ts.query()

class TestResourceFinder(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ENDPOINT = 'http://127.0.0.1:8805/sparql'
        BASE_IRI = 'https://w3id.org/oc/meta/'
        REAL_DATA_FILE = os.path.join('test', 'testcases', 'ts', 'real_data.nt')
        local_g = Graph()
        cls.finder = ResourceFinder(ENDPOINT, BASE_IRI, local_g)
        # Clear ts
        reset_server(server=ENDPOINT)
        # Upload data
        add_data_ts(server=ENDPOINT, data_path=REAL_DATA_FILE)
        cls.finder.get_everything_about_res(metavals={'omid:br/2373', 'omid:br/2380', 'omid:br/2730', 'omid:br/2374', 'omid:br/4435', 'omid:br/4436', 'omid:br/4437', 'omid:br/4438', 'omid:br/0604750', 'omid:br/0605379', 'omid:br/0606696'}, identifiers={'doi:10.1001/.391', 'orcid:0000-0001-6994-8412'}, vvis={})

    def test_retrieve_br_from_id(self):
        value = '10.1001/.391'
        schema = 'doi'
        output = self.finder.retrieve_br_from_id(schema, value)
        expected_output = [(
            '2373', 
            'Treatment Of Excessive Anticoagulation With Phytonadione (Vitamin K): A Meta-analysis', 
            [('2239', 'doi:10.1001/.391')]
        )]
        self.assertEqual(output, expected_output)

    def test_retrieve_br_from_id_multiple_ids(self):
        value = '10.1001/.405'
        schema = 'doi'
        output = self.finder.retrieve_br_from_id(schema, value)
        expected_output = [(
            '2374', 
            "Neutropenia In Human Immunodeficiency Virus Infection: Data From The Women's Interagency HIV Study", 
            [('2240', 'doi:10.1001/.405'), ('5000', 'doi:10.1001/.406')]
        )]
        self.assertEqual(output, expected_output)
    
    def test_retrieve_br_from_meta(self):
        metaid = '2373'
        output = self.finder.retrieve_br_from_meta(metaid)
        expected_output = ('Treatment Of Excessive Anticoagulation With Phytonadione (Vitamin K): A Meta-analysis', [('2239', 'doi:10.1001/.391')], True)
        self.assertEqual(output, expected_output)

    def test_retrieve_br_from_meta_multiple_ids(self):
        metaid = '2374'
        output = self.finder.retrieve_br_from_meta(metaid)
        output = (output[0], set(output[1]))
        expected_output = ("Neutropenia In Human Immunodeficiency Virus Infection: Data From The Women's Interagency HIV Study", {('2240', 'doi:10.1001/.405'), ('5000', 'doi:10.1001/.406')})
        self.assertEqual(output, expected_output)

    def test_retrieve_metaid_from_id(self):
        schema = 'doi'
        value = '10.1001/.391'
        output = self.finder.retrieve_metaid_from_id(schema, value)
        expected_output = '2239'
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_meta(self):
        metaid = '3308'
        output = self.finder.retrieve_ra_from_meta(metaid)
        expected_output = ('Dezee, K. J.', [], True)
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_meta_with_orcid(self):
        metaid = '4940'
        output = self.finder.retrieve_ra_from_meta(metaid)
        expected_output = ('Alarcon, Louis H.', [('4475', 'orcid:0000-0001-6994-8412')], True)
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_meta_if_publisher(self):
        metaid = '3309'
        output = self.finder.retrieve_ra_from_meta(metaid)
        expected_output = ('American Medical Association (ama)', [('4274', 'crossref:10')], True)
        self.assertEqual(output, expected_output)

    def test_retrieve_ra_from_id(self):
        schema = 'orcid'
        value = '0000-0001-6994-8412'
        output = self.finder.retrieve_ra_from_id(schema, value, publisher=False)
        expected_output = [
            ('1000000', 'Alarcon, Louis H.', [('4475', 'orcid:0000-0001-6994-8412')]),
            ('4940', 'Alarcon, Louis H.', [('4475', 'orcid:0000-0001-6994-8412')])
        ]
        self.assertEqual(sorted(output), expected_output)

    def test_retrieve_ra_from_id_if_publisher(self):
        schema = 'crossref'
        value = '10'
        output = self.finder.retrieve_ra_from_id(schema, value, publisher=True)
        expected_output = [('3309', 'American Medical Association (ama)', [('4274', 'crossref:10')])]
        self.assertEqual(output, expected_output)

    def test_retrieve_vvi_by_venue(self):
        venue_meta = '4387'
        output = self.finder.retrieve_venue_from_meta(venue_meta)
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

    def test_retrieve_vvi_issue_in_venue(self):
        venue_meta = '0604749'
        output = self.finder.retrieve_venue_from_meta(venue_meta)
        expected_output = {'issue': {'15': {'id': '0604750'}, '13': {'id': '0605379'}, '14': {'id': '0606696'}}, 'volume': {}}       
        self.assertEqual(output, expected_output)
    
    def test_retrieve_ra_sequence_from_br_meta(self):
        metaid = '2380'
        output = self.finder.retrieve_ra_sequence_from_br_meta(metaid, 'author')
        expected_output = [
            {'5343': ('Hodge, James G.', [], '3316')}, 
            {'5344': ('Anderson, Evan D.', [], '3317')}, 
            {'5345': ('Kirsch, Thomas D.', [], '3318')}, 
            {'5346': ('Kelen, Gabor D.', [('4278', 'orcid:0000-0002-3236-8286')], '3319')}
        ]
        self.assertEqual(output, expected_output)
    
    def test_retrieve_re_from_br_meta(self):
        metaid = '2373'
        output = self.finder.retrieve_re_from_br_meta(metaid)
        expected_output = ('2011', '391-397')
        self.assertEqual(output, expected_output)
    
    def test_retrieve_br_info_from_meta(self):
        metaid = '2373'
        output = self.finder.retrieve_br_info_from_meta(metaid)
        expected_output = {
            'pub_date': '2006-02-27', 
            'type': 'journal article', 
            'page': ('2011', '391-397'), 
            'issue': '4', 
            'volume': '166', 
            'venue': 'Archives Of Internal Medicine [omid:br/4387 issn:0003-9926]'
        }
        self.assertEqual(output, expected_output)
    

if __name__ == '__main__': # pragma: no cover
    unittest.main()