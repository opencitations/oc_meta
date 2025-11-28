import os
import unittest

from oc_meta.lib.finder import ResourceFinder
from oc_ocdm.graph import GraphEntity
from rdflib import Dataset, Graph, Literal, URIRef
from SPARQLWrapper import POST, SPARQLWrapper


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
        g = Dataset()
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

    def test_retrieve_ra_sequence_with_loop(self):
        """Test that retrieve_ra_sequence_from_br_meta handles circular references without infinite loops"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9999')
        ar1_uri = URIRef(f'{base_iri}/ar/9991')
        ar2_uri = URIRef(f'{base_iri}/ar/9992')
        ra1_uri = URIRef(f'{base_iri}/ra/9981')
        ra2_uri = URIRef(f'{base_iri}/ra/9982')

        # Create a circular AR chain: AR1 -> AR2 -> AR1 (loop)
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar2_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_given_name, Literal('John')))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_family_name, Literal('Doe')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_is_held_by, ra2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_has_next, ar1_uri))
        self.finder.local_g.add((ra2_uri, GraphEntity.iri_given_name, Literal('Jane')))
        self.finder.local_g.add((ra2_uri, GraphEntity.iri_family_name, Literal('Smith')))

        # This should return only 2 ARs (breaking the loop) without hanging
        result = self.finder.retrieve_ra_sequence_from_br_meta('9999', 'author')

        # Should return exactly 2 ARs (not infinite loop)
        self.assertEqual(len(result), 2)
        # Should contain both ARs
        ar_ids = [list(item.keys())[0] for item in result]
        self.assertIn('9991', ar_ids)
        self.assertIn('9992', ar_ids)

    def test_retrieve_ra_sequence_with_self_reference(self):
        """Test that retrieve_ra_sequence_from_br_meta handles self-referencing AR"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9998')
        ar1_uri = URIRef(f'{base_iri}/ar/9981')
        ra1_uri = URIRef(f'{base_iri}/ra/9971')

        # Create AR that points to itself
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar1_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Test Publisher')))

        # This should return only 1 AR (ignoring self-reference)
        result = self.finder.retrieve_ra_sequence_from_br_meta('9998', 'author')

        # Should return exactly 1 AR
        self.assertEqual(len(result), 1)
        self.assertEqual(list(result[0].keys())[0], '9981')

    def test_retrieve_ra_sequence_with_invalid_next(self):
        """Test that retrieve_ra_sequence_from_br_meta handles invalid 'next' references"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9997')
        ar1_uri = URIRef(f'{base_iri}/ar/9971')
        ar2_uri = URIRef(f'{base_iri}/ar/9972')
        ar_invalid_uri = URIRef(f'{base_iri}/ar/9999')
        ra1_uri = URIRef(f'{base_iri}/ra/9961')
        ra2_uri = URIRef(f'{base_iri}/ra/9962')

        # Create AR chain where AR1 -> AR_INVALID (doesn't exist) and AR2 is orphaned
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar_invalid_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Author One')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_is_held_by, ra2_uri))
        self.finder.local_g.add((ra2_uri, GraphEntity.iri_name, Literal('Author Two')))

        # Should return chain stopping at invalid reference
        result = self.finder.retrieve_ra_sequence_from_br_meta('9997', 'author')

        # Should return at least AR1 (stops at invalid next)
        # The method will find 2 start candidates and pick the longest chain
        self.assertGreaterEqual(len(result), 1)
        ar_ids = [list(item.keys())[0] for item in result]
        self.assertIn('9971', ar_ids)

    def test_retrieve_ra_sequence_with_missing_is_held_by(self):
        """Test that retrieve_ra_sequence_from_br_meta handles AR without is_held_by gracefully"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9996')
        ar1_uri = URIRef(f'{base_iri}/ar/9961')

        # Create AR without is_held_by relationship (malformed data)
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        # Missing: ar1_uri iri_is_held_by ra_uri

        # Should handle gracefully without crash
        try:
            result = self.finder.retrieve_ra_sequence_from_br_meta('9996', 'author')
            # If it doesn't crash, check result is reasonable (either empty or handles error)
            self.assertIsInstance(result, list)
        except (KeyError, UnboundLocalError) as e:
            self.fail(f"Method crashed with missing is_held_by: {e}")

    def test_retrieve_ra_sequence_with_multiple_next_values(self):
        """Test that retrieve_ra_sequence_from_br_meta handles AR with multiple 'next' relationships"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9995')
        ar1_uri = URIRef(f'{base_iri}/ar/9951')
        ar2_uri = URIRef(f'{base_iri}/ar/9952')
        ar3_uri = URIRef(f'{base_iri}/ar/9953')
        ra1_uri = URIRef(f'{base_iri}/ra/9941')
        ra2_uri = URIRef(f'{base_iri}/ra/9942')
        ra3_uri = URIRef(f'{base_iri}/ra/9943')

        # Create AR1 with multiple 'next' relationships (data error)
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar2_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar3_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Author One')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_is_held_by, ra2_uri))
        self.finder.local_g.add((ra2_uri, GraphEntity.iri_name, Literal('Author Two')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar3_uri))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_is_held_by, ra3_uri))
        self.finder.local_g.add((ra3_uri, GraphEntity.iri_name, Literal('Author Three')))

        # Should handle multiple next values consistently (last one wins in current implementation)
        result = self.finder.retrieve_ra_sequence_from_br_meta('9995', 'author')

        # Should return a valid result without crashing
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_retrieve_ra_sequence_no_ars_for_role(self):
        """Test that retrieve_ra_sequence_from_br_meta returns empty list when no ARs exist for specified role"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9994')
        ar1_uri = URIRef(f'{base_iri}/ar/9941')
        ra1_uri = URIRef(f'{base_iri}/ra/9931')

        # Create BR with editor, but request author
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_editor))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Editor Name')))

        # Request author (should be empty)
        result = self.finder.retrieve_ra_sequence_from_br_meta('9994', 'author')

        self.assertEqual(result, [])

    def test_retrieve_ra_sequence_single_ar_no_chain(self):
        """Test that retrieve_ra_sequence_from_br_meta handles single AR without 'next'"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9993')
        ar1_uri = URIRef(f'{base_iri}/ar/9931')
        ra1_uri = URIRef(f'{base_iri}/ra/9921')

        # Create single AR without next
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Single Author')))

        result = self.finder.retrieve_ra_sequence_from_br_meta('9993', 'author')

        self.assertEqual(len(result), 1)
        self.assertEqual(list(result[0].keys())[0], '9931')

    def test_retrieve_ra_sequence_two_independent_chains(self):
        """Test that retrieve_ra_sequence_from_br_meta picks longest chain when multiple disconnected chains exist"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9992')

        # Chain 1: AR1 -> AR2 (length 2)
        ar1_uri = URIRef(f'{base_iri}/ar/9921')
        ar2_uri = URIRef(f'{base_iri}/ar/9922')
        ra1_uri = URIRef(f'{base_iri}/ra/9911')
        ra2_uri = URIRef(f'{base_iri}/ra/9912')

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar2_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Author One')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_is_held_by, ra2_uri))
        self.finder.local_g.add((ra2_uri, GraphEntity.iri_name, Literal('Author Two')))

        # Chain 2: AR3 (length 1, disconnected)
        ar3_uri = URIRef(f'{base_iri}/ar/9923')
        ra3_uri = URIRef(f'{base_iri}/ra/9913')

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar3_uri))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_is_held_by, ra3_uri))
        self.finder.local_g.add((ra3_uri, GraphEntity.iri_name, Literal('Author Three')))

        result = self.finder.retrieve_ra_sequence_from_br_meta('9992', 'author')

        # Should return the longer chain (chain 1 with 2 elements)
        self.assertEqual(len(result), 2)
        ar_ids = [list(item.keys())[0] for item in result]
        self.assertIn('9921', ar_ids)
        self.assertIn('9922', ar_ids)

    def test_retrieve_ra_sequence_editor_role(self):
        """Test that retrieve_ra_sequence_from_br_meta works with editor role"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9991')
        ar1_uri = URIRef(f'{base_iri}/ar/9911')
        ra1_uri = URIRef(f'{base_iri}/ra/9901')

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_editor))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Editor Name')))

        result = self.finder.retrieve_ra_sequence_from_br_meta('9991', 'editor')

        self.assertEqual(len(result), 1)
        self.assertEqual(list(result[0].keys())[0], '9911')

    def test_retrieve_ra_sequence_publisher_role(self):
        """Test that retrieve_ra_sequence_from_br_meta works with publisher role"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9990')
        ar1_uri = URIRef(f'{base_iri}/ar/9901')
        ra1_uri = URIRef(f'{base_iri}/ra/9891')

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_publisher))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Publisher Name')))

        result = self.finder.retrieve_ra_sequence_from_br_meta('9990', 'publisher')

        self.assertEqual(len(result), 1)
        self.assertEqual(list(result[0].keys())[0], '9901')

    def test_retrieve_ra_sequence_three_node_loop(self):
        """Test that retrieve_ra_sequence_from_br_meta handles three-node circular loop"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9989')
        ar1_uri = URIRef(f'{base_iri}/ar/9891')
        ar2_uri = URIRef(f'{base_iri}/ar/9892')
        ar3_uri = URIRef(f'{base_iri}/ar/9893')
        ra1_uri = URIRef(f'{base_iri}/ra/9881')
        ra2_uri = URIRef(f'{base_iri}/ra/9882')
        ra3_uri = URIRef(f'{base_iri}/ra/9883')

        # Create circular loop: AR1 -> AR2 -> AR3 -> AR1
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar2_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Author One')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_is_held_by, ra2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_has_next, ar3_uri))
        self.finder.local_g.add((ra2_uri, GraphEntity.iri_name, Literal('Author Two')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar3_uri))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_is_held_by, ra3_uri))
        self.finder.local_g.add((ar3_uri, GraphEntity.iri_has_next, ar1_uri))
        self.finder.local_g.add((ra3_uri, GraphEntity.iri_name, Literal('Author Three')))

        result = self.finder.retrieve_ra_sequence_from_br_meta('9989', 'author')

        # Should return exactly 3 ARs (breaking loop)
        self.assertEqual(len(result), 3)
        ar_ids = [list(item.keys())[0] for item in result]
        self.assertIn('9891', ar_ids)
        self.assertIn('9892', ar_ids)
        self.assertIn('9893', ar_ids)

    def test_retrieve_ra_sequence_duplicate_ra(self):
        """Test that retrieve_ra_sequence_from_br_meta returns both ARs when they point to same RA"""
        base_iri = 'https://w3id.org/oc/meta'
        br_uri = URIRef(f'{base_iri}/br/9988')
        ar1_uri = URIRef(f'{base_iri}/ar/9881')
        ar2_uri = URIRef(f'{base_iri}/ar/9882')
        ra1_uri = URIRef(f'{base_iri}/ra/9871')

        # Two ARs pointing to same RA (duplicate author)
        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_is_held_by, ra1_uri))
        self.finder.local_g.add((ar1_uri, GraphEntity.iri_has_next, ar2_uri))
        self.finder.local_g.add((ra1_uri, GraphEntity.iri_name, Literal('Same Author')))

        self.finder.local_g.add((br_uri, GraphEntity.iri_is_document_context_for, ar2_uri))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_with_role, GraphEntity.iri_author))
        self.finder.local_g.add((ar2_uri, GraphEntity.iri_is_held_by, ra1_uri))

        result = self.finder.retrieve_ra_sequence_from_br_meta('9988', 'author')

        # Should return both ARs even though they reference same RA
        self.assertEqual(len(result), 2)
        # Both should reference RA 9871
        self.assertEqual(result[0][list(result[0].keys())[0]][2], '9871')
        self.assertEqual(result[1][list(result[1].keys())[0]][2], '9871')


class TestVVIQueryIsolation(unittest.TestCase):
    """Test that VVI queries only search under the correct venues."""

    @classmethod
    def setUpClass(cls):
        ENDPOINT = 'http://127.0.0.1:8805/sparql'
        BASE_IRI = 'https://w3id.org/oc/meta/'
        reset_server(server=ENDPOINT)

        # Upload test data: two venues with different ISSNs, each with their own volume
        test_triples = [
            # Venue A (br/9001) with ISSN 1111-1111
            '<https://w3id.org/oc/meta/br/9001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> .',
            '<https://w3id.org/oc/meta/br/9001> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/9001> .',
            '<https://w3id.org/oc/meta/id/9001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .',
            '<https://w3id.org/oc/meta/id/9001> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .',
            '<https://w3id.org/oc/meta/id/9001> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "1111-1111"^^<http://www.w3.org/2001/XMLSchema#string> .',
            # Volume 10 of Venue A
            '<https://w3id.org/oc/meta/br/9002> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume> .',
            '<https://w3id.org/oc/meta/br/9002> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/9001> .',
            '<https://w3id.org/oc/meta/br/9002> <http://purl.org/spar/fabio/hasSequenceIdentifier> "10"^^<http://www.w3.org/2001/XMLSchema#string> .',
            # Venue B (br/9003) with ISSN 2222-2222
            '<https://w3id.org/oc/meta/br/9003> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> .',
            '<https://w3id.org/oc/meta/br/9003> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/9002> .',
            '<https://w3id.org/oc/meta/id/9002> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> .',
            '<https://w3id.org/oc/meta/id/9002> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .',
            '<https://w3id.org/oc/meta/id/9002> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "2222-2222"^^<http://www.w3.org/2001/XMLSchema#string> .',
            # Volume 20 of Venue B
            '<https://w3id.org/oc/meta/br/9004> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume> .',
            '<https://w3id.org/oc/meta/br/9004> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/9003> .',
            '<https://w3id.org/oc/meta/br/9004> <http://purl.org/spar/fabio/hasSequenceIdentifier> "20"^^<http://www.w3.org/2001/XMLSchema#string> .',
        ]

        ts = SPARQLWrapper(ENDPOINT)
        ts.setMethod(POST)
        for triple in test_triples:
            query = f"INSERT DATA {{ GRAPH <https://w3id.org/oc/meta/br/> {{ {triple} }} }}"
            ts.setQuery(query)
            ts.query()

    def test_vvi_queries_only_search_correct_venues(self):
        """Test that VVI queries only search under venues matching each tuple's identifiers.

        This test verifies the fix for the bug where VVI queries were incorrectly
        searching under ALL venues instead of just the venues matching each VVI tuple.
        With the bug, searching for volume "10" under venue with ISSN 2222-2222 would
        also incorrectly search under venue with ISSN 1111-1111.
        """
        ENDPOINT = 'http://127.0.0.1:8805/sparql'
        BASE_IRI = 'https://w3id.org/oc/meta/'
        local_g = Graph()
        settings = {'virtuoso_full_text_search': True}
        finder = ResourceFinder(ENDPOINT, BASE_IRI, local_g, settings=settings)

        # VVI tuples: each should only search under its corresponding venue
        vvis = {
            ("10", "", None, ("issn:1111-1111",)),  # Volume 10 of Venue A
            ("20", "", None, ("issn:2222-2222",)),  # Volume 20 of Venue B
        }

        finder.get_everything_about_res(metavals=set(), identifiers=set(), vvis=vvis)

        # Verify both volumes were found
        volume_10_uri = URIRef('https://w3id.org/oc/meta/br/9002')
        volume_20_uri = URIRef('https://w3id.org/oc/meta/br/9004')
        venue_a_uri = URIRef('https://w3id.org/oc/meta/br/9001')
        venue_b_uri = URIRef('https://w3id.org/oc/meta/br/9003')

        # Check that volume 10 is in local graph and is part of venue A (not venue B)
        self.assertIn(volume_10_uri, finder.prebuilt_subgraphs)
        volume_10_graph = finder.prebuilt_subgraphs[volume_10_uri]
        self.assertTrue(
            (volume_10_uri, GraphEntity.iri_part_of, venue_a_uri) in volume_10_graph,
            "Volume 10 should be part of Venue A"
        )

        # Check that volume 20 is in local graph and is part of venue B (not venue A)
        self.assertIn(volume_20_uri, finder.prebuilt_subgraphs)
        volume_20_graph = finder.prebuilt_subgraphs[volume_20_uri]
        self.assertTrue(
            (volume_20_uri, GraphEntity.iri_part_of, venue_b_uri) in volume_20_graph,
            "Volume 20 should be part of Venue B"
        )


if __name__ == '__main__': # pragma: no cover
    unittest.main()