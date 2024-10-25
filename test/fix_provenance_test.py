import unittest
import os
import json
import tempfile
import zipfile
from datetime import datetime, UTC
from rdflib import ConjunctiveGraph, URIRef, Literal, Namespace
from rdflib.namespace import XSD

# Import the functions to test
from oc_meta.run.fixer.prov.fix import (
    extract_snapshot_number,
    get_entity_from_prov_graph,
    find_entity_for_snapshot,
    fix_provenance_file
)

class TestProvenanceFixing(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.temp_dir = tempfile.mkdtemp()
        
        # Sample JSON-LD data
        self.test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-31T22:08:21+00:00"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-31T22:08:21+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/2"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18.218917"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        # Create test zip file
        self.test_zip_path = os.path.join(self.temp_dir, "test_se.zip")
        with zipfile.ZipFile(self.test_zip_path, 'w') as zf:
            zf.writestr('se.json', json.dumps(self.test_data))

    def tearDown(self):
        # Clean up temporary files
        if os.path.exists(self.test_zip_path):
            os.remove(self.test_zip_path)
        os.rmdir(self.temp_dir)

    def test_extract_snapshot_number(self):
        """Test extracting snapshot numbers from URIs."""
        test_cases = [
            ("https://w3id.org/oc/meta/br/06504122264/prov/se/1", 1),
            ("https://w3id.org/oc/meta/br/06504122264/prov/se/42", 42),
            ("invalid_uri", 0)
        ]
        
        for uri, expected in test_cases:
            with self.subTest(uri=uri):
                self.assertEqual(extract_snapshot_number(uri), expected)

    def test_get_entity_from_prov_graph(self):
        """Test extracting entity URI from provenance graph URI."""
        test_cases = [
            ("https://w3id.org/oc/meta/br/06504122264/prov/", 
             "https://w3id.org/oc/meta/br/06504122264"),
            ("https://example.org/resource/prov/",
             "https://example.org/resource")
        ]
        
        for graph_uri, expected in test_cases:
            with self.subTest(graph_uri=graph_uri):
                self.assertEqual(get_entity_from_prov_graph(graph_uri), expected)

    def test_find_entity_for_snapshot(self):
        """Test finding the entity that a snapshot is a specialization of."""
        # Create a test graph
        g = ConjunctiveGraph()
        PROV = Namespace("http://www.w3.org/ns/prov#")
        
        # Add test data
        snapshot = URIRef("https://w3id.org/oc/meta/br/06504122264/prov/se/1")
        entity = URIRef("https://w3id.org/oc/meta/br/06504122264")
        g.add((snapshot, PROV.specializationOf, entity))
        
        # Test finding the entity
        found_entity = find_entity_for_snapshot(g, snapshot)
        self.assertEqual(str(found_entity), str(entity))

    def test_fix_provenance_file(self):
        """Test fixing a provenance file with real data."""
        # Run the fix_provenance_file function
        result = fix_provenance_file(self.test_zip_path)
        
        # Check if the function returned results
        self.assertIsNotNone(result)
        
        # Unpack the results
        fixed_file_path, modifications = result
        
        # Verify the file path
        self.assertEqual(fixed_file_path, self.test_zip_path)
        
        # Read the fixed file and verify its contents
        with zipfile.ZipFile(self.test_zip_path, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
        
        graph_data = fixed_data[0]['@graph']
        snapshot_ids = {item['@id'] for item in graph_data}
        expected_ids = {
            "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/3"
        }
        self.assertEqual(snapshot_ids, expected_ids)

        # Verify the chain of wasDerivedFrom relationships
        derived_from = {
            item['@id']: item.get('http://www.w3.org/ns/prov#wasDerivedFrom', [{}])[0].get('@id')
            for item in graph_data
            if 'http://www.w3.org/ns/prov#wasDerivedFrom' in item
        }
        expected_derived = {
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2": 
                "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/3": 
                "https://w3id.org/oc/meta/br/06504122264/prov/se/2"
        }
        self.assertEqual(derived_from, expected_derived)

        # Verify modifications dictionary contains specializationOf additions
        specialization_of = {
            item['@id']: item.get('http://www.w3.org/ns/prov#specializationOf', [{}])[0].get('@id')
            for item in graph_data
            if 'http://www.w3.org/ns/prov#specializationOf' in item
        }
        expected_specialization = {
            "https://w3id.org/oc/meta/br/06504122264/prov/se/1": 
                "https://w3id.org/oc/meta/br/06504122264",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2": 
                "https://w3id.org/oc/meta/br/06504122264",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/3": 
                "https://w3id.org/oc/meta/br/06504122264"
        }
        self.assertEqual(specialization_of, expected_specialization)

        # Verify generatedAtTime and invalidatedAtTime relationships
        generated_times = {
            item['@id']: item.get('http://www.w3.org/ns/prov#generatedAtTime', [{}])[0].get('@value')
            for item in graph_data
            if 'http://www.w3.org/ns/prov#generatedAtTime' in item
        }
        print(generated_times)
        expected_generated = {
            "https://w3id.org/oc/meta/br/06504122264/prov/se/1": 
                "2023-12-13T14:05:18.218917+00:00",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2": 
                "2023-12-22T18:06:49.609459+00:00",  # Data intermedia calcolata
            "https://w3id.org/oc/meta/br/06504122264/prov/se/3": 
                "2023-12-31T22:08:21+00:00"
        }
        self.assertEqual(generated_times, expected_generated)

        invalidated_times = {
            item['@id']: item.get('http://www.w3.org/ns/prov#invalidatedAtTime', [{}])[0].get('@value')
            for item in graph_data
            if 'http://www.w3.org/ns/prov#invalidatedAtTime' in item
        }
        expected_invalidated = {
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2": 
                "2023-12-31T22:08:21+00:00"
        }
        self.assertEqual(invalidated_times, expected_invalidated)

        # Test per verificare che timestamps siano in UTC
        for timestamp in generated_times.values():
            self.assertTrue(
                '+00:00' in timestamp or 'Z' in timestamp,
                f"Generated timestamp {timestamp} should be in UTC"
            )
        
        for timestamp in invalidated_times.values():
            self.assertTrue(
                '+00:00' in timestamp or 'Z' in timestamp,
                f"Invalidated timestamp {timestamp} should be in UTC"
            )


if __name__ == '__main__':
    unittest.main()