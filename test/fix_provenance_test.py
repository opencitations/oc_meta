import json
import os
import shutil
import unittest
import zipfile

from oc_meta.run.fixer.prov.fix import ProvenanceProcessor
from rdflib import ConjunctiveGraph, Literal, Namespace, URIRef
from rdflib.namespace import XSD


class TestProvenanceFixing(unittest.TestCase):
    def setUp(self):
        self.processor = ProvenanceProcessor(log_dir='test/fix_provenance_logs')

        self.temp_dir = "test_temp_dir"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
        
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
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_extract_snapshot_number(self):
        """Test extracting snapshot numbers from URIs."""
        test_cases = [
            ("https://w3id.org/oc/meta/br/06504122264/prov/se/1", 1),
            ("https://w3id.org/oc/meta/br/06504122264/prov/se/42", 42),
            ("invalid_uri", 0)
        ]
        
        for uri, expected in test_cases:
            with self.subTest(uri=uri):
                self.assertEqual(self.processor._extract_snapshot_number(uri), expected)

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
                self.assertEqual(self.processor._get_entity_from_prov_graph(graph_uri), expected)

    def test_collect_snapshot_info(self):
        """Test collecting snapshot information from the graph."""
        g = ConjunctiveGraph()
        PROV = Namespace("http://www.w3.org/ns/prov#")
        
        # Add test data
        snapshot1 = URIRef("https://w3id.org/oc/meta/br/06504122264/prov/se/1")
        snapshot2 = URIRef("https://w3id.org/oc/meta/br/06504122264/prov/se/2")
        gen_time = Literal("2023-12-13T15:05:18.218917", datatype=XSD.dateTime)
        
        g.add((snapshot1, PROV.generatedAtTime, gen_time))
        g.add((snapshot2, PROV.generatedAtTime, gen_time))
        
        snapshots = self.processor._collect_snapshot_info(g)
        
        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0]['number'], 1)
        self.assertEqual(snapshots[1]['number'], 2)
        self.assertEqual(len(snapshots[0]['generation_times']), 1)
        self.assertEqual(str(snapshots[0]['generation_times'][0]), str(gen_time))

    def test_multiple_timestamps(self):
        """Test handling of multiple timestamps for a snapshot."""
        g = ConjunctiveGraph()
        PROV = Namespace("http://www.w3.org/ns/prov#")
        
        snapshot = URIRef("https://w3id.org/oc/meta/br/06504122264/prov/se/1")
        time1 = Literal("2023-12-13T15:05:18+00:00", datatype=XSD.dateTime)
        time2 = Literal("2023-12-13T16:05:18+00:00", datatype=XSD.dateTime)
        
        g.add((snapshot, PROV.generatedAtTime, time1))
        g.add((snapshot, PROV.generatedAtTime, time2))
        
        # Test la rimozione dei timestamp multipli
        self.processor._remove_multiple_timestamps(
            g, snapshot, PROV.generatedAtTime, [time1, time2])
        
        # Verifica che non ci siano più timestamp
        remaining_times = list(g.objects(snapshot, PROV.generatedAtTime))
        self.assertEqual(len(remaining_times), 0)

    def test_process_file_with_multiple_timestamps(self):
        """Test processing a file that contains snapshots with multiple timestamps."""
        # Crea dati di test con timestamp multipli e la catena completa di snapshot
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-13T15:05:18+00:00"
                        },
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-13T16:05:18+00:00"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-22T18:06:49+00:00"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-31T22:08:21+00:00"
                        }
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "multiple_timestamps.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        # Processa il file
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertIsNotNone(result)
        
        # Verifica il contenuto del file risultante
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())

        # Verifica la struttura di base
        graph_data = fixed_data[0]['@graph']
        
        # Verifica gli ID degli snapshot
        snapshot_ids = {item['@id'] for item in graph_data}
        expected_ids = {
            "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
            "https://w3id.org/oc/meta/br/06504122264/prov/se/3"
        }
        self.assertEqual(snapshot_ids, expected_ids)

        # Verifica le relazioni wasDerivedFrom
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

        # Verifica le relazioni specializationOf
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

        # Verifica i timestamp
        generated_times = {
            item['@id']: item.get('http://www.w3.org/ns/prov#generatedAtTime', [{}])[0].get('@value')
            for item in graph_data
            if 'http://www.w3.org/ns/prov#generatedAtTime' in item
        }
        
        # Verifica che il primo snapshot abbia un solo timestamp
        first_snapshot = next(item for item in graph_data 
                            if item['@id'].endswith('/prov/se/1'))
        self.assertEqual(
            len(first_snapshot.get('http://www.w3.org/ns/prov#generatedAtTime', [])),
            1,
            "First snapshot should have exactly one generatedAtTime"
        )

        # Verifica i timestamp di invalidazione
        invalidated_times = {
            item['@id']: item.get('http://www.w3.org/ns/prov#invalidatedAtTime', [{}])[0].get('@value')
            for item in graph_data
            if 'http://www.w3.org/ns/prov#invalidatedAtTime' in item
        }
        expected_invalidated = {
            'https://w3id.org/oc/meta/br/06504122264/prov/se/1': 
                '2023-12-22T18:06:49+00:00',
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2": 
                "2023-12-31T22:08:21+00:00"
        }
        self.assertEqual(invalidated_times, expected_invalidated)

        # Verifica che tutti i timestamp siano in UTC
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

    def test_normalize_timestamps(self):
        """Test normalizing timestamps to UTC."""
        test_cases = [
            ("2023-12-13T15:05:18.218917", True),  # No timezone - should be converted
            ("2023-12-13T15:05:18+00:00", False),  # Already UTC - no change needed
            ("2023-12-13T15:05:18Z", False),       # Already UTC - no change needed
            ("2023-12-13T15:05:18+01:00", True)    # Different timezone - should be converted
        ]
        
        for timestamp_str, should_change in test_cases:
            with self.subTest(timestamp=timestamp_str):
                literal = Literal(timestamp_str, datatype=XSD.dateTime)
                new_literal, was_changed = self.processor._normalize_timestamp(literal)
                self.assertEqual(was_changed, should_change)
                if was_changed:
                    self.assertTrue('+00:00' in str(new_literal) or 'Z' in str(new_literal))

    def test_missing_snapshots(self):
        """Test handling of missing snapshots in the sequence."""
        # Test data with snapshot 2 missing from sequence 1,3,4
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18+00:00"
                    }],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-22T18:06:49+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
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
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/4",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-01-15T10:30:00+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "missing_snapshot.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        # Process the file
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        # Verify the resulting file
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
        print(json.dumps(fixed_data, indent=4))
        graph_data = fixed_data[0]['@graph']
        
        # Check if the missing snapshot 2 was created
        snapshot_ids = {item['@id'] for item in graph_data}
        self.assertIn(
            "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
            snapshot_ids,
            "Missing snapshot 2 should have been created"
        )
        
        # Find the created snapshot
        snapshot_2 = next(item for item in graph_data 
                        if item['@id'].endswith('/prov/se/2'))
        
        # Verify basic properties of the created snapshot
        self.assertIn('@type', snapshot_2)
        self.assertIn('http://www.w3.org/ns/prov#Entity', snapshot_2['@type'])
        
        # Verify specializationOf relationship
        self.assertIn('http://www.w3.org/ns/prov#specializationOf', snapshot_2)
        self.assertEqual(
            snapshot_2['http://www.w3.org/ns/prov#specializationOf'][0]['@id'],
            "https://w3id.org/oc/meta/br/06504122264"
        )
        
        # Verify wasDerivedFrom relationship
        self.assertIn('http://www.w3.org/ns/prov#wasDerivedFrom', snapshot_2)
        self.assertEqual(
            snapshot_2['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id'],
            "https://w3id.org/oc/meta/br/06504122264/prov/se/1"
        )
        
        # Verify timestamps
        self.assertIn('http://www.w3.org/ns/prov#generatedAtTime', snapshot_2)
        self.assertIn('http://www.w3.org/ns/prov#invalidatedAtTime', snapshot_2)

    def test_multiple_missing_snapshots(self):
        """Test handling of multiple consecutive missing snapshots."""
        # Test data with snapshots 2 and 3 missing from sequence 1,4,5
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18+00:00"
                    }],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-22T18:06:49+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/4",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-01-15T10:30:00+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/5",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-01-20T14:45:00+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "multiple_missing_snapshots.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        graph_data = fixed_data[0]['@graph']
        
        # Verify all snapshots exist
        snapshot_ids = {item['@id'] for item in graph_data}
        expected_ids = {
            f"https://w3id.org/oc/meta/br/06504122264/prov/se/{i}"
            for i in range(1, 6)
        }
        self.assertEqual(snapshot_ids, expected_ids)
        
        # Verify the chain of wasDerivedFrom relationships
        for i in range(2, 6):
            curr_snapshot = next(item for item in graph_data 
                            if item['@id'].endswith(f'/se/{i}'))
            self.assertIn('http://www.w3.org/ns/prov#wasDerivedFrom', curr_snapshot)
            self.assertEqual(
                curr_snapshot['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id'],
                f"https://w3id.org/oc/meta/br/06504122264/prov/se/{i-1}"
            )

    def test_timestamp_inference(self):
        """Test timestamp inference for missing snapshots."""
        # Create test data where we can verify timestamp inference logic
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T12:00:00+00:00"
                    }],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T14:00:00+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T18:00:00+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/br/06504122264"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "timestamp_inference.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        graph_data = fixed_data[0]['@graph']
        
        # Find the created snapshot 2
        snapshot_2 = next(item for item in graph_data 
                        if item['@id'].endswith('/prov/se/2'))
        
        # Verify timestamps were inferred correctly
        self.assertIn('http://www.w3.org/ns/prov#generatedAtTime', snapshot_2)
        gen_time = snapshot_2['http://www.w3.org/ns/prov#generatedAtTime'][0]['@value']
        self.assertEqual(gen_time, "2023-12-13T14:00:00+00:00")
        
        self.assertIn('http://www.w3.org/ns/prov#invalidatedAtTime', snapshot_2)
        inv_time = snapshot_2['http://www.w3.org/ns/prov#invalidatedAtTime'][0]['@value']
        self.assertEqual(inv_time, "2023-12-13T18:00:00+00:00")

if __name__ == '__main__':
    unittest.main()