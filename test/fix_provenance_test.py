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
        # Raccoglie gli snapshot e i loro numeri
        snapshots = {}
        for item in graph_data:
            if '/prov/se/' in item['@id']:
                num = int(item['@id'].split('/se/')[-1])
                snapshots[num] = item
        
        # Verifica che tutti gli snapshot abbiano le proprietà di base
        for num, snapshot in snapshots.items():
            # Verifica tipo
            self.assertIn('@type', snapshot)
            self.assertIn('http://www.w3.org/ns/prov#Entity', snapshot['@type'])
            
            # Verifica specializationOf
            self.assertIn('http://www.w3.org/ns/prov#specializationOf', snapshot)
            self.assertEqual(
                snapshot['http://www.w3.org/ns/prov#specializationOf'][0]['@id'],
                "https://w3id.org/oc/meta/br/06504122264"
            )
            
            # Verifica timestamp
            self.assertIn('http://www.w3.org/ns/prov#generatedAtTime', snapshot)
            gen_time = snapshot['http://www.w3.org/ns/prov#generatedAtTime'][0]['@value']
            self.assertTrue('+00:00' in gen_time or 'Z' in gen_time)
            
            # Verifica wasDerivedFrom per tutti tranne il primo snapshot
            if num > min(snapshots.keys()):
                self.assertIn('http://www.w3.org/ns/prov#wasDerivedFrom', snapshot)
        
        # Verifica la consistenza temporale
        ordered_nums = sorted(snapshots.keys())
        for i in range(len(ordered_nums)-1):
            curr_num = ordered_nums[i]
            next_num = ordered_nums[i+1]
            
            curr_snapshot = snapshots[curr_num]
            next_snapshot = snapshots[next_num]
            
            # Se lo snapshot corrente ha un tempo di invalidazione
            if 'http://www.w3.org/ns/prov#invalidatedAtTime' in curr_snapshot:
                curr_inv_time = self.processor._convert_to_utc(
                    curr_snapshot['http://www.w3.org/ns/prov#invalidatedAtTime'][0]['@value']
                )
                next_gen_time = self.processor._convert_to_utc(
                    next_snapshot['http://www.w3.org/ns/prov#generatedAtTime'][0]['@value']
                )
                self.assertEqual(
                    curr_inv_time, 
                    next_gen_time,
                    f"Invalidation time of snapshot {curr_num} should match generation time of {next_num}"
                )
        
        # Verifica che gli snapshot siano collegati correttamente
        for num in ordered_nums[1:]:  # Skip the first one
            curr_snapshot = snapshots[num]
            prev_num = ordered_nums[ordered_nums.index(num) - 1]
            
            # Verifica che wasDerivedFrom punti allo snapshot precedente
            derived_from = curr_snapshot['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id']
            expected_derived = f"https://w3id.org/oc/meta/br/06504122264/prov/se/{prev_num}"
            self.assertEqual(
                derived_from, 
                expected_derived,
                f"Snapshot {num} should be derived from snapshot {prev_num}"
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

    def test_multiple_descriptions_merge(self):
        """Test handling of multiple descriptions when merge descriptions are present."""
        
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been created."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18+00:00"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been merged with 'https://w3id.org/oc/meta/br/06504122265'."
                        },
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been merged with 'https://w3id.org/oc/meta/br/06504122266'."
                        },
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been modified."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-31T22:08:21+00:00"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
    
        test_file = os.path.join(self.temp_dir, "multiple_descriptions_merge.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        # Find the snapshot in the fixed data
        snapshot = next(item for item in fixed_data[0]['@graph'] 
                    if item['@id'].endswith('/prov/se/2'))
        
        descriptions = snapshot.get('http://purl.org/dc/terms/description', [])
        
        # Verify that both merge descriptions were kept
        merge_descriptions = [desc for desc in descriptions 
                            if "has been merged with" in desc['@value']]
        self.assertEqual(len(merge_descriptions), 2, 
                        "Both merge descriptions should be preserved")
        
        # Verify that non-merge description was removed
        non_merge_descriptions = [desc for desc in descriptions 
                                if "has been modified" in desc['@value']]
        self.assertEqual(len(non_merge_descriptions), 0, 
                        "Non-merge description should be removed")

    def test_multiple_descriptions_first_snapshot(self):
        """Test handling of multiple descriptions in the first snapshot."""
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been created."
                        },
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been modified."
                        },
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been merged with 'https://w3id.org/oc/meta/br/06504122265'."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18+00:00"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "first_snapshot_descriptions.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        snapshot = next(item for item in fixed_data[0]['@graph'] 
                    if item['@id'].endswith('/prov/se/1'))
        
        descriptions = snapshot.get('http://purl.org/dc/terms/description', [])
        
        # Verify that only creation description was kept
        self.assertEqual(len(descriptions), 1, 
                        "First snapshot should have exactly one description")
        self.assertTrue("has been created" in descriptions[0]['@value'], 
                    "First snapshot should keep only creation description")

    def test_multiple_descriptions_last_snapshot(self):
        """Test handling of multiple descriptions in the last snapshot."""
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been created."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18+00:00"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been modified."
                        },
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been deleted."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-31T22:08:21+00:00"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "last_snapshot_descriptions.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        last_snapshot = next(item for item in fixed_data[0]['@graph'] 
                            if item['@id'].endswith('/prov/se/2'))
        
        descriptions = last_snapshot.get('http://purl.org/dc/terms/description', [])
        
        # Verify that only deletion description was kept
        self.assertEqual(len(descriptions), 1, 
                        "Last snapshot should have exactly one description")
        self.assertTrue("has been deleted" in descriptions[0]['@value'], 
                    "Last snapshot should keep deletion description")

    def test_multiple_descriptions_middle_snapshot(self):
        """Test handling of multiple descriptions in a middle snapshot."""
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been created."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-13T15:05:18+00:00"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been modified."
                        },
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been created."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2023-12-31T22:08:21+00:00"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06504122264/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/br/06504122264' has been deleted."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-01-01T10:00:00+00:00"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06504122264/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "middle_snapshot_descriptions.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        middle_snapshot = next(item for item in fixed_data[0]['@graph'] 
                            if item['@id'].endswith('/prov/se/2'))
        
        descriptions = middle_snapshot.get('http://purl.org/dc/terms/description', [])
        
        # Verify that only modification description was kept
        self.assertEqual(len(descriptions), 1, 
                        "Middle snapshot should have exactly one description")
        self.assertTrue("has been modified" in descriptions[0]['@value'], 
                    "Middle snapshot should keep modification description")
        
    def test_real_case_multiple_timestamps_and_incomplete_snapshot(self):
        """Test the real case scenario of entity 0623074134 with multiple timestamps
        and an incomplete snapshot."""
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/id/0623074134/prov/se/4",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/id/0623074134' has been merged with 'https://w3id.org/oc/meta/id/063301371593'."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-10-23T20:52:32+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/id/0623074134"
                    }],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [{
                        "@id": "http://orcid.org/0000-0002-8420-0696"
                    }],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {
                            "@id": "https://w3id.org/oc/meta/id/0623074134/prov/se/3"
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/063301371593/prov/se/1"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/id/0623074134/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/id/0623074134' has been modified."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-06-06T18:55:36+00:00"
                    }],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-10-23T20:52:32+00:00"
                    }],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/id/0623074134"
                    }],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [{
                        "@id": "https://w3id.org/oc/meta/id/0623074134/prov/se/2"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/id/0623074134/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [{
                        "@value": "The entity 'https://w3id.org/oc/meta/id/0623074134' has been created."
                    }],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-03-27T18:03:23+00:00"
                        },
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-13T16:14:48.836637"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [{
                        "@id": "https://w3id.org/oc/meta/id/0623074134"
                    }]
                },
                {
                    "@id": "https://w3id.org/oc/meta/id/0623074134/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [{
                        "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                        "@value": "2024-06-06T18:55:36+00:00"
                    }]
                }
            ],
            "@id": "https://w3id.org/oc/meta/id/0623074134/prov/"
        }
        
        test_file = os.path.join(self.temp_dir, "real_case_test.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))
            
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        
        # Verify the fixed data
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
                
        graph_data = fixed_data[0]['@graph']

        # Test specific issues from the case study:
        
        # 1. Check that snapshot 1 has only one generatedAtTime (should keep the earliest)
        snapshot_1 = next(item for item in graph_data 
                        if item['@id'].endswith('/prov/se/1'))
        gen_times_1 = snapshot_1.get('http://www.w3.org/ns/prov#generatedAtTime', [])
        self.assertEqual(len(gen_times_1), 1, 
                        "Snapshot 1 should have exactly one generatedAtTime")
        self.assertEqual(
            gen_times_1[0]['@value'],
            "2023-12-13T15:14:48.836637+00:00",
            "Should keep the earliest timestamp"
        )
        
        # 2. Check that snapshot 2 is complete with all required properties
        snapshot_2 = next(item for item in graph_data 
                        if item['@id'].endswith('/prov/se/2'))
        self.assertIn('@type', snapshot_2)
        self.assertIn('http://www.w3.org/ns/prov#specializationOf', snapshot_2)
        self.assertIn('http://www.w3.org/ns/prov#wasDerivedFrom', snapshot_2)
        self.assertEqual(
            snapshot_2['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id'],
            "https://w3id.org/oc/meta/id/0623074134/prov/se/1"
        )
        
        # 3. Check that all snapshots form a proper chain
        for i in range(2, 5):  # Check snapshots 2 through 4
            current = next(item for item in graph_data 
                        if item['@id'].endswith(f'/prov/se/{i}'))
            self.assertIn('http://www.w3.org/ns/prov#wasDerivedFrom', current)
            derived_from = current['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id']
            self.assertTrue(
                derived_from.endswith(f'/prov/se/{i-1}'),
                f"Snapshot {i} should be derived from snapshot {i-1}"
            )
        
        # 4. Check bi-directional timestamp consistency
        snapshots = {}
        for i in range(1, 5):  # Get all snapshots
            snapshots[i] = next(item for item in graph_data 
                            if item['@id'].endswith(f'/prov/se/{i}'))
        
        # 4.1 Check forward: invalidatedAtTime matches next snapshot's generatedAtTime
        for i in range(1, 4):  # Check snapshots 1 through 3
            current = snapshots[i]
            next_snapshot = snapshots[i + 1]
            
            self.assertIn('http://www.w3.org/ns/prov#invalidatedAtTime', current,
                        f"Snapshot {i} should have invalidatedAtTime")
            self.assertIn('http://www.w3.org/ns/prov#generatedAtTime', next_snapshot,
                        f"Snapshot {i+1} should have generatedAtTime")
            
            inv_time = current['http://www.w3.org/ns/prov#invalidatedAtTime'][0]['@value']
            gen_time = next_snapshot['http://www.w3.org/ns/prov#generatedAtTime'][0]['@value']
            
            self.assertEqual(
                inv_time, 
                gen_time,
                f"Invalidation time of snapshot {i} should match generation time of snapshot {i+1}"
            )
        
        # 4.2 Check backward: generatedAtTime matches previous snapshot's invalidatedAtTime
        for i in range(2, 5):  # Check snapshots 2 through 4
            current = snapshots[i]
            prev_snapshot = snapshots[i - 1]
            
            self.assertIn('http://www.w3.org/ns/prov#generatedAtTime', current,
                        f"Snapshot {i} should have generatedAtTime")
            self.assertIn('http://www.w3.org/ns/prov#invalidatedAtTime', prev_snapshot,
                        f"Snapshot {i-1} should have invalidatedAtTime")
            
            gen_time = current['http://www.w3.org/ns/prov#generatedAtTime'][0]['@value']
            inv_time = prev_snapshot['http://www.w3.org/ns/prov#invalidatedAtTime'][0]['@value']
            
            self.assertEqual(
                gen_time,
                inv_time,
                f"Generation time of snapshot {i} should match invalidation time of snapshot {i-1}"
            )
            
            # 5. Check that merge-related wasDerivedFrom is preserved in snapshot 4
            snapshot_4 = next(item for item in graph_data 
                            if item['@id'].endswith('/prov/se/4'))
            derived_from_ids = [ref['@id'] for ref in 
                            snapshot_4['http://www.w3.org/ns/prov#wasDerivedFrom']]
            self.assertIn(
                "https://w3id.org/oc/meta/id/063301371593/prov/se/1",
                derived_from_ids,
                "Merge-related wasDerivedFrom should be preserved"
            )

    def test_original_unresolved_issues_scenario(self):
        # Dati di test presi dal messaggio iniziale nella conversazione
        original_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/5",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06440227509' has been deleted."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-08T01:23:24+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-08T01:23:24+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {
                            "@id": "https://orcid.org/0000-0002-8420-0696"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/4"
                        }
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06440227509> <http://xmlns.com/foaf/0.1/givenName> \"R.\" .<https://w3id.org/oc/meta/ra/06440227509> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/063501394354> .<https://w3id.org/oc/meta/ra/06440227509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .<https://w3id.org/oc/meta/ra/06440227509> <http://xmlns.com/foaf/0.1/familyName> \"Stępniewski\" . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06440227509' has been created."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-13T15:53:04.544275"
                        },
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-03-27T20:20:19+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#hadPrimarySource": [
                        {
                            "@id": "https://openalex.s3.amazonaws.com/browse.html"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {
                            "@id": "https://w3id.org/oc/meta/prov/pa/1"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/4",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06440227509' has been modified."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-04T21:15:55+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-08T01:23:24+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {
                            "@id": "https://orcid.org/0000-0002-8420-0696"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/3"
                        }
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06440227509> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06904873317> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06440227509> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/063501394354> . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-24T23:21:33+00:00"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06440227509' has been modified."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2023-12-24T23:21:33+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-04T21:15:55+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {
                            "@id": "https://w3id.org/oc/meta/prov/pa/1"
                        }
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/se/2"
                        }
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06440227509> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0644082006> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06440227509> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06904873317> . } }"
                        }
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/ra/06440227509/prov/"
        }

        test_file = os.path.join(self.temp_dir, "original_unresolved_issues.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(original_data))

        # Processa il file con lo script
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertIsNotNone(result, "Process should complete without errors")

        # Legge i dati modificati
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())

        graph_data = fixed_data[0]['@graph']

        # Verifica se alcuni problemi noti sono stati risolti:
        # 1. Snapshot se/1 ha multipli 'generatedAtTime', dovrebbe averne solo uno
        snapshot_1 = next((x for x in graph_data if x['@id'].endswith('/prov/se/1')), None)
        self.assertIsNotNone(snapshot_1, "Snapshot se/1 should exist")
        gen_times_1 = snapshot_1.get('http://www.w3.org/ns/prov#generatedAtTime', [])
        # Qui ci aspettiamo che lo script abbia risolto il problema tenendo il timestamp più vecchio.
        # Se notiamo che non è successo, il test fallirà, evidenziando che il problema non è stato risolto.
        self.assertEqual(
            len(gen_times_1), 1,
            "Snapshot se/1 should have only one generatedAtTime after processing"
        )

        # 2. Verifica coerenza descrizioni su se/3 e se/4: dovrebbero mantenere un'unica descrizione coerente
        snapshot_3 = next((x for x in graph_data if x['@id'].endswith('/prov/se/3')), None)
        self.assertIsNotNone(snapshot_3, "Snapshot se/3 should exist")
        desc_3 = snapshot_3.get('http://purl.org/dc/terms/description', [])
        self.assertEqual(len(desc_3), 1, "Snapshot se/3 should have exactly one description")

        snapshot_4 = next((x for x in graph_data if x['@id'].endswith('/prov/se/4')), None)
        self.assertIsNotNone(snapshot_4, "Snapshot se/4 should exist")
        desc_4 = snapshot_4.get('http://purl.org/dc/terms/description', [])
        self.assertEqual(len(desc_4), 1, "Snapshot se/4 should have exactly one description")

        # 3. Verifica la catena wasDerivedFrom: ogni snapshot (tranne il primo) dovrebbe avere un wasDerivedFrom che punta allo snapshot precedente
        #   La sequenza dovrebbe essere: se/1 (creato), se/2, se/3, se/4, se/5 (cancellato)
        #   Ci aspettiamo:
        #   se/2 -> se/1
        #   se/3 -> se/2
        #   se/4 -> se/3
        #   se/5 -> se/4
        # Se il problema non è stato risolto, tali collegamenti potrebbero non essere corretti.
        def get_derived_from(snap_id):
            snap = next((x for x in graph_data if x['@id'].endswith(snap_id)), None)
            if snap and 'http://www.w3.org/ns/prov#wasDerivedFrom' in snap:
                return snap['http://www.w3.org/ns/prov#wasDerivedFrom'][0]['@id'].split('/se/')[-1]
            return None

        self.assertEqual(get_derived_from('/prov/se/2'), '1', "se/2 should derive from se/1")
        self.assertEqual(get_derived_from('/prov/se/3'), '2', "se/3 should derive from se/2")
        self.assertEqual(get_derived_from('/prov/se/4'), '3', "se/4 should derive from se/3")
        self.assertEqual(get_derived_from('/prov/se/5'), '4', "se/5 should derive from se/4")

        # Infine, se alcuni di questi test falliscono, significa che lo script non ha risolto i problemi come previsto,
        # mostrando quindi il comportamento effettivo sullo scenario fornito.

    def test_complex_merge_chain_scenario(self):
        """Test handling of a complex chain of merges with oscillating property values."""
        test_data = {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/9",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been modified."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-04T18:44:08+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-16T03:14:25+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://orcid.org/0000-0002-8420-0696"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/8"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/06320156505> . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/063201438132> . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/5", 
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been merged with 'https://w3id.org/oc/meta/ra/06530192638'."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:29:52+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:30:27+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://w3id.org/oc/meta/prov/pa/1"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/4"},
                        {"@id": "https://w3id.org/oc/meta/ra/06530192638/prov/se/2"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E\" . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E.\" . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/4",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been merged with 'https://w3id.org/oc/meta/ra/065047414'."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:24:47+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:29:52+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://w3id.org/oc/meta/prov/pa/1"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/3"},
                        {"@id": "https://w3id.org/oc/meta/ra/065047414/prov/se/2"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora Elizabeth\" . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E\" . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/7",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been merged with 'https://w3id.org/oc/meta/ra/0612010691345'."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:31:00+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:31:43+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://w3id.org/oc/meta/prov/pa/1"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/0612010691345/prov/se/1"},
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/6"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora\" . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E.\" . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/10",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been deleted."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-16T03:14:25+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-16T03:14:25+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://orcid.org/0000-0002-8420-0696"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/9"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E.\" .<https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/familyName> \"Serralde-Zúñiga\" .<https://w3id.org/oc/meta/ra/06490509042> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .<https://w3id.org/oc/meta/ra/06490509042> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/063201438132> . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/3",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been modified."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:24:19+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:24:47+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://w3id.org/oc/meta/prov/pa/1"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/2"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E.\" . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora Elizabeth\" . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/8",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:31:43+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-12-04T18:44:08+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://w3id.org/oc/meta/prov/pa/1"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06320390920/prov/se/1"},
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/7"}
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/6",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@value": "The entity 'https://w3id.org/oc/meta/ra/06490509042' has been merged with 'https://w3id.org/oc/meta/ra/06520239458'."
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:30:27+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:31:00+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ],
                    "http://www.w3.org/ns/prov#wasAttributedTo": [
                        {"@id": "https://w3id.org/oc/meta/prov/pa/1"}
                    ],
                    "http://www.w3.org/ns/prov#wasDerivedFrom": [
                        {"@id": "https://w3id.org/oc/meta/ra/06520239458/prov/se/1"},
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/5"}
                    ],
                    "https://w3id.org/oc/ontology/hasUpdateQuery": [
                        {
                            "@value": "DELETE DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora E.\" . } }; INSERT DATA { GRAPH <https://w3id.org/oc/meta/ra/> { <https://w3id.org/oc/meta/ra/06490509042> <http://xmlns.com/foaf/0.1/givenName> \"Aurora\" . } }"
                        }
                    ]
                },
                {
                    "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/se/2",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2022-12-20T00:00:00+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#invalidatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-02-21T06:24:19+00:00"
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/ra/06490509042"}
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/ra/06490509042/prov/"
        }

        test_file = os.path.join(self.temp_dir, "complex_merge_chain.zip")
        with zipfile.ZipFile(test_file, 'w') as zf:
            zf.writestr('se.json', json.dumps(test_data))

        # Process the file
        result = self.processor.process_file(test_file, 'test/fix_provenance_logs')
        self.assertTrue(result)
        # Verify the processed data
        with zipfile.ZipFile(test_file, 'r') as zf:
            with zf.open('se.json') as f:
                fixed_data = json.loads(f.read())
        
        graph_data = fixed_data[0]['@graph']

        # Ordina gli snapshot per numero di snapshot
        def get_snapshot_number(snap):
            if '/prov/se/' in snap['@id']:
                return int(snap['@id'].split('/se/')[-1])
            return 0
            
        sorted_snapshots = sorted(graph_data, key=get_snapshot_number)

        # 1. Verifica la catena temporale
        def get_timestamp(snapshot, pred):
            return next((
                item['@value'] 
                for item in snapshot.get(pred, [])
                if '@value' in item
            ), None)

        for i in range(len(sorted_snapshots)-1):
            curr = sorted_snapshots[i]
            next_snap = sorted_snapshots[i+1]
            
            curr_inv = get_timestamp(curr, "http://www.w3.org/ns/prov#invalidatedAtTime")
            next_gen = get_timestamp(next_snap, "http://www.w3.org/ns/prov#generatedAtTime")
            
            if curr_inv and next_gen:
                self.assertEqual(curr_inv, next_gen, 
                    f"Timestamp mismatch between snapshots {curr['@id']} and {next_snap['@id']}")

        # 2. Verifica la coerenza dei merge
        merge_snapshots = [
            s for s in graph_data 
            if any("has been merged with" in str(d.get('@value', '')) 
                for d in s.get('http://purl.org/dc/terms/description', []))
        ]

        for merge in merge_snapshots:
            derived_from = [
                d['@id'] for d in merge.get('http://www.w3.org/ns/prov#wasDerivedFrom', [])
            ]
            self.assertGreaterEqual(len(derived_from), 2, 
                f"Merge snapshot {merge['@id']} should have at least 2 wasDerivedFrom relations")

        # Verify snapshot sequence completeness
        snapshot_numbers = set()
        for item in graph_data:
            if '/prov/se/' in item['@id']:
                num = int(item['@id'].split('/se/')[-1])
                snapshot_numbers.add(num)
        
        # Check that sequence starts at 1
        self.assertIn(1, snapshot_numbers, 
            "Snapshot sequence should start with number 1")
        
        # Check sequence continuity
        expected_numbers = set(range(1, max(snapshot_numbers) + 1))
        self.assertEqual(snapshot_numbers, expected_numbers,
            f"Snapshot sequence should be continuous from 1 to {max(snapshot_numbers)}")
        
        # Verify that snapshot 1 has creation description
        snapshot_1 = next((s for s in graph_data if s['@id'].endswith('/prov/se/1')), None)
        self.assertIsNotNone(snapshot_1, "Snapshot 1 should exist")
        descriptions = snapshot_1.get('http://purl.org/dc/terms/description', [])
        self.assertTrue(any(
            "has been created" in d.get('@value', '') 
            for d in descriptions
        ), "First snapshot should have creation description")


if __name__ == '__main__':
    unittest.main()