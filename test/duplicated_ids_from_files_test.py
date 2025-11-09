#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import csv
import json
import os
import shutil
import tempfile
import unittest
import zipfile
from collections import defaultdict

from oc_meta.run.find.duplicated_ids_from_files import (
    load_and_merge_temp_csv, process_chunk, process_zip_file,
    read_and_analyze_zip_files, save_chunk_to_temp_csv, save_duplicates_to_csv)


class TestDuplicatedIdsFromFiles(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.id_dir = os.path.join(self.test_dir, 'id')
        os.makedirs(self.id_dir)
        self.temp_dir = tempfile.mkdtemp()

        self.test_rdf_with_duplicates = self._create_test_rdf_data()
        self.test_zip_paths = self._create_test_zip_files()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_rdf_data(self):
        rdf_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/1",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test1"
                        }
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/2",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test1"
                        }
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/3",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test2"
                        }
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/4",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/orcid"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "0000-0001-2345-6789"
                        }
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/5",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/orcid"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "0000-0001-2345-6789"
                        }
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/6",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test3",
                            "@type": "http://www.w3.org/2001/XMLSchema#string"
                        }
                    }
                ]
            },
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/id/7",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": {
                            "@id": "http://purl.org/spar/datacite/doi"
                        },
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": {
                            "@value": "10.1234/test3"
                        }
                    }
                ]
            }
        ]
        return rdf_data

    def _create_test_zip_files(self):
        zip_paths = []
        for i in range(4):
            zip_path = os.path.join(self.id_dir, f'test_{i}.zip')
            with zipfile.ZipFile(zip_path, 'w') as zf:
                start_idx = i * 2
                end_idx = min(start_idx + 2, len(self.test_rdf_with_duplicates))
                for j in range(start_idx, end_idx):
                    rdf_content = json.dumps(self.test_rdf_with_duplicates[j], indent=2)
                    zf.writestr(f'rdf_{j}.json', rdf_content)
            zip_paths.append(zip_path)
        return zip_paths

    def test_process_zip_file(self):
        result = process_zip_file(self.test_zip_paths[0])

        self.assertIsInstance(result, dict)
        self.assertGreater(len(result), 0)

        doi_key = ("http://purl.org/spar/datacite/doi", "10.1234/test1")
        self.assertIn(doi_key, result)
        self.assertIn("https://w3id.org/oc/meta/id/1", result[doi_key])

    def test_save_and_load_chunk_csv(self):
        entity_info = defaultdict(set)
        entity_info[("http://purl.org/spar/datacite/doi", "10.1234/test1")] = {
            "https://w3id.org/oc/meta/id/1",
            "https://w3id.org/oc/meta/id/2"
        }
        entity_info[("http://purl.org/spar/datacite/doi", "10.1234/test2")] = {
            "https://w3id.org/oc/meta/id/3"
        }

        temp_file = os.path.join(self.temp_dir, 'test_chunk.csv')
        save_chunk_to_temp_csv(entity_info, temp_file)

        self.assertTrue(os.path.exists(temp_file))

        loaded_info = defaultdict(set)
        load_and_merge_temp_csv(temp_file, loaded_info)

        self.assertEqual(len(loaded_info), 2)
        doi_key = ("http://purl.org/spar/datacite/doi", "10.1234/test1")
        self.assertIn(doi_key, loaded_info)
        self.assertEqual(len(loaded_info[doi_key]), 2)

    def test_process_chunk(self):
        chunk_files = self.test_zip_paths[:2]
        temp_file = process_chunk(chunk_files, self.temp_dir, 0)

        self.assertTrue(os.path.exists(temp_file))

        with open(temp_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertGreater(len(rows), 0)

    def test_save_duplicates_to_csv(self):
        entity_info = defaultdict(set)
        entity_info[("http://purl.org/spar/datacite/doi", "10.1234/test1")] = {
            "https://w3id.org/oc/meta/id/1",
            "https://w3id.org/oc/meta/id/2",
            "https://w3id.org/oc/meta/id/3"
        }
        entity_info[("http://purl.org/spar/datacite/doi", "10.1234/test2")] = {
            "https://w3id.org/oc/meta/id/4"
        }

        output_file = os.path.join(self.temp_dir, 'duplicates.csv')
        save_duplicates_to_csv(entity_info, output_file)

        self.assertTrue(os.path.exists(output_file))

        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            self.assertEqual(len(rows), 1)

            self.assertIn(rows[0]['surviving_entity'], {
                "https://w3id.org/oc/meta/id/1",
                "https://w3id.org/oc/meta/id/2",
                "https://w3id.org/oc/meta/id/3"
            })

            merged_entities = rows[0]['merged_entities'].split('; ')
            self.assertEqual(len(merged_entities), 2)

    def test_read_and_analyze_zip_files(self):
        output_csv = os.path.join(self.temp_dir, 'output.csv')

        read_and_analyze_zip_files(self.test_dir, output_csv, chunk_size=2)

        self.assertTrue(os.path.exists(output_csv))

        with open(output_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            self.assertGreater(len(rows), 0)

    def test_chunking_behavior(self):
        output_csv = os.path.join(self.temp_dir, 'output_chunked.csv')

        read_and_analyze_zip_files(self.test_dir, output_csv, chunk_size=1)

        self.assertTrue(os.path.exists(output_csv))

        with open(output_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            for row in rows:
                self.assertIn('surviving_entity', row)
                self.assertIn('merged_entities', row)

    def test_datatype_normalization(self):
        output_csv = os.path.join(self.temp_dir, 'output_datatype.csv')

        read_and_analyze_zip_files(self.test_dir, output_csv, chunk_size=2)

        self.assertTrue(os.path.exists(output_csv))

        with open(output_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            id6_and_id7_merged = False
            for row in rows:
                all_entities = {row['surviving_entity']} | set(row['merged_entities'].split('; '))

                if ('https://w3id.org/oc/meta/id/6' in all_entities and
                    'https://w3id.org/oc/meta/id/7' in all_entities):
                    id6_and_id7_merged = True
                    break

            self.assertTrue(
                id6_and_id7_merged,
                "ID 6 (with xsd:string datatype) and ID 7 (without datatype) should be merged as duplicates"
            )


if __name__ == "__main__":
    unittest.main()
