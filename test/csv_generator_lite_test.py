#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2024 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import csv
import json
import os
import unittest
from shutil import rmtree
from zipfile import ZipFile

import redis
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.plugins.csv_generator_lite.csv_generator_lite import (
    generate_csv,
    init_redis_connection,
    is_omid_processed,
    load_processed_omids_to_redis,
)


class TestCSVGeneratorLite(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.join("test", "csv_generator_lite")
        self.input_dir = os.path.join(self.base_dir, "input")
        self.output_dir = os.path.join(self.base_dir, "output")

        # Create test directories if they don't exist
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        # Create test RDF structure
        self.rdf_dir = os.path.join(self.input_dir, "rdf")
        self.br_dir = os.path.join(self.rdf_dir, "br")
        os.makedirs(self.br_dir, exist_ok=True)

        # Initialize Redis connection for tests
        self.redis_client = init_redis_connection(db=5)  # Use DB 5 for testing
        self.redis_client.flushdb()  # Clear test database

    def tearDown(self):
        if os.path.exists(self.base_dir):
            rmtree(self.base_dir)
        # Clean up Redis test database
        self.redis_client.flushdb()

    def _write_test_data(self, data):
        """Helper method to write test data to the input directory"""
        os.makedirs(os.path.join(self.br_dir, "060", "10000"), exist_ok=True)
        test_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/{item['id'].replace('omid:', '')}",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": item["title"]}],
                    }
                    for item in data
                ]
            }
        ]
        with ZipFile(
            os.path.join(self.br_dir, "060", "10000", "1000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("1000.json", json.dumps(test_data))

    def test_redis_connection_and_caching(self):
        """Test Redis connection and basic caching operations"""
        # Test connection initialization
        redis_client = init_redis_connection(db=5)
        self.assertIsInstance(redis_client, redis.Redis)

        # Create a test CSV file with some OMIDs
        test_data = [
            {"id": "omid:br/0601", "title": "Test 1"},
            {"id": "omid:br/0602", "title": "Test 2"},
            {"id": "omid:br/0603 issn:456", "title": "Test 3"},
        ]
        os.makedirs(self.output_dir, exist_ok=True)
        with open(
            os.path.join(self.output_dir, "test.csv"), "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerows(test_data)

        # Test loading OMIDs into Redis
        count = load_processed_omids_to_redis(self.output_dir, redis_client)
        self.assertEqual(count, 3)

        # Test OMID lookup
        self.assertTrue(is_omid_processed("omid:br/0601", redis_client))
        self.assertTrue(is_omid_processed("omid:br/0602", redis_client))
        self.assertTrue(is_omid_processed("omid:br/0603", redis_client))
        self.assertFalse(is_omid_processed("omid:br/0604", redis_client))

    def test_redis_cache_persistence(self):
        """Test that Redis is populated from existing CSV files and cleared after completion"""
        # Create initial test data
        test_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "First Run"}],
                    }
                ]
            }
        ]

        os.makedirs(os.path.join(self.br_dir, "060", "10000"), exist_ok=True)
        with ZipFile(
            os.path.join(self.br_dir, "060", "10000", "1000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("1000.json", json.dumps(test_data))

        # First run - creates initial CSV
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
            redis_db=5,
        )

        # Verify Redis is empty after first run
        self.assertFalse(is_omid_processed("omid:br/0601", self.redis_client))

        # Create new test data
        test_data_2 = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",  # Same OMID as before
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Should Be Skipped"}
                        ],
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/br/0602",  # New OMID
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Should Be Processed"}
                        ],
                    },
                ]
            }
        ]

        with ZipFile(
            os.path.join(self.br_dir, "060", "10000", "1000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("1000.json", json.dumps(test_data_2))

        # Second run - should load OMIDs from existing CSV and skip already processed resources
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
            redis_db=5,
        )

        # Check output files
        output_data = []
        for filename in os.listdir(self.output_dir):
            if filename.endswith(".csv"):
                output_data.extend(
                    get_csv_data(os.path.join(self.output_dir, filename))
                )

        # Verify results
        # Should find exactly two entries - one from first run and one new one
        self.assertEqual(len(output_data), 2)

        # Find entries by title
        first_run_entry = next(
            item for item in output_data if item["title"] == "First Run"
        )
        second_run_entry = next(
            item for item in output_data if item["title"] == "Should Be Processed"
        )

        # Verify the first entry wasn't overwritten with "Should Be Skipped"
        self.assertEqual(first_run_entry["title"], "First Run")
        self.assertEqual(first_run_entry["id"], "omid:br/0601")

        # Verify the new entry was processed
        self.assertEqual(second_run_entry["title"], "Should Be Processed")
        self.assertEqual(second_run_entry["id"], "omid:br/0602")

        # Verify Redis is empty after completion
        self.assertFalse(is_omid_processed("omid:br/0601", self.redis_client))
        self.assertFalse(is_omid_processed("omid:br/0602", self.redis_client))

    def test_redis_cache_cleanup(self):
        """Test that Redis cache is properly cleaned up in various scenarios"""
        # First run - should process successfully and clear Redis
        input_data = [{"id": "omid:br/0601", "title": "First Entry"}]
        self._write_test_data(input_data)

        # Run with valid directory - should process and clear Redis
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
            redis_db=5,
        )

        # Verify Redis is empty after successful run
        self.assertFalse(is_omid_processed("omid:br/0601", self.redis_client))

        # Load processed OMIDs into Redis
        load_processed_omids_to_redis(self.output_dir, self.redis_client)

        # Verify that after loading from CSV, the OMID is in Redis
        self.assertTrue(is_omid_processed("omid:br/0601", self.redis_client))

        # Run with non-existent directory - should fail but keep Redis populated
        generate_csv(
            input_dir="/nonexistent/dir",
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
            redis_db=5,
        )

        # Verify Redis still has the data after failed run
        self.assertTrue(
            is_omid_processed("omid:br/0601", self.redis_client),
            "Redis cache should be retained after a failed run",
        )

    def test_redis_error_handling(self):
        """Test handling of Redis connection errors"""
        # Test with invalid Redis connection
        with self.assertRaises(redis.ConnectionError):
            init_redis_connection(port=12345)  # Invalid port

        # Test loading OMIDs with non-existent directory
        count = load_processed_omids_to_redis("/nonexistent/dir", self.redis_client)
        self.assertEqual(count, 0)

    def test_concurrent_processing_with_redis(self):
        """Test concurrent processing with Redis caching"""
        # Create multiple test files
        test_data = []
        for i in range(100):  # Create 100 test entries
            test_data.append(
                {
                    "@id": f"https://w3id.org/oc/meta/br/06{i:02d}",
                    "@type": [
                        "http://purl.org/spar/fabio/Expression",
                        "http://purl.org/spar/fabio/JournalArticle",
                    ],
                    "http://purl.org/dc/terms/title": [{"@value": f"Article {i}"}],
                }
            )

        # Split into multiple files
        os.makedirs(os.path.join(self.br_dir, "060", "10000"), exist_ok=True)
        for i in range(0, 100, 10):  # Create 10 files with 10 entries each
            file_data = [{"@graph": test_data[i : i + 10]}]
            with ZipFile(
                os.path.join(self.br_dir, "060", "10000", f"{i+1000}.zip"), "w"
            ) as zip_file:
                zip_file.writestr(f"{i+1000}.json", json.dumps(file_data))

        # First run to create some CSV files
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
            redis_db=5,
        )

        # Create more test entries
        more_test_data = []
        for i in range(100, 200):  # Create 100 more test entries
            more_test_data.append(
                {
                    "@id": f"https://w3id.org/oc/meta/br/06{i:02d}",
                    "@type": [
                        "http://purl.org/spar/fabio/Expression",
                        "http://purl.org/spar/fabio/JournalArticle",
                    ],
                    "http://purl.org/dc/terms/title": [{"@value": f"Article {i}"}],
                }
            )

        # Add new files
        for i in range(0, 100, 10):
            file_data = [{"@graph": more_test_data[i : i + 10]}]
            with ZipFile(
                os.path.join(self.br_dir, "060", "10000", f"{i+2000}.zip"), "w"
            ) as zip_file:
                zip_file.writestr(f"{i+2000}.json", json.dumps(file_data))

        # Second run with existing cache
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
            redis_db=5,
        )

        # Verify results
        all_output_data = []
        for filename in os.listdir(self.output_dir):
            if filename.endswith(".csv"):
                all_output_data.extend(
                    get_csv_data(os.path.join(self.output_dir, filename))
                )

        # Should have processed all 200 entries
        self.assertEqual(len(all_output_data), 200)

        # Verify no duplicates
        processed_ids = {row["id"] for row in all_output_data}
        self.assertEqual(len(processed_ids), 200)

    def test_basic_br_processing(self):
        """Test basic bibliographic resource processing"""
        test_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/0601",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Test Article"}],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@value": "2024-01-01"}
                        ],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/0601"}
                        ],
                    }
                ],
                "@id": "https://w3id.org/oc/meta/br/",
            }
        ]

        # Write test data to file
        os.makedirs(os.path.join(self.br_dir, "060", "10000"), exist_ok=True)
        with ZipFile(
            os.path.join(self.br_dir, "060", "10000", "1000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("1000.json", json.dumps(test_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)

        output_data = get_csv_data(os.path.join(self.output_dir, output_files[0]))
        self.assertEqual(len(output_data), 1)
        self.assertEqual(output_data[0]["title"], "Test Article")
        self.assertEqual(output_data[0]["pub_date"], "2024-01-01")
        self.assertEqual(output_data[0]["type"], "journal article")
        self.assertEqual(output_data[0]["id"], "omid:br/0601")

    def test_complex_br_with_related_entities(self):
        """Test processing of BR with authors, venue, and other related entities"""
        # Create directory structure for each entity type
        supplier_prefix = "060"
        for entity_type in ["br", "ra", "ar", "id"]:
            os.makedirs(
                os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000"),
                exist_ok=True,
            )

        # BR data including both the article and the venue
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Complex Article"}
                        ],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {"@value": "2024-02-01"}
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/Journal",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Test Journal"}],
                    },
                ],
                "@id": "https://w3id.org/oc/meta/br/",
            }
        ]

        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                        ],
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ar/",
            }
        ]

        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Test Author"}],
                    }
                ],
                "@id": "https://w3id.org/oc/meta/ra/",
            }
        ]

        # Write test data files in correct locations
        data_files = {"br": br_data, "ra": ra_data, "ar": ar_data}

        for entity_type, data in data_files.items():
            zip_path = os.path.join(
                self.rdf_dir, entity_type, supplier_prefix, "10000", "1000.zip"
            )
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 2)  # Should have 2 rows: article and journal

        # Find article and journal entries
        article = next(
            (item for item in output_data if item["type"] == "journal article"), None
        )
        journal = next(
            (item for item in output_data if item["type"] == "journal"), None
        )

        # Verify article data
        self.assertIsNotNone(article)
        self.assertEqual(article["title"], "Complex Article")
        self.assertEqual(article["venue"], f"Test Journal [omid:br/{supplier_prefix}3]")
        self.assertEqual(article["author"], "Test Author [omid:ra/0601]")
        self.assertEqual(article["id"], f"omid:br/{supplier_prefix}2")

        # Verify journal data
        self.assertIsNotNone(journal)
        self.assertEqual(journal["title"], "Test Journal")
        self.assertEqual(journal["type"], "journal")
        self.assertEqual(journal["id"], f"omid:br/{supplier_prefix}3")

    def test_empty_input_directory(self):
        """Test behavior with empty input directory"""
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        self.assertEqual(len(os.listdir(self.output_dir)), 0)

    def test_br_with_multiple_authors_and_editors(self):
        """Test processing of BR with multiple authors and editors"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/Book",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Multi-Author Book"}
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"
                            },  # First author
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"
                            },  # Second author
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"
                            },  # First editor
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}4"
                            },  # Second editor
                        ],
                    }
                ]
            }
        ]

        # Setup agent roles for authors and editors with hasNext relations
        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/editor"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}4"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}4",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/editor"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}4"}
                        ],
                    },
                ]
            }
        ]

        # Setup responsible agents
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Smith"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "John"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Doe"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Jane"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Brown"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Bob"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}4",
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Wilson"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Alice"}],
                    },
                ]
            }
        ]

        # Write test data files
        data_files = {"br": br_data, "ra": ra_data, "ar": ar_data}

        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)

        # Verify authors and editors are in the correct order
        expected_authors = (
            f"Smith, John [omid:ra/{supplier_prefix}1]; "
            f"Doe, Jane [omid:ra/{supplier_prefix}2]"
        )
        expected_editors = (
            f"Brown, Bob [omid:ra/{supplier_prefix}3]; "
            f"Wilson, Alice [omid:ra/{supplier_prefix}4]"
        )

        self.assertEqual(output_data[0]["author"], expected_authors)
        self.assertEqual(output_data[0]["editor"], expected_editors)

    def test_br_with_identifiers(self):
        """Test processing of BR with multiple identifiers"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Article With DOI"}
                        ],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}1"},
                            {"@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}2"},
                        ],
                    }
                ]
            }
        ]

        id_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}1",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": [
                            {"@id": "http://purl.org/spar/datacite/doi"}
                        ],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                            {"@value": "10.1234/test.123"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}2",
                        "http://purl.org/spar/datacite/usesIdentifierScheme": [
                            {"@id": "http://purl.org/spar/datacite/isbn"}
                        ],
                        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                            {"@value": "978-0-123456-47-2"}
                        ],
                    },
                ]
            }
        ]

        # Write test data files in correct locations
        data_files = {"br": br_data, "id": id_data}

        for entity_type, data in data_files.items():
            # Create all necessary directories
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)

        # Verify all identifiers are included
        expected_ids = (
            f"omid:br/{supplier_prefix}1 doi:10.1234/test.123 isbn:978-0-123456-47-2"
        )
        self.assertEqual(output_data[0]["id"], expected_ids)

    def test_br_with_page_numbers(self):
        """Test processing of BR with page information"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Paged Article"}],
                        "http://purl.org/vocab/frbr/core#embodiment": [
                            {"@id": f"https://w3id.org/oc/meta/re/{supplier_prefix}1"}
                        ],
                    }
                ]
            }
        ]

        re_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/re/{supplier_prefix}1",
                        "http://prismstandard.org/namespaces/basic/2.0/startingPage": [
                            {"@value": "100"}
                        ],
                        "http://prismstandard.org/namespaces/basic/2.0/endingPage": [
                            {"@value": "120"}
                        ],
                    }
                ]
            }
        ]

        # Write test data files in correct locations
        data_files = {"br": br_data, "re": re_data}

        for entity_type, data in data_files.items():
            # Create all necessary directories
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)
        self.assertEqual(output_data[0]["page"], "100-120")

    def test_malformed_data_handling(self):
        """Test handling of malformed or incomplete data"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        # Missing title
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {"@id": "invalid_uri"},  # Invalid URI
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {"@id": "non_existent_venue"}  # Non-existent venue
                        ],
                    }
                ]
            }
        ]

        # Write test data files in correct locations
        data_files = {"br": br_data}

        for entity_type, data in data_files.items():
            # Create all necessary directories
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)
        # Verify graceful handling of missing/invalid data
        self.assertEqual(output_data[0]["title"], "")
        self.assertEqual(output_data[0]["author"], "")
        self.assertEqual(output_data[0]["venue"], "")

    def test_br_with_hierarchical_venue_structures(self):
        """Test different hierarchical venue structures (issue->volume->journal, issue->journal, volume->journal, direct journal)"""
        supplier_prefix = "060"

        # Create test data for different hierarchical structures
        br_data = [
            {
                "@graph": [
                    # Article in issue->volume->journal structure
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Article in Full Hierarchy"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2"
                            }  # Issue
                        ],
                    },
                    # Article in issue->journal structure (no volume)
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}5",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Article in Issue-Journal"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}6"
                            }  # Issue
                        ],
                    },
                    # Article in volume->journal structure (no issue)
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}9",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Article in Volume-Journal"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}10"
                            }  # Volume
                        ],
                    },
                    # Article directly in journal
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}13",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Article in Journal"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"
                            }  # Journal
                        ],
                    },
                    # Issue in full hierarchy
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2",
                        "@type": ["http://purl.org/spar/fabio/JournalIssue"],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                            {"@value": "2"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3"
                            }  # Volume
                        ],
                    },
                    # Volume in full hierarchy
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3",
                        "@type": ["http://purl.org/spar/fabio/JournalVolume"],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                            {"@value": "42"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"
                            }  # Journal
                        ],
                    },
                    # Journal
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4",
                        "@type": ["http://purl.org/spar/fabio/Journal"],
                        "http://purl.org/dc/terms/title": [{"@value": "Test Journal"}],
                    },
                    # Issue directly in journal
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}6",
                        "@type": ["http://purl.org/spar/fabio/JournalIssue"],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                            {"@value": "3"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"
                            }  # Journal
                        ],
                    },
                    # Volume directly in journal
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}10",
                        "@type": ["http://purl.org/spar/fabio/JournalVolume"],
                        "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                            {"@value": "5"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"
                            }  # Journal
                        ],
                    },
                ]
            }
        ]

        # Write test data files
        dir_path = os.path.join(self.rdf_dir, "br", supplier_prefix, "10000")
        os.makedirs(dir_path, exist_ok=True)

        zip_path = os.path.join(dir_path, "1000.zip")
        with ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("1000.json", json.dumps(br_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))

        # Verify we only have the articles and journal in the output
        self.assertEqual(len(output_data), 5)  # 4 articles + 1 journal

        # Verify no JournalVolume or JournalIssue entries exist
        volume_or_issue_entries = [
            item
            for item in output_data
            if item["type"] in ["journal volume", "journal issue"]
        ]
        self.assertEqual(len(volume_or_issue_entries), 0)

        # Find each article by title
        full_hierarchy = next(
            item for item in output_data if item["title"] == "Article in Full Hierarchy"
        )
        issue_journal = next(
            item for item in output_data if item["title"] == "Article in Issue-Journal"
        )
        volume_journal = next(
            item for item in output_data if item["title"] == "Article in Volume-Journal"
        )
        direct_journal = next(
            item for item in output_data if item["title"] == "Article in Journal"
        )

        # Test full hierarchy (issue->volume->journal)
        self.assertEqual(full_hierarchy["issue"], "2")
        self.assertEqual(full_hierarchy["volume"], "42")
        self.assertEqual(
            full_hierarchy["venue"], f"Test Journal [omid:br/{supplier_prefix}4]"
        )

        # Test issue->journal (no volume)
        self.assertEqual(issue_journal["issue"], "3")
        self.assertEqual(issue_journal["volume"], "")
        self.assertEqual(
            issue_journal["venue"], f"Test Journal [omid:br/{supplier_prefix}4]"
        )

        # Test volume->journal (no issue)
        self.assertEqual(volume_journal["issue"], "")
        self.assertEqual(volume_journal["volume"], "5")
        self.assertEqual(
            volume_journal["venue"], f"Test Journal [omid:br/{supplier_prefix}4]"
        )

        # Test direct journal connection
        self.assertEqual(direct_journal["issue"], "")
        self.assertEqual(direct_journal["volume"], "")
        self.assertEqual(
            direct_journal["venue"], f"Test Journal [omid:br/{supplier_prefix}4]"
        )

    def test_book_in_series(self):
        """Test processing of a book that is part of a book series"""
        supplier_prefix = "060"

        # Create test data for book in series
        br_data = [
            {
                "@graph": [
                    # Book
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/Book",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Test Book"}],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {
                                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2"
                            }  # Series
                        ],
                    },
                    # Book Series
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2",
                        "@type": ["http://purl.org/spar/fabio/BookSeries"],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Test Book Series"}
                        ],
                    },
                ]
            }
        ]

        # Write test data
        dir_path = os.path.join(self.rdf_dir, "br", supplier_prefix, "10000")
        os.makedirs(dir_path, exist_ok=True)

        zip_path = os.path.join(dir_path, "1000.zip")
        with ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("1000.json", json.dumps(br_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))

        # Find book entry
        book = next(item for item in output_data if item["type"] == "book")

        # Verify book is correctly linked to series
        self.assertEqual(book["title"], "Test Book")
        self.assertEqual(
            book["venue"], f"Test Book Series [omid:br/{supplier_prefix}2]"
        )
        self.assertEqual(book["volume"], "")  # Should not have volume
        self.assertEqual(book["issue"], "")  # Should not have issue

    def test_br_with_multiple_roles(self):
        """Test processing of BR with authors, editors and publishers"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/Book",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Multi-Role Book"}
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"
                            },  # Author
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"
                            },  # Editor
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"
                            },  # Publisher
                        ],
                    }
                ]
            }
        ]

        # Setup agent roles for authors, editors and publishers
        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/editor"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/publisher"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}
                        ],
                    },
                ]
            }
        ]

        # Setup responsible agents with different name formats
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Smith"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "John"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Editor Name"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                        "http://xmlns.com/foaf/0.1/name": [
                            {"@value": "Publisher House"}
                        ],
                    },
                ]
            }
        ]

        # Write test data files
        data_files = {"br": br_data, "ra": ra_data, "ar": ar_data}

        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)

        # Verify all roles are correctly processed
        book = output_data[0]
        self.assertEqual(book["title"], "Multi-Role Book")
        self.assertEqual(book["author"], f"Smith, John [omid:ra/{supplier_prefix}1]")
        self.assertEqual(book["editor"], f"Editor Name [omid:ra/{supplier_prefix}2]")
        self.assertEqual(
            book["publisher"], f"Publisher House [omid:ra/{supplier_prefix}3]"
        )

    def test_ordered_authors(self):
        """Test that authors are ordered according to hasNext relations"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Ordered Authors Article"}
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"},
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"},
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"},
                        ],
                    }
                ]
            }
        ]

        # Setup agent roles with hasNext relations
        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}
                        ],
                    },
                ]
            }
        ]

        # Setup responsible agents with different names
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "First Author"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Second Author"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Third Author"}],
                    },
                ]
            }
        ]

        # Write test data files
        data_files = {"br": br_data, "ra": ra_data, "ar": ar_data}

        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)

        # Verify authors are in the correct order
        expected_authors = (
            f"First Author [omid:ra/{supplier_prefix}1]; "
            f"Second Author [omid:ra/{supplier_prefix}2]; "
            f"Third Author [omid:ra/{supplier_prefix}3]"
        )
        self.assertEqual(output_data[0]["author"], expected_authors)

    def test_cyclic_hasNext_relations(self):
        """Test handling of cyclic hasNext relations between agent roles"""
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Cyclic Authors Article"}
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"},
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"},
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"},
                        ],
                    }
                ]
            }
        ]

        # Setup agent roles with cyclic hasNext relations
        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}
                        ],
                        # Creates a cycle: 1 -> 2 -> 3 -> 1
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}
                        ],
                        # Cycle completion
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"}
                        ],
                    },
                ]
            }
        ]

        # Setup responsible agents
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "First Author"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Second Author"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Third Author"}],
                    },
                ]
            }
        ]

        # Write test data files
        data_files = {"br": br_data, "ra": ra_data, "ar": ar_data}

        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)

        # Verify that we get at least some authors before the cycle is detected
        # The order should be maintained until the cycle is detected
        authors = output_data[0]["author"].split("; ")
        self.assertGreater(len(authors), 0)

        # Verify the presence and order of authors
        self.assertTrue(
            any(
                f"First Author [omid:ra/{supplier_prefix}1]" in author
                for author in authors
            )
        )
        self.assertTrue(
            any(
                f"Second Author [omid:ra/{supplier_prefix}2]" in author
                for author in authors
            )
        )

        # Verify no duplicates in the output
        author_set = set(authors)
        self.assertEqual(
            len(authors),
            len(author_set),
            "Found duplicate authors in output: each author should appear exactly once",
        )

        # Verify the exact order and number of authors
        expected_authors = [
            f"First Author [omid:ra/{supplier_prefix}1]",
            f"Second Author [omid:ra/{supplier_prefix}2]",
            f"Third Author [omid:ra/{supplier_prefix}3]",
        ]
        self.assertEqual(
            authors,
            expected_authors,
            "Authors should be in correct order and each should appear exactly once",
        )

    def test_multiple_input_files(self):
        """Test processing of multiple input files with sequential entity IDs"""
        supplier_prefix = "060"

        # Create test data spanning multiple files
        # First file (entities 1-1000)
        br_data_1 = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Article 1"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1000",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Article 1000"}],
                    },
                ]
            }
        ]

        # Second file (entities 1001-2000)
        br_data_2 = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1001",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Article 1001"}],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2000",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Article 2000"}],
                    },
                ]
            }
        ]

        # Third file (entities 2001-3000)
        br_data_3 = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2001",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [{"@value": "Article 2001"}],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2001"
                            }
                        ],
                    }
                ]
            }
        ]

        # Create agent role data in a different file
        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2001",
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {
                                "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2001"
                            }
                        ],
                    }
                ]
            }
        ]

        # Create responsible agent data in a different file
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2001",
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Test Author"}],
                    }
                ]
            }
        ]

        # Write test data to appropriate locations based on ID ranges
        os.makedirs(os.path.join(self.br_dir, supplier_prefix, "10000"), exist_ok=True)
        os.makedirs(
            os.path.join(self.rdf_dir, "ar", supplier_prefix, "10000"), exist_ok=True
        )
        os.makedirs(
            os.path.join(self.rdf_dir, "ra", supplier_prefix, "10000"), exist_ok=True
        )

        # Write BR files
        with ZipFile(
            os.path.join(self.br_dir, supplier_prefix, "10000", "1000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("1000.json", json.dumps(br_data_1))
        with ZipFile(
            os.path.join(self.br_dir, supplier_prefix, "10000", "2000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("2000.json", json.dumps(br_data_2))
        with ZipFile(
            os.path.join(self.br_dir, supplier_prefix, "10000", "3000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("3000.json", json.dumps(br_data_3))

        # Write AR and RA files
        with ZipFile(
            os.path.join(self.rdf_dir, "ar", supplier_prefix, "10000", "3000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("3000.json", json.dumps(ar_data))
        with ZipFile(
            os.path.join(self.rdf_dir, "ra", supplier_prefix, "10000", "3000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("3000.json", json.dumps(ra_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_files = sorted(os.listdir(self.output_dir))
        self.assertGreater(len(output_files), 0)

        # Collect all output data
        all_output_data = []
        for output_file in output_files:
            all_output_data.extend(
                get_csv_data(os.path.join(self.output_dir, output_file))
            )

        # Verify we have all expected entries
        self.assertEqual(len(all_output_data), 5)  # Should have 5 articles total

        # Verify specific entries
        article_1 = next(
            item
            for item in all_output_data
            if item["id"] == f"omid:br/{supplier_prefix}1"
        )
        article_1000 = next(
            item
            for item in all_output_data
            if item["id"] == f"omid:br/{supplier_prefix}1000"
        )
        article_1001 = next(
            item
            for item in all_output_data
            if item["id"] == f"omid:br/{supplier_prefix}1001"
        )
        article_2000 = next(
            item
            for item in all_output_data
            if item["id"] == f"omid:br/{supplier_prefix}2000"
        )
        article_2001 = next(
            item
            for item in all_output_data
            if item["id"] == f"omid:br/{supplier_prefix}2001"
        )

        # Check titles
        self.assertEqual(article_1["title"], "Article 1")
        self.assertEqual(article_1000["title"], "Article 1000")
        self.assertEqual(article_1001["title"], "Article 1001")
        self.assertEqual(article_2000["title"], "Article 2000")
        self.assertEqual(article_2001["title"], "Article 2001")

        # Check author for article 2001 (which has related entities)
        self.assertEqual(
            article_2001["author"], f"Test Author [omid:ra/{supplier_prefix}2001]"
        )

    def test_max_rows_per_file_and_data_integrity(self):
        """Test that output files respect max rows limit and no data is lost in multiprocessing"""
        supplier_prefix = "060"

        # Create test data with more than 3000 entries
        br_data = [
            {
                "@graph": [
                    # Generate 3500 test entries
                    *[
                        {
                            "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}{i}",
                            "@type": [
                                "http://purl.org/spar/fabio/Expression",
                                "http://purl.org/spar/fabio/JournalArticle",
                            ],
                            "http://purl.org/dc/terms/title": [
                                {"@value": f"Article {i}"}
                            ],
                            "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                                {"@value": "2024-01-01"}
                            ],
                        }
                        for i in range(1, 3501)
                    ]  # This will create 3500 entries
                ]
            }
        ]

        # Split data into multiple files to test multiprocessing
        entries_per_file = 1000
        for i in range(0, 3500, entries_per_file):
            file_data = [{"@graph": br_data[0]["@graph"][i : i + entries_per_file]}]

            # Create directory structure for the file
            file_number = i + entries_per_file
            dir_path = os.path.join(self.br_dir, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            # Write the file
            with ZipFile(os.path.join(dir_path, f"{file_number}.zip"), "w") as zip_file:
                zip_file.writestr(f"{file_number}.json", json.dumps(file_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output files
        output_files = sorted(os.listdir(self.output_dir))

        # Verify number of output files
        # We expect at least 2 files: 3500 entries should create 2 files (3000 + 500)
        self.assertGreaterEqual(
            len(output_files), 2, "Should have at least 2 output files for 3500 entries"
        )

        # Collect all entries from all output files
        all_entries = []
        for output_file in output_files:
            entries = get_csv_data(os.path.join(self.output_dir, output_file))

            # Verify each file has at most 3000 rows
            self.assertLessEqual(
                len(entries),
                3000,
                f"File {output_file} has more than 3000 rows: {len(entries)}",
            )

            all_entries.extend(entries)

        # Verify total number of entries
        self.assertEqual(
            len(all_entries),
            3500,
            f"Expected 3500 total entries, got {len(all_entries)}",
        )

        # Verify no duplicate entries
        unique_ids = {entry["id"] for entry in all_entries}
        self.assertEqual(
            len(unique_ids),
            3500,
            f"Expected 3500 unique entries, got {len(unique_ids)}",
        )

        # Verify all entries are present (no missing entries)
        expected_ids = {f"omid:br/{supplier_prefix}{i}" for i in range(1, 3501)}
        self.assertEqual(
            unique_ids,
            expected_ids,
            "Some entries are missing or unexpected entries are present",
        )

        # Verify data integrity
        for i in range(1, 3501):
            entry = next(
                e for e in all_entries if e["id"] == f"omid:br/{supplier_prefix}{i}"
            )
            self.assertEqual(entry["title"], f"Article {i}")
            self.assertEqual(entry["pub_date"], "2024-01-01")
            self.assertEqual(entry["type"], "journal article")

    def test_csv_field_limit_handling(self):
        """Test handling of CSV files with large fields that exceed the default limit"""
        # Create a test CSV with a very large field
        large_field = "omid:br/0601 " + " ".join(
            [f"id:{i}" for i in range(10000)]
        )  # This will create a field > 131072 chars
        test_data = {"id": large_field, "title": "Test Large Field"}

        os.makedirs(self.output_dir, exist_ok=True)
        with open(
            os.path.join(self.output_dir, "large_field.csv"),
            "w",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerow(test_data)

        # Try loading the data - this should trigger the field limit increase
        count = load_processed_omids_to_redis(self.output_dir, self.redis_client)

        # Verify the OMID was loaded despite the large field
        self.assertEqual(count, 1)
        self.assertTrue(is_omid_processed("omid:br/0601", self.redis_client))

    def test_complex_br_with_missing_authors(self):
        """Test processing of a complex BR with multiple related entities where authors might be missing"""
        supplier_prefix = "06250"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/br/062501777134",
                        "@type": [
                            "http://purl.org/spar/fabio/JournalArticle",
                            "http://purl.org/spar/fabio/Expression",
                        ],
                        "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                            {
                                "@type": "http://www.w3.org/2001/XMLSchema#gYearMonth",
                                "@value": "2020-02",
                            }
                        ],
                        "http://purl.org/dc/terms/title": [
                            {
                                "@value": "OpenCitations, An Infrastructure Organization For Open Scholarship"
                            }
                        ],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/062501806985"},
                            {"@id": "https://w3id.org/oc/meta/id/06850624745"},
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {"@id": "https://w3id.org/oc/meta/ar/062507977761"},
                            {"@id": "https://w3id.org/oc/meta/ar/062507977760"},
                            {"@id": "https://w3id.org/oc/meta/ar/062507977759"},
                        ],
                        "http://purl.org/vocab/frbr/core#embodiment": [
                            {"@id": "https://w3id.org/oc/meta/re/062501477439"}
                        ],
                        "http://purl.org/vocab/frbr/core#partOf": [
                            {"@id": "https://w3id.org/oc/meta/br/062501778111"}
                        ],
                    }
                ]
            }
        ]

        ar_data = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ar/062507977761",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": "https://w3id.org/oc/meta/ra/0610116105"}
                        ],
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/publisher"}
                        ],
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/ar/062507977760",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": "https://w3id.org/oc/meta/ra/0621010775619"}
                        ],
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                    },
                    {
                        "@id": "https://w3id.org/oc/meta/ar/062507977759",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": "https://w3id.org/oc/meta/ra/0614010840729"}
                        ],
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": "https://w3id.org/oc/meta/ar/062507977760"}
                        ],
                    },
                ]
            }
        ]

        ra_data_peroni = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/0614010840729",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/06304949238"}
                        ],
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Peroni"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Silvio"}],
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Peroni Silvio"}],
                    }
                ]
            }
        ]

        ra_data_shotton = [
            {
                "@graph": [
                    {
                        "@id": "https://w3id.org/oc/meta/ra/0621010775619",
                        "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                        "http://purl.org/spar/datacite/hasIdentifier": [
                            {"@id": "https://w3id.org/oc/meta/id/062404672414"}
                        ],
                        "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Shotton"}],
                        "http://xmlns.com/foaf/0.1/givenName": [{"@value": "D M"}],
                        "http://xmlns.com/foaf/0.1/name": [{"@value": "Shotton David"}],
                    }
                ]
            }
        ]

        # Create directory structure for BR data
        br_dir_path = os.path.join(self.rdf_dir, "br", supplier_prefix, "1780000")
        os.makedirs(br_dir_path, exist_ok=True)

        # Create directory structure for AR data
        ar_dir_path = os.path.join(self.rdf_dir, "ar", supplier_prefix, "7980000")
        os.makedirs(ar_dir_path, exist_ok=True)

        # Create directory structure for RA data (Peroni)
        ra_peroni_dir_path = os.path.join(self.rdf_dir, "ra", "06140", "10850000")
        os.makedirs(ra_peroni_dir_path, exist_ok=True)

        # Create directory structure for RA data (Shotton)
        ra_shotton_dir_path = os.path.join(self.rdf_dir, "ra", "06210", "10780000")
        os.makedirs(ra_shotton_dir_path, exist_ok=True)

        # Write BR data
        with ZipFile(os.path.join(br_dir_path, "1778000.zip"), "w") as zip_file:
            zip_file.writestr("1778000.json", json.dumps(br_data))

        # Write AR data
        with ZipFile(os.path.join(ar_dir_path, "7978000.zip"), "w") as zip_file:
            zip_file.writestr("7978000.json", json.dumps(ar_data))

        # Write RA data (Peroni)
        with ZipFile(os.path.join(ra_peroni_dir_path, "10841000.zip"), "w") as zip_file:
            zip_file.writestr("10841000.json", json.dumps(ra_data_peroni))

        # Write RA data (Shotton)
        with ZipFile(
            os.path.join(ra_shotton_dir_path, "10776000.zip"), "w"
        ) as zip_file:
            zip_file.writestr("10776000.json", json.dumps(ra_data_shotton))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)
        # Verify basic metadata
        article = output_data[0]
        self.assertEqual(
            article["title"],
            "OpenCitations, An Infrastructure Organization For Open Scholarship",
        )
        self.assertEqual(article["pub_date"], "2020-02")
        self.assertEqual(article["type"], "journal article")
        self.assertEqual(article["id"], "omid:br/062501777134")

        # Now we expect the authors to be present in the correct order
        expected_authors = (
            "Peroni, Silvio [omid:ra/0614010840729]; "
            "Shotton, D M [omid:ra/0621010775619]"
        )
        self.assertEqual(article["author"], expected_authors)

        # Publisher field should still be empty since we haven't added the publisher RA data
        self.assertEqual(article["publisher"], "")

    def test_multiple_first_ars(self):
        """Test behavior when there are multiple first ARs in the same chain (no hasNext pointing to them).
        The current behavior is to process only one of the first ARs and its hasNext chain.
        """
        supplier_prefix = "060"
        br_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                        "@type": [
                            "http://purl.org/spar/fabio/Expression",
                            "http://purl.org/spar/fabio/JournalArticle",
                        ],
                        "http://purl.org/dc/terms/title": [
                            {"@value": "Article With Multiple First Authors"}
                        ],
                        "http://purl.org/spar/pro/isDocumentContextFor": [
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"
                            },  # First potential author (will be processed)
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"
                            },  # Second potential author (will be ignored)
                            {
                                "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"
                            },  # Connected to author 1 (will be processed)
                        ],
                    }
                ]
            }
        ]

        # Setup agent roles with two potential "first" authors (no hasNext pointing to them)
        # and one author connected to the first one
        ar_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                        ],
                        "https://w3id.org/oc/ontology/hasNext": [
                            {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}
                        ],
                        # This is also a potential first author but will be ignored
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                        "@type": ["http://purl.org/spar/pro/RoleInTime"],
                        "http://purl.org/spar/pro/withRole": [
                            {"@id": "http://purl.org/spar/pro/author"}
                        ],
                        "http://purl.org/spar/pro/isHeldBy": [
                            {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}
                        ],
                        # This one is connected to author 1 via hasNext and will be processed
                    },
                ]
            }
        ]

        # Setup responsible agents
        ra_data = [
            {
                "@graph": [
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                        "http://xmlns.com/foaf/0.1/name": [
                            {"@value": "First Potential Author"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                        "http://xmlns.com/foaf/0.1/name": [
                            {"@value": "Second Potential Author"}
                        ],
                    },
                    {
                        "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                        "http://xmlns.com/foaf/0.1/name": [
                            {"@value": "Connected Author"}
                        ],
                    },
                ]
            }
        ]

        # Write test data files
        data_files = {"br": br_data, "ra": ra_data, "ar": ar_data}

        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, "10000")
            os.makedirs(dir_path, exist_ok=True)

            zip_path = os.path.join(dir_path, "1000.zip")
            with ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("1000.json", json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True,
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, "output_0.csv"))
        self.assertEqual(len(output_data), 1)

        article = output_data[0]
        authors = article["author"].split("; ")

        # Verify we have exactly two authors (the first one found and its connected author)
        self.assertEqual(
            len(authors),
            2,
            "Should have exactly two authors (first author and connected one)",
        )

        # Verify the specific authors we expect
        expected_authors = [
            f"First Potential Author [omid:ra/{supplier_prefix}1]",
            f"Connected Author [omid:ra/{supplier_prefix}3]",
        ]
        self.assertEqual(
            authors,
            expected_authors,
            "Should have first author and connected author in correct order",
        )

        # Verify the second potential author is NOT in the output
        self.assertNotIn(
            f"Second Potential Author [omid:ra/{supplier_prefix}2]",
            article["author"],
            "Second potential author should not be in the output",
        )


if __name__ == "__main__":
    unittest.main()
