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

import json
import os
import unittest
from shutil import rmtree
from zipfile import ZipFile

from oc_meta.lib.file_manager import get_csv_data
from oc_meta.plugins.csv_generator_lite.csv_generator_lite import generate_csv


class TestCSVGeneratorLite(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.join('test', 'csv_generator_lite')
        self.input_dir = os.path.join(self.base_dir, 'input')
        self.output_dir = os.path.join(self.base_dir, 'output')
        
        # Create test directories if they don't exist
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create test RDF structure
        self.rdf_dir = os.path.join(self.input_dir, 'rdf')
        self.br_dir = os.path.join(self.rdf_dir, 'br')
        os.makedirs(self.br_dir, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.base_dir):
            rmtree(self.base_dir)

    def test_basic_br_processing(self):
        """Test basic bibliographic resource processing"""
        test_data = [{
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/0601",
                    "@type": [
                        "http://purl.org/spar/fabio/Expression",
                        "http://purl.org/spar/fabio/JournalArticle"
                    ],
                    "http://purl.org/dc/terms/title": [
                        {"@value": "Test Article"}
                    ],
                    "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                        {"@value": "2024-01-01"}
                    ],
                    "http://purl.org/spar/datacite/hasIdentifier": [
                        {"@id": "https://w3id.org/oc/meta/id/0601"}
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/"
        }]
        
        # Write test data to file
        os.makedirs(os.path.join(self.br_dir, '060', '10000'), exist_ok=True)
        with ZipFile(os.path.join(self.br_dir, '060', '10000', '1000.zip'), 'w') as zip_file:
            zip_file.writestr('1000.json', json.dumps(test_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_files = os.listdir(self.output_dir)
        self.assertEqual(len(output_files), 1)
        
        output_data = get_csv_data(os.path.join(self.output_dir, output_files[0]))
        self.assertEqual(len(output_data), 1)
        self.assertEqual(output_data[0]['title'], 'Test Article')
        self.assertEqual(output_data[0]['pub_date'], '2024-01-01')
        self.assertEqual(output_data[0]['type'], 'journal article')
        self.assertEqual(output_data[0]['id'], 'omid:br/0601')

    def test_complex_br_with_related_entities(self):
        """Test processing of BR with authors, venue, and other related entities"""
        # Create directory structure for each entity type
        supplier_prefix = '060'
        for entity_type in ['br', 'ra', 'ar', 'id']:
            os.makedirs(os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000'), exist_ok=True)
        
        # BR data including both the article and the venue
        br_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2",
                    "@type": [
                        "http://purl.org/spar/fabio/Expression",
                        "http://purl.org/spar/fabio/JournalArticle"
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
                    ]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3",
                    "@type": [
                        "http://purl.org/spar/fabio/Expression",
                        "http://purl.org/spar/fabio/Journal"
                    ],
                    "http://purl.org/dc/terms/title": [
                        {"@value": "Test Journal"}
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/"
        }]
        
        ar_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                    "http://purl.org/spar/pro/withRole": [
                        {"@id": "http://purl.org/spar/pro/author"}
                    ],
                    "http://purl.org/spar/pro/isHeldBy": [
                        {"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/ar/"
        }]
        
        ra_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                    "http://xmlns.com/foaf/0.1/name": [
                        {"@value": "Test Author"}
                    ]
                }
            ],
            "@id": "https://w3id.org/oc/meta/ra/"
        }]

        # Write test data files in correct locations
        data_files = {
            'br': br_data,
            'ra': ra_data,
            'ar': ar_data
        }
        
        for entity_type, data in data_files.items():
            zip_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000', '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 2)  # Should have 2 rows: article and journal
        
        # Find article and journal entries
        article = next((item for item in output_data if item['type'] == 'journal article'), None)
        journal = next((item for item in output_data if item['type'] == 'journal'), None)
        
        # Verify article data
        self.assertIsNotNone(article)
        self.assertEqual(article['title'], 'Complex Article')
        self.assertEqual(article['venue'], f'Test Journal [omid:br/{supplier_prefix}3]')
        self.assertEqual(article['author'], 'Test Author [omid:ra/0601]')
        self.assertEqual(article['id'], f'omid:br/{supplier_prefix}2')
        
        # Verify journal data
        self.assertIsNotNone(journal)
        self.assertEqual(journal['title'], 'Test Journal')
        self.assertEqual(journal['type'], 'journal')
        self.assertEqual(journal['id'], f'omid:br/{supplier_prefix}3')

    def test_empty_input_directory(self):
        """Test behavior with empty input directory"""
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )
        
        self.assertEqual(len(os.listdir(self.output_dir)), 0)

    def test_br_with_multiple_authors_and_editors(self):
        """Test processing of BR with multiple authors and editors"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/Book"],
                "http://purl.org/dc/terms/title": [{"@value": "Multi-Author Book"}],
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"},  # First author
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"},  # Second author
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"},  # First editor
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}4"}   # Second editor
                ]
            }]
        }]
        
        # Setup agent roles for authors and editors with hasNext relations
        ar_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/editor"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}4"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}4",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/editor"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}4"}]
                }
            ]
        }]
        
        # Setup responsible agents
        ra_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                    "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Smith"}],
                    "http://xmlns.com/foaf/0.1/givenName": [{"@value": "John"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                    "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Doe"}],
                    "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Jane"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                    "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Brown"}],
                    "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Bob"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}4",
                    "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Wilson"}],
                    "http://xmlns.com/foaf/0.1/givenName": [{"@value": "Alice"}]
                }
            ]
        }]
        
        # Write test data files
        data_files = {
            'br': br_data,
            'ra': ra_data,
            'ar': ar_data
        }
        
        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 1)
        
        # Verify authors and editors are in the correct order
        expected_authors = (
            f'Smith, John [omid:ra/{supplier_prefix}1]; '
            f'Doe, Jane [omid:ra/{supplier_prefix}2]'
        )
        expected_editors = (
            f'Brown, Bob [omid:ra/{supplier_prefix}3]; '
            f'Wilson, Alice [omid:ra/{supplier_prefix}4]'
        )
        
        self.assertEqual(output_data[0]['author'], expected_authors)
        self.assertEqual(output_data[0]['editor'], expected_editors)

    def test_br_with_identifiers(self):
        """Test processing of BR with multiple identifiers"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                "http://purl.org/dc/terms/title": [{"@value": "Article With DOI"}],
                "http://purl.org/spar/datacite/hasIdentifier": [
                    {"@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}1"},
                    {"@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}2"}
                ]
            }]
        }]
        
        id_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}1",
                    "http://purl.org/spar/datacite/usesIdentifierScheme": [
                        {"@id": "http://purl.org/spar/datacite/doi"}
                    ],
                    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                        {"@value": "10.1234/test.123"}
                    ]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/id/{supplier_prefix}2",
                    "http://purl.org/spar/datacite/usesIdentifierScheme": [
                        {"@id": "http://purl.org/spar/datacite/isbn"}
                    ],
                    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                        {"@value": "978-0-123456-47-2"}
                    ]
                }
            ]
        }]
        
        # Write test data files in correct locations
        data_files = {
            'br': br_data,
            'id': id_data
        }
        
        for entity_type, data in data_files.items():
            # Create all necessary directories
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 1)
        
        # Verify all identifiers are included
        expected_ids = f'omid:br/{supplier_prefix}1 doi:10.1234/test.123 isbn:978-0-123456-47-2'
        self.assertEqual(output_data[0]['id'], expected_ids)

    def test_br_with_page_numbers(self):
        """Test processing of BR with page information"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                "http://purl.org/dc/terms/title": [{"@value": "Paged Article"}],
                "http://purl.org/vocab/frbr/core#embodiment": [
                    {"@id": f"https://w3id.org/oc/meta/re/{supplier_prefix}1"}
                ]
            }]
        }]
        
        re_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/re/{supplier_prefix}1",
                "http://prismstandard.org/namespaces/basic/2.0/startingPage": [{"@value": "100"}],
                "http://prismstandard.org/namespaces/basic/2.0/endingPage": [{"@value": "120"}]
            }]
        }]
        
        # Write test data files in correct locations
        data_files = {
            'br': br_data,
            're': re_data
        }
        
        for entity_type, data in data_files.items():
            # Create all necessary directories
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 1)
        self.assertEqual(output_data[0]['page'], "100-120")

    def test_malformed_data_handling(self):
        """Test handling of malformed or incomplete data"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                # Missing title
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": "invalid_uri"},  # Invalid URI
                ],
                "http://purl.org/vocab/frbr/core#partOf": [
                    {"@id": "non_existent_venue"}  # Non-existent venue
                ]
            }]
        }]
        
        # Write test data files in correct locations
        data_files = {
            'br': br_data
        }
        
        for entity_type, data in data_files.items():
            # Create all necessary directories
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 1)
        # Verify graceful handling of missing/invalid data
        self.assertEqual(output_data[0]['title'], '')
        self.assertEqual(output_data[0]['author'], '')
        self.assertEqual(output_data[0]['venue'], '')

    def test_br_with_hierarchical_venue_structures(self):
        """Test different hierarchical venue structures (issue->volume->journal, issue->journal, volume->journal, direct journal)"""
        supplier_prefix = '060'
        
        # Create test data for different hierarchical structures
        br_data = [{
            "@graph": [
                # Article in issue->volume->journal structure
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article in Full Hierarchy"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2"}  # Issue
                    ]
                },
                # Article in issue->journal structure (no volume)
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}5",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article in Issue-Journal"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}6"}  # Issue
                    ]
                },
                # Article in volume->journal structure (no issue)
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}9",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article in Volume-Journal"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}10"}  # Volume
                    ]
                },
                # Article directly in journal
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}13",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article in Journal"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"}  # Journal
                    ]
                },
                # Issue in full hierarchy
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2",
                    "@type": ["http://purl.org/spar/fabio/JournalIssue"],
                    "http://purl.org/spar/fabio/hasSequenceIdentifier": [{"@value": "2"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3"}  # Volume
                    ]
                },
                # Volume in full hierarchy
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}3",
                    "@type": ["http://purl.org/spar/fabio/JournalVolume"],
                    "http://purl.org/spar/fabio/hasSequenceIdentifier": [{"@value": "42"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"}  # Journal
                    ]
                },
                # Journal
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4",
                    "@type": ["http://purl.org/spar/fabio/Journal"],
                    "http://purl.org/dc/terms/title": [{"@value": "Test Journal"}]
                },
                # Issue directly in journal
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}6",
                    "@type": ["http://purl.org/spar/fabio/JournalIssue"],
                    "http://purl.org/spar/fabio/hasSequenceIdentifier": [{"@value": "3"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"}  # Journal
                    ]
                },
                # Volume directly in journal
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}10",
                    "@type": ["http://purl.org/spar/fabio/JournalVolume"],
                    "http://purl.org/spar/fabio/hasSequenceIdentifier": [{"@value": "5"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}4"}  # Journal
                    ]
                }
            ]
        }]

        # Write test data files
        dir_path = os.path.join(self.rdf_dir, 'br', supplier_prefix, '10000')
        os.makedirs(dir_path, exist_ok=True)
        
        zip_path = os.path.join(dir_path, '1000.zip')
        with ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('1000.json', json.dumps(br_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        
        # Verify we only have the articles and journal in the output
        self.assertEqual(len(output_data), 5)  # 4 articles + 1 journal
        
        # Verify no JournalVolume or JournalIssue entries exist
        volume_or_issue_entries = [item for item in output_data 
                                 if item['type'] in ['journal volume', 'journal issue']]
        self.assertEqual(len(volume_or_issue_entries), 0)
        
        # Find each article by title
        full_hierarchy = next(item for item in output_data if item['title'] == 'Article in Full Hierarchy')
        issue_journal = next(item for item in output_data if item['title'] == 'Article in Issue-Journal')
        volume_journal = next(item for item in output_data if item['title'] == 'Article in Volume-Journal')
        direct_journal = next(item for item in output_data if item['title'] == 'Article in Journal')

        # Test full hierarchy (issue->volume->journal)
        self.assertEqual(full_hierarchy['issue'], '2')
        self.assertEqual(full_hierarchy['volume'], '42')
        self.assertEqual(full_hierarchy['venue'], f'Test Journal [omid:br/{supplier_prefix}4]')

        # Test issue->journal (no volume)
        self.assertEqual(issue_journal['issue'], '3')
        self.assertEqual(issue_journal['volume'], '')
        self.assertEqual(issue_journal['venue'], f'Test Journal [omid:br/{supplier_prefix}4]')

        # Test volume->journal (no issue)
        self.assertEqual(volume_journal['issue'], '')
        self.assertEqual(volume_journal['volume'], '5')
        self.assertEqual(volume_journal['venue'], f'Test Journal [omid:br/{supplier_prefix}4]')

        # Test direct journal connection
        self.assertEqual(direct_journal['issue'], '')
        self.assertEqual(direct_journal['volume'], '')
        self.assertEqual(direct_journal['venue'], f'Test Journal [omid:br/{supplier_prefix}4]')

    def test_book_in_series(self):
        """Test processing of a book that is part of a book series"""
        supplier_prefix = '060'
        
        # Create test data for book in series
        br_data = [{
            "@graph": [
                # Book
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                    "@type": [
                        "http://purl.org/spar/fabio/Expression",
                        "http://purl.org/spar/fabio/Book"
                    ],
                    "http://purl.org/dc/terms/title": [{"@value": "Test Book"}],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2"}  # Series
                    ]
                },
                # Book Series
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2",
                    "@type": [
                        "http://purl.org/spar/fabio/BookSeries"
                    ],
                    "http://purl.org/dc/terms/title": [{"@value": "Test Book Series"}]
                }
            ]
        }]

        # Write test data
        dir_path = os.path.join(self.rdf_dir, 'br', supplier_prefix, '10000')
        os.makedirs(dir_path, exist_ok=True)
        
        zip_path = os.path.join(dir_path, '1000.zip')
        with ZipFile(zip_path, 'w') as zip_file:
            zip_file.writestr('1000.json', json.dumps(br_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        
        # Find book entry
        book = next(item for item in output_data if item['type'] == 'book')
        
        # Verify book is correctly linked to series
        self.assertEqual(book['title'], 'Test Book')
        self.assertEqual(book['venue'], f'Test Book Series [omid:br/{supplier_prefix}2]')
        self.assertEqual(book['volume'], '')  # Should not have volume
        self.assertEqual(book['issue'], '')   # Should not have issue

    def test_br_with_multiple_roles(self):
        """Test processing of BR with authors, editors and publishers"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/Book"],
                "http://purl.org/dc/terms/title": [{"@value": "Multi-Role Book"}],
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"},  # Author
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"},  # Editor
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}   # Publisher
                ]
            }]
        }]
        
        # Setup agent roles for authors, editors and publishers
        ar_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/editor"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/publisher"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}]
                }
            ]
        }]
        
        # Setup responsible agents with different name formats
        ra_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                    "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Smith"}],
                    "http://xmlns.com/foaf/0.1/givenName": [{"@value": "John"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Editor Name"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Publisher House"}]
                }
            ]
        }]
        
        # Write test data files
        data_files = {
            'br': br_data,
            'ra': ra_data,
            'ar': ar_data
        }
        
        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 1)
        
        # Verify all roles are correctly processed
        book = output_data[0]
        self.assertEqual(book['title'], 'Multi-Role Book')
        self.assertEqual(book['author'], f'Smith, John [omid:ra/{supplier_prefix}1]')
        self.assertEqual(book['editor'], f'Editor Name [omid:ra/{supplier_prefix}2]')
        self.assertEqual(book['publisher'], f'Publisher House [omid:ra/{supplier_prefix}3]')

    def test_ordered_authors(self):
        """Test that authors are ordered according to hasNext relations"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                "http://purl.org/dc/terms/title": [{"@value": "Ordered Authors Article"}],
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"},
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"},
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                ]
            }]
        }]
        
        # Setup agent roles with hasNext relations
        ar_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}]
                }
            ]
        }]
        
        # Setup responsible agents with different names
        ra_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "First Author"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Second Author"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Third Author"}]
                }
            ]
        }]
        
        # Write test data files
        data_files = {
            'br': br_data,
            'ra': ra_data,
            'ar': ar_data
        }
        
        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        self.assertEqual(len(output_data), 1)
        
        # Verify authors are in the correct order
        expected_authors = (
            f'First Author [omid:ra/{supplier_prefix}1]; '
            f'Second Author [omid:ra/{supplier_prefix}2]; '
            f'Third Author [omid:ra/{supplier_prefix}3]'
        )
        self.assertEqual(output_data[0]['author'], expected_authors)

    def test_cyclic_hasNext_relations(self):
        """Test handling of cyclic hasNext relations between agent roles"""
        supplier_prefix = '060'
        br_data = [{
            "@graph": [{
                "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                "http://purl.org/dc/terms/title": [{"@value": "Cyclic Authors Article"}],
                "http://purl.org/spar/pro/isDocumentContextFor": [
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"},
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"},
                    {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}
                ]
            }]
        }]
        
        # Setup agent roles with cyclic hasNext relations
        ar_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1"}],
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2"}],
                    # Creates a cycle: 1 -> 2 -> 3 -> 1
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}3",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3"}],
                    # Cycle completion
                    "https://w3id.org/oc/ontology/hasNext": [{"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}1"}]
                }
            ]
        }]
        
        # Setup responsible agents
        ra_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}1",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "First Author"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Second Author"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}3",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Third Author"}]
                }
            ]
        }]
        
        # Write test data files
        data_files = {
            'br': br_data,
            'ra': ra_data,
            'ar': ar_data
        }
        
        for entity_type, data in data_files.items():
            dir_path = os.path.join(self.rdf_dir, entity_type, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            zip_path = os.path.join(dir_path, '1000.zip')
            with ZipFile(zip_path, 'w') as zip_file:
                zip_file.writestr('1000.json', json.dumps(data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_data = get_csv_data(os.path.join(self.output_dir, 'output_0.csv'))
        print(output_data)
        self.assertEqual(len(output_data), 1)
        
        # Verify that we get at least some authors before the cycle is detected
        # The order should be maintained until the cycle is detected
        authors = output_data[0]['author'].split('; ')
        self.assertGreater(len(authors), 0)
        
        # Verify the presence and order of authors
        self.assertTrue(any(f'First Author [omid:ra/{supplier_prefix}1]' in author for author in authors))
        self.assertTrue(any(f'Second Author [omid:ra/{supplier_prefix}2]' in author for author in authors))
        
        # Verify no duplicates in the output
        author_set = set(authors)
        self.assertEqual(len(authors), len(author_set), 
            "Found duplicate authors in output: each author should appear exactly once")
        
        # Verify the exact order and number of authors
        expected_authors = [
            f'First Author [omid:ra/{supplier_prefix}1]',
            f'Second Author [omid:ra/{supplier_prefix}2]',
            f'Third Author [omid:ra/{supplier_prefix}3]'
        ]
        self.assertEqual(authors, expected_authors, 
            "Authors should be in correct order and each should appear exactly once")

    def test_multiple_input_files(self):
        """Test processing of multiple input files with sequential entity IDs"""
        supplier_prefix = '060'
        
        # Create test data spanning multiple files
        # First file (entities 1-1000)
        br_data_1 = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article 1"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1000",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article 1000"}]
                }
            ]
        }]

        # Second file (entities 1001-2000)
        br_data_2 = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}1001",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article 1001"}]
                },
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2000",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article 2000"}]
                }
            ]
        }]

        # Third file (entities 2001-3000)
        br_data_3 = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}2001",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": "Article 2001"}],
                    "http://purl.org/spar/pro/isDocumentContextFor": [
                        {"@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2001"}
                    ]
                }
            ]
        }]

        # Create agent role data in a different file
        ar_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ar/{supplier_prefix}2001",
                    "http://purl.org/spar/pro/withRole": [{"@id": "http://purl.org/spar/pro/author"}],
                    "http://purl.org/spar/pro/isHeldBy": [{"@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2001"}]
                }
            ]
        }]

        # Create responsible agent data in a different file
        ra_data = [{
            "@graph": [
                {
                    "@id": f"https://w3id.org/oc/meta/ra/{supplier_prefix}2001",
                    "http://xmlns.com/foaf/0.1/name": [{"@value": "Test Author"}]
                }
            ]
        }]

        # Write test data to appropriate locations based on ID ranges
        os.makedirs(os.path.join(self.br_dir, supplier_prefix, '10000'), exist_ok=True)
        os.makedirs(os.path.join(self.rdf_dir, 'ar', supplier_prefix, '10000'), exist_ok=True)
        os.makedirs(os.path.join(self.rdf_dir, 'ra', supplier_prefix, '10000'), exist_ok=True)

        # Write BR files
        with ZipFile(os.path.join(self.br_dir, supplier_prefix, '10000', '1000.zip'), 'w') as zip_file:
            zip_file.writestr('1000.json', json.dumps(br_data_1))
        with ZipFile(os.path.join(self.br_dir, supplier_prefix, '10000', '2000.zip'), 'w') as zip_file:
            zip_file.writestr('2000.json', json.dumps(br_data_2))
        with ZipFile(os.path.join(self.br_dir, supplier_prefix, '10000', '3000.zip'), 'w') as zip_file:
            zip_file.writestr('3000.json', json.dumps(br_data_3))

        # Write AR and RA files
        with ZipFile(os.path.join(self.rdf_dir, 'ar', supplier_prefix, '10000', '3000.zip'), 'w') as zip_file:
            zip_file.writestr('3000.json', json.dumps(ar_data))
        with ZipFile(os.path.join(self.rdf_dir, 'ra', supplier_prefix, '10000', '3000.zip'), 'w') as zip_file:
            zip_file.writestr('3000.json', json.dumps(ra_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output
        output_files = sorted(os.listdir(self.output_dir))
        self.assertGreater(len(output_files), 0)

        # Collect all output data
        all_output_data = []
        for output_file in output_files:
            all_output_data.extend(get_csv_data(os.path.join(self.output_dir, output_file)))

        # Verify we have all expected entries
        self.assertEqual(len(all_output_data), 5)  # Should have 5 articles total

        # Verify specific entries
        article_1 = next(item for item in all_output_data if item['id'] == f'omid:br/{supplier_prefix}1')
        article_1000 = next(item for item in all_output_data if item['id'] == f'omid:br/{supplier_prefix}1000')
        article_1001 = next(item for item in all_output_data if item['id'] == f'omid:br/{supplier_prefix}1001')
        article_2000 = next(item for item in all_output_data if item['id'] == f'omid:br/{supplier_prefix}2000')
        article_2001 = next(item for item in all_output_data if item['id'] == f'omid:br/{supplier_prefix}2001')

        # Check titles
        self.assertEqual(article_1['title'], 'Article 1')
        self.assertEqual(article_1000['title'], 'Article 1000')
        self.assertEqual(article_1001['title'], 'Article 1001')
        self.assertEqual(article_2000['title'], 'Article 2000')
        self.assertEqual(article_2001['title'], 'Article 2001')

        # Check author for article 2001 (which has related entities)
        self.assertEqual(article_2001['author'], f'Test Author [omid:ra/{supplier_prefix}2001]')

    def test_max_rows_per_file_and_data_integrity(self):
        """Test that output files respect max rows limit and no data is lost in multiprocessing"""
        supplier_prefix = '060'
        
        # Create test data with more than 3000 entries
        br_data = [{
            "@graph": [
                # Generate 3500 test entries
                *[{
                    "@id": f"https://w3id.org/oc/meta/br/{supplier_prefix}{i}",
                    "@type": ["http://purl.org/spar/fabio/Expression", "http://purl.org/spar/fabio/JournalArticle"],
                    "http://purl.org/dc/terms/title": [{"@value": f"Article {i}"}],
                    "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [{"@value": "2024-01-01"}]
                } for i in range(1, 3501)]  # This will create 3500 entries
            ]
        }]
        
        # Split data into multiple files to test multiprocessing
        entries_per_file = 1000
        for i in range(0, 3500, entries_per_file):
            file_data = [{
                "@graph": br_data[0]["@graph"][i:i + entries_per_file]
            }]
            
            # Create directory structure for the file
            file_number = i + entries_per_file
            dir_path = os.path.join(self.br_dir, supplier_prefix, '10000')
            os.makedirs(dir_path, exist_ok=True)
            
            # Write the file
            with ZipFile(os.path.join(dir_path, f'{file_number}.zip'), 'w') as zip_file:
                zip_file.writestr(f'{file_number}.json', json.dumps(file_data))

        # Run generator
        generate_csv(
            input_dir=self.rdf_dir,
            output_dir=self.output_dir,
            dir_split_number=10000,
            items_per_file=1000,
            zip_output_rdf=True
        )

        # Check output files
        output_files = sorted(os.listdir(self.output_dir))
        
        # Verify number of output files
        # We expect at least 2 files: 3500 entries should create 2 files (3000 + 500)
        self.assertGreaterEqual(len(output_files), 2, 
            "Should have at least 2 output files for 3500 entries")

        # Collect all entries from all output files
        all_entries = []
        for output_file in output_files:
            entries = get_csv_data(os.path.join(self.output_dir, output_file))
            
            # Verify each file has at most 3000 rows
            self.assertLessEqual(
                len(entries), 3000,
                f"File {output_file} has more than 3000 rows: {len(entries)}"
            )
            
            all_entries.extend(entries)

        # Verify total number of entries
        self.assertEqual(
            len(all_entries), 3500,
            f"Expected 3500 total entries, got {len(all_entries)}"
        )

        # Verify no duplicate entries
        unique_ids = {entry['id'] for entry in all_entries}
        self.assertEqual(
            len(unique_ids), 3500,
            f"Expected 3500 unique entries, got {len(unique_ids)}"
        )

        # Verify all entries are present (no missing entries)
        expected_ids = {f"omid:br/{supplier_prefix}{i}" for i in range(1, 3501)}
        self.assertEqual(
            unique_ids, expected_ids,
            "Some entries are missing or unexpected entries are present"
        )

        # Verify data integrity
        for i in range(1, 3501):
            entry = next(e for e in all_entries if e['id'] == f"omid:br/{supplier_prefix}{i}")
            self.assertEqual(entry['title'], f"Article {i}")
            self.assertEqual(entry['pub_date'], "2024-01-01")
            self.assertEqual(entry['type'], "journal article")

if __name__ == '__main__':
    unittest.main() 