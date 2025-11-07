#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2025, Arcangelo Massari <arcangelo.massari@unibo.it>
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

import unittest
from unittest.mock import patch

from oc_meta.run.meta.check_results import (
    check_omids_existence,
    check_provenance_existence,
    find_file,
    find_prov_file,
    parse_identifiers,
)


class TestParseIdentifiers(unittest.TestCase):
    """Test cases for parse_identifiers function."""

    def test_parse_single_identifier(self):
        """Test parsing a single identifier."""
        result = parse_identifiers("doi:10.1234/test")
        expected = [{'schema': 'doi', 'value': '10.1234/test'}]
        self.assertEqual(result, expected)

    def test_parse_multiple_identifiers(self):
        """Test parsing multiple space-separated identifiers."""
        result = parse_identifiers("doi:10.1234/test isbn:978-3-16-148410-0")
        expected = [
            {'schema': 'doi', 'value': '10.1234/test'},
            {'schema': 'isbn', 'value': '978-3-16-148410-0'}
        ]
        self.assertEqual(result, expected)

    def test_parse_identifier_with_colon_in_value(self):
        """Test parsing identifier where value contains colons."""
        result = parse_identifiers("url:http://example.com:8080/path")
        expected = [{'schema': 'url', 'value': 'http://example.com:8080/path'}]
        self.assertEqual(result, expected)

    def test_parse_empty_string(self):
        """Test parsing empty string returns empty list."""
        result = parse_identifiers("")
        self.assertEqual(result, [])

    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only string returns empty list."""
        result = parse_identifiers("   ")
        self.assertEqual(result, [])

    def test_parse_none(self):
        """Test parsing None returns empty list."""
        result = parse_identifiers(None)
        self.assertEqual(result, [])

    def test_parse_with_uppercase_schema(self):
        """Test that schema is converted to lowercase."""
        result = parse_identifiers("DOI:10.1234/test")
        expected = [{'schema': 'doi', 'value': '10.1234/test'}]
        self.assertEqual(result, expected)

    def test_parse_malformed_identifier(self):
        """Test parsing identifier without colon."""
        result = parse_identifiers("malformed")
        self.assertEqual(result, [])


class TestFindFile(unittest.TestCase):
    """Test cases for find_file function."""

    def test_find_file_zip_format(self):
        """Test finding file path for ZIP format."""
        uri = "https://w3id.org/oc/meta/br/0605"
        result = find_file(
            rdf_dir="/base/rdf",
            dir_split_number=10000,
            items_per_file=1000,
            uri=uri,
            zip_output_rdf=True
        )
        expected = "/base/rdf/br/060/10000/1000.zip"
        self.assertEqual(result, expected)

    def test_find_file_json_format(self):
        """Test finding file path for JSON format."""
        uri = "https://w3id.org/oc/meta/br/0605"
        result = find_file(
            rdf_dir="/base/rdf",
            dir_split_number=10000,
            items_per_file=1000,
            uri=uri,
            zip_output_rdf=False
        )
        expected = "/base/rdf/br/060/10000/1000.json"
        self.assertEqual(result, expected)

    def test_find_file_with_subfolder(self):
        """Test finding file path with subfolder prefix."""
        uri = "https://w3id.org/oc/meta/br/06012345"
        result = find_file(
            rdf_dir="/base/rdf",
            dir_split_number=10000,
            items_per_file=1000,
            uri=uri,
            zip_output_rdf=True
        )
        expected = "/base/rdf/br/060/20000/13000.zip"
        self.assertEqual(result, expected)

    def test_find_file_different_entity_type(self):
        """Test finding file for different entity types."""
        uri = "https://w3id.org/oc/meta/ra/0605"
        result = find_file(
            rdf_dir="/base/rdf",
            dir_split_number=10000,
            items_per_file=1000,
            uri=uri,
            zip_output_rdf=True
        )
        expected = "/base/rdf/ra/060/10000/1000.zip"
        self.assertEqual(result, expected)

    def test_find_file_invalid_uri(self):
        """Test that invalid URI returns None."""
        uri = "invalid-uri"
        result = find_file(
            rdf_dir="/base/rdf",
            dir_split_number=10000,
            items_per_file=1000,
            uri=uri,
            zip_output_rdf=True
        )
        self.assertIsNone(result)

    def test_find_file_boundary_values(self):
        """Test file finding with boundary values."""
        uri = "https://w3id.org/oc/meta/br/06010000"
        result = find_file(
            rdf_dir="/base/rdf",
            dir_split_number=10000,
            items_per_file=1000,
            uri=uri,
            zip_output_rdf=True
        )
        expected = "/base/rdf/br/060/10000/10000.zip"
        self.assertEqual(result, expected)


class TestFindProvFile(unittest.TestCase):
    """Test cases for find_prov_file function."""

    def test_find_prov_file_exists(self):
        """Test finding provenance file when it exists."""
        data_zip_path = "/base/rdf/br/060/10000/1000.zip"

        with patch('os.path.exists', return_value=True):
            result = find_prov_file(data_zip_path)
            expected = "/base/rdf/br/060/10000/1000/prov/se.zip"
            self.assertEqual(result, expected)

    def test_find_prov_file_not_exists(self):
        """Test finding provenance file when it doesn't exist."""
        data_zip_path = "/base/rdf/br/060/10000/1000.zip"

        with patch('os.path.exists', return_value=False):
            result = find_prov_file(data_zip_path)
            self.assertIsNone(result)

    def test_find_prov_file_with_exception(self):
        """Test that exceptions in find_prov_file are handled gracefully."""
        data_zip_path = "/base/rdf/br/060/10000/1000.zip"

        with patch('os.path.dirname', side_effect=Exception("Test error")):
            result = find_prov_file(data_zip_path)
            self.assertIsNone(result)


class TestCheckOMIDsExistence(unittest.TestCase):
    """Test cases for check_omids_existence function."""

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_single_identifier_found(self, mock_sparql_wrapper, mock_execute):
        """Test checking single identifier that exists."""
        mock_results = {
            "results": {
                "bindings": [
                    {"omid": {"value": "https://w3id.org/oc/meta/br/0601"}}
                ]
            }
        }
        mock_execute.return_value = mock_results

        identifiers = [{'schema': 'doi', 'value': '10.1234/test'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        expected = {'doi:10.1234/test': {'https://w3id.org/oc/meta/br/0601'}}
        self.assertEqual(result, expected)
        mock_execute.assert_called_once()

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_identifier_not_found(self, mock_sparql_wrapper, mock_execute):
        """Test checking identifier that doesn't exist."""
        mock_results = {"results": {"bindings": []}}
        mock_execute.return_value = mock_results

        identifiers = [{'schema': 'doi', 'value': '10.9999/notfound'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        expected = {'doi:10.9999/notfound': set()}
        self.assertEqual(result, expected)

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_multiple_omids_for_identifier(self, mock_sparql_wrapper, mock_execute):
        """Test identifier with multiple OMIDs."""
        mock_results = {
            "results": {
                "bindings": [
                    {"omid": {"value": "https://w3id.org/oc/meta/br/0601"}},
                    {"omid": {"value": "https://w3id.org/oc/meta/br/0602"}}
                ]
            }
        }
        mock_execute.return_value = mock_results

        identifiers = [{'schema': 'doi', 'value': '10.1234/duplicate'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        expected = {
            'doi:10.1234/duplicate': {
                'https://w3id.org/oc/meta/br/0601',
                'https://w3id.org/oc/meta/br/0602'
            }
        }
        self.assertEqual(result, expected)

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_empty_identifiers_list(self, mock_sparql_wrapper, mock_execute):
        """Test with empty identifiers list."""
        result = check_omids_existence([], "http://example.com/sparql")
        self.assertEqual(result, {})
        mock_execute.assert_not_called()

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_sparql_exception_handling(self, mock_sparql_wrapper, mock_execute):
        """Test that SPARQL exceptions are handled gracefully."""
        mock_execute.side_effect = Exception("SPARQL endpoint unavailable")

        identifiers = [{'schema': 'doi', 'value': '10.1234/test'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        # Should return empty set for the identifier
        expected = {'doi:10.1234/test': set()}
        self.assertEqual(result, expected)


class TestCheckProvenanceExistence(unittest.TestCase):
    """Test cases for check_provenance_existence function."""

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_provenance_exists(self, mock_sparql_wrapper, mock_execute):
        """Test checking provenance that exists."""
        mock_results = {
            "results": {
                "bindings": [
                    {"entity": {"value": "https://w3id.org/oc/meta/br/0601"}}
                ]
            }
        }
        mock_execute.return_value = mock_results

        omids = ["https://w3id.org/oc/meta/br/0601"]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        expected = {"https://w3id.org/oc/meta/br/0601": True}
        self.assertEqual(result, expected)

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_provenance_not_exists(self, mock_sparql_wrapper, mock_execute):
        """Test checking provenance that doesn't exist."""
        mock_results = {"results": {"bindings": []}}
        mock_execute.return_value = mock_results

        omids = ["https://w3id.org/oc/meta/br/0601"]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        expected = {"https://w3id.org/oc/meta/br/0601": False}
        self.assertEqual(result, expected)

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_multiple_omids_mixed_results(self, mock_sparql_wrapper, mock_execute):
        """Test checking multiple OMIDs with mixed provenance results."""
        mock_results = {
            "results": {
                "bindings": [
                    {"entity": {"value": "https://w3id.org/oc/meta/br/0601"}},
                    {"entity": {"value": "https://w3id.org/oc/meta/br/0603"}}
                ]
            }
        }
        mock_execute.return_value = mock_results

        omids = [
            "https://w3id.org/oc/meta/br/0601",
            "https://w3id.org/oc/meta/br/0602",
            "https://w3id.org/oc/meta/br/0603"
        ]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        # 0601 and 0603 have provenance, 0602 doesn't
        self.assertTrue(result["https://w3id.org/oc/meta/br/0601"])
        self.assertFalse(result["https://w3id.org/oc/meta/br/0602"])
        self.assertTrue(result["https://w3id.org/oc/meta/br/0603"])

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_empty_omids_list(self, mock_sparql_wrapper, mock_execute):
        """Test with empty OMIDs list."""
        result = check_provenance_existence([], "http://example.com/prov-sparql")
        self.assertEqual(result, {})
        mock_execute.assert_not_called()

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_provenance_batching(self, mock_sparql_wrapper, mock_execute):
        """Test that large lists are batched correctly."""
        # Create 25 OMIDs (should result in 3 batches with BATCH_SIZE=10)
        omids = [f"https://w3id.org/oc/meta/br/06{i:02d}" for i in range(1, 26)]

        mock_results = {"results": {"bindings": []}}
        mock_execute.return_value = mock_results

        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        # Should have made 3 calls (batches of 10, 10, 5)
        self.assertEqual(mock_execute.call_count, 3)

        # All OMIDs should be in result with False
        self.assertEqual(len(result), 25)
        self.assertTrue(all(not v for v in result.values()))

    @patch('oc_meta.run.meta.check_results.execute_sparql_query')
    @patch('oc_meta.run.meta.check_results.SPARQLWrapper')
    def test_check_provenance_exception_handling(self, mock_sparql_wrapper, mock_execute):
        """Test that SPARQL exceptions are handled gracefully."""
        mock_execute.side_effect = Exception("SPARQL endpoint unavailable")

        omids = ["https://w3id.org/oc/meta/br/0601"]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        # Should still return the OMID with False
        expected = {"https://w3id.org/oc/meta/br/0601": False}
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
