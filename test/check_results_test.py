#!/usr/bin/python

# Copyright 2025 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from unittest.mock import MagicMock, patch

import pytest

from oc_meta.run.meta.check_results import (
    check_omids_existence,
    check_provenance_existence,
    find_file,
    find_prov_file,
    parse_identifiers,
)


class TestParseIdentifiers:
    """Test cases for parse_identifiers function."""

    def test_parse_single_identifier(self):
        """Test parsing a single identifier."""
        result = parse_identifiers("doi:10.1234/test")
        expected = [{'schema': 'doi', 'value': '10.1234/test'}]
        assert result == expected

    def test_parse_multiple_identifiers(self):
        """Test parsing multiple space-separated identifiers."""
        result = parse_identifiers("doi:10.1234/test isbn:978-3-16-148410-0")
        expected = [
            {'schema': 'doi', 'value': '10.1234/test'},
            {'schema': 'isbn', 'value': '978-3-16-148410-0'}
        ]
        assert result == expected

    def test_parse_identifier_with_colon_in_value(self):
        """Test parsing identifier where value contains colons."""
        result = parse_identifiers("url:http://example.com:8080/path")
        expected = [{'schema': 'url', 'value': 'http://example.com:8080/path'}]
        assert result == expected

    def test_parse_empty_string(self):
        """Test parsing empty string returns empty list."""
        result = parse_identifiers("")
        assert result == []

    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only string returns empty list."""
        result = parse_identifiers("   ")
        assert result == []

    def test_parse_none(self):
        """Test parsing None returns empty list."""
        result = parse_identifiers(None)
        assert result == []

    def test_parse_with_uppercase_schema(self):
        """Test that schema is converted to lowercase."""
        result = parse_identifiers("DOI:10.1234/test")
        expected = [{'schema': 'doi', 'value': '10.1234/test'}]
        assert result == expected

    def test_parse_malformed_identifier(self):
        """Test parsing identifier without colon."""
        result = parse_identifiers("malformed")
        assert result == []


class TestFindFile:
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
        assert result == expected

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
        assert result == expected

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
        assert result == expected

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
        assert result == expected

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
        assert result is None

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
        assert result == expected


class TestFindProvFile:
    """Test cases for find_prov_file function."""

    def test_find_prov_file_exists(self):
        """Test finding provenance file when it exists."""
        data_zip_path = "/base/rdf/br/060/10000/1000.zip"

        with patch('os.path.exists', return_value=True):
            result = find_prov_file(data_zip_path)
            expected = "/base/rdf/br/060/10000/1000/prov/se.zip"
            assert result == expected

    def test_find_prov_file_not_exists(self):
        """Test finding provenance file when it doesn't exist."""
        data_zip_path = "/base/rdf/br/060/10000/1000.zip"

        with patch('os.path.exists', return_value=False):
            result = find_prov_file(data_zip_path)
            assert result is None

    def test_find_prov_file_with_exception(self):
        """Test that exceptions in find_prov_file are handled gracefully."""
        data_zip_path = "/base/rdf/br/060/10000/1000.zip"

        with patch('os.path.dirname', side_effect=Exception("Test error")):
            result = find_prov_file(data_zip_path)
            assert result is None


class TestCheckOMIDsExistence:
    """Test cases for check_omids_existence function."""

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_single_identifier_found(self, mock_sparql_client):
        """Test checking single identifier that exists."""
        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.query.return_value = {
            "results": {
                "bindings": [
                    {"omid": {"value": "https://w3id.org/oc/meta/br/0601"}}
                ]
            }
        }

        identifiers = [{'schema': 'doi', 'value': '10.1234/test'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        expected = {'doi:10.1234/test': {'https://w3id.org/oc/meta/br/0601'}}
        assert result == expected
        mock_client.query.assert_called_once()

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_identifier_not_found(self, mock_sparql_client):
        """Test checking identifier that doesn't exist."""
        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.query.return_value = {"results": {"bindings": []}}

        identifiers = [{'schema': 'doi', 'value': '10.9999/notfound'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        expected = {'doi:10.9999/notfound': set()}
        assert result == expected

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_multiple_omids_for_identifier(self, mock_sparql_client):
        """Test identifier with multiple OMIDs."""
        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.query.return_value = {
            "results": {
                "bindings": [
                    {"omid": {"value": "https://w3id.org/oc/meta/br/0601"}},
                    {"omid": {"value": "https://w3id.org/oc/meta/br/0602"}}
                ]
            }
        }

        identifiers = [{'schema': 'doi', 'value': '10.1234/duplicate'}]
        result = check_omids_existence(identifiers, "http://example.com/sparql")

        expected = {
            'doi:10.1234/duplicate': {
                'https://w3id.org/oc/meta/br/0601',
                'https://w3id.org/oc/meta/br/0602'
            }
        }
        assert result == expected

    def test_check_empty_identifiers_list(self):
        """Test with empty identifiers list."""
        result = check_omids_existence([], "http://example.com/sparql")
        assert result == {}

    @patch('oc_meta.run.meta.check_results.time.sleep')
    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_sparql_exception_handling(self, mock_sparql_client, mock_sleep):
        """Test that SPARQL exceptions are retried and eventually raised."""
        from sparqlite.exceptions import EndpointError

        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.query.side_effect = EndpointError("SPARQL endpoint unavailable")

        identifiers = [{'schema': 'doi', 'value': '10.1234/test'}]

        # After MAX_RETRIES, the exception should be raised
        with pytest.raises(EndpointError):
            check_omids_existence(identifiers, "http://example.com/sparql")


class TestCheckProvenanceExistence:
    """Test cases for check_provenance_existence function."""

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_provenance_exists(self, mock_sparql_client):
        """Test checking provenance that exists."""
        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.ask.return_value = True

        omids = ["https://w3id.org/oc/meta/br/0601"]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        expected = {"https://w3id.org/oc/meta/br/0601": True}
        assert result == expected
        mock_client.ask.assert_called_once()

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_provenance_not_exists(self, mock_sparql_client):
        """Test checking provenance that doesn't exist."""
        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.ask.return_value = False

        omids = ["https://w3id.org/oc/meta/br/0601"]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        expected = {"https://w3id.org/oc/meta/br/0601": False}
        assert result == expected

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_multiple_omids_mixed_results(self, mock_sparql_client):
        """Test checking multiple OMIDs with mixed provenance results."""
        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.ask.side_effect = [True, False, True]

        omids = [
            "https://w3id.org/oc/meta/br/0601",
            "https://w3id.org/oc/meta/br/0602",
            "https://w3id.org/oc/meta/br/0603"
        ]
        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        assert result["https://w3id.org/oc/meta/br/0601"]
        assert not result["https://w3id.org/oc/meta/br/0602"]
        assert result["https://w3id.org/oc/meta/br/0603"]

    def test_check_empty_omids_list(self):
        """Test with empty OMIDs list."""
        result = check_provenance_existence([], "http://example.com/prov-sparql")
        assert result == {}

    @patch('oc_meta.run.meta.check_results.SPARQLClient')
    def test_check_provenance_individual_queries(self, mock_sparql_client):
        """Test that each OMID gets an individual ASK query."""
        omids = [f"https://w3id.org/oc/meta/br/06{i:02d}" for i in range(1, 6)]

        mock_client = MagicMock()
        mock_sparql_client.return_value.__enter__.return_value = mock_client
        mock_client.ask.return_value = False

        result = check_provenance_existence(omids, "http://example.com/prov-sparql")

        # Should have made 5 individual ASK calls
        assert mock_client.ask.call_count == 5
        assert len(result) == 5
        assert all(not v for v in result.values())
