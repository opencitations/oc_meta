# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from unittest.mock import MagicMock, patch

from SPARQLWrapper import GET, POST, URLENCODED

from oc_meta.lib.sparql import execute_sparql


@patch("oc_meta.lib.sparql._make_sparql_client")
def test_execute_sparql_uses_get_by_default(mock_make_sparql_client):
    client = MagicMock()
    expected = {"results": {"bindings": []}}
    client.queryAndConvert.return_value = expected
    mock_make_sparql_client.return_value = client

    result = execute_sparql("http://endpoint", "SELECT * WHERE { ?s ?p ?o }")

    assert result == expected
    mock_make_sparql_client.assert_called_once_with("http://endpoint", 3600)
    client.setMethod.assert_called_once_with(GET)
    assert client.setRequestMethod.call_count == 0
    client.setQuery.assert_called_once_with("SELECT * WHERE { ?s ?p ?o }")
    assert client.queryAndConvert.call_count == 1


@patch("oc_meta.lib.sparql._make_sparql_client")
def test_execute_sparql_supports_urlencoded_post(mock_make_sparql_client):
    client = MagicMock()
    expected = {"results": {"bindings": []}}
    client.queryAndConvert.return_value = expected
    mock_make_sparql_client.return_value = client

    result = execute_sparql(
        "http://endpoint",
        "SELECT * WHERE { ?s ?p ?o }",
        method=POST,
    )

    assert result == expected
    mock_make_sparql_client.assert_called_once_with("http://endpoint", 3600)
    client.setMethod.assert_called_once_with(POST)
    client.setRequestMethod.assert_called_once_with(URLENCODED)
    client.setQuery.assert_called_once_with("SELECT * WHERE { ?s ?p ?o }")
    assert client.queryAndConvert.call_count == 1
