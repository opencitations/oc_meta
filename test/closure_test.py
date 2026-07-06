# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from unittest.mock import patch

from oc_meta.run.merge.closure import (
    _one_hop_neighbours,
    _partof_ancestors,
    _publisher_agents,
    compute_related_closure,
)


def _normalize_sparql(query):
    return " ".join(query.split())


def _bindings(*uris):
    return {
        "results": {"bindings": [{"entity": {"value": u, "type": "uri"}} for u in uris]}
    }


def _ancestor_bindings(*uris):
    return {
        "results": {
            "bindings": [{"ancestor": {"value": u, "type": "uri"}} for u in uris]
        }
    }


class TestOneHopNeighbours:
    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_returns_uri_entities(self, mock_execute_sparql):
        mock_execute_sparql.return_value = _bindings(
            "https://example.org/related1", "https://example.org/related2"
        )

        result = _one_hop_neighbours(
            "http://endpoint",
            {"https://example.org/test1", "https://example.org/test2"},
            10,
        )

        assert result == {
            "https://example.org/related1",
            "https://example.org/related2",
        }

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_batches_large_input(self, mock_execute_sparql):
        mock_execute_sparql.return_value = {"results": {"bindings": []}}

        _one_hop_neighbours(
            "http://endpoint", {f"https://example.org/entity{i}" for i in range(25)}, 10
        )

        assert mock_execute_sparql.call_count == 3

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_empty_results(self, mock_execute_sparql):
        mock_execute_sparql.return_value = {"results": {"bindings": []}}

        result = _one_hop_neighbours(
            "http://endpoint", {"https://example.org/test"}, 10
        )

        assert result == set()

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_filters_non_uri_entities(self, mock_execute_sparql):
        mock_execute_sparql.return_value = {
            "results": {
                "bindings": [
                    {"entity": {"value": "https://example.org/uri1", "type": "uri"}},
                    {"entity": {"value": "Some Literal", "type": "literal"}},
                    {"entity": {"value": "https://example.org/uri2", "type": "uri"}},
                ]
            }
        }

        result = _one_hop_neighbours(
            "http://endpoint", {"https://example.org/test"}, 10
        )

        assert result == {"https://example.org/uri1", "https://example.org/uri2"}

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_includes_responsible_agent_context(self, mock_execute_sparql):
        mock_execute_sparql.return_value = {"results": {"bindings": []}}

        _one_hop_neighbours("http://endpoint", {"https://w3id.org/oc/meta/ra/0601"}, 10)

        query = mock_execute_sparql.call_args.args[1]
        assert "pro:isHeldBy <https://w3id.org/oc/meta/ra/0601>" in query
        assert "pro:isDocumentContextFor ?agent_role" in query


class TestPartofAncestors:
    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_walks_the_full_container_chain(self, mock_execute_sparql):
        mock_execute_sparql.side_effect = [
            _ancestor_bindings("https://example.org/issue"),
            _ancestor_bindings("https://example.org/volume"),
            _ancestor_bindings("https://example.org/journal"),
            _ancestor_bindings(),
        ]

        result = _partof_ancestors(
            "http://endpoint", {"https://example.org/article"}, 10
        )

        assert result == {
            "https://example.org/issue",
            "https://example.org/volume",
            "https://example.org/journal",
        }
        assert mock_execute_sparql.call_count == 4

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_no_ancestors(self, mock_execute_sparql):
        mock_execute_sparql.return_value = _ancestor_bindings()

        result = _partof_ancestors(
            "http://endpoint", {"https://example.org/journal"}, 10
        )

        assert result == set()

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_excludes_seeds_from_result(self, mock_execute_sparql):
        mock_execute_sparql.side_effect = [
            _ancestor_bindings(
                "https://example.org/seed", "https://example.org/volume"
            ),
            _ancestor_bindings(),
            _ancestor_bindings(),
        ]

        result = _partof_ancestors(
            "http://endpoint",
            {"https://example.org/issue", "https://example.org/seed"},
            10,
        )

        assert result == {"https://example.org/volume"}


class TestComputeRelatedClosure:
    def test_empty_seeds_makes_no_queries(self):
        with patch("oc_meta.run.merge.closure.execute_sparql") as mock_execute_sparql:
            result = compute_related_closure("http://endpoint", [])

        assert result == set()
        assert mock_execute_sparql.call_count == 0

    @patch("oc_meta.run.merge.closure._publisher_agents")
    @patch("oc_meta.run.merge.closure._one_hop_neighbours")
    @patch("oc_meta.run.merge.closure._partof_ancestors")
    def test_combines_seeds_ancestors_and_neighbours(
        self, mock_ancestors, mock_neighbours, mock_publisher_agents
    ):
        mock_ancestors.return_value = {"https://example.org/journal"}
        mock_neighbours.return_value = {"https://example.org/sibling"}
        mock_publisher_agents.return_value = {"https://example.org/publisher"}

        result = compute_related_closure(
            "http://endpoint",
            ["https://example.org/article", "https://example.org/duplicate"],
        )

        assert result == {
            "https://example.org/article",
            "https://example.org/duplicate",
            "https://example.org/journal",
            "https://example.org/sibling",
            "https://example.org/publisher",
        }
        assert mock_neighbours.call_args.args[1] == {
            "https://example.org/article",
            "https://example.org/duplicate",
            "https://example.org/journal",
        }
        assert mock_publisher_agents.call_args.args[1] == {
            "https://example.org/article",
            "https://example.org/duplicate",
            "https://example.org/journal",
            "https://example.org/sibling",
        }


def _agent_bindings(*rows):
    bindings = []
    for row in rows:
        binding = {"agent": {"value": row[0], "type": "uri"}}
        if len(row) > 1:
            binding["identifier"] = {"value": row[1], "type": "uri"}
        bindings.append(binding)
    return {"results": {"bindings": bindings}}


class TestPublisherAgents:
    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_returns_agents_and_their_identifiers(self, mock_execute_sparql):
        mock_execute_sparql.return_value = _agent_bindings(
            (
                "https://w3id.org/oc/meta/ra/0601",
                "https://w3id.org/oc/meta/id/0601",
            ),
        )

        result = _publisher_agents(
            "http://endpoint", {"https://w3id.org/oc/meta/ar/0601"}, 10
        )

        assert result == {
            "https://w3id.org/oc/meta/ra/0601",
            "https://w3id.org/oc/meta/id/0601",
        }

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_agent_without_identifier(self, mock_execute_sparql):
        mock_execute_sparql.return_value = _agent_bindings(
            ("https://w3id.org/oc/meta/ra/0601",),
        )

        result = _publisher_agents(
            "http://endpoint", {"https://w3id.org/oc/meta/ar/0601"}, 10
        )

        assert result == {"https://w3id.org/oc/meta/ra/0601"}

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_query_filters_publisher_roles(self, mock_execute_sparql):
        mock_execute_sparql.return_value = {"results": {"bindings": []}}

        _publisher_agents("http://endpoint", {"https://w3id.org/oc/meta/ar/0601"}, 10)

        query = mock_execute_sparql.call_args.args[1]
        expected_query = """
            SELECT DISTINCT ?agent ?identifier WHERE {
                { {<https://w3id.org/oc/meta/ar/0601> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/publisher> . <https://w3id.org/oc/meta/ar/0601> <http://purl.org/spar/pro/isHeldBy> ?agent} }
                OPTIONAL { ?agent <http://purl.org/spar/datacite/hasIdentifier> ?identifier }
            }
        """
        assert _normalize_sparql(query) == _normalize_sparql(expected_query)

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_batches_large_input(self, mock_execute_sparql):
        mock_execute_sparql.return_value = {"results": {"bindings": []}}

        _publisher_agents(
            "http://endpoint",
            {f"https://w3id.org/oc/meta/ar/060{i}" for i in range(25)},
            10,
        )

        assert mock_execute_sparql.call_count == 3

    @patch("oc_meta.run.merge.closure.execute_sparql")
    def test_empty_input_makes_no_queries(self, mock_execute_sparql):
        result = _publisher_agents("http://endpoint", set(), 10)

        assert result == set()
        assert mock_execute_sparql.call_count == 0
