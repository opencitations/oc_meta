# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import os
import tempfile

import pytest
from oc_ocdm.prov.prov_entity import ProvEntity
from sparqlite import SPARQLClient

from oc_meta.lib.finder import ResourceFinder
from test.test_utils import PROV_SERVER, SERVER, reset_server


BASE_IRI = "https://w3id.org/oc/meta/"


@pytest.fixture
def prov_config_file():
    config = {
        "dataset": {
            "triplestore_urls": [SERVER],
            "file_paths": [],
            "is_quadstore": True,
        },
        "provenance": {
            "triplestore_urls": [PROV_SERVER],
            "file_paths": [],
            "is_quadstore": True,
        },
        "blazegraph_full_text_search": False,
        "fuseki_full_text_search": False,
        "virtuoso_full_text_search": False,
        "graphdb_connector_name": "",
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name
    yield config_path
    os.unlink(config_path)


def _insert_data(triples: list[str], graph_uri: str) -> None:
    with SPARQLClient(SERVER, timeout=60) as client:
        for triple in triples:
            client.update(f"INSERT DATA {{ GRAPH <{graph_uri}> {{ {triple} }} }}")


class TestTimeAgnosticLibraryIntegration:
    def test_retrieve_metaid_from_merged_entity(self, prov_config_file):
        reset_server()

        source_entity_uri = f"{BASE_IRI}br/0601"
        target_entity_uri = f"{BASE_IRI}br/0602"
        source_prov_graph = f"{source_entity_uri}/prov/"
        target_prov_graph = f"{target_entity_uri}/prov/"

        source_se1 = f"{source_prov_graph}se/1"
        source_se2 = f"{source_prov_graph}se/2"
        target_se1 = f"{target_prov_graph}se/1"
        target_se2 = f"{target_prov_graph}se/2"

        target_triples = [
            f'<{target_entity_uri}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> .',
            f'<{target_entity_uri}> <http://purl.org/dc/terms/title> "Merged Article" .',
        ]
        _insert_data(target_triples, f"{BASE_IRI}br/")

        source_prov = [
            f'GRAPH <{source_prov_graph}> {{ <{source_se1}> a <{ProvEntity.iri_entity}> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se1}> <{ProvEntity.iri_specialization_of}> <{source_entity_uri}> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se1}> <{ProvEntity.iri_generated_at_time}> "2024-01-01T00:00:00+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se1}> <{ProvEntity.iri_was_attributed_to}> <https://w3id.org/oc/meta/prov/pa/1> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se1}> <{ProvEntity.iri_description}> "The entity has been created." }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se2}> a <{ProvEntity.iri_entity}> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se2}> <{ProvEntity.iri_specialization_of}> <{source_entity_uri}> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se2}> <{ProvEntity.iri_generated_at_time}> "2024-02-01T00:00:00+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se2}> <{ProvEntity.iri_was_attributed_to}> <https://w3id.org/oc/meta/prov/pa/1> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se2}> <{ProvEntity.iri_was_derived_from}> <{source_se1}> }}',
            f'GRAPH <{source_prov_graph}> {{ <{source_se2}> <{ProvEntity.iri_description}> "The entity has been deleted." }}',
        ]

        target_prov = [
            f'GRAPH <{target_prov_graph}> {{ <{target_se1}> a <{ProvEntity.iri_entity}> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se1}> <{ProvEntity.iri_specialization_of}> <{target_entity_uri}> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se1}> <{ProvEntity.iri_generated_at_time}> "2024-01-01T00:00:00+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se1}> <{ProvEntity.iri_was_attributed_to}> <https://w3id.org/oc/meta/prov/pa/1> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se1}> <{ProvEntity.iri_description}> "The entity has been created." }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> a <{ProvEntity.iri_entity}> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> <{ProvEntity.iri_specialization_of}> <{target_entity_uri}> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> <{ProvEntity.iri_generated_at_time}> "2024-02-01T00:00:00+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> <{ProvEntity.iri_was_attributed_to}> <https://w3id.org/oc/meta/prov/pa/1> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> <{ProvEntity.iri_was_derived_from}> <{target_se1}> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> <{ProvEntity.iri_was_derived_from}> <{source_se1}> }}',
            f'GRAPH <{target_prov_graph}> {{ <{target_se2}> <{ProvEntity.iri_description}> "The entity has been merged." }}',
        ]

        with SPARQLClient(PROV_SERVER, timeout=60) as client:
            for triple in source_prov + target_prov:
                client.update(f"INSERT DATA {{ {triple} }}")

        finder = ResourceFinder(ts_url=SERVER, base_iri=BASE_IRI)
        result = finder.retrieve_metaid_from_merged_entity(
            metaid_uri=source_entity_uri, prov_config=prov_config_file
        )

        assert result == "0602"
