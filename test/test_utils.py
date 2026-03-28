#!/usr/bin/python

# Copyright (C) 2022-2025 OpenCitations
# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import time
from concurrent.futures import ThreadPoolExecutor

import redis
import re
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from rdflib import Dataset, Graph, URIRef
from rdflib.term import Node
from sparqlite import SPARQLClient


QLEVER_ACCESS_TOKEN = "qlever_test_token"
SERVER = f"http://127.0.0.1:8805?access-token={QLEVER_ACCESS_TOKEN}"
PROV_SERVER = f"http://127.0.0.1:8806?access-token={QLEVER_ACCESS_TOKEN}"
QLEVER_CONTAINER = "oc-meta-test-qlever"
QLEVER_PROV_CONTAINER = "oc-meta-test-qlever-prov"

REDIS_HOST = "localhost"
REDIS_PORT = 6381
REDIS_DB = 5
REDIS_CACHE_DB = 2

BASE_IRI = "https://w3id.org/oc/meta/"

GRAPHS_TO_CLEAR = {
    "https://w3id.org/oc/meta/br/",
    "https://w3id.org/oc/meta/ra/",
    "https://w3id.org/oc/meta/re/",
    "https://w3id.org/oc/meta/id/",
    "https://w3id.org/oc/meta/ar/",
    "http://default.graph/",
    "https://w3id.org/oc/meta/",
}


def _clear_all_meta_graphs(endpoint: str, max_retries: int = 3) -> None:
    for attempt in range(max_retries):
        try:
            with SPARQLClient(endpoint, timeout=60) as client:
                result = client.query(
                    "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } "
                    "FILTER(STRSTARTS(STR(?g), 'https://w3id.org/oc/meta/')) }"
                )
                graphs = [b["g"]["value"] for b in result["results"]["bindings"]]
                for graph in GRAPHS_TO_CLEAR:
                    client.update(f"CLEAR GRAPH <{graph}>")
                for graph in graphs:
                    client.update(f"CLEAR GRAPH <{graph}>")
            return
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(0.5 * (2**attempt))


def reset_server() -> None:
    for _ in range(10):
        with ThreadPoolExecutor(max_workers=2) as executor:
            main_future = executor.submit(_clear_all_meta_graphs, SERVER)
            prov_future = executor.submit(_clear_all_meta_graphs, PROV_SERVER)
            main_future.result()
            prov_future.result()
        time.sleep(0.2)
        data_result = execute_sparql_query(
            SERVER,
            "SELECT (COUNT(*) AS ?c) WHERE { GRAPH ?g { ?s ?p ?o } "
            "FILTER(STRSTARTS(STR(?g), 'https://w3id.org/oc/meta/')) }"
        )
        prov_result = execute_sparql_query(
            PROV_SERVER,
            "SELECT (COUNT(*) AS ?c) WHERE { GRAPH ?g { ?s ?p ?o } "
            "FILTER(STRSTARTS(STR(?g), 'https://w3id.org/oc/meta/')) }"
        )
        data_count = int(data_result['results']['bindings'][0]['c']['value'])
        prov_count = int(prov_result['results']['bindings'][0]['c']['value'])
        if data_count == 0 and prov_count == 0:
            return


def reset_triplestore(server: str = SERVER) -> None:
    _clear_all_meta_graphs(server)


def reset_redis_counters() -> None:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_cache_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_CACHE_DB
    )
    redis_client.flushdb()
    redis_cache_client.flushdb()


def get_counter_handler() -> RedisCounterHandler:
    return RedisCounterHandler(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def execute_sparql_query(
    endpoint: str, query: str, max_retries: int = 3, delay: int = 5
) -> dict:
    try:
        with SPARQLClient(
            endpoint, max_retries=max_retries, backoff_factor=delay, timeout=60
        ) as client:
            return client.query(query)
    except Exception as e:
        from urllib.error import URLError

        raise URLError(
            f"Failed to connect to SPARQL endpoint after {max_retries} attempts: {str(e)}"
        )


def execute_sparql_construct(
    endpoint: str, query: str, max_retries: int = 3, delay: int = 5
) -> Graph:
    try:
        with SPARQLClient(
            endpoint, max_retries=max_retries, backoff_factor=delay, timeout=60
        ) as client:
            g = Graph()
            g.parse(data=client.construct(query), format="nt")
            return g
    except Exception as e:
        from urllib.error import URLError

        raise URLError(
            f"Failed to connect to SPARQL endpoint after {max_retries} attempts: {str(e)}"
        )


def wait_for_triplestore(server: str, max_wait: int = 60) -> bool:
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            with SPARQLClient(
                server, max_retries=1, backoff_factor=1, timeout=60
            ) as client:
                client.query("SELECT * WHERE { ?s ?p ?o } LIMIT 1")
            return True
        except Exception:
            time.sleep(2)
    return False


def wait_for_redis(
    host: str = REDIS_HOST, port: int = REDIS_PORT, max_wait: int = 10
) -> bool:
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            client = redis.Redis(host=host, port=port)
            client.ping()
            return True
        except Exception:
            time.sleep(1)
    return False


def get_path(path: str) -> str:
    return path.replace("\\", "/")


def normalize_ids_in_brackets(s: str) -> str:
    """Sort IDs inside square brackets to make comparison order-independent."""
    def sort_ids(match):
        ids = match.group(1).split()
        return f"[{' '.join(sorted(ids))}]"
    return re.sub(r'\[([^\]]+)\]', sort_ids, s)


def normalize_row_ids(row: dict) -> None:
    """Normalize ID ordering in all fields that may contain IDs."""
    if row.get('id') and isinstance(row['id'], str):
        row['id'] = ' '.join(sorted(row['id'].split()))
    for field in ['author', 'venue', 'publisher', 'editor']:
        if row.get(field):
            row[field] = normalize_ids_in_brackets(row[field])


def add_data_ts(
    server: str = SERVER,
    data_path: str = os.path.abspath(
        os.path.join("test", "testcases", "ts", "real_data.nt")
    ).replace("\\", "/"),
    batch_size: int = 100,
    default_graph_uri: URIRef = URIRef("http://default.graph/"),
) -> None:
    reset_triplestore(server)
    f_path = get_path(data_path)

    file_extension = os.path.splitext(f_path)[1].lower()
    if file_extension == ".nt":
        g = Graph()
        g.parse(location=f_path, format="nt")
    elif file_extension == ".nq":
        g = Dataset(default_union=True)
        g.parse(location=f_path, format="nquads")
    elif file_extension == ".ttl":
        g = Graph()
        g.parse(location=f_path, format="turtle")
    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")

    triples_list: list[tuple[Node, Node, Node, Node | None]] = []
    if file_extension in {".nt", ".ttl"}:
        assert isinstance(g, Graph)
        for subj, pred, obj in g.triples((None, None, None)):
            triples_list.append((subj, pred, obj, default_graph_uri))
    elif file_extension == ".nq":
        assert isinstance(g, Dataset)
        for subj, pred, obj, ctx in g.quads():
            triples_list.append((subj, pred, obj, ctx))

    with SPARQLClient(server, timeout=60) as client:
        for i in range(0, len(triples_list), batch_size):
            batch_triples = triples_list[i : i + batch_size]

            triples_str = ""
            for subj, pred, obj, ctx in batch_triples:
                if ctx:
                    triples_str += f"GRAPH {ctx.n3().replace('[', '').replace(']', '')} {{ {subj.n3()} {pred.n3()} {obj.n3()} }} "
                else:
                    triples_str += f"{subj.n3()} {pred.n3()} {obj.n3()} . "

            query = f"INSERT DATA {{ {triples_str} }}"
            client.update(query)
