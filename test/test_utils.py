#!/usr/bin/python

# SPDX-FileCopyrightText: 2022-2025 OpenCitations
# SPDX-FileCopyrightText: 2025-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from rdflib import Dataset, Graph, URIRef
from rdflib.term import Node
from SPARQLWrapper import N3, SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError, QueryBadFormed

from oc_meta.lib.sparql import execute_sparql, execute_sparql_update


QLEVER_ACCESS_TOKEN = "qlever_test_token"
SERVER = f"http://127.0.0.1:8805?access-token={QLEVER_ACCESS_TOKEN}"
PROV_SERVER = f"http://127.0.0.1:8806?access-token={QLEVER_ACCESS_TOKEN}"
QLEVER_CONTAINER = "oc-meta-test-qlever"
QLEVER_PROV_CONTAINER = "oc-meta-test-qlever-prov"

SUPPLIER_PREFIX = "060"

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
            result = execute_sparql(endpoint, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } FILTER(STRSTARTS(STR(?g), 'https://w3id.org/oc/meta/')) }", max_retries=1, backoff_factor=0.5)
            graphs = {b["g"]["value"] for b in result["results"]["bindings"]}
            all_graphs = GRAPHS_TO_CLEAR | graphs
            if all_graphs:
                clear_query = "; ".join(f"CLEAR GRAPH <{g}>" for g in all_graphs)
                execute_sparql_update(endpoint, clear_query, max_retries=1, backoff_factor=0.5)
            return
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(0.5 * (2**attempt))


def reset_server() -> None:
    with ThreadPoolExecutor(max_workers=2) as executor:
        main_future = executor.submit(_clear_all_meta_graphs, SERVER)
        prov_future = executor.submit(_clear_all_meta_graphs, PROV_SERVER)
        main_future.result()
        prov_future.result()


def reset_triplestore(server: str = SERVER) -> None:
    _clear_all_meta_graphs(server)


def get_counter_handler(info_dir: str | None = None, supplier_prefix: str = SUPPLIER_PREFIX) -> FilesystemCounterHandler:
    if info_dir is None:
        info_dir = tempfile.mkdtemp()
    info_dir_with_prefix = os.path.join(info_dir, supplier_prefix) + os.sep
    return FilesystemCounterHandler(info_dir=info_dir_with_prefix, supplier_prefix=supplier_prefix)


def execute_sparql_query(
    endpoint: str, query: str, max_retries: int = 3, delay: int = 5
) -> dict:
    return execute_sparql(endpoint, query, max_retries=max_retries, backoff_factor=delay)


def execute_sparql_construct(
    endpoint: str, query: str, max_retries: int = 3, delay: int = 5
) -> Graph:
    parsed = urlparse(endpoint)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    sparql = SPARQLWrapper(base_url)
    for key, values in parse_qs(parsed.query).items():
        sparql.addParameter(key, values[0])
    sparql.setReturnFormat(N3)
    sparql.setTimeout(60)
    sparql.setQuery(query)
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        if attempt > 0:
            time.sleep(delay * (2 ** attempt))
        try:
            result: bytes = sparql.query().convert()  # type: ignore[assignment]
            g = Graph()
            g.parse(data=result, format="n3")
            return g
        except QueryBadFormed:
            raise
        except (EndPointInternalError, URLError) as e:
            last_error = e
    raise last_error  # type: ignore[misc]


def wait_for_triplestore(server: str, max_wait: int = 60) -> bool:
    start_time = time.time()
    delay = 0.1
    while time.time() - start_time < max_wait:
        try:
            execute_sparql(server, "SELECT * WHERE { ?s ?p ?o } LIMIT 1", max_retries=1, backoff_factor=1)
            return True
        except Exception:
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
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
    batch_size: int = 10000,
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

    for i in range(0, len(triples_list), batch_size):
        batch_triples = triples_list[i : i + batch_size]

        triples_str = ""
        for subj, pred, obj, ctx in batch_triples:
            if ctx:
                triples_str += f"GRAPH {ctx.n3().replace('[', '').replace(']', '')} {{ {subj.n3()} {pred.n3()} {obj.n3()} }} "
            else:
                triples_str += f"{subj.n3()} {pred.n3()} {obj.n3()} . "

        query = f"INSERT DATA {{ {triples_str} }}"
        execute_sparql_update(server, query, max_retries=3, backoff_factor=0.5)
