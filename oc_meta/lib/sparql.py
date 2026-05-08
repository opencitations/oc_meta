# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

from SPARQLWrapper import JSON, POST, SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError, QueryBadFormed

from oc_meta.constants import QLEVER_MAX_WORKERS, QLEVER_QUERIES_PER_GROUP


def _make_sparql_client(endpoint_url: str) -> SPARQLWrapper:
    parsed = urlparse(endpoint_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    sparql = SPARQLWrapper(base_url)
    for key, values in parse_qs(parsed.query).items():
        sparql.addParameter(key, values[0])
    sparql.setReturnFormat(JSON)
    sparql.setTimeout(3600)
    return sparql


def execute_sparql(
    endpoint_url: str,
    query: str,
    max_retries: int = 5,
    backoff_factor: float = 5,
) -> dict:
    sparql = _make_sparql_client(endpoint_url)
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        if attempt > 0:
            time.sleep(backoff_factor * (2 ** attempt))
        try:
            sparql.setQuery(query)
            return sparql.queryAndConvert()  # type: ignore[return-value]
        except QueryBadFormed:
            raise
        except (EndPointInternalError, URLError) as e:
            last_error = e
    raise last_error  # type: ignore[misc]


def execute_sparql_update(
    endpoint_url: str,
    query: str,
    max_retries: int = 5,
    backoff_factor: float = 5,
) -> None:
    sparql = _make_sparql_client(endpoint_url)
    sparql.setMethod(POST)
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        if attempt > 0:
            time.sleep(backoff_factor * (2 ** attempt))
        try:
            sparql.setQuery(query)
            sparql.query()
            return
        except QueryBadFormed:
            raise
        except (EndPointInternalError, URLError) as e:
            last_error = e
    raise last_error  # type: ignore[misc]


def execute_sparql_queries(
    endpoint_url: str,
    queries: list[str],
    max_retries: int = 5,
    backoff_factor: float = 5,
) -> list[list[dict[str, dict[str, str]]]]:
    results: list[list[dict[str, dict[str, str]]]] = []
    sparql = _make_sparql_client(endpoint_url)
    for query in queries:
        last_error: Exception | None = None
        for attempt in range(max_retries + 1):
            if attempt > 0:
                time.sleep(backoff_factor * (2 ** attempt))
            try:
                sparql.setQuery(query)
                result: dict[str, dict[str, list[dict[str, dict[str, str]]]]] = sparql.queryAndConvert()  # type: ignore[assignment]
                results.append(result['results']['bindings'])
                break
            except QueryBadFormed:
                raise
            except (EndPointInternalError, URLError) as e:
                last_error = e
        else:
            raise last_error  # type: ignore[misc]
    return results


def run_queries_parallel(
    endpoint_url: str,
    batch_queries: list[str],
    batch_sizes: list[int],
    workers: int = QLEVER_MAX_WORKERS,
    progress_callback: Callable[[int], None] | None = None,
    max_retries: int = 5,
    backoff_factor: int = 5,
) -> list[list]:
    if not batch_queries:
        return []

    all_bindings: list[list] = []

    if len(batch_queries) > 1 and workers > 1:
        query_groups: list[list[str]] = []
        grouped_sizes: list[int] = []
        for i in range(0, len(batch_queries), QLEVER_QUERIES_PER_GROUP):
            query_groups.append(batch_queries[i:i + QLEVER_QUERIES_PER_GROUP])
            grouped_sizes.append(sum(batch_sizes[i:i + QLEVER_QUERIES_PER_GROUP]))

        with ProcessPoolExecutor(
            max_workers=min(len(query_groups), workers),
            mp_context=multiprocessing.get_context('forkserver')
        ) as executor:
            future_to_size = {
                executor.submit(
                    execute_sparql_queries,
                    endpoint_url=endpoint_url,
                    queries=group,
                    max_retries=max_retries,
                    backoff_factor=backoff_factor,
                ): size
                for group, size in zip(query_groups, grouped_sizes)
            }
            for future in as_completed(future_to_size):
                all_bindings.extend(future.result())
                if progress_callback:
                    progress_callback(future_to_size[future])
    else:
        results = execute_sparql_queries(
            endpoint_url=endpoint_url,
            queries=batch_queries,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
        all_bindings.extend(results)
        if progress_callback:
            progress_callback(sum(batch_sizes))

    return all_bindings
