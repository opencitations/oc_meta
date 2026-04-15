# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable

from sparqlite import SPARQLClient

from oc_meta.constants import QLEVER_MAX_WORKERS, QLEVER_QUERIES_PER_GROUP


def execute_sparql_queries(
    endpoint_url: str,
    queries: list[str],
    max_retries: int = 5,
    backoff_factor: int = 5,
) -> list:
    results = []
    with SPARQLClient(endpoint_url, max_retries=max_retries, backoff_factor=backoff_factor, timeout=3600) as client:
        for query in queries:
            result = client.query(query)
            results.append(result['results']['bindings'] if result else [])
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
