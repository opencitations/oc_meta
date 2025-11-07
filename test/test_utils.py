#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022-2025 OpenCitations
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

import random
import subprocess
import time

import redis
from SPARQLWrapper import JSON, SPARQLWrapper

# Common constants
SERVER = "http://127.0.0.1:8805/sparql"
PROV_SERVER = "http://127.0.0.1:8806/sparql"
VIRTUOSO_CONTAINER = "oc-meta-test-virtuoso"
VIRTUOSO_PROV_CONTAINER = "oc-meta-test-virtuoso-prov"

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6381
REDIS_DB = 5  # For counters
REDIS_CACHE_DB = 2  # For cache


def reset_server() -> None:
    """
    Reset the SPARQL servers using Virtuoso's RDF_GLOBAL_RESET() via docker exec isql.
    """
    max_retries = 5
    base_delay = 2
    
    # Reset main triplestore
    main_command = [
        "docker", "exec", VIRTUOSO_CONTAINER,
        "/opt/virtuoso-opensource/bin/isql", "1111", "dba", "dba",
        "exec=RDF_GLOBAL_RESET();"
    ]
    
    # Reset provenance triplestore
    prov_command = [
        "docker", "exec", VIRTUOSO_PROV_CONTAINER,
        "/opt/virtuoso-opensource/bin/isql", "1111", "dba", "dba",
        "exec=RDF_GLOBAL_RESET();"
    ]

    # Reset main triplestore
    for attempt in range(max_retries):
        try:
            # Add small random delay to avoid race conditions
            time.sleep(base_delay + random.uniform(0, 1))

            result = subprocess.run(
                main_command,
                capture_output=True, # Use capture_output instead of stdout/stderr pipes
                text=True, # Decode output as text
                check=True, # Raise CalledProcessError on non-zero exit code
                timeout=20, # Increased timeout slightly
            )
            # If successful, break the loop
            break

        except subprocess.CalledProcessError as e:
            print(f"Error resetting main triplestore (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
            # Exponential backoff with jitter
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))
        
        except subprocess.TimeoutExpired:
            print(f"Timeout resetting main triplestore (attempt {attempt+1}/{max_retries})")
            if attempt == max_retries - 1:
                raise
            # Exponential backoff with jitter
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))

    # Reset provenance triplestore
    for attempt in range(max_retries):
        try:
            # Add small random delay to avoid race conditions
            time.sleep(base_delay + random.uniform(0, 1))

            result = subprocess.run(
                prov_command,
                capture_output=True, # Use capture_output instead of stdout/stderr pipes
                text=True, # Decode output as text
                check=True, # Raise CalledProcessError on non-zero exit code
                timeout=20, # Increased timeout slightly
            )
            # If successful, break the loop
            break

        except subprocess.CalledProcessError as e:
            print(f"Error resetting provenance triplestore (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                raise
            # Exponential backoff with jitter
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))
        
        except subprocess.TimeoutExpired:
            print(f"Timeout resetting provenance triplestore (attempt {attempt+1}/{max_retries})")
            if attempt == max_retries - 1:
                raise
            # Exponential backoff with jitter
            time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 1))


def reset_redis_counters():
    """
    Reset the Redis counters and cache databases.
    """
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_cache_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_CACHE_DB
    )
    redis_client.flushdb()
    redis_cache_client.flushdb()


def execute_sparql_query(endpoint, query, return_format=JSON, max_retries=3, delay=5):
    """
    Execute a SPARQL query with retry logic and better error handling.

    Args:
        endpoint (str): SPARQL endpoint URL
        query (str): SPARQL query to execute
        return_format: Query return format (JSON, XML etc)
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds

    Returns:
        Query results in specified format

    Raises:
        URLError: If connection fails after all retries
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(return_format)

    retry_count = 0
    last_error = None

    while retry_count < max_retries:
        try:
            sparql.setTimeout(30)  # Increase timeout
            return sparql.queryAndConvert()
        except Exception as e:
            last_error = e
            retry_count += 1
            if retry_count == max_retries:
                from urllib.error import URLError
                raise URLError(
                    f"Failed to connect to SPARQL endpoint after {max_retries} attempts: {str(last_error)}"
                )
            print(
                f"Connection attempt {retry_count} failed, retrying in {delay} seconds..."
            )
            time.sleep(delay)  # Increased delay between retries
