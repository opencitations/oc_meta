"""
Utility functions for safe SPARQL query execution with proper connection management.

This module provides helpers to prevent HTTP connection leaks when using SPARQLWrapper.
"""

import contextlib
import time
from typing import Any

from SPARQLWrapper import SPARQLWrapper


def safe_sparql_query(sparql: SPARQLWrapper) -> Any:
    """
    Execute a SPARQL query and ensure the HTTP connection is properly closed.

    This function wraps SPARQLWrapper.query() to guarantee that the underlying
    HTTP response is closed, preventing connection leaks.

    Args:
        sparql: Configured SPARQLWrapper instance with query already set

    Returns:
        The converted query result (format depends on SPARQLWrapper configuration)

    Raises:
        Any exception raised by the SPARQL query execution

    Example:
        >>> from SPARQLWrapper import SPARQLWrapper, JSON
        >>> sparql = SPARQLWrapper("http://localhost:8890/sparql")
        >>> sparql.setReturnFormat(JSON)
        >>> sparql.setQuery("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        >>> results = safe_sparql_query(sparql)
    """
    result = sparql.query()
    with contextlib.closing(result.response):
        converted = result.convert()
        # Read any remaining response data to ensure proper connection closure
        result.response.read()
        return converted


def safe_sparql_query_with_retry(
    sparql: SPARQLWrapper,
    max_retries: int = 3,
    backoff_base: float = 0.3,
    backoff_exponential: bool = True
) -> Any:
    """
    Execute a SPARQL query with retry logic and proper connection management.

    This function wraps safe_sparql_query() with exponential backoff retry logic.
    Useful for unreliable network conditions or busy triplestore endpoints.

    Args:
        sparql: Configured SPARQLWrapper instance with query already set
        max_retries: Maximum number of retry attempts (default: 3)
        backoff_base: Base wait time in seconds (default: 0.3)
        backoff_exponential: If True, use exponential backoff (2^attempt * base).
                            If False, use linear backoff (base * attempt) (default: True)

    Returns:
        The converted query result

    Raises:
        The last exception encountered if all retries fail

    Example:
        >>> sparql = SPARQLWrapper("http://localhost:8890/sparql")
        >>> sparql.setQuery("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        >>> results = safe_sparql_query_with_retry(sparql, max_retries=5)
    """

    last_exception = None
    for attempt in range(max_retries):
        try:
            return safe_sparql_query(sparql)
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                if backoff_exponential:
                    wait_time = backoff_base * (2 ** attempt)
                else:
                    wait_time = backoff_base
                time.sleep(wait_time)
            else:
                raise last_exception
