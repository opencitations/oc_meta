# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from oc_meta.lib.sparql import execute_sparql

PRO = "http://purl.org/spar/pro/"
OCO = "https://w3id.org/oc/ontology/"


def has_next_chain_issues(endpoint: str, br_pattern: str) -> list[str]:
    """Return corruption messages for the ``oco:hasNext`` chains of the agent
    roles held by the bibliographic resources matched by ``br_pattern``, a
    SPARQL group binding ``?br``. A healthy chain is a simple path: no cycles,
    at most one successor per role, at most one predecessor per role and one
    head per author/editor role type."""
    issues: list[str] = []

    cycle_query = f"""
    PREFIX pro: <{PRO}>
    PREFIX oco: <{OCO}>
    SELECT DISTINCT ?ar WHERE {{
        {br_pattern}
        ?br pro:isDocumentContextFor ?ar .
        ?ar oco:hasNext+ ?ar .
    }}
    """
    results = execute_sparql(endpoint, cycle_query, max_retries=3, backoff_factor=1)
    for result in results["results"]["bindings"]:
        issues.append(
            f"Agent role {result['ar']['value']} is part of an oco:hasNext cycle"
        )

    fork_query = f"""
    PREFIX pro: <{PRO}>
    PREFIX oco: <{OCO}>
    SELECT ?ar (COUNT(DISTINCT ?next) AS ?count) WHERE {{
        {br_pattern}
        ?br pro:isDocumentContextFor ?ar .
        ?ar oco:hasNext ?next .
    }}
    GROUP BY ?ar
    HAVING (COUNT(DISTINCT ?next) > 1)
    """
    results = execute_sparql(endpoint, fork_query, max_retries=3, backoff_factor=1)
    for result in results["results"]["bindings"]:
        issues.append(
            f"Agent role {result['ar']['value']} has "
            f"{result['count']['value']} oco:hasNext successors"
        )

    shared_next_query = f"""
    PREFIX pro: <{PRO}>
    PREFIX oco: <{OCO}>
    SELECT ?next (COUNT(DISTINCT ?ar) AS ?count) WHERE {{
        {br_pattern}
        ?br pro:isDocumentContextFor ?ar .
        ?ar oco:hasNext ?next .
    }}
    GROUP BY ?next
    HAVING (COUNT(DISTINCT ?ar) > 1)
    """
    results = execute_sparql(
        endpoint, shared_next_query, max_retries=3, backoff_factor=1
    )
    for result in results["results"]["bindings"]:
        issues.append(
            f"Agent role {result['next']['value']} has "
            f"{result['count']['value']} oco:hasNext predecessors"
        )

    disconnected_chain_query = f"""
    PREFIX pro: <{PRO}>
    PREFIX oco: <{OCO}>
    SELECT ?br ?roleType (COUNT(DISTINCT ?ar) AS ?count) WHERE {{
        {br_pattern}
        ?br pro:isDocumentContextFor ?ar .
        ?ar pro:withRole ?roleType .
        FILTER (?roleType IN (pro:author, pro:editor))
        FILTER NOT EXISTS {{
            ?previous oco:hasNext ?ar .
            ?br pro:isDocumentContextFor ?previous .
            ?previous pro:withRole ?roleType .
        }}
    }}
    GROUP BY ?br ?roleType
    HAVING (COUNT(DISTINCT ?ar) > 1)
    """
    results = execute_sparql(
        endpoint, disconnected_chain_query, max_retries=3, backoff_factor=1
    )
    for result in results["results"]["bindings"]:
        role_type = result["roleType"]["value"].split("/")[-1]
        issues.append(
            f"Bibliographic resource {result['br']['value']} has "
            f"{result['count']['value']} disconnected {role_type} chains"
        )

    return issues
