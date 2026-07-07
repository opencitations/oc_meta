# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

"""Definition of the set of entities a merge touches.

Merging bibliographic resources cascades onto their ``frbr:partOf`` containers
(issue, volume, journal). When a container is merged it is deleted and every
reference to it must be redirected, otherwise entities left outside the loaded
graph keep pointing at a deleted resource. The closure therefore contains:

- the entities being merged (the seeds);
- their full ``frbr:partOf`` ancestor chain;
- the one-hop neighbourhood (both directions, plus responsible-agent role
  context) of that set, which brings in the siblings and children that refer to
  any container that could be deleted;
- every agent role attached to the bibliographic resources in the closure,
  together with the responsible agent and its identifiers.

The merge (:mod:`oc_meta.run.merge.entities`) uses it to decide what a batch
must import so that every entity it mutates is in memory.
"""

from __future__ import annotations

from typing import Iterable, List, Set

from oc_meta.lib.sparql import execute_sparql

FRBR_PART_OF = "http://purl.org/vocab/frbr/core#partOf"
PRO_IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"
PRO_IS_DOCUMENT_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
PRO_WITH_ROLE = "http://purl.org/spar/pro/withRole"
PRO_PUBLISHER = "http://purl.org/spar/pro/publisher"
DATACITE_HAS_IDENTIFIER = "http://purl.org/spar/datacite/hasIdentifier"


def _batched(items: List[str], batch_size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def _partof_ancestors(endpoint: str, seeds: Set[str], batch_size: int) -> Set[str]:
    ancestors: Set[str] = set()
    frontier = list(seeds)
    while frontier:
        parents: Set[str] = set()
        for batch in _batched(frontier, batch_size):
            clauses = " UNION ".join(
                f"{{<{uri}> <{FRBR_PART_OF}> ?ancestor}}" for uri in batch
            )
            query = f"SELECT DISTINCT ?ancestor WHERE {{ {clauses} }}"
            results = execute_sparql(endpoint, query, max_retries=5, backoff_factor=0.3)
            for result in results["results"]["bindings"]:
                if result["ancestor"]["type"] == "uri":
                    parents.add(result["ancestor"]["value"])
        frontier = list(parents - ancestors - seeds)
        ancestors |= parents
    return ancestors - seeds


def _one_hop_neighbours(endpoint: str, entities: Set[str], batch_size: int) -> Set[str]:
    neighbours: Set[str] = set()
    ordered = list(entities)
    for batch in _batched(ordered, batch_size):
        subject_clauses = []
        object_clauses = []
        agent_context_clauses = []
        for uri in batch:
            subject_clauses.append(f"{{?entity ?p <{uri}>}}")
            object_clauses.append(f"{{<{uri}> ?p ?entity}}")
            agent_context_clauses.append(f"{{?entity pro:isHeldBy <{uri}>}}")
            agent_context_clauses.append(
                f"{{?agent_role pro:isHeldBy <{uri}> . ?entity pro:isDocumentContextFor ?agent_role}}"
            )

        query = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?entity WHERE {{
                {{
                    {{
                        {" UNION ".join(subject_clauses + object_clauses)}
                    }}
                    FILTER (?p != rdf:type)
                    FILTER (?p != datacite:usesIdentifierScheme)
                    FILTER (?p != pro:withRole)
                }}
                UNION
                {{
                    {" UNION ".join(agent_context_clauses)}
                }}
                ?entity ?p2 ?o2 .
            }}
        """
        results = execute_sparql(endpoint, query, max_retries=5, backoff_factor=0.3)
        for result in results["results"]["bindings"]:
            if result["entity"]["type"] == "uri":
                neighbours.add(result["entity"]["value"])
    return neighbours


def _publisher_agents(endpoint: str, entities: Set[str], batch_size: int) -> Set[str]:
    agents: Set[str] = set()
    ordered = list(entities)
    for batch in _batched(ordered, batch_size):
        role_clauses = " UNION ".join(
            f"{{<{uri}> <{PRO_WITH_ROLE}> <{PRO_PUBLISHER}> . <{uri}> <{PRO_IS_HELD_BY}> ?agent}}"
            for uri in batch
        )
        query = f"""
            SELECT DISTINCT ?agent ?identifier WHERE {{
                {{ {role_clauses} }}
                OPTIONAL {{ ?agent <{DATACITE_HAS_IDENTIFIER}> ?identifier }}
            }}
        """
        results = execute_sparql(endpoint, query, max_retries=5, backoff_factor=0.3)
        for result in results["results"]["bindings"]:
            if result["agent"]["type"] == "uri":
                agents.add(result["agent"]["value"])
            if "identifier" in result and result["identifier"]["type"] == "uri":
                agents.add(result["identifier"]["value"])
    return agents


def _br_role_context(endpoint: str, entities: Set[str], batch_size: int) -> Set[str]:
    context: Set[str] = set()
    ordered = list(entities)
    for batch in _batched(ordered, batch_size):
        role_clauses = " UNION ".join(
            f"{{<{uri}> <{PRO_IS_DOCUMENT_CONTEXT_FOR}> ?role}}" for uri in batch
        )
        query = f"""
            SELECT DISTINCT ?entity WHERE {{
                {{
                    {role_clauses}
                    BIND(?role AS ?entity)
                }}
                UNION
                {{
                    {role_clauses}
                    ?role <{PRO_IS_HELD_BY}> ?entity .
                }}
                UNION
                {{
                    {role_clauses}
                    ?role <{PRO_IS_HELD_BY}> ?agent .
                    ?agent <{DATACITE_HAS_IDENTIFIER}> ?entity .
                }}
                ?entity ?p ?o .
            }}
        """
        results = execute_sparql(endpoint, query, max_retries=5, backoff_factor=0.3)
        for result in results["results"]["bindings"]:
            if result["entity"]["type"] == "uri":
                context.add(result["entity"]["value"])
    return context


def compute_related_closure(
    endpoint: str, entities: Iterable[str], batch_size: int = 10
) -> Set[str]:
    """Return every entity touched by merging ``entities`` (seeds included).

    The result is the seeds, their ``frbr:partOf`` ancestors, the one-hop
    neighbourhood of that set, every role attached to loaded bibliographic
    resources, and the responsible agents (with their identifiers) behind those
    roles. It bounds the cascade: ancestors are followed only through
    ``frbr:partOf`` (issue -> volume -> journal), the neighbourhood is expanded
    a single time, so citation chains are not traversed.
    """
    seeds = set(entities)
    core = seeds | _partof_ancestors(endpoint, seeds, batch_size)
    related = core | _one_hop_neighbours(endpoint, core, batch_size)
    role_context = _br_role_context(endpoint, related, batch_size)
    return (
        related
        | role_context
        | _publisher_agents(endpoint, related | role_context, batch_size)
    )
