# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import time
from urllib.parse import quote

from oc_ds_converter.oc_idmanager.support import call_api
from oc_ocdm.graph.graph_entity import GraphEntity
from rapidfuzz.distance import Levenshtein

from oc_meta.lib.sparql import execute_sparql

DATACITE_DOI = GraphEntity.iri_doi
DATACITE_ISSN = GraphEntity.iri_issn
DATACITE_HAS_ID = GraphEntity.iri_has_identifier
DATACITE_USES_SCHEME = GraphEntity.iri_uses_identifier_scheme
LITERAL_HAS_VALUE = GraphEntity.iri_has_literal_value
DCTERMS_TITLE = GraphEntity.iri_title
PRISM_PUB_DATE = GraphEntity.iri_has_publication_date
PRISM_START_PAGE = GraphEntity.iri_starting_page
PRISM_END_PAGE = GraphEntity.iri_ending_page
FRBR_PART_OF = GraphEntity.iri_part_of
FRBR_EMBODIMENT = GraphEntity.iri_embodiment
FABIO_HAS_SEQ_ID = GraphEntity.iri_has_sequence_identifier
FABIO_JOURNAL_VOLUME = GraphEntity.iri_journal_volume
FABIO_JOURNAL_ISSUE = GraphEntity.iri_journal_issue
PRO_IS_DOC_CONTEXT = GraphEntity.iri_is_document_context_for
PRO_WITH_ROLE = GraphEntity.iri_with_role
PRO_IS_HELD_BY = GraphEntity.iri_is_held_by
PRO_AUTHOR = GraphEntity.iri_author
FOAF_FAMILY_NAME = GraphEntity.iri_family_name
FOAF_GIVEN_NAME = GraphEntity.iri_given_name
OCO_HAS_NEXT = GraphEntity.iri_has_next

CROSSREF_API = "https://api.crossref.org/works/"
CROSSREF_RATE_LIMIT = 50
MATCHING_THRESHOLD = 25.0


def crossref_headers(mailto: str) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": f"oc_meta/mailto:{mailto}",
    }


def fetch_crossref_metadata(
    doi: str, cache: dict[str, dict | None], mailto: str
) -> dict | None:
    if doi in cache:
        return cache[doi]
    time.sleep(1.0 / CROSSREF_RATE_LIMIT)
    url = CROSSREF_API + quote(doi, safe="")
    response = call_api(url=url, headers=crossref_headers(mailto))
    if response is None or not isinstance(response, dict):
        cache[doi] = None
        return None
    msg: dict = response["message"]  # type: ignore[assignment]
    titles: list = msg.get("title", [])
    authors: list = msg.get("author", [])
    first_author: dict = authors[0] if authors else {}
    date_parts: list | None = msg.get("issued", {}).get("date-parts")
    year = str(date_parts[0][0]) if date_parts and date_parts[0] else ""
    page: str = msg.get("page", "")
    pages = page.split("-", 1) if page else []
    meta = {
        "title": titles[0].lower().strip() if titles else "",
        "first_author_family": first_author.get("family", "").lower().strip(),
        "first_author_given": first_author.get("given", "").strip(),
        "year": year,
        "venue": (msg.get("container-title") or [""])[0].lower().strip(),
        "issn": (msg.get("ISSN") or [""])[0],
        "volume": msg.get("volume", ""),
        "issue": msg.get("issue", ""),
        "start_page": pages[0].strip() if pages else "",
        "end_page": pages[1].strip() if len(pages) > 1 else "",
    }
    cache[doi] = meta
    return meta


def fetch_triplestore_metadata(endpoint: str, br_uri: str) -> dict:
    query = f"""
        SELECT ?title ?date ?venue_title ?venue_issn ?volume ?issue
               ?start_page ?end_page
               ?ar ?author_family ?author_given ?ar_next WHERE {{
            OPTIONAL {{ <{br_uri}> <{DCTERMS_TITLE}> ?title }}
            OPTIONAL {{ <{br_uri}> <{PRISM_PUB_DATE}> ?date }}
            OPTIONAL {{
                <{br_uri}> <{FRBR_PART_OF}>+ ?venue .
                ?venue <{DCTERMS_TITLE}> ?venue_title .
                OPTIONAL {{
                    ?venue <{DATACITE_HAS_ID}> ?venue_id_ent .
                    ?venue_id_ent <{DATACITE_USES_SCHEME}> <{DATACITE_ISSN}> .
                    ?venue_id_ent <{LITERAL_HAS_VALUE}> ?venue_issn .
                }}
            }}
            OPTIONAL {{
                <{br_uri}> <{FRBR_PART_OF}>* ?vol_parent .
                ?vol_parent <{FRBR_PART_OF}> ?vol_container .
                ?vol_container a <{FABIO_JOURNAL_VOLUME}> .
                ?vol_container <{FABIO_HAS_SEQ_ID}> ?volume .
            }}
            OPTIONAL {{
                <{br_uri}> <{FRBR_PART_OF}>* ?iss_parent .
                ?iss_parent <{FRBR_PART_OF}> ?iss_container .
                ?iss_container a <{FABIO_JOURNAL_ISSUE}> .
                ?iss_container <{FABIO_HAS_SEQ_ID}> ?issue .
            }}
            OPTIONAL {{
                <{br_uri}> <{FRBR_EMBODIMENT}> ?re .
                ?re <{PRISM_START_PAGE}> ?start_page .
                OPTIONAL {{ ?re <{PRISM_END_PAGE}> ?end_page }}
            }}
            OPTIONAL {{
                <{br_uri}> <{PRO_IS_DOC_CONTEXT}> ?ar .
                ?ar <{PRO_WITH_ROLE}> <{PRO_AUTHOR}> .
                ?ar <{PRO_IS_HELD_BY}> ?ra .
                ?ra <{FOAF_FAMILY_NAME}> ?author_family .
                OPTIONAL {{ ?ra <{FOAF_GIVEN_NAME}> ?author_given }}
                OPTIONAL {{ ?ar <{OCO_HAS_NEXT}> ?ar_next }}
            }}
        }}
    """
    result = execute_sparql(endpoint, query)
    bindings = result["results"]["bindings"]
    if not bindings:
        return {}

    first = bindings[0]
    _val = _binding_value

    ar_to_family: dict[str, str] = {}
    ar_to_given: dict[str, str] = {}
    ar_to_next: dict[str, str] = {}
    for row in bindings:
        ar = _val(row, "ar")
        if ar:
            family = _val(row, "author_family")
            if family:
                ar_to_family[ar] = family
            given = _val(row, "author_given")
            if given:
                ar_to_given[ar] = given
            nxt = _val(row, "ar_next")
            if nxt:
                ar_to_next[ar] = nxt

    first_author_family = ""
    first_author_given = ""
    if ar_to_family:
        pointed_to = set(ar_to_next.values())
        first_ar_candidates = [ar for ar in ar_to_family if ar not in pointed_to]
        first_ar = first_ar_candidates[0] if first_ar_candidates else next(iter(ar_to_family))
        first_author_family = ar_to_family[first_ar].lower().strip()
        first_author_given = ar_to_given.get(first_ar, "").strip()

    year = _val(first, "date") or ""
    if year and len(year) >= 4:
        year = year[:4]

    return {
        "title": (_val(first, "title") or "").lower().strip(),
        "first_author_family": first_author_family,
        "first_author_given": first_author_given,
        "year": year,
        "venue": (_val(first, "venue_title") or "").lower().strip(),
        "issn": _val(first, "venue_issn") or "",
        "volume": _val(first, "volume") or "",
        "issue": _val(first, "issue") or "",
        "start_page": _val(first, "start_page") or "",
        "end_page": _val(first, "end_page") or "",
    }


def _binding_value(row: dict, key: str) -> str:
    return row[key]["value"] if key in row else ""


def compute_matching_score(meta_a: dict, meta_b: dict) -> float:
    m_first_author = _score_first_author(
        meta_a["first_author_family"],
        meta_b["first_author_family"],
        meta_a["first_author_given"],
        meta_b["first_author_given"],
    )
    m_title = _score_title(meta_a["title"], meta_b["title"])
    m_source = _score_source(
        meta_a["venue"], meta_b["venue"],
        meta_a["issn"], meta_b["issn"],
    )
    m_other = _score_other(meta_a, meta_b)
    return 7 * m_first_author + 14 * m_title + 5 * m_source + 14 * m_other


def _score_first_author(
    family_a: str, family_b: str, given_a: str, given_b: str
) -> float:
    if not family_a or not family_b:
        return 0.0
    max_len = max(len(family_a), len(family_b))
    dist = Levenshtein.distance(family_a, family_b)
    family_sim = 0.8 * (1.0 - dist / max_len)
    initial_a = given_a[0].lower() if given_a else ""
    initial_b = given_b[0].lower() if given_b else ""
    initial_match = 0.2 if (initial_a and initial_b and initial_a == initial_b) else 0.0
    return family_sim + initial_match


def _score_title(title_a: str, title_b: str) -> float:
    if not title_a or not title_b:
        return 0.0
    max_len = max(len(title_a), len(title_b))
    dist = Levenshtein.distance(title_a, title_b)
    return 1.0 - dist / max_len


def _score_source(
    venue_a: str, venue_b: str, issn_a: str, issn_b: str
) -> float:
    if issn_a and issn_b and issn_a == issn_b:
        return 1.0
    if not venue_a or not venue_b:
        return 0.0
    min_len = min(len(venue_a), len(venue_b))
    if min_len == 0:
        return 0.0
    dist = Levenshtein.distance(venue_a, venue_b)
    len_diff = abs(len(venue_a) - len(venue_b))
    score = 1.0 - (dist - len_diff) / min_len
    return max(score, 0.0)


def _score_other(meta_a: dict, meta_b: dict) -> float:
    score = 0.0
    if meta_a["year"] and meta_b["year"] and meta_a["year"] == meta_b["year"]:
        score += 0.1
    if meta_a["volume"] and meta_b["volume"] and meta_a["volume"] == meta_b["volume"]:
        score += 0.2
    if meta_a["issue"] and meta_b["issue"] and meta_a["issue"] == meta_b["issue"]:
        score += 0.1
    if meta_a["start_page"] and meta_b["start_page"] and meta_a["start_page"] == meta_b["start_page"]:
        score += 0.3
    if meta_a["end_page"] and meta_b["end_page"] and meta_a["end_page"] == meta_b["end_page"]:
        score += 0.3
    return score
