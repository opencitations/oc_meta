#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import quote

import yaml
from oc_ds_converter.oc_idmanager.support import call_api
from oc_ocdm.graph import GraphSet
from oc_ocdm.support import get_prefix
from rapidfuzz.distance import Levenshtein
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.sparql import execute_sparql

DATACITE_DOI = "http://purl.org/spar/datacite/doi"
DATACITE_ISSN = "http://purl.org/spar/datacite/issn"
DATACITE_HAS_ID = "http://purl.org/spar/datacite/hasIdentifier"
DATACITE_USES_SCHEME = "http://purl.org/spar/datacite/usesIdentifierScheme"
LITERAL_HAS_VALUE = (
    "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
)
DCTERMS_TITLE = "http://purl.org/dc/terms/title"
PRISM_PUB_DATE = "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
PRISM_START_PAGE = "http://prismstandard.org/namespaces/basic/2.0/startingPage"
PRISM_END_PAGE = "http://prismstandard.org/namespaces/basic/2.0/endingPage"
FRBR_PART_OF = "http://purl.org/vocab/frbr/core#partOf"
FRBR_EMBODIMENT = "http://purl.org/vocab/frbr/core#embodiment"
FABIO_HAS_SEQ_ID = "http://purl.org/spar/fabio/hasSequenceIdentifier"
FABIO_JOURNAL_VOLUME = "http://purl.org/spar/fabio/JournalVolume"
FABIO_JOURNAL_ISSUE = "http://purl.org/spar/fabio/JournalIssue"
PRO_IS_DOC_CONTEXT = "http://purl.org/spar/pro/isDocumentContextFor"
PRO_WITH_ROLE = "http://purl.org/spar/pro/withRole"
PRO_IS_HELD_BY = "http://purl.org/spar/pro/isHeldBy"
PRO_AUTHOR = "http://purl.org/spar/pro/author"
FOAF_FAMILY_NAME = "http://xmlns.com/foaf/0.1/familyName"
FOAF_GIVEN_NAME = "http://xmlns.com/foaf/0.1/givenName"
OCO_HAS_NEXT = "https://w3id.org/oc/ontology/hasNext"

CROSSREF_API = "https://api.crossref.org/works/"
CROSSREF_MAILTO = "arcangelo.massari@unibo.it"
CROSSREF_HEADERS = {
    "Accept": "application/json",
    "User-Agent": f"oc_meta/mailto:{CROSSREF_MAILTO}",
}
CROSSREF_RATE_LIMIT = 50
MATCHING_THRESHOLD = 25.0

_stop_requested = False


def _handle_signal(_signum: int, _frame: object) -> None:
    global _stop_requested
    _stop_requested = True
    console.print("[yellow]Interrupt received, finishing current operation...[/yellow]")


def _sparql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


@dataclass
class CorrectionCase:
    truncated_doi: str
    candidate_doi: str
    duplicate_entity: str
    surviving_entity: str
    duplicate_id_entity: str | None = None
    action: str = ""
    matching_score: float = 0.0
    is_1_to_n: bool = False
    all_expected_omids: list[str] = field(default_factory=list)
    reason: str = ""


def extract_sici_mismatch_errors(errors: list[dict]) -> list[dict]:
    return [
        e for e in errors
        if e["type"] == "omid_mismatch"
        and e["schema"] == "doi"
        and e["value"].lower().endswith("co;2-")
    ]


def build_sici_cases(errors: list[dict]) -> list[CorrectionCase]:
    found_to_errors: dict[str, list[dict]] = defaultdict(list)
    for error in errors:
        found_to_errors[error["found_omids"][0]].append(error)

    cases = []
    for found_omid, group in found_to_errors.items():
        first = group[0]
        all_expected = [e["expected_omid"] for e in group]
        cases.append(
            CorrectionCase(
                truncated_doi=first["value"],
                candidate_doi=first["value"] + "#",
                duplicate_entity=found_omid,
                surviving_entity=first["expected_omid"],
                is_1_to_n=len(group) > 1,
                all_expected_omids=all_expected,
                reason=f"{len(group)} expected_omid(s)"
                if len(group) > 1
                else "",
            )
        )
    return cases



def fetch_crossref_metadata(
    doi: str, cache: dict[str, dict | None]
) -> dict | None:
    if doi in cache:
        return cache[doi]
    time.sleep(1.0 / CROSSREF_RATE_LIMIT)
    url = CROSSREF_API + quote(doi, safe="")
    response = call_api(url=url, headers=CROSSREF_HEADERS)
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


def _find_id_entity(endpoint: str, br_uri: str, doi_value: str) -> str | None:
    escaped = _sparql_escape(doi_value)
    query = f"""
        SELECT ?id_entity WHERE {{
            <{br_uri}> <{DATACITE_HAS_ID}> ?id_entity .
            ?id_entity <{DATACITE_USES_SCHEME}> <{DATACITE_DOI}> .
            ?id_entity <{LITERAL_HAS_VALUE}> "{escaped}" .
        }}
        LIMIT 1
    """
    result = execute_sparql(endpoint, query)
    bindings = result["results"]["bindings"]
    return bindings[0]["id_entity"]["value"] if bindings else None


def _find_entity_by_doi(endpoint: str, doi_value: str) -> str | None:
    escaped = _sparql_escape(doi_value)
    query = f"""
        SELECT ?br_entity WHERE {{
            ?id_entity <{LITERAL_HAS_VALUE}> "{escaped}" .
            ?id_entity <{DATACITE_USES_SCHEME}> <{DATACITE_DOI}> .
            ?br_entity <{DATACITE_HAS_ID}> ?id_entity .
        }}
        LIMIT 1
    """
    result = execute_sparql(endpoint, query)
    bindings = result["results"]["bindings"]
    return bindings[0]["br_entity"]["value"] if bindings else None


def determine_actions(
    cases: list[CorrectionCase],
    endpoint: str,
) -> list[CorrectionCase]:
    crossref_cache: dict[str, dict | None] = {}

    with create_progress() as progress:
        task = progress.add_task(
            "Fetching metadata and computing scores", total=len(cases)
        )

        for case in cases:
            if _stop_requested:
                break

            crossref_meta = fetch_crossref_metadata(
                case.candidate_doi, crossref_cache
            )
            if crossref_meta is None:
                case.action = "manual_review"
                case.reason += " Candidate DOI not found on Crossref."
                progress.advance(task)
                continue

            ts_meta = fetch_triplestore_metadata(endpoint, case.duplicate_entity)
            if not ts_meta:
                case.action = "manual_review"
                case.reason += " No metadata found in triplestore for entity."
                progress.advance(task)
                continue

            case.matching_score = compute_matching_score(ts_meta, crossref_meta)

            if case.matching_score < MATCHING_THRESHOLD:
                case.action = "manual_review"
                case.reason += f" Matching score {case.matching_score:.1f} below threshold {MATCHING_THRESHOLD}."
                progress.advance(task)
                continue

            _resolve_id_entity(case, endpoint)
            case.action = "merge"
            progress.advance(task)

    return cases


def _resolve_id_entity(case: CorrectionCase, endpoint: str) -> None:
    if not case.duplicate_id_entity:
        case.duplicate_id_entity = _find_id_entity(
            endpoint, case.duplicate_entity, case.truncated_doi
        )


def build_report(cases: list[CorrectionCase]) -> dict:
    actions: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        entry = {
            "truncated_doi": case.truncated_doi,
            "correct_doi": case.candidate_doi,
            "duplicate_entity": case.duplicate_entity,
            "duplicate_id_entity": case.duplicate_id_entity,
            "surviving_entity": case.surviving_entity,
            "matching_score": round(case.matching_score, 2),
            "reason": case.reason,
        }
        if case.is_1_to_n:
            entry["all_expected_omids"] = case.all_expected_omids
        actions[case.action].append(entry)

    merge_cases = [c for c in cases if c.action == "merge"]
    entities_removed = sum(
        1 + len(c.all_expected_omids[1:]) for c in merge_cases
    )

    return {
        "summary": {
            "total": len(cases),
            "auto_merge": len(actions.get("merge", [])),
            "manual_review": len(actions.get("manual_review", [])),
            "entities_removed": entities_removed,
        },
        "merge": actions.get("merge", []),
        "manual_review": actions.get("manual_review", []),
    }


def _load_progress(path: str) -> set[str]:
    if os.path.exists(path):
        with open(path) as f:
            return set(json.load(f))
    return set()


def _save_progress(path: str, completed: set[str]) -> None:
    with open(path, "w") as f:
        json.dump(sorted(completed), f)


def _execute_merge(editor: MetaEditor, case: CorrectionCase) -> None:
    supplier_prefix = get_prefix(case.surviving_entity)
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=editor.counter_handler,
    )
    entities_to_merge = [case.duplicate_entity, *case.all_expected_omids[1:]]
    for other in entities_to_merge:
        editor.merge(g_set, case.surviving_entity, other)
    editor.save(g_set, supplier_prefix)

    if case.duplicate_id_entity:
        editor.delete(
            case.surviving_entity,
            DATACITE_HAS_ID,
            case.duplicate_id_entity,
        )
        editor.delete(case.duplicate_id_entity)


def execute_actions(
    cases: list[CorrectionCase],
    config_path: str,
    resp_agent: str,
    progress_file: str,
) -> tuple[int, int]:
    actionable = [c for c in cases if c.action == "merge"]
    if not actionable:
        console.print("[green]No actions to execute.[/green]")
        return 0, 0

    editor = MetaEditor(config_path, resp_agent)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    completed = _load_progress(progress_file)
    if completed:
        console.print(f"Resuming: {len(completed)} already processed")

    fixed = 0
    failed = 0

    with create_progress() as progress:
        task = progress.add_task("Applying corrections", total=len(actionable))
        for case in actionable:
            if _stop_requested:
                console.print(
                    "[yellow]Interrupted, saving progress...[/yellow]"
                )
                break

            case_key = f"{case.action}:{case.duplicate_entity}"
            if case_key in completed:
                progress.advance(task)
                continue

            try:
                _execute_merge(editor, case)

                completed.add(case_key)
                _save_progress(progress_file, completed)
                fixed += 1
            except Exception as e:
                console.print(
                    f"[red]Error processing {case.duplicate_entity}: {e}[/red]"
                )
                failed += 1

            progress.advance(task)

    console.print(f"Fixed: {fixed}, Failed: {failed}")

    if not _stop_requested and failed == 0 and os.path.exists(progress_file):
        os.remove(progress_file)

    return fixed, failed


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Fix DOIs corrupted by oc_ds_converter suffix_regex bug",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to meta_config.yaml"
    )
    parser.add_argument(
        "--check-results",
        required=True,
        help="Path to check_results.json",
    )
    parser.add_argument("-r", "--resp-agent", help="Responsible agent URI")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Report only, no modifications",
    )
    mode.add_argument(
        "--no-dry-run",
        action="store_true",
        dest="no_dry_run",
        help="Apply corrections to triplestore and RDF files",
    )
    parser.add_argument(
        "--report-file",
        default="fix_corrupted_dois_report.json",
        help="Output report path",
    )
    parser.add_argument(
        "--progress-file",
        default="fix_corrupted_dois_progress.json",
        help="Progress tracking file for resumability",
    )
    args = parser.parse_args()
    args.dry_run = not args.no_dry_run

    if not args.dry_run and not args.resp_agent:
        parser.error("--resp-agent is required when using --no-dry-run")

    with open(args.config) as f:
        settings = yaml.safe_load(f)

    if settings.get("rdf_files_only", False):
        raise ValueError(
            "rdf_files_only must be False: this script must update the triplestore "
            "directly so it remains the single source of truth and every case reads "
            "consistent state."
        )

    endpoint = settings["triplestore_url"]

    console.print("[bold]Loading check_results.json...[/bold]")
    with open(args.check_results) as f:
        check_data = json.load(f)

    sici_errors = extract_sici_mismatch_errors(check_data["errors"])
    console.print(f"  SICI mismatch errors: {len(sici_errors)}")

    console.print("\n[bold]Building correction cases...[/bold]")
    cases = build_sici_cases(sici_errors)
    console.print(f"  Unique cases: {len(cases)}")

    if not cases:
        console.print("[green]No correction cases found.[/green]")
        return

    console.print("\n[bold]Fetching metadata and computing matching scores...[/bold]")
    cases = determine_actions(cases, endpoint)

    report = build_report(cases)
    with open(args.report_file, "w") as f:
        json.dump(report, f, indent=2)

    summary = report["summary"]
    console.print("\n[bold]Summary:[/bold]")
    console.print(f"  Total cases: {summary['total']}")
    console.print(f"  Auto merge: {summary['auto_merge']}")
    console.print(f"  Manual review: {summary['manual_review']}")
    console.print(f"  Entities to be removed: {summary['entities_removed']}")
    console.print(f"\nReport: {args.report_file}")

    if not args.dry_run:
        console.print("\n[bold]Applying corrections...[/bold]")
        execute_actions(
            cases, args.config, args.resp_agent, args.progress_file
        )
    else:
        actionable = sum(1 for c in cases if c.action == "merge")
        console.print(
            f"\n[dim]Dry run complete. {actionable} corrections pending."
            f" Use --no-dry-run to apply.[/dim]"
        )


if __name__ == "__main__":
    main()
