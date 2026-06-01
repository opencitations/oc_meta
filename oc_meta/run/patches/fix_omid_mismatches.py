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

import requests
import yaml
from oc_ocdm.graph import GraphSet
from oc_ocdm.support import get_prefix
from rapidfuzz.distance import Levenshtein
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.bibliographic_matching import (
    CROSSREF_RATE_LIMIT,
    DATACITE_DOI,
    DATACITE_HAS_ID,
    DATACITE_USES_SCHEME,
    LITERAL_HAS_VALUE,
    MATCHING_THRESHOLD,
    SPARSE_MATCHING_THRESHOLD,
    compute_matching_score,
    fetch_crossref_metadata,
    fetch_triplestore_metadata,
    is_sparse,
)
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.sparql import execute_sparql

VENUE_TITLE_THRESHOLD = 0.6

_stop_requested = False


def _handle_signal(_signum: int, _frame: object) -> None:
    global _stop_requested
    _stop_requested = True
    console.print("[yellow]Interrupt received, finishing current operation...[/yellow]")


def _sparql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


@dataclass
class MismatchCase:
    doi_value: str
    expected_entity: str
    found_entity: str
    column: str
    category: str = ""
    surviving_entity: str = ""
    duplicate_entities: list[str] = field(default_factory=list)
    duplicate_id_entities: list[str] = field(default_factory=list)
    expected_doi: str = ""
    found_doi: str = ""
    action: str = ""
    reason: str = ""
    validation: str = ""
    occurrence_count: int = 1


def _get_doi(endpoint: str, entity_uri: str) -> str:
    query = f"""
        SELECT ?val WHERE {{
            <{entity_uri}> <{DATACITE_HAS_ID}> ?id .
            ?id <{DATACITE_USES_SCHEME}> <{DATACITE_DOI}> .
            ?id <{LITERAL_HAS_VALUE}> ?val .
        }}
        LIMIT 1
    """
    result = execute_sparql(endpoint, query)
    bindings = result["results"]["bindings"]
    return bindings[0]["val"]["value"] if bindings else ""


def _find_id_entity(endpoint: str, br_uri: str, doi_value: str) -> str:
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
    return bindings[0]["id_entity"]["value"] if bindings else ""


def _find_entity_by_doi(endpoint: str, doi_value: str) -> str:
    escaped = _sparql_escape(doi_value)
    query = f"""
        SELECT ?entity WHERE {{
            ?id <{LITERAL_HAS_VALUE}> "{escaped}" .
            ?id <{DATACITE_USES_SCHEME}> <{DATACITE_DOI}> .
            ?entity <{DATACITE_HAS_ID}> ?id .
        }}
        LIMIT 1
    """
    result = execute_sparql(endpoint, query)
    bindings = result["results"]["bindings"]
    return bindings[0]["entity"]["value"] if bindings else ""


def load_errors(check_results_path: str) -> list[dict]:
    with open(check_results_path) as f:
        data = json.load(f)
    return [
        e for e in data["errors"]
        if e["type"] == "omid_mismatch" and e["schema"] == "doi"
    ]


def build_cases(errors: list[dict]) -> list[MismatchCase]:
    key_to_case: dict[str, MismatchCase] = {}
    for e in errors:
        found = e["found_omids"][0]
        key = f"{e['expected_omid']}|{found}"
        if key not in key_to_case:
            key_to_case[key] = MismatchCase(
                doi_value=e["value"],
                expected_entity=e["expected_omid"],
                found_entity=found,
                column=e["column"],
            )
        else:
            key_to_case[key].occurrence_count += 1
    return list(key_to_case.values())


def classify_and_resolve(
    cases: list[MismatchCase], endpoint: str
) -> list[MismatchCase]:
    with create_progress() as progress:
        task = progress.add_task("Classifying cases", total=len(cases))

        for case in cases:
            if _stop_requested:
                break

            case.expected_doi = _get_doi(endpoint, case.expected_entity)
            case.found_doi = _get_doi(endpoint, case.found_entity)

            if "sj....bjc" in case.doi_value:
                _classify_bjc(case, endpoint)
            elif "(asce)" in case.doi_value.lower():
                _classify_asce(case, endpoint)
            elif case.expected_doi.endswith("<"):
                _classify_angle_bracket(case, endpoint)
            elif (case.expected_doi.endswith(";")
                  and not case.found_doi.endswith(";")):
                case.category = "false_positive"
                case.action = "false_positive"
                case.reason = (
                    f"Semicolon is part of DOI '{case.expected_doi}'"
                )
            elif ("#" in case.expected_doi
                  and "#" not in case.found_doi):
                case.category = "false_positive"
                case.action = "false_positive"
                case.reason = (
                    f"Fragment is part of DOI '{case.expected_doi}'"
                )
            elif (case.expected_doi == case.doi_value + "."
                  or case.expected_doi == case.doi_value + "..."
                  or case.expected_doi.rstrip(".")
                  == case.doi_value.rstrip(".")):
                _classify_trailing_period(case, endpoint)
            else:
                case.category = "manual_review"
                case.action = "manual_review"
                case.reason = (
                    f"Unrecognized pattern: expected='{case.expected_doi}'"
                    f" found='{case.found_doi}'"
                )

            progress.advance(task)

    return cases


def _classify_trailing_period(
    case: MismatchCase, endpoint: str
) -> None:
    case.category = "trailing_period"
    case.surviving_entity = case.expected_entity
    case.duplicate_entities = [case.found_entity]

    found_id = _find_id_entity(
        endpoint, case.found_entity, case.found_doi
    )
    case.duplicate_id_entities = [found_id] if found_id else []
    case.action = "merge"
    case.reason = (
        f"DOI '{case.expected_doi}' (with .) is correct; "
        f"'{case.found_doi}' (without .) is duplicate"
    )


def _classify_bjc(case: MismatchCase, endpoint: str) -> None:
    case.category = "bjc"
    correct_doi = case.doi_value.replace("sj....bjc", "sj.bjc")
    correct_entity = _find_entity_by_doi(endpoint, correct_doi)

    if not correct_entity:
        correct_doi_dot = correct_doi + "."
        correct_entity = _find_entity_by_doi(endpoint, correct_doi_dot)

    if not correct_entity:
        case.action = "manual_review"
        case.reason = (
            f"Correct entity for '{correct_doi}' not found in triplestore"
        )
        return

    case.surviving_entity = correct_entity
    case.duplicate_entities = [case.expected_entity, case.found_entity]

    exp_id = _find_id_entity(
        endpoint, case.expected_entity, case.expected_doi
    )
    found_id = _find_id_entity(
        endpoint, case.found_entity, case.found_doi
    )
    case.duplicate_id_entities = [
        x for x in [exp_id, found_id] if x
    ]
    case.action = "merge"
    case.reason = (
        f"BJC: merge corrupted entities into correct entity "
        f"{correct_entity} (DOI {correct_doi})"
    )


def _classify_asce(case: MismatchCase, endpoint: str) -> None:
    case.category = "asce"
    case.surviving_entity = case.expected_entity
    case.duplicate_entities = [case.found_entity]

    found_id = _find_id_entity(
        endpoint, case.found_entity, case.found_doi
    )
    case.duplicate_id_entities = [found_id] if found_id else []
    case.action = "merge"
    case.reason = (
        f"ASCE: full DOI '{case.expected_doi}' on expected; "
        f"truncated '{case.found_doi}' on found"
    )


def _classify_angle_bracket(
    case: MismatchCase, endpoint: str
) -> None:
    case.category = "angle_bracket"
    case.surviving_entity = case.expected_entity
    case.duplicate_entities = [case.found_entity]

    found_id = _find_id_entity(
        endpoint, case.found_entity, case.found_doi
    )
    case.duplicate_id_entities = [found_id] if found_id else []
    case.action = "merge"
    case.reason = (
        f"'<' is part of DOI '{case.expected_doi}'; "
        f"'{case.found_doi}' is truncated"
    )


def _doi_resolves(doi: str, mailto: str) -> bool:
    time.sleep(1.0 / CROSSREF_RATE_LIMIT)
    try:
        r = requests.head(
            f"https://doi.org/{quote(doi, safe='')}",
            headers={"User-Agent": f"oc_meta/mailto:{mailto}"},
            allow_redirects=False,
            timeout=10,
        )
        return r.status_code in (301, 302, 303)
    except requests.RequestException:
        return False


def validate_cases(
    cases: list[MismatchCase], endpoint: str, mailto: str
) -> list[MismatchCase]:
    crossref_cache: dict[str, dict | None] = {}

    with create_progress() as progress:
        task = progress.add_task("Validating via API", total=len(cases))

        for case in cases:
            if _stop_requested:
                break

            if case.action != "merge":
                progress.advance(task)
                continue

            surviving_doi = ""
            if case.category == "bjc":
                surviving_doi = case.doi_value.replace(
                    "sj....bjc", "sj.bjc"
                )
            else:
                surviving_doi = case.expected_doi

            if not _doi_resolves(surviving_doi, mailto):
                case.action = "manual_review"
                case.reason += (
                    f" [VALIDATION FAILED: DOI '{surviving_doi}'"
                    f" does not resolve on doi.org]"
                )
                case.validation = "doi_not_resolved"
                progress.advance(task)
                continue

            if case.column == "venue":
                ts_surv = fetch_triplestore_metadata(
                    endpoint, case.surviving_entity
                )
                dup_entity = case.duplicate_entities[0]
                ts_dup = fetch_triplestore_metadata(endpoint, dup_entity)
                if ts_surv and ts_dup and ts_surv["title"] and ts_dup["title"]:
                    max_len = max(
                        len(ts_surv["title"]), len(ts_dup["title"])
                    )
                    dist = Levenshtein.distance(
                        ts_surv["title"], ts_dup["title"]
                    )
                    sim = 1.0 - dist / max_len
                    if sim >= VENUE_TITLE_THRESHOLD:
                        case.validation = f"venue_title_match:{sim:.2f}"
                    else:
                        case.action = "manual_review"
                        case.reason += (
                            f" [VALIDATION FAILED: venue title"
                            f" similarity {sim:.2f}]"
                        )
                        case.validation = f"venue_title_mismatch:{sim:.2f}"
                else:
                    case.validation = "doi_resolved_no_metadata"
            else:
                cr_meta = fetch_crossref_metadata(
                    surviving_doi, crossref_cache, mailto
                )
                if cr_meta:
                    best_score = 0.0
                    best_threshold = MATCHING_THRESHOLD
                    any_ts_meta = False
                    entities_to_check = [
                        *case.duplicate_entities, case.surviving_entity
                    ]
                    for ent in entities_to_check:
                        ts_meta = fetch_triplestore_metadata(
                            endpoint, ent
                        )
                        if not ts_meta or not ts_meta["title"]:
                            continue
                        any_ts_meta = True
                        score = compute_matching_score(ts_meta, cr_meta)
                        threshold = (
                            SPARSE_MATCHING_THRESHOLD
                            if is_sparse(ts_meta)
                            or is_sparse(cr_meta)
                            else MATCHING_THRESHOLD
                        )
                        if score > best_score:
                            best_score = score
                            best_threshold = threshold

                    if not any_ts_meta:
                        case.validation = "doi_resolved_no_ts_metadata"
                    elif best_score >= best_threshold:
                        case.validation = (
                            f"crossref_match:{best_score:.1f}"
                        )
                    else:
                        case.action = "manual_review"
                        case.reason += (
                            f" [VALIDATION FAILED: best score"
                            f" {best_score:.1f} < {best_threshold}]"
                        )
                        case.validation = (
                            f"crossref_mismatch:{best_score:.1f}"
                        )
                else:
                    case.validation = "doi_resolved_not_on_crossref"

            progress.advance(task)

    return cases


def build_report(cases: list[MismatchCase]) -> dict:
    actions: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        entry = {
            "doi_value": case.doi_value,
            "category": case.category,
            "expected_entity": case.expected_entity,
            "expected_doi": case.expected_doi,
            "found_entity": case.found_entity,
            "found_doi": case.found_doi,
            "surviving_entity": case.surviving_entity,
            "duplicate_entities": case.duplicate_entities,
            "duplicate_id_entities": case.duplicate_id_entities,
            "reason": case.reason,
            "validation": case.validation,
            "occurrence_count": case.occurrence_count,
        }
        actions[case.action].append(entry)

    categories = defaultdict(int)
    for case in cases:
        categories[case.category] += 1

    return {
        "summary": {
            "total_unique_cases": len(cases),
            "total_occurrences": sum(c.occurrence_count for c in cases),
            "merge": len(actions.get("merge", [])),
            "false_positive": len(actions.get("false_positive", [])),
            "manual_review": len(actions.get("manual_review", [])),
            "by_category": dict(categories),
        },
        "merge": actions.get("merge", []),
        "false_positive": actions.get("false_positive", []),
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


def execute_merges(
    cases: list[MismatchCase],
    config_path: str,
    resp_agent: str,
    progress_file: str,
) -> tuple[int, int]:
    actionable = [c for c in cases if c.action == "merge"]
    if not actionable:
        console.print("[green]No merges to execute.[/green]")
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
        task = progress.add_task("Applying merges", total=len(actionable))
        for case in actionable:
            if _stop_requested:
                console.print("[yellow]Interrupted, saving progress...[/yellow]")
                break

            case_key = f"{case.surviving_entity}|{','.join(case.duplicate_entities)}"
            if case_key in completed:
                progress.advance(task)
                continue

            try:
                supplier_prefix = get_prefix(case.surviving_entity)
                g_set = GraphSet(
                    editor.base_iri,
                    supplier_prefix=supplier_prefix,
                    custom_counter_handler=editor.counter_handler,
                )
                for dup in case.duplicate_entities:
                    editor.merge(g_set, case.surviving_entity, dup)
                editor.save(g_set, supplier_prefix)

                for id_ent in case.duplicate_id_entities:
                    editor.delete(
                        case.surviving_entity, DATACITE_HAS_ID, id_ent
                    )
                    editor.delete(id_ent)

                completed.add(case_key)
                _save_progress(progress_file, completed)
                fixed += 1
            except Exception as e:
                console.print(
                    f"[red]Error on {case.surviving_entity}: {e}[/red]"
                )
                failed += 1

            progress.advance(task)

    console.print(f"Fixed: {fixed}, Failed: {failed}")
    if not _stop_requested and failed == 0 and os.path.exists(progress_file):
        os.remove(progress_file)

    return fixed, failed


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Fix omid_mismatch errors by merging duplicate entities",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", required=True, help="Path to meta_config.yaml"
    )
    parser.add_argument(
        "--check-results", required=True, help="Path to check_results.json"
    )
    parser.add_argument("-r", "--resp-agent", help="Responsible agent URI")
    parser.add_argument(
        "--mailto",
        required=True,
        help="Email for the Crossref / doi.org polite pool User-Agent",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run", action="store_true", dest="dry_run",
        help="Report only, no modifications",
    )
    mode.add_argument(
        "--no-dry-run", action="store_true", dest="no_dry_run",
        help="Apply corrections",
    )
    parser.add_argument(
        "--report-file",
        default="fix_omid_mismatches_report.json",
        help="Output report path",
    )
    parser.add_argument(
        "--progress-file",
        default="fix_omid_mismatches_progress.json",
        help="Progress tracking file",
    )
    args = parser.parse_args()
    args.dry_run = not args.no_dry_run

    if not args.dry_run and not args.resp_agent:
        parser.error("--resp-agent is required when using --no-dry-run")

    with open(args.config) as f:
        settings = yaml.safe_load(f)

    if not args.dry_run and settings.get("rdf_files_only", False):
        raise ValueError(
            "rdf_files_only must be False: this script must update "
            "the triplestore directly."
        )

    endpoint = settings["triplestore_url"]

    console.print("[bold]Loading check_results.json...[/bold]")
    errors = load_errors(args.check_results)
    console.print(f"  omid_mismatch DOI errors: {len(errors)}")

    console.print("\n[bold]Building unique cases...[/bold]")
    cases = build_cases(errors)
    console.print(f"  Unique (expected, found) pairs: {len(cases)}")

    console.print("\n[bold]Classifying and resolving...[/bold]")
    cases = classify_and_resolve(cases, endpoint)

    console.print("\n[bold]Validating surviving DOIs via API...[/bold]")
    cases = validate_cases(cases, endpoint, args.mailto)

    report = build_report(cases)
    with open(args.report_file, "w") as f:
        json.dump(report, f, indent=2)

    summary = report["summary"]
    console.print("\n[bold]Summary:[/bold]")
    console.print(f"  Unique cases: {summary['total_unique_cases']}")
    console.print(f"  Total occurrences: {summary['total_occurrences']}")
    console.print(f"  Merge: {summary['merge']}")
    console.print(f"  False positive: {summary['false_positive']}")
    console.print(f"  Manual review: {summary['manual_review']}")
    console.print(f"  By category: {summary['by_category']}")
    console.print(f"\nReport: {args.report_file}")

    if not args.dry_run:
        if summary["merge"]:
            console.print("\n[bold]Applying merges...[/bold]")
            execute_merges(
                cases, args.config, args.resp_agent, args.progress_file
            )
    else:
        console.print(
            f"\n[dim]Dry run complete. {summary['merge']} merges pending."
            f" Use --no-dry-run to apply.[/dim]"
        )


if __name__ == "__main__":
    main()
