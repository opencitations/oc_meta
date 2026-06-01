#!/usr/bin/python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import json
import os
import signal
from collections import defaultdict
from dataclasses import dataclass, field

import yaml
from oc_ocdm.graph import GraphSet
from oc_ocdm.support import get_prefix
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.bibliographic_matching import (
    DATACITE_DOI,
    DATACITE_HAS_ID,
    DATACITE_USES_SCHEME,
    LITERAL_HAS_VALUE,
    MATCHING_THRESHOLD,
    compute_matching_score,
    fetch_crossref_metadata,
    fetch_triplestore_metadata,
)
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.sparql import execute_sparql

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


def determine_actions(
    cases: list[CorrectionCase],
    endpoint: str,
    mailto: str,
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
                case.candidate_doi, crossref_cache, mailto
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
    parser.add_argument(
        "--mailto",
        required=True,
        help="Email for the Crossref polite pool User-Agent",
    )
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
    cases = determine_actions(cases, endpoint, args.mailto)

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
