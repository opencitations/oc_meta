# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import re
import signal
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from zipfile import ZipFile

import orjson
import yaml
from oc_ocdm.graph import GraphSet
from rich_argparse import RichHelpFormatter

from oc_meta.constants import CONTAINER_EDITOR_TYPES
from oc_meta.core.editor import MetaEditor
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import collect_files, collect_zip_files
from oc_meta.lib.finder import ResourceFinder


FRBR_PART_OF = "http://purl.org/vocab/frbr/core#partOf"
IS_DOC_CONTEXT_FOR = "http://purl.org/spar/pro/isDocumentContextFor"
WITH_ROLE = "http://purl.org/spar/pro/withRole"
EDITOR_ROLE = "http://purl.org/spar/pro/editor"

CONTAINER_EDITOR_TYPE_IRIS = frozenset(
    iri for iri, label in ResourceFinder._IRI_TO_TYPE.items()
    if label in CONTAINER_EDITOR_TYPES
)

BATCH_SIZE = 100

_stop_requested = False


def _worker_init() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _handle_signal(_signum: int, _frame: object) -> None:
    global _stop_requested
    _stop_requested = True
    console.print("[yellow]Interrupt received, finishing current entity...[/yellow]")


def _get_supplier_prefix(uri: str) -> str:
    match = re.match(r"^(.+)/([a-z][a-z])/(0[1-9]+0)([1-9][0-9]*)$", uri)
    assert match is not None, f"Cannot extract supplier prefix from: {uri}"
    return match.group(3)


def _iter_entities(files: list, zip_output: bool):
    for fpath in files:
        if zip_output:
            with ZipFile(fpath, "r") as zf:
                data = orjson.loads(zf.read(zf.namelist()[0]))
        else:
            with open(fpath, "rb") as f:
                data = orjson.loads(f.read())
        for graph in data:
            for entity in graph.get("@graph", []):
                yield entity


def _scan_br_batch(
    files: list[str], zip_output: bool
) -> tuple[dict[str, str], dict[str, set[str]], list[str]]:
    frbr_part_of: dict[str, str] = {}
    br_ars: dict[str, set[str]] = {}
    warnings: list[str] = []
    for entity in _iter_entities(files, zip_output):
        eid = entity["@id"]
        if IS_DOC_CONTEXT_FOR in entity:
            br_ars[eid] = {x["@id"] for x in entity[IS_DOC_CONTEXT_FOR]}
        entity_types = set(entity.get("@type", []))
        if entity_types & CONTAINER_EDITOR_TYPE_IRIS and FRBR_PART_OF in entity:
            parents = entity[FRBR_PART_OF]
            if len(parents) > 1:
                parent_ids = [p["@id"] for p in parents]
                warnings.append(
                    f"{eid} has {len(parents)} frbr:partOf values {parent_ids}; using first only"
                )
            frbr_part_of[eid] = parents[0]["@id"]
    return frbr_part_of, br_ars, warnings


def _scan_ar_batch(files: list[str], zip_output: bool) -> dict[str, str]:
    ar_role: dict[str, str] = {}
    for entity in _iter_entities(files, zip_output):
        eid = entity["@id"]
        if WITH_ROLE in entity:
            ar_role[eid] = entity[WITH_ROLE][0]["@id"]
    return ar_role


def find_misplaced_editor_ars(
    base_dir: str, zip_output: bool, workers: int = 4, batch_size: int = BATCH_SIZE
) -> list[dict]:
    br_dir = os.path.join(base_dir, "br")
    ar_dir = os.path.join(base_dir, "ar")

    if zip_output:
        br_files = collect_zip_files(br_dir, only_data=True)
        ar_files = collect_zip_files(ar_dir, only_data=True)
    else:
        br_files = collect_files(br_dir, "*.json", lambda p: "prov" not in p)
        ar_files = collect_files(ar_dir, "*.json", lambda p: "prov" not in p)

    br_batches = [br_files[i:i + batch_size] for i in range(0, len(br_files), batch_size)]
    ar_batches = [ar_files[i:i + batch_size] for i in range(0, len(ar_files), batch_size)]

    frbr_part_of: dict[str, str] = {}
    br_ars: dict[str, set[str]] = {}
    ar_role: dict[str, str] = {}

    ctx = multiprocessing.get_context("forkserver")

    with create_progress() as progress:
        br_task = progress.add_task("Scanning BR files", total=len(br_files))
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        try:
            futures = {
                executor.submit(_scan_br_batch, batch, zip_output): batch
                for batch in br_batches
            }
            for future in as_completed(futures):
                partial_frbr, partial_br_ars, warnings = future.result()
                frbr_part_of.update(partial_frbr)
                br_ars.update(partial_br_ars)
                for w in warnings:
                    console.print(f"[yellow]Warning:[/yellow] {w}")
                progress.advance(br_task, len(futures[future]))
        finally:
            executor.shutdown(wait=True)

        ar_task = progress.add_task("Scanning AR files", total=len(ar_files))
        executor = ProcessPoolExecutor(
            max_workers=workers, initializer=_worker_init, mp_context=ctx
        )
        try:
            futures = {
                executor.submit(_scan_ar_batch, batch, zip_output): batch
                for batch in ar_batches
            }
            for future in as_completed(futures):
                ar_role.update(future.result())
                progress.advance(ar_task, len(futures[future]))
        finally:
            executor.shutdown(wait=True)

    results = []
    for chapter, book in frbr_part_of.items():
        book_ars = br_ars.get(book, set())
        for ar in br_ars.get(chapter, set()):
            if ar_role.get(ar) == EDITOR_ROLE and ar not in book_ars:
                results.append({"chapter": chapter, "ar": ar, "book": book})
    return results


def fix_chapter(
    editor: MetaEditor,
    chapter_uri: str,
    book_uri: str,
    ar_uris: list[str],
) -> None:
    supplier_prefix = _get_supplier_prefix(chapter_uri)
    g_set = GraphSet(
        editor.base_iri,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=editor.counter_handler,
        wanted_label=False,
    )

    file_paths: set[str] = set()
    for uri in [chapter_uri, book_uri] + ar_uris:
        fp = editor.find_file(
            editor.base_dir, editor.dir_split, editor.n_file_item, uri, editor.zip_output_rdf
        )
        if fp is not None:
            file_paths.add(fp)

    for fp in file_paths:
        imported_graph = editor.reader.load(fp)
        if imported_graph is not None:
            editor.reader.import_entities_from_graph(g_set, imported_graph, editor.resp_agent)

    chapter_entity = g_set.get_entity(chapter_uri)
    book_entity = g_set.get_entity(book_uri)
    assert chapter_entity is not None, f"chapter not found: {chapter_uri}"
    assert book_entity is not None, f"book not found: {book_uri}"

    for ar_uri in ar_uris:
        ar_entity = g_set.get_entity(ar_uri)
        assert ar_entity is not None, f"AR not found: {ar_uri}"
        chapter_entity.remove_contributor(ar_entity)  # type: ignore[attr-defined]
        book_entity.has_contributor(ar_entity)  # type: ignore[attr-defined]

    editor.save(g_set, supplier_prefix)


def _load_progress(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(json.load(f))


def _save_progress(path: str, completed: set[str]) -> None:
    with open(path, "w") as f:
        json.dump(list(completed), f)


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description=(
            "Fix misplaced editor ARs: move pro:isDocumentContextFor "
            "from content entity to its frbr:partOf container"
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Path to meta_config.yaml")
    parser.add_argument("-r", "--resp-agent", help="Responsible agent URI (required without --dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Report cases without modifying")
    parser.add_argument(
        "-w", "--workers", type=int, default=4, help="Number of parallel workers for scanning"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=BATCH_SIZE, help="Files per batch for scanning"
    )
    parser.add_argument(
        "--progress-file",
        default="fix_misplaced_editor_ars_progress.json",
        help=(
            "Path to a JSON file used to track completed chapters for resumable execution. "
            "Created automatically if it does not exist; deleted on successful completion. "
            "Default: fix_misplaced_editor_ars_progress.json in the current working directory."
        ),
    )
    args = parser.parse_args()

    if not args.dry_run and not args.resp_agent:
        parser.error("--resp-agent is required when not using --dry-run")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    rdf_dir = os.path.join(config["base_output_dir"], "rdf")
    zip_output = config.get("zip_output_rdf", False)

    console.print("Scanning RDF files for misplaced editor ARs...")
    cases = find_misplaced_editor_ars(rdf_dir, zip_output, args.workers, args.batch_size)

    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for case in cases:
        groups[(case["chapter"], case["book"])].append(case["ar"])

    console.print(f"Found {len(cases)} misplaced editor ARs across {len(groups)} chapters")

    if args.dry_run:
        for (chapter, book), ars in sorted(groups.items()):
            ch = chapter.split("/")[-1]
            bk = book.split("/")[-1]
            ar_ids = [a.split("/")[-1] for a in ars]
            console.print(f"  {ch} → {bk}: {ar_ids}")
        return

    completed = _load_progress(args.progress_file)
    if completed:
        console.print(f"Resuming: {len(completed)} chapters already processed")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    editor = MetaEditor(args.config, args.resp_agent)
    succeeded = failed = skipped = 0

    with create_progress() as progress:
        task = progress.add_task("Fixing misplaced editor ARs", total=len(groups))
        for (chapter, book), ar_uris in groups.items():
            if _stop_requested:
                break
            if chapter in completed:
                skipped += 1
                progress.advance(task)
                continue
            try:
                fix_chapter(editor, chapter, book, ar_uris)
                completed.add(chapter)
                _save_progress(args.progress_file, completed)
                succeeded += 1
            except Exception as e:
                console.print(f"  [red]Error[/red] {chapter.split('/')[-1]}: {e}")
                failed += 1
            progress.advance(task)

    if _stop_requested:
        console.print(f"Stopped: {succeeded} fixed, {failed} failed, {skipped} skipped")
    else:
        console.print(f"Done: {succeeded} chapters fixed, {failed} failed, {skipped} skipped")
        if os.path.exists(args.progress_file) and not failed:
            os.remove(args.progress_file)


if __name__ == "__main__":
    main()
