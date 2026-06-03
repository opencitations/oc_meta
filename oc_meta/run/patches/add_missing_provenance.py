# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC


from __future__ import annotations

import argparse
import multiprocessing
import os
import zipfile
from collections import Counter
from datetime import datetime, timezone

import orjson
import yaml
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov import ProvSet
from oc_ocdm.reader import Reader
from oc_ocdm.storer import Storer
from oc_ocdm.support import get_prefix
from rich_argparse import RichHelpFormatter

from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import collect_zip_files

PROV_SPECIALIZATION_OF = "http://www.w3.org/ns/prov#specializationOf"


def _read_json_zip(path: str) -> list:
    with zipfile.ZipFile(path) as z:
        json_name = next(n for n in z.namelist() if n.endswith(".json"))
        return orjson.loads(z.read(json_name))


def _prov_file(data_zip: str) -> str:
    return os.path.join(os.path.splitext(data_zip)[0], "prov", "se.zip")


def _entity_type(uri: str) -> str:
    return uri.rstrip("/").split("/")[-2]


def _check_file(data_zip: str) -> tuple[str, int, list[str], bool]:
    """Return (data_zip, entity_count, missing_uris, prov_file_absent)."""
    data = _read_json_zip(data_zip)
    entities = [ent["@id"] for graph in data for ent in graph["@graph"]]

    prov_path = _prov_file(data_zip)
    prov_absent = not os.path.exists(prov_path)
    covered: set[str] = set()
    if not prov_absent:
        for graph in _read_json_zip(prov_path):
            for snapshot in graph["@graph"]:
                if PROV_SPECIALIZATION_OF in snapshot:
                    for spec in snapshot[PROV_SPECIALIZATION_OF]:
                        covered.add(spec["@id"])

    missing = [e for e in entities if e not in covered]
    return data_zip, len(entities), missing, prov_absent


def _backfill_file(
    data_file: str,
    base_iri: str,
    base_dir: str,
    info_dir_root: str,
    dir_split: int,
    items_per_file: int,
    zip_output: bool,
    resp_agent: str,
    prov_endpoint: str,
    hotfix_dir: str,
    upload: bool,
) -> int:
    supplier_prefix = get_prefix(_read_json_zip(data_file)[0]["@graph"][0]["@id"])
    info_dir = os.path.join(info_dir_root, supplier_prefix) + os.sep
    counter_handler = FilesystemCounterHandler(
        info_dir=info_dir, supplier_prefix=supplier_prefix
    )
    g_set = GraphSet(
        base_iri,
        supplier_prefix=supplier_prefix,
        wanted_label=False,
        custom_counter_handler=counter_handler,
    )
    reader = Reader()
    graph = reader.load(data_file)
    if graph is None:
        raise ValueError(f"Could not load RDF data from {data_file}")
    reader.import_entities_from_graph(g_set, graph, resp_agent)

    prov_set = ProvSet(
        g_set,
        base_iri,
        wanted_label=False,
        supplier_prefix=supplier_prefix,
        custom_counter_handler=counter_handler,
    )
    created = prov_set.generate_provenance()

    storer = Storer(
        prov_set,
        dir_split=dir_split,
        n_file_item=items_per_file,
        zip_output=zip_output,
    )
    storer.store_all(base_dir, base_iri)
    if upload:
        storer.upload_all(prov_endpoint, base_dir=hotfix_dir)
    return len(created)


def _find(
    rdf_dir: str, workers: int, entities_path: str
) -> tuple[int, Counter, list[str], int]:
    all_files = collect_zip_files(rdf_dir, only_data=True)
    console.print(f"Scanning {len(all_files)} data files with {workers} workers...")

    total_missing = 0
    by_type: Counter = Counter()
    affected: set[str] = set()
    prov_files_absent = 0

    ctx = multiprocessing.get_context("forkserver")
    with open(entities_path, "w", encoding="utf-8") as ent_out:
        with ctx.Pool(workers) as pool:
            with create_progress() as progress:
                task = progress.add_task(
                    "Checking provenance", total=len(all_files)
                )
                for data_zip, _count, missing, prov_absent in pool.imap_unordered(
                    _check_file, all_files, chunksize=200
                ):
                    if prov_absent:
                        prov_files_absent += 1
                    if missing:
                        affected.add(data_zip)
                        total_missing += len(missing)
                        for uri in missing:
                            by_type[_entity_type(uri)] += 1
                        ent_out.write("\n".join(missing) + "\n")
                    progress.update(task, advance=1)

    return total_missing, by_type, sorted(affected), prov_files_absent


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill the missing se/1 provenance snapshot of data entities",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("-c", "--config", required=True, help="Meta config YAML path")
    parser.add_argument("-o", "--output", required=True, help="Output JSON report path")
    parser.add_argument(
        "--workers", type=int, default=4, help="Scan workers (default: 4)"
    )
    parser.add_argument(
        "-r", "--resp-agent", help="Responsible agent URI (default: config resp_agent)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", dest="dry_run", help="Report only")
    mode.add_argument(
        "--no-dry-run", action="store_true", dest="no_dry_run", help="Apply the backfill"
    )
    args = parser.parse_args()
    args.dry_run = not args.no_dry_run

    with open(args.config, encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    base_output_dir = settings["base_output_dir"]
    base_dir = os.path.join(base_output_dir, "rdf") + os.sep
    info_dir_root = os.path.join(base_output_dir, "info_dir")
    hotfix_dir = os.path.join(base_output_dir, "to_be_uploaded_hotfix")
    base_iri = settings["base_iri"]
    dir_split = settings["dir_split_number"]
    items_per_file = settings["items_per_file"]
    zip_output = settings["zip_output_rdf"]
    rdf_files_only = settings.get("rdf_files_only", False)
    prov_endpoint = settings["provenance_triplestore_url"]
    resp_agent = args.resp_agent or settings["resp_agent"]

    if not os.path.exists(base_dir):
        parser.error(f"RDF directory not found at {base_dir}")

    entities_path = os.path.splitext(os.path.abspath(args.output))[0] + ".entities.txt"
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    total_missing, by_type, affected, prov_files_absent = _find(
        base_dir, args.workers, entities_path
    )

    console.print(
        f"\n[bold]Found {total_missing} entities without provenance[/bold] "
        f"in {len(affected)} data files {dict(sorted(by_type.items()))}"
    )

    upload = (not args.dry_run) and (not rdf_files_only)
    created_total = 0
    fixed_files = 0
    failures: list[dict] = []

    if not args.dry_run:
        console.print(
            f"\n[bold]Backfilling se/1[/bold] (upload to triplestore: {upload})..."
        )
        with create_progress() as progress:
            task = progress.add_task("Backfilling se/1", total=len(affected))
            for data_file in affected:
                try:
                    created_total += _backfill_file(
                        data_file, base_iri, base_dir, info_dir_root, dir_split,
                        items_per_file, zip_output, resp_agent, prov_endpoint,
                        hotfix_dir, upload,
                    )
                    fixed_files += 1
                except Exception as e:  # noqa: BLE001 - record per-file and continue
                    failures.append({"file": data_file, "error": str(e)})
                    console.print(f"[red]Error on {data_file}: {e}[/red]")
                progress.update(task, advance=1)

    report = {
        "config": os.path.abspath(args.config),
        "rdf_dir": base_dir,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run,
        "resp_agent": resp_agent,
        "entities_without_provenance": total_missing,
        "missing_by_type": dict(sorted(by_type.items())),
        "affected_data_files": len(affected),
        "prov_files_absent": prov_files_absent,
        "snapshots_created": created_total,
        "files_fixed": fixed_files,
        "failures": failures,
        "entities_file": entities_path,
    }
    with open(args.output, "wb") as f:
        f.write(orjson.dumps(report, option=orjson.OPT_INDENT_2))

    if args.dry_run:
        console.print(
            f"\n[dim]Dry run: would create {total_missing} se/1 snapshots "
            f"across {len(affected)} files. Use --no-dry-run to apply.[/dim]"
        )
    else:
        console.print(
            f"\nCreated {created_total} se/1 snapshots in {fixed_files} files "
            f"({len(failures)} failures)"
        )
    console.print(f"Report saved to {args.output}")


if __name__ == "__main__":
    main()
