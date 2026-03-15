from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from oc_ocdm.support.support import get_datatype_from_iso_8601
from rdflib import XSD, Dataset, Literal, URIRef
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich_argparse import RichHelpFormatter

BATCH_SIZE = 100
PUBLICATION_DATE_PREDICATE = URIRef(
    "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
)
PUBLICATION_DATE_PREDICATE_STR = str(PUBLICATION_DATE_PREDICATE)
VALID_DATE_TYPES = {XSD.date, XSD.gYear, XSD.gYearMonth}
VALID_DATE_TYPES_STR = {str(t) for t in VALID_DATE_TYPES}


def _worker_init() -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def collect_zip_files(input_dir: Path) -> list[Path]:
    result = []
    for root, _, files in os.walk(input_dir):
        for f in files:
            if f.endswith(".zip"):
                result.append(Path(root) / f)
    return result


def is_provenance_file(path: Path) -> bool:
    return "/prov/" in str(path) or "\\prov\\" in str(path)


def _has_untyped_literal(data: object) -> bool:
    if isinstance(data, list):
        return any(_has_untyped_literal(item) for item in data)
    if isinstance(data, dict):
        if "@value" in data and "@type" not in data and "@language" not in data:
            return True
        return any(
            _has_untyped_literal(v) for v in data.values() if isinstance(v, (dict, list))
        )
    return False


def _has_invalid_date_type(data: object) -> bool:
    if isinstance(data, list):
        return any(_has_invalid_date_type(item) for item in data)
    if isinstance(data, dict):
        if PUBLICATION_DATE_PREDICATE_STR in data:
            pub_date_values = data[PUBLICATION_DATE_PREDICATE_STR]
            if isinstance(pub_date_values, list):
                for v in pub_date_values:
                    if isinstance(v, dict) and "@type" in v:
                        if v["@type"] not in VALID_DATE_TYPES_STR:
                            return True
        return any(
            _has_invalid_date_type(v)
            for v in data.values()
            if isinstance(v, (dict, list))
        )
    return False


def needs_modification(data: object, is_prov: bool) -> bool:
    if _has_untyped_literal(data):
        return True
    if not is_prov and _has_invalid_date_type(data):
        return True
    return False


def process_dataset(ds: Dataset, is_prov: bool) -> dict[str, int]:
    modifications: dict[str, int] = defaultdict(int)
    quads_to_remove = []
    quads_to_add = []

    for s, p, o, g in ds.quads():
        if not isinstance(o, Literal):
            continue

        if not is_prov and p == PUBLICATION_DATE_PREDICATE and o.datatype not in VALID_DATE_TYPES:
            date_value = str(o)
            correct_datatype, normalized_value = get_datatype_from_iso_8601(date_value)
            quads_to_remove.append((s, p, o, g))
            new_literal = Literal(normalized_value, datatype=correct_datatype)
            quads_to_add.append((s, p, new_literal, g))
            modifications[f"date:{correct_datatype}"] += 1
        elif o.datatype is None and o.language is None:
            quads_to_remove.append((s, p, o, g))
            new_literal = Literal(str(o), datatype=XSD.string)
            quads_to_add.append((s, p, new_literal, g))
            modifications[f"untyped:{p}"] += 1

    for quad in quads_to_remove:
        ds.remove(quad)
    for quad in quads_to_add:
        ds.add(quad)

    return dict(modifications)


def process_zip_file(
    input_path: Path, input_dir: Path, output_dir: Path
) -> dict[str, int]:
    relative_path = input_path.relative_to(input_dir)
    output_path = output_dir / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    is_prov = is_provenance_file(input_path)

    with zipfile.ZipFile(input_path, "r") as zf_in:
        json_name = zf_in.namelist()[0]
        raw_bytes = zf_in.read(json_name)

    data = json.loads(raw_bytes.decode("utf-8"))

    if not needs_modification(data, is_prov):
        shutil.copy2(input_path, output_path)
        return {}

    ds = Dataset()
    ds.parse(data=raw_bytes, format="json-ld")
    modifications = process_dataset(ds, is_prov)

    if not modifications:
        shutil.copy2(input_path, output_path)
        return {}

    serialized = ds.serialize(format="json-ld")
    result = json.loads(serialized)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf_out:
        zf_out.writestr(json_name, json.dumps(result, ensure_ascii=False, indent=None))

    return modifications


def process_batch(batch: list[tuple[Path, Path, Path]]) -> list[dict[str, int]]:
    return [
        process_zip_file(input_path, input_dir, output_dir)
        for input_path, input_dir, output_dir in batch
    ]


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Fix literal datatypes: add xsd:string to untyped literals and fix date datatypes",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("input_dir", type=Path, help="Input directory with RDF data")
    parser.add_argument(
        "output_dir", type=Path, help="Output directory for modified data"
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=4, help="Number of parallel workers"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=BATCH_SIZE, help="Files per batch"
    )
    args = parser.parse_args()

    console = Console()
    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    console.print(f"Scanning {input_dir} for ZIP files...")
    zip_files = collect_zip_files(input_dir)
    console.print(f"Found {len(zip_files)} ZIP files to process")

    output_dir.mkdir(parents=True, exist_ok=True)

    total_modifications: dict[str, int] = defaultdict(int)
    files_modified = 0
    files_unchanged = 0

    work_items = [(zf, input_dir, output_dir) for zf in zip_files]
    batches = [
        work_items[i : i + args.batch_size]
        for i in range(0, len(work_items), args.batch_size)
    ]

    executor = ProcessPoolExecutor(max_workers=args.workers, initializer=_worker_init)
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing files", total=len(zip_files))
            futures = {
                executor.submit(process_batch, batch): batch for batch in batches
            }
            for future in as_completed(futures):
                batch_results = future.result()
                for modifications in batch_results:
                    if modifications:
                        files_modified += 1
                        for key, count in modifications.items():
                            total_modifications[key] += count
                    else:
                        files_unchanged += 1
                progress.update(task, advance=len(futures[future]))
    except KeyboardInterrupt:
        console.print("\n[bold red]Interrupted. Shutting down workers...[/bold red]")
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        executor.shutdown(wait=True)

    console.print("\n[bold]Statistics:[/bold]")
    console.print(f"  Files processed: {len(zip_files)}")
    console.print(f"  Files modified: {files_modified}")
    console.print(f"  Files unchanged: {files_unchanged}")

    if total_modifications:
        untyped_mods = {
            k[8:]: v for k, v in total_modifications.items() if k.startswith("untyped:")
        }
        date_mods = {
            k[5:]: v for k, v in total_modifications.items() if k.startswith("date:")
        }

        if untyped_mods:
            console.print("\n[bold]Untyped literals fixed (by property):[/bold]")
            sorted_mods = sorted(untyped_mods.items(), key=lambda x: x[1], reverse=True)
            for prop, count in sorted_mods:
                console.print(f"  {prop}: {count}")
            console.print(
                f"  [bold]Total: {sum(untyped_mods.values())}[/bold]"
            )

        if date_mods:
            console.print("\n[bold]Date datatypes fixed (by target type):[/bold]")
            sorted_mods = sorted(date_mods.items(), key=lambda x: x[1], reverse=True)
            for dtype, count in sorted_mods:
                console.print(f"  {dtype}: {count}")
            console.print(f"  [bold]Total: {sum(date_mods.values())}[/bold]")
    else:
        console.print("\nNo modifications needed")


if __name__ == "__main__":  # pragma: no cover
    main()
