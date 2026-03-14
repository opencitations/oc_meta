from __future__ import annotations

import argparse
import json
import os
import shutil
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from rdflib import XSD, Dataset, Literal
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


def collect_zip_files(input_dir: Path) -> list[Path]:
    result = []
    for root, _, files in os.walk(input_dir):
        for f in files:
            if f.endswith(".zip"):
                result.append(Path(root) / f)
    return result


def needs_modification(data: object) -> bool:
    if isinstance(data, list):
        return any(needs_modification(item) for item in data)
    if isinstance(data, dict):
        if "@value" in data and "@type" not in data and "@language" not in data:
            return True
        return any(needs_modification(v) for v in data.values() if isinstance(v, (dict, list)))
    return False


def process_dataset(ds: Dataset) -> dict[str, int]:
    modifications: dict[str, int] = defaultdict(int)
    quads_to_remove = []
    quads_to_add = []
    for s, p, o, g in ds.quads():
        if isinstance(o, Literal) and o.datatype is None and o.language is None:
            quads_to_remove.append((s, p, o, g))
            new_literal = Literal(str(o), datatype=XSD.string)
            quads_to_add.append((s, p, new_literal, g))
            modifications[str(p)] += 1
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

    with zipfile.ZipFile(input_path, "r") as zf_in:
        json_name = zf_in.namelist()[0]
        raw_bytes = zf_in.read(json_name)

    data = json.loads(raw_bytes.decode("utf-8"))

    if not needs_modification(data):
        shutil.copy2(input_path, output_path)
        return {}

    ds = Dataset()
    ds.parse(data=raw_bytes, format="json-ld")
    modifications = process_dataset(ds)
    serialized = ds.serialize(format="json-ld")
    result = json.loads(serialized)

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf_out:
        zf_out.writestr(json_name, json.dumps(result, ensure_ascii=False))

    return modifications


def process_batch(
    batch: list[tuple[Path, Path, Path]]
) -> list[dict[str, int]]:
    return [process_zip_file(input_path, input_dir, output_dir) for input_path, input_dir, output_dir in batch]


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Add xsd:string datatype to untyped literals in RDF data",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("input_dir", type=Path, help="Input directory with RDF data")
    parser.add_argument("output_dir", type=Path, help="Output directory for modified data")
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
    batches = [work_items[i:i + args.batch_size] for i in range(0, len(work_items), args.batch_size)]

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

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_batch, batch): batch for batch in batches}
            for future in as_completed(futures):
                batch_results = future.result()
                for modifications in batch_results:
                    if modifications:
                        files_modified += 1
                        for prop, count in modifications.items():
                            total_modifications[prop] += count
                    else:
                        files_unchanged += 1
                progress.update(task, advance=len(futures[future]))

    console.print("\n[bold]Statistics:[/bold]")
    console.print(f"  Files processed: {len(zip_files)}")
    console.print(f"  Files modified: {files_modified}")
    console.print(f"  Files unchanged: {files_unchanged}")

    if total_modifications:
        console.print("\n[bold]Modifications by property:[/bold]")
        sorted_mods = sorted(
            total_modifications.items(), key=lambda x: x[1], reverse=True
        )
        for prop, count in sorted_mods:
            console.print(f"  {prop}: {count}")
        console.print(f"\n[bold]Total literals modified: {sum(total_modifications.values())}[/bold]")
    else:
        console.print("\nNo modifications needed")


if __name__ == "__main__":  # pragma: no cover
    main()
