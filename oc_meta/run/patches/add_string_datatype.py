from __future__ import annotations

import argparse
import json
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


def collect_zip_files(input_dir: Path) -> list[Path]:
    return list(input_dir.rglob("*.zip"))


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
        with zf_in.open(json_name) as f:
            data = json.load(f)
    ds = Dataset()
    ds.parse(data=json.dumps(data), format="json-ld")
    modifications = process_dataset(ds)
    serialized = ds.serialize(format="json-ld")
    result = json.loads(serialized)
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf_out:
        zf_out.writestr(json_name, json.dumps(result, ensure_ascii=False))
    return modifications


def process_single_file(args: tuple[Path, Path, Path]) -> dict[str, int]:
    input_path, input_dir, output_dir = args
    return process_zip_file(input_path, input_dir, output_dir)


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
            futures = {
                executor.submit(process_single_file, item): item
                for item in work_items
            }
            for future in as_completed(futures):
                modifications = future.result()
                if modifications:
                    files_modified += 1
                    for prop, count in modifications.items():
                        total_modifications[prop] += count
                else:
                    files_unchanged += 1
                progress.update(task, advance=1)

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
