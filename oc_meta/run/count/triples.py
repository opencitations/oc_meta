"""Count RDF triples or quads in compressed or uncompressed files."""

from __future__ import annotations

import argparse
import gzip
import multiprocessing
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from rdflib import Dataset
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

QUAD_FORMATS = {"nquads", "trig"}


def parse_args() -> argparse.Namespace:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Count RDF triples or quads in compressed or uncompressed files."
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory containing the RDF files.",
    )
    parser.add_argument(
        "--pattern",
        default="*.nq.gz",
        help="Glob pattern for locating files (default: '*.nq.gz').",
    )
    parser.add_argument(
        "--format",
        default="nquads",
        choices=["nquads", "json-ld", "turtle", "trig"],
        help="RDF format of the input files (default: nquads).",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search recursively under the provided directory.",
    )
    parser.add_argument(
        "--prov-only",
        action="store_true",
        help="Count only files in 'prov' subdirectories.",
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Count only files not in 'prov' subdirectories.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: CPU count).",
    )
    parser.add_argument(
        "--show-per-file",
        action="store_true",
        help="Print the count for each processed file.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue processing even if errors occur.",
    )
    return parser.parse_args()


def discover_files(
    directory: Path,
    pattern: str,
    recursive: bool,
    prov_only: bool,
    data_only: bool,
) -> list[Path]:
    path = directory.expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"'{path}' does not exist or is not a directory.")

    globber = path.rglob if recursive else path.glob
    files: list[Path] = []

    for file_path in globber(pattern):
        if not file_path.is_file():
            continue
        is_prov = "prov" in file_path.parts
        if prov_only and not is_prov:
            continue
        if data_only and is_prov:
            continue
        files.append(file_path)

    return sorted(files)


def count_in_file(file_path: Path, rdf_format: str) -> tuple[str, int, str | None]:
    try:
        suffix = file_path.suffix.lower()
        if suffix == ".zip":
            with zipfile.ZipFile(file_path, "r") as z:
                inner_name = z.namelist()[0]
                with z.open(inner_name) as f:
                    content = f.read().decode("utf-8")
            dataset: Dataset = Dataset(default_union=True)
            dataset.parse(data=content, format=rdf_format)
        elif suffix == ".gz":
            dataset = Dataset(default_union=True)
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                dataset.parse(f, format=rdf_format)
        else:
            dataset = Dataset(default_union=True)
            with open(file_path, "r", encoding="utf-8") as f:
                dataset.parse(f, format=rdf_format)
        return str(file_path), len(dataset), None
    except Exception as exc:
        return str(file_path), 0, str(exc)


def process_files(
    files: list[Path],
    rdf_format: str,
    max_workers: int | None,
    show_per_file: bool,
    keep_going: bool,
    unit_name: str,
) -> tuple[int, list[tuple[str, str]]]:
    workers = max_workers or multiprocessing.cpu_count()
    if workers < 1:
        workers = 1

    total_count = 0
    results: list[tuple[str, int]] = []
    failures: list[tuple[str, str]] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task(f"Counting {unit_name}", total=len(files))

        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(count_in_file, fp, rdf_format): fp for fp in files
            }

            for future in as_completed(futures):
                file_path, count, error = future.result()

                if error:
                    failures.append((file_path, error))
                    if not keep_going:
                        progress.console.print(
                            f"[red]Error processing {file_path}: {error}[/red]"
                        )
                        progress.advance(task)
                        break
                    progress.console.print(
                        f"[yellow]Error processing {file_path}: {error} (continuing)[/yellow]"
                    )
                else:
                    total_count += count
                    if show_per_file:
                        results.append((file_path, count))

                progress.advance(task)

    if show_per_file and results:
        width = max(len(path) for path, _ in results)
        for path, count in sorted(results):
            print(f"{path.ljust(width)} : {count}")

    return total_count, failures


def main() -> None:  # pragma: no cover
    args = parse_args()

    if args.prov_only and args.data_only:
        print("Error: --prov-only and --data-only are mutually exclusive.")
        return

    try:
        files = discover_files(
            args.directory,
            args.pattern,
            args.recursive,
            args.prov_only,
            args.data_only,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        return

    if not files:
        print("No files found matching the provided pattern.")
        return

    unit_name = "quads" if args.format in QUAD_FORMATS else "triples"

    total, failures = process_files(
        files,
        args.format,
        args.workers,
        args.show_per_file,
        args.keep_going,
        unit_name,
    )

    print(f"Total {unit_name}: {total}")

    if failures:
        print(f"\nFiles with errors ({len(failures)}):")
        for path, error in failures:
            print(f"  {path}: {error}")


if __name__ == "__main__":  # pragma: no cover
    main()
