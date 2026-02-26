"""Utility to count RDF quads in gzip-compressed N-Quads files."""
from __future__ import annotations

import argparse
import gzip
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

from rdflib import Dataset
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Count RDF quads stored in gzip-compressed files (e.g. '.nq.gz') "
            "with RDFLib."
        )
    )
    parser.add_argument(
        "directory",
        type=Path,
        help="Directory containing the files to be analysed.",
    )
    parser.add_argument(
        "--pattern",
        default="*.nq.gz",
        help=(
            "Glob pattern used to locate files (default: '*.nq.gz'). "
            "Use '--pattern *.nq' for uncompressed files."
        ),
    )
    parser.add_argument(
        "--format",
        default="nquads",
        choices=["nquads", "trig"],
        help="RDF format of the input files (default: nquads).",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search recursively under the provided directory.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Maximum number of worker processes (default: CPU count).",
    )
    parser.add_argument(
        "--show-per-file",
        action="store_true",
        help="Print the quad count for each processed file.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help=(
            "Keep processing files even if errors occur. "
            "Errors are reported at the end."
        ),
    )
    return parser.parse_args()


def discover_files(directory: Path, pattern: str, recursive: bool) -> list[Path]:
    path = directory.expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"Directory '{path}' does not exist or is not a directory.")

    globber = path.rglob if recursive else path.glob
    return sorted(globber(pattern))


def count_quads_in_file(file_path: Path, rdf_format: str) -> tuple[str, int, str | None]:
    try:
        dataset: Dataset = Dataset()
        open_fn = gzip.open if file_path.suffix == ".gz" else open
        mode = "rt"
        with open_fn(file_path, mode, encoding="utf-8") as handle:  # type: ignore[arg-type]
            dataset.parse(handle, format=rdf_format)
        return str(file_path), len(dataset), None
    except Exception as exc:
        return str(file_path), 0, str(exc)


def iter_counts(
    files: Iterable[Path],
    rdf_format: str,
    max_workers: int | None,
) -> Iterable[tuple[str, int, str | None]]:
    workers = max_workers or multiprocessing.cpu_count()
    if workers < 1:
        workers = 1

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_file = {
            executor.submit(count_quads_in_file, file_path, rdf_format): file_path
            for file_path in files
        }
        for future in tqdm(
            as_completed(future_to_file),
            total=len(future_to_file),
            desc="Counting quads",
            unit="file",
        ):
            yield future.result()


def main() -> None:
    args = parse_args()

    try:
        files = discover_files(args.directory, args.pattern, args.recursive)
    except ValueError as exc:
        logging.error(exc)
        return

    if not files:
        logging.warning("No files found for the provided pattern.")
        return

    results = []
    total_quads = 0
    failures: list[tuple[str, str]] = []

    for file_path, quad_count, error in iter_counts(files, args.format, args.workers):
        if error:
            failures.append((file_path, error))
            if not args.keep_going:
                logging.error("Error while processing %s: %s", file_path, error)
                break
            logging.warning(
                "Error while processing %s: %s (continuing)",
                file_path,
                error,
            )
            continue

        total_quads += quad_count
        if args.show_per_file:
            results.append((file_path, quad_count))

    if args.show_per_file and results:
        width = max(len(path) for path, _ in results)
        for path, quad_count in results:
            print(f"{path.ljust(width)} : {quad_count}")

    print(f"Total quads: {total_quads}")

    if failures:
        print("\nFiles with errors:")
        for path, error in failures:
            print(f"- {path}: {error}")


if __name__ == "__main__":
    main()
