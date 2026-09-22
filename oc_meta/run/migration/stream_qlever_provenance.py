# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import multiprocessing
import os
from pathlib import Path

from rdflib import URIRef
from rich_argparse import RichHelpFormatter
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.qlever import generate_qlever_index

from oc_meta.lib.file_manager import collect_zip_files
from oc_meta.run.migration.stream_nquads import (
    bounded_nquads_results,
    create_progress,
    read_zip_dataset,
    write_nquads_stdout,
)


def convert_qlever_provenance(zip_path: str) -> bytes:
    dataset = read_zip_dataset(zip_path)
    associations = dataset.graph(URIRef("urn:tal:qlever:prov/"))
    for snapshot, update in dataset.subject_objects(
        URIRef(ProvEntity.iri_has_update_query)
    ):
        for triple in generate_qlever_index([(str(snapshot), str(update))]):
            associations.add(triple)
    return dataset.serialize(format="nquads").encode("utf-8")


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Stream provenance N-Quads with TAL snapshot URI associations.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument("rdf_dir", type=Path)
    parser.add_argument(
        "--workers", type=int, default=min(8, multiprocessing.cpu_count())
    )
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if not args.rdf_dir.is_dir():
        parser.error("rdf_dir must be an existing directory")
    paths = collect_zip_files(str(args.rdf_dir.resolve()), only_prov=True)
    if not paths:
        parser.error("No provenance ZIP files found")
    context = multiprocessing.get_context("spawn" if os.name == "nt" else "forkserver")
    with context.Pool(args.workers) as pool, create_progress() as progress:
        results = bounded_nquads_results(
            pool, paths, args.workers, converter=convert_qlever_provenance
        )
        write_nquads_stdout(
            progress.track(
                results, total=len(paths), description="Converting provenance"
            )
        )


if __name__ == "__main__":  # pragma: no cover
    main()
