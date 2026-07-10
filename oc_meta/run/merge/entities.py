# SPDX-FileCopyrightText: 2024-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import csv
import logging
import os
import tempfile
from typing import Dict, List, Sequence, TypedDict

from oc_graphenricher.deduplication import GraphDeduplicator
from oc_graphenricher.storage import DirectoryStorage, directory_storage
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.graph import GraphSet
from rich_argparse import RichHelpFormatter

from oc_meta.core.editor import MetaEditor
from oc_meta.run.merge.closure import (
    compute_identifier_merge_closure,
    compute_related_closure,
)
from oc_meta.run.merge.csv_utils import parse_merged_entities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

REINDEX_SENTINEL_FILENAME = "reindex_required.out"


class MergeRow(TypedDict):
    surviving_entity: str
    merged_entities: list[str]


class EntityMerger:
    def __init__(
        self,
        meta_config: str,
        resp_agent: str,
        entity_types: Sequence[str] = ("ra", "br", "id"),
        stop_file_path: str = "stop.out",
    ):
        self.meta_config = meta_config
        self.resp_agent = resp_agent
        self.entity_types = entity_types
        self.stop_file_path = stop_file_path
        self.batch_size = 10
        self.identifier_batch_size = 1000

    @staticmethod
    def get_entity_type(entity_url: str) -> str | None:
        parts = entity_url.split("/")
        if "oc" in parts and "meta" in parts:
            try:
                return parts[parts.index("meta") + 1]
            except IndexError:
                return None
        return None

    @staticmethod
    def read_csv(csv_file: str) -> List[Dict]:
        data = []
        with open(csv_file, mode="r", newline="", encoding="utf-8") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if "Done" not in row:
                    row["Done"] = "False"
                data.append(row)
        return data

    @staticmethod
    def write_csv(csv_file: str, data: List[Dict]):
        fieldnames = data[0].keys()
        directory = os.path.dirname(os.path.abspath(csv_file))
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", dir=directory, delete=False
        ) as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
            tmp_path = file.name
        os.replace(tmp_path, csv_file)

    @staticmethod
    def count_csv_rows(csv_file: str) -> int:
        with open(csv_file, "r", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1

    @staticmethod
    def build_merge_clusters(
        rows_to_process: List[tuple[str, List[str]]],
    ) -> Dict[str, List[str]]:
        clusters: Dict[str, List[str]] = {}
        for surviving_entity, merged_entities in rows_to_process:
            if surviving_entity not in clusters:
                clusters[surviving_entity] = []
            for merged_entity in merged_entities:
                if merged_entity not in clusters[surviving_entity]:
                    clusters[surviving_entity].append(merged_entity)
        return clusters

    @staticmethod
    def create_storage(meta_editor: MetaEditor, g_set: GraphSet) -> DirectoryStorage:
        return directory_storage(
            meta_editor.base_dir,
            items_per_directory=meta_editor.dir_split,
            items_per_file=meta_editor.n_file_item,
            supplier_prefix="",
            zip_output=meta_editor.zip_output_rdf,
            modified_entities=set(g_set.res_to_entity.keys()),
            wanted_label=False,
            counter_handler=meta_editor.counter_handler,
        )

    @staticmethod
    def merge_clusters_and_save(
        g_set: GraphSet,
        storage: DirectoryStorage,
        clusters: Dict[str, List[str]],
    ) -> None:
        deduplicator = GraphDeduplicator(g_set, storage=storage)
        deduplicator.merge_clusters_and_save(clusters)

    def should_stop_processing(self) -> bool:
        return os.path.exists(self.stop_file_path)

    def process_rows(self, rows: List[MergeRow]) -> None:
        """Merge the given rows in one batch against a single triplestore snapshot"""
        meta_editor = MetaEditor(self.meta_config, self.resp_agent, save_queries=True)
        g_set = GraphSet(
            meta_editor.base_iri, custom_counter_handler=meta_editor.counter_handler
        )

        surviving_entities = [row["surviving_entity"] for row in rows]
        merged_entities = [merged for row in rows for merged in row["merged_entities"]]
        logger.info(
            f"Computing merge closure for {len(merged_entities)} merged entities and {len(surviving_entities)} surviving entities"
        )

        identifiers_only = (
            bool(surviving_entities or merged_entities)
            and all(
                self.get_entity_type(entity) == "id" for entity in surviving_entities
            )
            and all(self.get_entity_type(entity) == "id" for entity in merged_entities)
        )
        if identifiers_only:
            closure = compute_identifier_merge_closure(
                meta_editor.endpoint,
                surviving_entities,
                merged_entities,
                self.identifier_batch_size,
            )
            import_batch_size = self.identifier_batch_size
        else:
            closure = compute_related_closure(
                meta_editor.endpoint,
                set(surviving_entities) | set(merged_entities),
                self.batch_size,
            )
            import_batch_size = self.batch_size
        logger.info(f"Merge closure contains {len(closure)} entities")

        entities_to_import = {
            e for e in closure if not meta_editor.entity_cache.is_cached(e)
        }

        if entities_to_import:
            logger.info(f"Importing {len(entities_to_import)} new entities")
            meta_editor.reader.import_entities_from_triplestore(
                g_set=g_set,
                ts_url=meta_editor.endpoint,
                entities=list(entities_to_import),
                resp_agent=meta_editor.resp_agent,
                enable_validation=False,
                batch_size=import_batch_size,
            )
            for entity in entities_to_import:
                meta_editor.entity_cache.add(entity)
            logger.info("Entity import completed successfully")

        clusters = self.build_merge_clusters(
            [(row["surviving_entity"], row["merged_entities"]) for row in rows]
        )
        logger.info(
            f"Merging {len(merged_entities)} entities in {len(clusters)} survivor clusters"
        )
        self.merge_clusters_and_save(
            g_set,
            self.create_storage(meta_editor, g_set),
            clusters,
        )
        if isinstance(meta_editor.counter_handler, FilesystemCounterHandler):
            meta_editor.counter_handler.flush()

        logger.info(f"Successfully processed {len(merged_entities)} merges")

    def process_file(self, csv_file: str) -> bool:
        """Process a single CSV file of merge instructions.

        Return True when merges were applied, False when there was nothing to
        process or a stop file halted the run."""
        logger.info(f"Starting to process file: {csv_file}")
        data = self.read_csv(csv_file)
        logger.info(f"Read {len(data)} rows from {csv_file}")

        if self.should_stop_processing():
            logger.info("Stop file detected, halting processing")
            return False

        rows: List[MergeRow] = [
            {
                "surviving_entity": row["surviving_entity"],
                "merged_entities": parse_merged_entities(row["merged_entities"]),
            }
            for row in data
            if row["Done"] != "True"
            and self.get_entity_type(row["surviving_entity"]) in self.entity_types
        ]

        if not rows:
            logger.info(f"No rows to process in {csv_file}")
            return False

        logger.info(f"Found {len(rows)} rows to process in {csv_file}")
        self.process_rows(rows)

        marked_done = 0
        for row in data:
            if (
                row["Done"] != "True"
                and self.get_entity_type(row["surviving_entity"]) in self.entity_types
            ):
                row["Done"] = "True"
                marked_done += 1

        logger.info(f"Marked {marked_done} rows as done")
        self.write_csv(csv_file, data)
        logger.info(f"Saved changes to {csv_file}")

        return True

    @staticmethod
    def reindex_sentinel_path(csv_folder: str) -> str:
        return os.path.join(csv_folder, REINDEX_SENTINEL_FILENAME)

    @staticmethod
    def reindex_sentinel_path_for_csv(csv_file: str) -> str:
        return os.path.join(
            os.path.dirname(os.path.abspath(csv_file)), REINDEX_SENTINEL_FILENAME
        )

    def process_path(self, csv_path: str) -> str | None:
        if os.path.isfile(csv_path):
            return self.process_single_csv(csv_path)
        return self.process_folder(csv_path)

    def process_single_csv(self, csv_file: str) -> str | None:
        if os.path.exists(self.stop_file_path):
            os.remove(self.stop_file_path)

        sentinel_path = self.reindex_sentinel_path_for_csv(csv_file)
        if os.path.exists(sentinel_path):
            raise RuntimeError(
                f"{sentinel_path} exists: a previous run already merged a CSV file "
                "on top of the current triplestore snapshot. Re-index the "
                "triplestore from the RDF files, then delete the sentinel to "
                "process another CSV file."
            )

        if self.process_file(csv_file):
            with open(sentinel_path, "w", encoding="utf-8") as sentinel:
                sentinel.write(
                    f"{csv_file} was merged on top of the current triplestore "
                    "snapshot.\nRe-index the triplestore from the RDF files, "
                    "then delete this file to let another merge run start.\n"
                )
            logger.info(
                f"Processed {csv_file}. Re-index the triplestore from the RDF "
                f"files, then delete {sentinel_path} to process another CSV file."
            )
            return csv_file

        return None

    def process_folder(self, csv_folder: str) -> str | None:
        """Process the first CSV file with pending rows, then require a re-index.

        Every merge batch is computed against one triplestore snapshot, so a
        second file must not be processed until the triplestore has been
        re-indexed from the RDF files this run wrote. After a file is merged a
        sentinel is created next to the CSVs and the next invocation refuses to
        start until the sentinel is removed. Return the processed file, or None
        when no file had pending rows."""
        if os.path.exists(self.stop_file_path):
            os.remove(self.stop_file_path)

        sentinel_path = self.reindex_sentinel_path(csv_folder)
        if os.path.exists(sentinel_path):
            raise RuntimeError(
                f"{sentinel_path} exists: a previous run already merged a CSV file "
                "on top of the current triplestore snapshot. Re-index the "
                "triplestore from the RDF files, then delete the sentinel to "
                "process the next CSV file."
            )

        csv_files = sorted(
            os.path.join(csv_folder, file)
            for file in os.listdir(csv_folder)
            if file.endswith(".csv")
        )

        for csv_file in csv_files:
            if self.should_stop_processing():
                break
            if self.process_file(csv_file):
                with open(sentinel_path, "w", encoding="utf-8") as sentinel:
                    sentinel.write(
                        f"{csv_file} was merged on top of the current triplestore "
                        "snapshot.\nRe-index the triplestore from the RDF files, "
                        "then delete this file to let the next merge run start.\n"
                    )
                logger.info(
                    f"Processed {csv_file}. Re-index the triplestore from the RDF "
                    f"files, then delete {sentinel_path} to process the next file."
                )
                return csv_file

        logger.info("No CSV files with pending rows found")
        return None


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Merge entities from a CSV file or from the first pending CSV file "
            "in a folder. One file is processed per run: re-index the "
            f"triplestore from the RDF files and delete {REINDEX_SENTINEL_FILENAME} "
            "before the next run."
        ),
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "csv_path", type=str, help="Path to a merge CSV file or folder of CSV files"
    )
    parser.add_argument("meta_config", type=str, help="Meta configuration string")
    parser.add_argument("resp_agent", type=str, help="Responsible agent string")
    parser.add_argument(
        "--entity_types",
        nargs="+",
        default=["ra", "br", "id"],
        help="Types of entities to merge (ra, br, id)",
    )
    parser.add_argument(
        "--stop_file", type=str, default="stop.out", help="Path to the stop file"
    )

    args = parser.parse_args()

    merger = EntityMerger(
        meta_config=args.meta_config,
        resp_agent=args.resp_agent,
        entity_types=args.entity_types,
        stop_file_path=args.stop_file,
    )

    merger.process_path(args.csv_path)


if __name__ == "__main__":
    main()
