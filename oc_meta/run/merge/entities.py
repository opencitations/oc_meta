import argparse
import concurrent.futures
import csv
import logging
import os
import traceback
from typing import Dict, List, Set

from oc_meta.plugins.editor import MetaEditor
from oc_ocdm.graph import GraphSet
from rdflib import URIRef
from sparqlite import SPARQLClient
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class EntityMerger:
    def __init__(
        self,
        meta_config: str,
        resp_agent: str,
        entity_types: List[str],
        stop_file_path: str,
        workers: int,
    ):
        self.meta_config = meta_config
        self.resp_agent = resp_agent
        self.entity_types = entity_types
        self.stop_file_path = stop_file_path
        self.workers = workers
        self.batch_size = 10

    @staticmethod
    def get_entity_type(entity_url: str) -> str:
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
        with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    @staticmethod
    def count_csv_rows(csv_file: str) -> int:
        with open(csv_file, "r", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1

    def fetch_related_entities_batch(
        self,
        meta_editor: MetaEditor,
        merged_entities: List[str],
        surviving_entities: List[str],
        batch_size: int = 10,
    ) -> Set[URIRef]:
        """
        Fetch all related entities in batches and populate the relationship cache.

        Args:
            meta_editor: MetaEditor instance
            merged_entities: List of entities to be merged
            surviving_entities: List of surviving entities
            batch_size: Maximum number of entities to process in a single SPARQL query

        Returns:
            Set of all related entities
        """
        all_related_entities = set()

        with SPARQLClient(meta_editor.endpoint, max_retries=5, backoff_factor=0.3, timeout=3600) as client:
            for i in range(0, len(merged_entities), batch_size):
                batch_merged = merged_entities[i : i + batch_size]
                merged_clauses = []
                for entity in batch_merged:
                    merged_clauses.extend(
                        [f"{{?entity ?p <{entity}>}}", f"{{<{entity}> ?p ?entity}}"]
                    )

                if not merged_clauses:
                    continue

                query = f"""
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX datacite: <http://purl.org/spar/datacite/>
                    PREFIX pro: <http://purl.org/spar/pro/>
                    SELECT DISTINCT ?entity WHERE {{
                        {{
                            {' UNION '.join(merged_clauses)}
                        }}
                        FILTER (?p != rdf:type)
                        FILTER (?p != datacite:usesIdentifierScheme)
                        FILTER (?p != pro:withRole)
                    }}
                """

                try:
                    results = client.query(query)
                    for result in results["results"]["bindings"]:
                        if result["entity"]["type"] == "uri":
                            related_uri = URIRef(result["entity"]["value"])
                            all_related_entities.add(related_uri)

                            for entity in batch_merged:
                                entity_uri = URIRef(entity)
                                if entity_uri not in meta_editor.relationship_cache:
                                    meta_editor.relationship_cache[entity_uri] = set()
                                meta_editor.relationship_cache[entity_uri].add(related_uri)

                except Exception as e:
                    print(
                        f"Error fetching related entities for merged batch {i}-{i+batch_size}: {e}"
                    )

            for i in range(0, len(surviving_entities), batch_size):
                batch_surviving = surviving_entities[i : i + batch_size]
                surviving_clauses = []
                for entity in batch_surviving:
                    surviving_clauses.append(f"{{<{entity}> ?p ?entity}}")

                if not surviving_clauses:
                    continue

                query = f"""
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX datacite: <http://purl.org/spar/datacite/>
                    PREFIX pro: <http://purl.org/spar/pro/>
                    SELECT DISTINCT ?entity WHERE {{
                        {{
                            {' UNION '.join(surviving_clauses)}
                        }}
                        FILTER (?p != rdf:type)
                        FILTER (?p != datacite:usesIdentifierScheme)
                        FILTER (?p != pro:withRole)
                    }}
                """

                try:
                    results = client.query(query)
                    for result in results["results"]["bindings"]:
                        if result["entity"]["type"] == "uri":
                            related_uri = URIRef(result["entity"]["value"])
                            all_related_entities.add(related_uri)

                            for entity in batch_surviving:
                                entity_uri = URIRef(entity)
                                if entity_uri not in meta_editor.relationship_cache:
                                    meta_editor.relationship_cache[entity_uri] = set()
                                meta_editor.relationship_cache[entity_uri].add(related_uri)

                except Exception as e:
                    print(
                        f"Error fetching related entities for surviving batch {i}-{i+batch_size}: {e}"
                    )

        return all_related_entities

    def should_stop_processing(self) -> bool:
        return os.path.exists(self.stop_file_path)

    def process_file(self, csv_file: str) -> str:
        """Process a single CSV file with cross-row batch processing"""
        logger.info(f"Starting to process file: {csv_file}")
        data = self.read_csv(csv_file)
        logger.info(f"Read {len(data)} rows from {csv_file}")
        meta_editor = MetaEditor(self.meta_config, self.resp_agent, save_queries=True)
        modified = False

        if self.should_stop_processing():
            logger.info("Stop file detected, halting processing")
            return csv_file

        g_set = GraphSet(
            meta_editor.base_iri, custom_counter_handler=meta_editor.counter_handler
        )

        batch_merged_entities = []
        batch_surviving_entities = []
        rows_to_process = []

        for row in data:
            if row.get("Done") == "True":
                continue

            entity_type = self.get_entity_type(row["surviving_entity"])
            if entity_type in self.entity_types:
                surviving_entity = row["surviving_entity"]
                merged_entities = row["merged_entities"].split("; ")
                batch_surviving_entities.append(surviving_entity)
                batch_merged_entities.extend(merged_entities)
                rows_to_process.append((surviving_entity, merged_entities))

        if not rows_to_process:
            logger.info(f"No rows to process in {csv_file}")
            return csv_file

        logger.info(f"Found {len(rows_to_process)} rows to process in {csv_file}")
        logger.info(
            f"Fetching related entities for {len(batch_merged_entities)} merged entities and {len(batch_surviving_entities)} surviving entities"
        )

        all_related_entities = self.fetch_related_entities_batch(
            meta_editor,
            batch_merged_entities,
            batch_surviving_entities,
            self.batch_size,
        )
        logger.info(f"Found {len(all_related_entities)} related entities")

        entities_to_import = all_related_entities.copy()
        entities_to_import.update(URIRef(e) for e in batch_surviving_entities)
        entities_to_import.update(URIRef(e) for e in batch_merged_entities)

        entities_to_import = {
            e for e in entities_to_import if not meta_editor.entity_cache.is_cached(e)
        }

        if entities_to_import:
            logger.info(f"Importing {len(entities_to_import)} new entities")
            try:
                meta_editor.reader.import_entities_from_triplestore(
                    g_set=g_set,
                    ts_url=meta_editor.endpoint,
                    entities=list(entities_to_import),
                    resp_agent=meta_editor.resp_agent,
                    enable_validation=False,
                    batch_size=self.batch_size,
                )

                for entity in entities_to_import:
                    meta_editor.entity_cache.add(entity)
                logger.info("Entity import completed successfully")

            except ValueError as e:
                logger.error(f"Error importing entities: {e}")
                modified = True

        processed_count = 0
        for surviving_entity, merged_entities in rows_to_process:
            logger.info(f"Processing row - surviving entity: {surviving_entity}")
            surviving_uri = URIRef(surviving_entity)
            for merged_entity in merged_entities:
                logger.info(
                    f"  Attempting to merge {merged_entity} into {surviving_entity}"
                )
                try:
                    meta_editor.merge(g_set, surviving_uri, URIRef(merged_entity))
                    modified = True
                    processed_count += 1
                    logger.info(f"  Successfully merged {merged_entity}")
                except ValueError as e:
                    logger.error(
                        f"Error merging {merged_entity} into {surviving_entity}: {e}"
                    )
                    continue
            logger.info(
                f"Completed processing row with surviving entity: {surviving_entity}"
            )

        logger.info(f"Successfully processed {processed_count} merges")

        if modified:
            marked_done = 0
            for row in data:
                if (
                    row.get("Done") != "True"
                    and self.get_entity_type(row["surviving_entity"])
                    in self.entity_types
                ):
                    row["Done"] = "True"
                    marked_done += 1

            logger.info(f"Marked {marked_done} rows as done")
            meta_editor.save(g_set)
            self.write_csv(csv_file, data)
            logger.info(f"Saved changes to {csv_file}")

        return csv_file

    def process_folder(self, csv_folder: str):
        if os.path.exists(self.stop_file_path):
            os.remove(self.stop_file_path)

        csv_files = [
            os.path.join(csv_folder, file)
            for file in os.listdir(csv_folder)
            if file.endswith(".csv")
        ]

        if self.workers > 4:
            csv_files = [
                file for file in csv_files if self.count_csv_rows(file) <= 10000
            ]

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=self.workers
        ) as executor:
            futures = {}
            for csv_file in csv_files:
                if self.should_stop_processing():
                    break
                futures[executor.submit(self.process_file, csv_file)] = csv_file

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Overall Progress",
            ):
                csv_file = futures[future]
                try:
                    future.result()
                except Exception as e:
                    error_trace = traceback.format_exc()
                    print(
                        f"""
                        Error processing file {csv_file}:
                        Type: {type(e).__name__}
                        Details: {str(e)}
                        Full Traceback:
                        {error_trace}
                        Suggestion: This is an unexpected error. Please check the traceback for more details.
                    """
                    )


def main():
    parser = argparse.ArgumentParser(
        description="Merge entities from CSV files in a folder."
    )
    parser.add_argument(
        "csv_folder", type=str, help="Path to the folder containing CSV files"
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
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers"
    )

    args = parser.parse_args()

    merger = EntityMerger(
        meta_config=args.meta_config,
        resp_agent=args.resp_agent,
        entity_types=args.entity_types,
        stop_file_path=args.stop_file,
        workers=args.workers,
    )

    merger.process_folder(args.csv_folder)


if __name__ == "__main__":
    main()
