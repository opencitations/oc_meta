# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import csv
import logging
import multiprocessing as mp
import os
import shutil
import tempfile
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence

import orjson
from rdflib import Dataset, URIRef
from rich_argparse import RichHelpFormatter
from tqdm import tqdm

from oc_meta.lib.file_manager import collect_files

ERROR_LOG_FILENAME = "error_log_find_duplicated_resources.txt"
LOGGER = logging.getLogger(__name__)

PathLikeString = str | os.PathLike[str]
IdentifierKey = tuple[str, str]


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def find(self, item: str) -> str:
        if item not in self.parent:
            self.parent[item] = item
            self.rank[item] = 0
            return item

        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, x: str, y: str) -> None:
        xroot = self.find(x)
        yroot = self.find(y)

        if xroot == yroot:
            return

        if self.rank[xroot] < self.rank[yroot]:
            self.parent[xroot] = yroot
        elif self.rank[xroot] > self.rank[yroot]:
            self.parent[yroot] = xroot
        else:
            self.parent[yroot] = xroot
            self.rank[xroot] += 1


def get_zip_files(folder_path: PathLikeString) -> list[str]:
    return sorted(
        collect_files(
            os.fspath(folder_path),
            pattern="*.zip",
            path_filter=lambda p: os.path.basename(p) != "se.zip",
        )
    )


def save_merge_rows_to_csv(
    duplicates: Iterable[tuple[str, Sequence[str]]], csv_path: PathLikeString
) -> None:
    with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["surviving_entity", "merged_entities"])

        for surviving_entity, merged_group in duplicates:
            csv_writer.writerow([surviving_entity, "; ".join(merged_group)])


def process_identifier_zip_file(
    zip_path: PathLikeString,
) -> dict[IdentifierKey, set[str]]:
    entity_info: defaultdict[IdentifierKey, set[str]] = defaultdict(set)
    datacite_uses_identifier_scheme = URIRef(
        "http://purl.org/spar/datacite/usesIdentifierScheme"
    )
    literal_reification_has_literal_value = URIRef(
        "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
    )

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for zip_file in zip_ref.namelist():
                try:
                    with zip_ref.open(zip_file) as rdf_file:
                        graph = Dataset(default_union=True)
                        graph.parse(data=rdf_file.read(), format="json-ld")

                        for subject, _, identifier_scheme_ref in graph.triples(
                            (None, datacite_uses_identifier_scheme, None)
                        ):
                            entity_id = str(subject)
                            identifier_scheme = str(identifier_scheme_ref)
                            literal_value = graph.value(
                                subject, literal_reification_has_literal_value
                            )
                            if identifier_scheme and literal_value:
                                key = (identifier_scheme, str(literal_value))
                                entity_info[key].add(entity_id)
                except Exception as e:
                    print(f"Error processing file {zip_file} in {zip_path}: {str(e)}")
    except zipfile.BadZipFile:
        print(f"Corrupted or invalid ZIP file: {zip_path}")
    except Exception as e:
        print(f"Error opening ZIP file {zip_path}: {str(e)}")

    return dict(entity_info)


def save_identifier_chunk_to_temp_csv(
    entity_info: Mapping[IdentifierKey, set[str]], temp_file_path: PathLikeString
) -> None:
    with open(temp_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["identifier_scheme", "literal_value", "entity_ids"])
        for (scheme, value), ids in entity_info.items():
            csv_writer.writerow([scheme, value, ";".join(sorted(ids))])


def load_and_merge_identifier_temp_csv(
    temp_file_path: PathLikeString, entity_info: defaultdict[IdentifierKey, set[str]]
) -> None:
    with open(temp_file_path, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            key = (row["identifier_scheme"], row["literal_value"])
            ids = set(row["entity_ids"].split(";"))
            entity_info[key].update(ids)


def process_identifier_chunk(
    zip_files_chunk: Sequence[str], temp_dir: PathLikeString, chunk_index: int
) -> str:
    entity_info: defaultdict[IdentifierKey, set[str]] = defaultdict(set)

    # Use forkserver to avoid deadlocks when forking in a multi-threaded environment.
    ctx = mp.get_context("spawn") if os.name == "nt" else mp.get_context("forkserver")
    with ctx.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(process_identifier_zip_file, zip_files_chunk)

    for result in results:
        for key, value in result.items():
            entity_info[key].update(value)

    temp_file_path = get_chunk_temp_file_path(temp_dir, chunk_index)
    save_identifier_chunk_to_temp_csv(entity_info, temp_file_path)

    return temp_file_path


def get_chunk_temp_file_path(temp_dir: PathLikeString, chunk_index: int) -> str:
    return os.path.join(temp_dir, f"chunk_{chunk_index}.csv")


def find_duplicate_ids(
    folder_path: PathLikeString, csv_path: PathLikeString, chunk_size: int = 5000
) -> None:
    id_folder_path = os.path.join(folder_path, "id")

    if not os.path.exists(id_folder_path):
        print(f"Error: The 'id' subfolder does not exist in path: {folder_path}")
        return

    zip_files = get_zip_files(id_folder_path)
    output_dir = os.path.dirname(os.path.abspath(csv_path))
    temp_dir = tempfile.mkdtemp(prefix="oc_meta_duplicates_", dir=output_dir)

    try:
        chunks = [
            zip_files[i : i + chunk_size] for i in range(0, len(zip_files), chunk_size)
        ]
        temp_files = [
            get_chunk_temp_file_path(temp_dir, chunk_index)
            for chunk_index in range(len(chunks))
        ]

        print(
            f"Processing {len(zip_files)} ZIP files in {len(chunks)} chunks of max {chunk_size} files each"
        )
        print(f"Temporary files will be stored in: {temp_dir}")

        for chunk_index, chunk in enumerate(tqdm(chunks, desc="Processing chunks")):
            process_identifier_chunk(chunk, temp_dir, chunk_index)

        print("Merging chunk results...")
        entity_info: defaultdict[IdentifierKey, set[str]] = defaultdict(set)
        for temp_file in tqdm(temp_files, desc="Merging chunks"):
            load_and_merge_identifier_temp_csv(temp_file, entity_info)

        save_identifier_duplicates_to_csv(entity_info, csv_path)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def get_identifier_duplicate_rows(
    entity_info: Mapping[IdentifierKey, set[str]],
) -> list[tuple[str, list[str]]]:
    duplicates = []
    for ids in entity_info.values():
        if len(ids) > 1:
            ids_list = sorted(ids)
            duplicates.append((ids_list[0], ids_list[1:]))
    return duplicates


def save_identifier_duplicates_to_csv(
    entity_info: Mapping[IdentifierKey, set[str]], csv_path: PathLikeString
) -> None:
    try:
        save_merge_rows_to_csv(get_identifier_duplicate_rows(entity_info), csv_path)
    except Exception as e:
        print(f"Error saving CSV file {csv_path}: {str(e)}")


def find_duplicate_brs(folder_path: PathLikeString, csv_path: PathLikeString) -> None:
    find_duplicate_resources_by_type(folder_path, csv_path, "br")


def find_duplicate_ras(folder_path: PathLikeString, csv_path: PathLikeString) -> None:
    find_duplicate_resources_by_type(folder_path, csv_path, "ra")


def find_duplicate_resources_by_type(
    folder_path: PathLikeString, csv_path: PathLikeString, resource_dir: str
) -> None:
    resources: dict[str, set[str]] = {}
    qualities: dict[str, tuple[int, ...]] = {}
    error_log_handler, error_log_path = configure_error_log(csv_path)

    try:
        entity_folder_path = os.path.join(folder_path, resource_dir)
        process_entity_folder(entity_folder_path, resources, qualities, resource_dir)

        save_entity_duplicates_to_csv(resources, csv_path, qualities)
    finally:
        close_error_log(error_log_handler, error_log_path)


def configure_error_log(csv_path: PathLikeString) -> tuple[logging.FileHandler, str]:
    error_log_path = os.path.join(
        os.path.dirname(os.path.abspath(csv_path)), ERROR_LOG_FILENAME
    )
    handler = logging.FileHandler(error_log_path, mode="w", encoding="utf-8")
    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.ERROR)
    return handler, error_log_path


def close_error_log(
    handler: logging.FileHandler, error_log_path: PathLikeString
) -> None:
    LOGGER.removeHandler(handler)
    handler.close()
    if os.path.exists(error_log_path) and os.path.getsize(error_log_path) == 0:
        os.remove(error_log_path)


def process_entity_folder(
    folder_path: PathLikeString,
    resources: dict[str, set[str]],
    qualities: dict[str, tuple[int, ...]],
    expected_type: str,
) -> None:
    if not os.path.exists(folder_path):
        LOGGER.error(
            f"La sottocartella '{expected_type}' non esiste nel percorso: {folder_path}"
        )
        return

    zip_files = get_zip_files(folder_path)

    for zip_path in tqdm(zip_files, desc=f"Analizzando i file ZIP in {expected_type}"):
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for zip_file in zip_ref.namelist():
                    try:
                        with zip_ref.open(zip_file) as json_file:
                            data = orjson.loads(json_file.read())
                            analyze_entity_json(
                                data,
                                resources,
                                qualities,
                                zip_path,
                                zip_file,
                                expected_type,
                            )
                    except orjson.JSONDecodeError:
                        LOGGER.error(
                            f"Errore nel parsing JSON del file {zip_file} in {zip_path}"
                        )
                    except Exception as e:
                        LOGGER.error(
                            f"Errore nell'elaborazione del file {zip_file} in {zip_path}: {str(e)}"
                        )
        except zipfile.BadZipFile:
            LOGGER.error(f"File ZIP corrotto o non valido: {zip_path}")
        except Exception as e:
            LOGGER.error(f"Errore nell'apertura del file ZIP {zip_path}: {str(e)}")


def analyze_entity_json(
    data,
    resources: dict[str, set[str]],
    qualities: dict[str, tuple[int, ...]],
    zip_path: PathLikeString,
    zip_file: str,
    expected_type: str,
) -> None:
    for graph in data:
        for entity in graph["@graph"]:
            try:
                entity_id = entity["@id"]
                entity_type = get_entity_type(entity)

                if entity_type is None:
                    print(
                        f"Tipo non specificato per l'entità {entity_id} nel file {zip_file} all'interno di {zip_path}. Assumendo tipo {expected_type}."
                    )
                    entity_type = expected_type

                if entity_type == expected_type:
                    identifiers = get_identifiers(entity)

                    if entity_id not in resources:
                        resources[entity_id] = set()
                    resources[entity_id].update(identifiers)
                    qualities[entity_id] = get_entity_quality(entity, entity_type)
            except KeyError as e:
                entity_id = entity["@id"] if "@id" in entity else "ID sconosciuto"
                LOGGER.error(
                    f"Chiave mancante nell'entità {entity_id} "
                    f"nel file {zip_file} all'interno di {zip_path}: {str(e)}"
                )
            except Exception as e:
                entity_id = entity["@id"] if "@id" in entity else "ID sconosciuto"
                LOGGER.error(
                    f"Errore nell'analisi dell'entità {entity_id} "
                    f"nel file {zip_file} all'interno di {zip_path}: {str(e)}"
                )


def get_entity_type(entity) -> str | None:
    entity_types = entity["@type"] if "@type" in entity else []
    if "http://purl.org/spar/fabio/Expression" in entity_types:
        return "br"
    if "http://xmlns.com/foaf/0.1/Agent" in entity_types:
        return "ra"
    return None


def get_identifiers(entity) -> list[str]:
    identifiers = []
    predicate = "http://purl.org/spar/datacite/hasIdentifier"
    if predicate not in entity:
        return identifiers

    for identifier in entity[predicate]:
        if isinstance(identifier, dict) and "@id" in identifier:
            identifiers.append(identifier["@id"])
    return identifiers


def get_entity_quality(entity, entity_type: str) -> tuple[int, ...]:
    if entity_type == "br":
        pub_date = get_literal(
            entity, "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
        )
        return (
            int(bool(get_literal(entity, "http://purl.org/dc/terms/title"))),
            len(pub_date),
            int(bool(get_literal(entity, "http://purl.org/spar/fabio/hasSubtitle"))),
            int(bool(get_uri(entity, "http://purl.org/vocab/frbr/core#partOf"))),
            int(
                bool(
                    get_literal(
                        entity, "http://purl.org/spar/fabio/hasSequenceIdentifier"
                    )
                )
            ),
            int(bool(get_literal(entity, "http://purl.org/spar/fabio/hasEdition"))),
            len(entity["@type"] if "@type" in entity else []),
        )
    return (
        sum(
            bool(get_literal(entity, predicate))
            for predicate in [
                "http://xmlns.com/foaf/0.1/name",
                "http://xmlns.com/foaf/0.1/givenName",
                "http://xmlns.com/foaf/0.1/familyName",
            ]
        ),
        sum(
            len(get_literal(entity, predicate))
            for predicate in [
                "http://xmlns.com/foaf/0.1/name",
                "http://xmlns.com/foaf/0.1/givenName",
                "http://xmlns.com/foaf/0.1/familyName",
            ]
        ),
    )


def get_literal(entity, predicate: str) -> str:
    if predicate not in entity:
        return ""
    values = entity[predicate]
    if isinstance(values, dict):
        values = [values]
    for value in values:
        if isinstance(value, dict) and "@value" in value:
            return value["@value"]
    return ""


def get_uri(entity, predicate: str) -> str:
    if predicate not in entity:
        return ""
    values = entity[predicate]
    if isinstance(values, dict):
        values = [values]
    for value in values:
        if isinstance(value, dict) and "@id" in value:
            return value["@id"]
    return ""


def save_entity_duplicates_to_csv(
    resources: Mapping[str, set[str]],
    csv_path: PathLikeString,
    qualities: Mapping[str, tuple[int, ...]] | None = None,
) -> None:
    try:
        save_merge_rows_to_csv(find_entity_duplicates(resources, qualities), csv_path)
    except Exception as e:
        LOGGER.error(f"Errore nel salvataggio del file CSV {csv_path}: {str(e)}")


def find_entity_duplicates(
    resources: Mapping[str, set[str]],
    qualities: Mapping[str, tuple[int, ...]] | None = None,
) -> list[tuple[str, list[str]]]:
    union_find = UnionFind()

    for entity, identifiers in resources.items():
        for identifier in identifiers:
            union_find.union(entity, identifier)

    groups: dict[str, list[str]] = {}
    for entity in resources:
        representative = union_find.find(entity)
        if representative not in groups:
            groups[representative] = []
        groups[representative].append(entity)

    duplicate_groups = []
    for group in groups.values():
        if len(group) > 1:
            surviving_entity = select_surviving_entity(group, qualities)
            merged_entities = sorted(
                entity for entity in group if entity != surviving_entity
            )
            duplicate_groups.append((surviving_entity, merged_entities))
    return duplicate_groups


def select_surviving_entity(
    group: Sequence[str], qualities: Mapping[str, tuple[int, ...]] | None = None
) -> str:
    if qualities is None:
        qualities = {}

    def sort_key(entity: str):
        quality = qualities[entity] if entity in qualities else (0,)
        return tuple(-value for value in quality) + (entity,)

    return min(group, key=sort_key)


def add_folder_and_csv_arguments(
    parser: argparse.ArgumentParser, folder_help: str
) -> None:
    parser.add_argument("folder_path", type=str, help=folder_help)
    parser.add_argument(
        "csv_path", type=str, help="Path to the CSV file to save duplicates"
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find duplicate IDs, RAs, and BRs in RDF ZIP archives.",
        formatter_class=RichHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ids_parser = subparsers.add_parser(
        "ids",
        help="Find duplicate IDs in the 'id' subfolder",
        formatter_class=RichHelpFormatter,
    )
    add_folder_and_csv_arguments(
        ids_parser, "Path to the folder containing the 'id' subfolder"
    )
    ids_parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of ZIP files to process per chunk (default: 5000)",
    )

    ras_parser = subparsers.add_parser(
        "ras",
        help="Find duplicate responsible agents in the 'ra' subfolder",
        formatter_class=RichHelpFormatter,
    )
    add_folder_and_csv_arguments(
        ras_parser, "Path to the folder containing the 'ra' subfolder"
    )

    brs_parser = subparsers.add_parser(
        "brs",
        help="Find duplicate bibliographic resources in the 'br' subfolder",
        formatter_class=RichHelpFormatter,
    )
    add_folder_and_csv_arguments(
        brs_parser, "Path to the folder containing the 'br' subfolder"
    )

    return parser


def main() -> None:  # pragma: no cover
    parser = build_argument_parser()
    args = parser.parse_args()

    if args.command == "ids":
        find_duplicate_ids(args.folder_path, args.csv_path, args.chunk_size)
    elif args.command == "ras":
        find_duplicate_ras(args.folder_path, args.csv_path)
    elif args.command == "brs":
        find_duplicate_brs(args.folder_path, args.csv_path)


if __name__ == "__main__":  # pragma: no cover
    main()
