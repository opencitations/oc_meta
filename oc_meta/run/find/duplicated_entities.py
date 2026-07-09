# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import csv
import logging
import os
import zipfile

import orjson
from rich_argparse import RichHelpFormatter
from tqdm import tqdm

from oc_meta.lib.file_manager import collect_files

ERROR_LOG_FILENAME = "error_log_find_duplicated_resources.txt"
LOGGER = logging.getLogger(__name__)


class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, item):
        if item not in self.parent:
            self.parent[item] = item
            self.rank[item] = 0
            return item

        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, x, y):
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


def read_and_analyze_zip_files(folder_path, csv_path, resource_type):
    resources = {}
    qualities = {}
    error_log_handler, error_log_path = configure_error_log(csv_path)

    try:
        if resource_type in ["br", "both"]:
            br_folder_path = os.path.join(folder_path, "br")
            process_folder(br_folder_path, resources, qualities, "br")

        if resource_type in ["ra", "both"]:
            ra_folder_path = os.path.join(folder_path, "ra")
            process_folder(ra_folder_path, resources, qualities, "ra")

        save_duplicates_to_csv(resources, csv_path, qualities)
    finally:
        close_error_log(error_log_handler, error_log_path)


def configure_error_log(csv_path):
    error_log_path = os.path.join(
        os.path.dirname(os.path.abspath(csv_path)), ERROR_LOG_FILENAME
    )
    handler = logging.FileHandler(error_log_path, mode="w", encoding="utf-8")
    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.ERROR)
    return handler, error_log_path


def close_error_log(handler, error_log_path):
    LOGGER.removeHandler(handler)
    handler.close()
    if os.path.exists(error_log_path) and os.path.getsize(error_log_path) == 0:
        os.remove(error_log_path)


def process_folder(folder_path, resources, qualities, expected_type):
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
                            analyze_json(
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


def get_zip_files(folder_path: str) -> list[str]:
    return sorted(
        collect_files(
            folder_path,
            pattern="*.zip",
            path_filter=lambda p: os.path.basename(p) != "se.zip",
        )
    )


def analyze_json(data, resources, qualities, zip_path, zip_file, expected_type):
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
                LOGGER.error(
                    f"Chiave mancante nell'entità {entity.get('@id', 'ID sconosciuto')} "
                    f"nel file {zip_file} all'interno di {zip_path}: {str(e)}"
                )
            except Exception as e:
                LOGGER.error(
                    f"Errore nell'analisi dell'entità {entity.get('@id', 'ID sconosciuto')} "
                    f"nel file {zip_file} all'interno di {zip_path}: {str(e)}"
                )


def get_entity_type(entity):
    if "http://purl.org/spar/fabio/Expression" in entity.get("@type", []):
        return "br"
    elif "http://xmlns.com/foaf/0.1/Agent" in entity.get("@type", []):
        return "ra"
    return None


def get_identifiers(entity):
    identifiers = []
    for identifier in entity.get("http://purl.org/spar/datacite/hasIdentifier", []):
        if isinstance(identifier, dict) and "@id" in identifier:
            identifiers.append(identifier["@id"])
    return identifiers


def get_entity_quality(entity, entity_type):
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


def get_literal(entity, predicate):
    if predicate not in entity:
        return ""
    values = entity[predicate]
    if isinstance(values, dict):
        values = [values]
    for value in values:
        if isinstance(value, dict) and "@value" in value:
            return value["@value"]
    return ""


def get_uri(entity, predicate):
    if predicate not in entity:
        return ""
    values = entity[predicate]
    if isinstance(values, dict):
        values = [values]
    for value in values:
        if isinstance(value, dict) and "@id" in value:
            return value["@id"]
    return ""


def save_duplicates_to_csv(resources, csv_path, qualities=None):
    try:
        with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["surviving_entity", "merged_entities"])

            duplicates = find_duplicates(resources, qualities)
            for surviving_entity, merged_group in duplicates:
                merged_entities = "; ".join(merged_group)
                csv_writer.writerow([surviving_entity, merged_entities])
    except Exception as e:
        LOGGER.error(f"Errore nel salvataggio del file CSV {csv_path}: {str(e)}")


def find_duplicates(resources, qualities=None):
    uf = UnionFind()

    # First, create sets of identifiers for each entity
    for entity, identifiers in resources.items():
        for identifier in identifiers:
            uf.union(entity, identifier)

    # Then, group entities by their representative
    groups = {}
    for entity in resources:
        rep = uf.find(entity)
        if rep not in groups:
            groups[rep] = []
        groups[rep].append(entity)

    # Filter out groups with only one entity
    duplicate_groups = []
    for group in groups.values():
        if len(group) > 1:
            surviving_entity = select_surviving_entity(group, qualities)
            merged_entities = sorted(
                entity for entity in group if entity != surviving_entity
            )
            duplicate_groups.append((surviving_entity, merged_entities))
    return duplicate_groups


def select_surviving_entity(group, qualities=None):
    if qualities is None:
        qualities = {}

    def sort_key(entity):
        quality = qualities[entity] if entity in qualities else (0,)
        return (*[-value for value in quality], entity)

    return min(group, key=sort_key)


def main():
    parser = argparse.ArgumentParser(
        description="Trova risorse duplicate in base ai loro ID.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "folder_path",
        type=str,
        help="Percorso della cartella contenente le sottocartelle 'br' e 'ra'",
    )
    parser.add_argument(
        "csv_path", type=str, help="Percorso del file CSV per salvare i duplicati"
    )
    parser.add_argument(
        "resource_type",
        type=str,
        choices=["br", "ra", "both"],
        help="Tipo di risorsa da analizzare: 'br' per risorse bibliografiche, 'ra' per agenti responsabili, 'both' per entrambi",
    )
    args = parser.parse_args()

    read_and_analyze_zip_files(args.folder_path, args.csv_path, args.resource_type)


if __name__ == "__main__":
    main()
