# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import os
import sqlite3
from contextlib import closing
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import orjson

from oc_meta.lib.console import create_progress
from oc_meta.lib.rdf_patch import (
    IS_DOCUMENT_CONTEXT_FOR,
    IS_HELD_BY,
    PROV_SPECIALIZATION_OF,
    EntityFileLocator,
    data_files,
    ids,
    load_audit_config,
    provenance_path,
    snapshot_number,
)


def read_entities(path: str) -> dict[str, dict[str, object]]:
    entities = {}
    if path.endswith(".zip"):
        with ZipFile(path) as archive:
            for name in archive.namelist():
                if name.endswith(".json"):
                    for graph in orjson.loads(archive.read(name)):
                        for entity in graph["@graph"]:
                            entities[entity["@id"]] = entity
    else:
        with open(path, "rb") as stream:
            for graph in orjson.loads(stream.read()):
                for entity in graph["@graph"]:
                    entities[entity["@id"]] = entity
    return entities


def find_dangling_ras(config_path: str, report_path: str) -> dict[str, int]:
    config = load_audit_config(config_path)
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    for family in ("ra", "ar", "br"):
        directory = os.path.join(config.rdf_dir, family)
        if not os.path.isdir(directory):
            raise FileNotFoundError(directory)
    report_path = os.path.abspath(report_path)
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with (
        TemporaryDirectory(
            prefix="oc_meta_dangling_ras_", dir=os.path.dirname(report_path)
        ) as temporary,
        closing(sqlite3.connect(os.path.join(temporary, "scan.sqlite"))) as connection,
    ):
        connection.executescript("""
            PRAGMA cache_size = -32768;
            PRAGMA temp_store = FILE;
            PRAGMA mmap_size = 0;
            CREATE TABLE agents (uri TEXT PRIMARY KEY) WITHOUT ROWID;
            CREATE TABLE holders (
                ar TEXT NOT NULL, ra TEXT NOT NULL
            );
            CREATE TABLE missing (
                ar TEXT NOT NULL, ra TEXT NOT NULL,
                PRIMARY KEY (ar, ra)
            ) WITHOUT ROWID;
            CREATE TABLE contexts (
                ar TEXT NOT NULL, br TEXT NOT NULL,
                PRIMARY KEY (ar, br)
            ) WITHOUT ROWID;
        """)
        with create_progress() as progress:
            for family in ("ra", "ar", "br"):
                if (
                    family == "br"
                    and connection.execute("SELECT COUNT(*) FROM missing").fetchone()[0]
                    == 0
                ):
                    break
                paths = data_files(
                    os.path.join(config.rdf_dir, family), config.zip_output
                )
                task = progress.add_task(f"Reading {family} archives", total=len(paths))
                for position, path in enumerate(paths, 1):
                    entities = read_entities(path)
                    if family == "ra":
                        connection.executemany(
                            "INSERT OR IGNORE INTO agents VALUES (?)",
                            ((uri,) for uri in entities),
                        )
                    elif family == "ar":
                        connection.executemany(
                            "INSERT INTO holders VALUES (?, ?)",
                            (
                                (uri, ra)
                                for uri, entity in entities.items()
                                for ra in ids(entity, IS_HELD_BY)
                            ),
                        )
                        connection.execute("""
                            INSERT OR IGNORE INTO missing
                            SELECT ar, ra FROM holders
                            WHERE NOT EXISTS (SELECT 1 FROM agents WHERE uri = ra)
                        """)
                        connection.execute("DELETE FROM holders")
                    else:
                        connection.executemany(
                            """INSERT OR IGNORE INTO contexts
                            SELECT ?, ? WHERE EXISTS
                            (SELECT 1 FROM missing WHERE ar = ?)""",
                            (
                                (ar, uri, ar)
                                for uri, entity in entities.items()
                                for ar in ids(entity, IS_DOCUMENT_CONTEXT_FOR)
                            ),
                        )
                    if position % 1000 == 0:
                        connection.commit()
                    progress.advance(task)
                connection.commit()

            summary = {
                "missing_ras": connection.execute(
                    "SELECT COUNT(DISTINCT ra) FROM missing"
                ).fetchone()[0],
                "dangling_references": connection.execute(
                    "SELECT COUNT(*) FROM missing"
                ).fetchone()[0],
                "affected_ars": connection.execute(
                    "SELECT COUNT(DISTINCT ar) FROM missing"
                ).fetchone()[0],
                "affected_brs": connection.execute(
                    "SELECT COUNT(DISTINCT br) FROM contexts"
                ).fetchone()[0],
            }
            task = progress.add_task(
                "Writing references and provenance",
                total=summary["dangling_references"],
            )
            output = os.path.join(temporary, "report.jsonl")
            with open(output, "wb") as stream:
                stream.write(orjson.dumps({"summary": summary}) + b"\n")
                for ar, ra in connection.execute(
                    "SELECT ar, ra FROM missing ORDER BY ar, ra"
                ):
                    expected_path = locator.path(ra)
                    prov_path = provenance_path(expected_path, config.zip_output)
                    snapshots = []
                    if os.path.exists(prov_path):
                        snapshots = [
                            snapshot
                            for snapshot in read_entities(prov_path).values()
                            if ra in ids(snapshot, PROV_SPECIALIZATION_OF)
                        ]
                        snapshots.sort(
                            key=lambda item: snapshot_number(str(item["@id"]))
                        )
                    record = {
                        "ra": ra,
                        "ar": ar,
                        "expected_ra_file": expected_path,
                        "role": read_entities(locator.path(ar))[ar],
                        "works": [
                            read_entities(locator.path(br))[br]
                            for (br,) in connection.execute(
                                "SELECT br FROM contexts WHERE ar = ? ORDER BY br",
                                (ar,),
                            )
                        ],
                        "provenance": snapshots,
                    }
                    stream.write(orjson.dumps(record) + b"\n")
                    progress.advance(task)
            os.replace(output, report_path)
    return summary


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Find AR references to absent RAs without editing RDF."
    )
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("--report-file", required=True, help="Output JSON Lines report")
    args = parser.parse_args()
    print(orjson.dumps(find_dangling_ras(args.config, args.report_file)).decode())


if __name__ == "__main__":  # pragma: no cover
    main()
