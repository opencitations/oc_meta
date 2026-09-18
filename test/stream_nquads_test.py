# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import gzip
import json
import multiprocessing
import zipfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from rdflib import Dataset

from oc_meta.run.migration.stream_nquads import (
    bounded_nquads_results,
    convert_zip_to_nquads,
    convert_zip_to_nquads_file,
    create_progress,
    read_nquads_file_groups,
    write_nquads_chunks,
    write_nquads_line_groups,
    write_nquads_stdout,
)

SAMPLE_DATA_JSONLD = json.dumps(
    [
        {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06790727",
                    "@type": [
                        "http://purl.org/spar/fabio/JournalIssue",
                        "http://purl.org/spar/fabio/Expression",
                    ],
                    "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#string",
                            "@value": "7322",
                        }
                    ],
                    "http://purl.org/vocab/frbr/core#partOf": [
                        {"@id": "https://w3id.org/oc/meta/br/062203516555"}
                    ],
                },
                {
                    "@id": "https://w3id.org/oc/meta/br/06790671",
                    "@type": ["http://purl.org/spar/fabio/Expression"],
                    "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#string",
                            "@value": "6",
                        }
                    ],
                },
            ],
            "@id": "https://w3id.org/oc/meta/br/",
        }
    ]
)

SAMPLE_PROV_JSONLD = json.dumps(
    [
        {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#string",
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06790181' has been created.",
                        }
                    ],
                    "http://www.w3.org/ns/prov#generatedAtTime": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#dateTime",
                            "@value": "2024-03-27T17:31:42+00:00",
                        }
                    ],
                    "http://www.w3.org/ns/prov#specializationOf": [
                        {"@id": "https://w3id.org/oc/meta/br/06790181"}
                    ],
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06790181/prov/",
        },
        {
            "@graph": [
                {
                    "@id": "https://w3id.org/oc/meta/br/06790575/prov/se/1",
                    "@type": ["http://www.w3.org/ns/prov#Entity"],
                    "http://purl.org/dc/terms/description": [
                        {
                            "@type": "http://www.w3.org/2001/XMLSchema#string",
                            "@value": "The entity 'https://w3id.org/oc/meta/br/06790575' has been created.",
                        }
                    ],
                }
            ],
            "@id": "https://w3id.org/oc/meta/br/06790575/prov/",
        },
    ]
)


def _make_zip(tmp_path: Path, filename: str, content: str) -> str:
    zip_path = tmp_path / filename
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.json", content)
    return str(zip_path)


def _extract_quads(nquads_bytes: bytes) -> set[tuple[str, str, str, str]]:
    graph = Dataset(default_union=True)
    graph.parse(data=nquads_bytes.decode("utf-8"), format="nquads")
    return {(str(s), str(p), str(o), str(g)) for s, p, o, g in graph.quads()}


@pytest.mark.parametrize("file_count,workers", [(0, 2), (1, 2), (5, 2), (5, 1)])
def test_stdout_bounds_pending_conversions(tmp_path, file_count, workers):
    paths = [
        _make_zip(
            tmp_path,
            f"{index}.zip",
            SAMPLE_DATA_JSONLD if index % 2 == 0 else SAMPLE_PROV_JSONLD,
        )
        for index in range(file_count)
    ]
    expected_lines = sorted(
        line for path in paths for line in convert_zip_to_nquads(path).splitlines()
    )
    submitted = []
    writes = []
    context = multiprocessing.get_context("spawn")
    with context.Pool(workers) as pool:
        apply_async = pool.apply_async

        def submit(function, args):
            result = apply_async(function, args)
            submitted.append((args[0], result))
            return result

        class PausedOutput(BytesIO):
            def write(self, data):
                for _, result in submitted:
                    result.wait(timeout=20)
                    assert result.ready() is True
                assert [path for path, _ in submitted] == paths[
                    : min(file_count, workers + len(writes))
                ]
                writes.append(data)
                return super().write(data)

        output = PausedOutput()
        with (
            patch.object(pool, "apply_async", side_effect=submit),
            patch(
                "oc_meta.run.migration.stream_nquads.sys.stdout",
                SimpleNamespace(buffer=output),
            ),
        ):
            write_nquads_stdout(bounded_nquads_results(pool, paths, workers))

    assert len(writes) == file_count
    assert [path for path, _ in submitted] == paths
    assert sorted(output.getvalue().splitlines()) == expected_lines


def test_bounded_conversion_propagates_error(tmp_path):
    path = _make_zip(tmp_path, "invalid.zip", "not valid json {")
    with multiprocessing.get_context("spawn").Pool(1) as pool:
        with pytest.raises(json.JSONDecodeError):
            list(bounded_nquads_results(pool, [path], 1))


class TestConvertZipToNquads:
    def test_data_zip(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "1000.zip", SAMPLE_DATA_JSONLD)
        quads = _extract_quads(convert_zip_to_nquads(zip_path))

        assert quads == {
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://purl.org/spar/fabio/JournalIssue",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://purl.org/spar/fabio/Expression",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://purl.org/spar/fabio/hasSequenceIdentifier",
                "7322",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790727",
                "http://purl.org/vocab/frbr/core#partOf",
                "https://w3id.org/oc/meta/br/062203516555",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790671",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://purl.org/spar/fabio/Expression",
                "https://w3id.org/oc/meta/br/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790671",
                "http://purl.org/spar/fabio/hasSequenceIdentifier",
                "6",
                "https://w3id.org/oc/meta/br/",
            ),
        }

    def test_prov_zip(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "se.zip", SAMPLE_PROV_JSONLD)
        quads = _extract_quads(convert_zip_to_nquads(zip_path))

        assert quads == {
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://www.w3.org/ns/prov#Entity",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://purl.org/dc/terms/description",
                "The entity 'https://w3id.org/oc/meta/br/06790181' has been created.",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://www.w3.org/ns/prov#generatedAtTime",
                "2024-03-27T17:31:42+00:00",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790181/prov/se/1",
                "http://www.w3.org/ns/prov#specializationOf",
                "https://w3id.org/oc/meta/br/06790181",
                "https://w3id.org/oc/meta/br/06790181/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790575/prov/se/1",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://www.w3.org/ns/prov#Entity",
                "https://w3id.org/oc/meta/br/06790575/prov/",
            ),
            (
                "https://w3id.org/oc/meta/br/06790575/prov/se/1",
                "http://purl.org/dc/terms/description",
                "The entity 'https://w3id.org/oc/meta/br/06790575' has been created.",
                "https://w3id.org/oc/meta/br/06790575/prov/",
            ),
        }

    def test_bad_zip_raises(self, tmp_path: Path) -> None:
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"not a zip file")

        with pytest.raises(zipfile.BadZipFile):
            convert_zip_to_nquads(str(bad_zip))

    def test_no_json_files_raises(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "nojson.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "no json here")

        with pytest.raises(StopIteration):
            convert_zip_to_nquads(str(zip_path))

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "invalid.zip", "not valid json {")

        with pytest.raises(json.JSONDecodeError):
            convert_zip_to_nquads(str(zip_path))


class TestWriteNquadsChunks:
    def test_plain_chunks(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "chunks"

        write_nquads_chunks(
            [b"<s1> <p> <o> <g> .\n<s2> <p> <o> <g> .\n", b"<s3> <p> <o> <g> .\n"],
            output_dir,
            "meta-data",
            2,
            False,
        )

        files = sorted(path.name for path in output_dir.iterdir())
        assert files == ["meta-data.000000.nq", "meta-data.000001.nq"]
        assert (output_dir / "meta-data.000000.nq").read_bytes() == (
            b"<s1> <p> <o> <g> .\n<s2> <p> <o> <g> .\n"
        )
        assert (output_dir / "meta-data.000001.nq").read_bytes() == (
            b"<s3> <p> <o> <g> .\n"
        )

    def test_gzip_chunks(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "chunks"

        write_nquads_chunks(
            [b"<s1> <p> <o> <g> .\n<s2> <p> <o> <g> .\n<s3> <p> <o> <g> .\n"],
            output_dir,
            "meta-data",
            2,
            True,
        )

        files = sorted(path.name for path in output_dir.iterdir())
        assert files == ["meta-data.000000.nq.gz", "meta-data.000001.nq.gz"]
        with gzip.open(output_dir / "meta-data.000000.nq.gz", "rb") as file:
            first_chunk = file.read()
        with gzip.open(output_dir / "meta-data.000001.nq.gz", "rb") as file:
            second_chunk = file.read()
        assert first_chunk == b"<s1> <p> <o> <g> .\n<s2> <p> <o> <g> .\n"
        assert second_chunk == b"<s3> <p> <o> <g> .\n"

    def test_progress_counts_results(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "chunks"
        progress = create_progress()
        task_id = progress.add_task("Writing N-Quads files", total=2)

        write_nquads_chunks(
            [b"<s1> <p> <o> <g> .\n", b"<s2> <p> <o> <g> .\n"],
            output_dir,
            "meta-data",
            10,
            False,
            progress,
            task_id,
        )

        assert progress.tasks[0].completed == 2

    def test_file_chunks_remove_sources(self, tmp_path: Path) -> None:
        first_source = tmp_path / "first.nq"
        second_source = tmp_path / "second.nq"
        output_dir = tmp_path / "chunks"
        first_source.write_bytes(b"<s1> <p> <o> <g> .\n<s2> <p> <o> <g> .\n")
        second_source.write_bytes(b"<s3> <p> <o> <g> .\n")

        write_nquads_line_groups(
            read_nquads_file_groups([str(first_source), str(second_source)]),
            output_dir,
            "meta-data",
            2,
            False,
        )

        files = sorted(path.name for path in output_dir.iterdir())
        assert files == ["meta-data.000000.nq", "meta-data.000001.nq"]
        assert (output_dir / "meta-data.000000.nq").read_bytes() == (
            b"<s1> <p> <o> <g> .\n<s2> <p> <o> <g> .\n"
        )
        assert (output_dir / "meta-data.000001.nq").read_bytes() == (
            b"<s3> <p> <o> <g> .\n"
        )
        assert first_source.exists() is False
        assert second_source.exists() is False

    def test_convert_zip_to_nquads_file(self, tmp_path: Path) -> None:
        zip_path = _make_zip(tmp_path, "1000.zip", SAMPLE_DATA_JSONLD)
        output_path = tmp_path / "result.nq"

        result_path = convert_zip_to_nquads_file((zip_path, str(output_path)))

        assert result_path == str(output_path)
        assert _extract_quads(output_path.read_bytes()) == _extract_quads(
            convert_zip_to_nquads(zip_path)
        )
