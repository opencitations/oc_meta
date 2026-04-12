# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import shutil
import tempfile
import zipfile
from pathlib import Path

import py7zr
import pytest
from rdflib import Dataset

from oc_meta.run.migration.rdf_to_nquads import process_zip_file


class TestProcessZipFile:
    @pytest.fixture
    def temp_dirs(self):
        test_dir = Path(tempfile.mkdtemp())
        input_dir = test_dir / "input"
        output_dir = test_dir / "output"
        input_dir.mkdir()
        output_dir.mkdir()
        yield input_dir, output_dir, test_dir
        shutil.rmtree(test_dir)

    def test_success_uncompressed(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        prov_dir = input_dir / "ra" / "0610" / "prov"
        prov_dir.mkdir(parents=True)
        zip_path = prov_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", SAMPLE_JSONLD)

        process_zip_file(zip_path, output_dir, input_dir, compress=False)

        output_file = output_dir / "ra-0610-prov-se.nq"
        assert output_file.exists()
        output_graph = Dataset(default_union=True)
        output_graph.parse(output_file, format="nquads")
        assert len(output_graph) == 2

    def test_success_compressed(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        prov_dir = input_dir / "ra" / "0610" / "prov"
        prov_dir.mkdir(parents=True)
        zip_path = prov_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", SAMPLE_JSONLD)

        process_zip_file(zip_path, output_dir, input_dir, compress=True)

        output_file = output_dir / "ra-0610-prov-se.nq.7z"
        assert output_file.exists()
        extract_dir = output_dir / "extracted"
        extract_dir.mkdir()
        with py7zr.SevenZipFile(output_file, "r") as archive:
            archive.extractall(path=extract_dir)
        nquads_file = extract_dir / "ra-0610-prov-se.nq"
        output_graph = Dataset(default_union=True)
        output_graph.parse(nquads_file, format="nquads")
        assert len(output_graph) == 2

    def test_no_json_files_raises(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "no json here")

        with pytest.raises(StopIteration):
            process_zip_file(zip_path, output_dir, input_dir, compress=False)

    def test_bad_zip_raises(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        bad_zip = input_dir / "se.zip"
        bad_zip.write_bytes(b"not a zip file")

        with pytest.raises(zipfile.BadZipFile):
            process_zip_file(bad_zip, output_dir, input_dir, compress=False)

    def test_invalid_jsonld_raises(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", INVALID_JSONLD)

        with pytest.raises(Exception):
            process_zip_file(zip_path, output_dir, input_dir, compress=False)
