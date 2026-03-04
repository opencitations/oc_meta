# Copyright 2026, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import shutil
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Dataset

from oc_meta.run.migration.rdf_to_nquads import convert_jsonld_to_nquads, find_zip_files, main, process_zip_file

SAMPLE_JSONLD = """{
  "@context": "https://schema.org",
  "@id": "http://example.org/entity1",
  "@type": "CreativeWork",
  "name": "Test Entity"
}"""

INVALID_JSONLD = "not valid json at all {"


class TestConvertJsonldToNquads:
    def test_success(self) -> None:
        graph, nquads = convert_jsonld_to_nquads(SAMPLE_JSONLD)

        assert len(graph) == 2
        assert isinstance(nquads, str)
        assert "http://example.org/entity1" in nquads

    def test_invalid_jsonld_raises(self) -> None:
        with pytest.raises(Exception):
            convert_jsonld_to_nquads(INVALID_JSONLD)


class TestFindZipFiles:
    @pytest.fixture
    def temp_dirs(self):
        test_dir = Path(tempfile.mkdtemp())
        yield test_dir
        shutil.rmtree(test_dir)

    def test_prov_mode(self, temp_dirs: Path) -> None:
        test_dir = temp_dirs
        prov_dir = test_dir / "br" / "060" / "1000" / "prov"
        prov_dir.mkdir(parents=True)
        (prov_dir / "se.zip").touch()
        (test_dir / "br" / "060" / "1000.zip").touch()

        result = find_zip_files(test_dir, "prov")

        assert len(result) == 1
        assert result[0].name == "se.zip"

    def test_data_mode(self, temp_dirs: Path) -> None:
        test_dir = temp_dirs
        prov_dir = test_dir / "br" / "060" / "1000" / "prov"
        prov_dir.mkdir(parents=True)
        (prov_dir / "se.zip").touch()
        data_zip = test_dir / "br" / "060" / "1000.zip"
        data_zip.touch()

        result = find_zip_files(test_dir, "data")

        assert len(result) == 1
        assert result[0] == data_zip

    def test_data_mode_excludes_prov_folder(self, temp_dirs: Path) -> None:
        test_dir = temp_dirs
        prov_dir = test_dir / "br" / "060" / "prov"
        prov_dir.mkdir(parents=True)
        (prov_dir / "other.zip").touch()
        data_zip = test_dir / "br" / "060" / "1000.zip"
        data_zip.touch()

        result = find_zip_files(test_dir, "data")

        assert len(result) == 1
        assert result[0] == data_zip

    def test_all_mode(self, temp_dirs: Path) -> None:
        test_dir = temp_dirs
        prov_dir = test_dir / "br" / "060" / "1000" / "prov"
        prov_dir.mkdir(parents=True)
        (prov_dir / "se.zip").touch()
        data_zip = test_dir / "br" / "060" / "1000.zip"
        data_zip.touch()

        result = find_zip_files(test_dir, "all")

        assert len(result) == 2


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

    def test_success(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        prov_dir = input_dir / "ra" / "0610" / "prov"
        prov_dir.mkdir(parents=True)
        zip_path = prov_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", SAMPLE_JSONLD)

        result = process_zip_file(zip_path, output_dir, input_dir)

        assert result is True
        output_file = output_dir / "ra-0610-prov-se.nq"
        assert output_file.exists()
        output_graph = Dataset(default_union=True)
        output_graph.parse(output_file, format="nquads")
        assert len(output_graph) == 2

    def test_no_json_files_raises_index_error(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.txt", "no json here")

        with pytest.raises(IndexError):
            process_zip_file(zip_path, output_dir, input_dir)

    def test_bad_zip_raises(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        bad_zip = input_dir / "se.zip"
        bad_zip.write_bytes(b"not a zip file")

        with pytest.raises(zipfile.BadZipFile):
            process_zip_file(bad_zip, output_dir, input_dir)

    def test_invalid_jsonld_raises(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", INVALID_JSONLD)

        with pytest.raises(Exception):
            process_zip_file(zip_path, output_dir, input_dir)

    def test_checksum_mismatch_returns_false(self, temp_dirs: tuple[Path, Path, Path]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", SAMPLE_JSONLD)

        original_len = Dataset.__len__

        call_count = [0]

        def mock_len(self: Dataset) -> int:
            call_count[0] += 1
            if call_count[0] == 2:
                return 999
            return original_len(self)

        with patch.object(Dataset, "__len__", mock_len):
            result = process_zip_file(zip_path, output_dir, input_dir)

        assert result is False


class TestMain:
    @pytest.fixture
    def temp_dirs(self):
        test_dir = Path(tempfile.mkdtemp())
        input_dir = test_dir / "input"
        output_dir = test_dir / "output"
        input_dir.mkdir()
        yield input_dir, output_dir, test_dir
        shutil.rmtree(test_dir)

    def test_success(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs
        prov_dir = input_dir / "entity" / "prov"
        prov_dir.mkdir(parents=True)
        zip_path = prov_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", SAMPLE_JSONLD)

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir), "-w", "1"]):
            main()

        captured = capsys.readouterr()
        assert "Found 1 ZIP files" in captured.out
        assert "Success: 1" in captured.out
        assert "Failed:  0" in captured.out

    def test_no_files_found(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir)]):
            main()

        captured = capsys.readouterr()
        assert "Found 0 ZIP files" in captured.out
        assert "Success: 0" in captured.out
        assert "Failed:  0" in captured.out

    def test_worker_exception_handled(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", INVALID_JSONLD)

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir), "-w", "1"]):
            main()

        captured = capsys.readouterr()
        assert "Found 1 ZIP files" in captured.out
        assert "Failed:  1" in captured.out

    def test_default_workers(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir)]):
            with patch("multiprocessing.cpu_count", return_value=4):
                main()

        captured = capsys.readouterr()
        assert "Workers: 4" in captured.out

    def test_checksum_failure_counted(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs
        zip_path = input_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.json", SAMPLE_JSONLD)

        class MockFuture:
            def result(self) -> Iterator[bool]:
                return iter([False])

        class MockPool:
            def __init__(self, max_workers: int) -> None:
                pass

            def __enter__(self) -> "MockPool":
                return self

            def __exit__(self, *args: object) -> None:
                pass

            def map(self, func: object, items: list[object]) -> MockFuture:
                return MockFuture()

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir), "-w", "1"]):
            with patch("oc_meta.run.migration.rdf_to_nquads.ProcessPool", MockPool):
                main()

        captured = capsys.readouterr()
        assert "Failed:  1" in captured.out

    def test_data_mode_success(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs
        data_dir = input_dir / "br" / "060"
        data_dir.mkdir(parents=True)
        zip_path = data_dir / "1000.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("1000.json", SAMPLE_JSONLD)

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir), "-m", "data", "-w", "1"]):
            main()

        captured = capsys.readouterr()
        assert "Found 1 data ZIP files" in captured.out
        assert "Success: 1" in captured.out
        assert "Failed:  0" in captured.out
        output_file = output_dir / "br-060-1000.nq"
        assert output_file.exists()

    def test_prov_mode_success(self, temp_dirs: tuple[Path, Path, Path], capsys: pytest.CaptureFixture[str]) -> None:
        input_dir, output_dir, _ = temp_dirs
        prov_dir = input_dir / "br" / "060" / "1000" / "prov"
        prov_dir.mkdir(parents=True)
        zip_path = prov_dir / "se.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("se.json", SAMPLE_JSONLD)
        data_zip = input_dir / "br" / "060" / "1000.zip"
        with zipfile.ZipFile(data_zip, "w") as zf:
            zf.writestr("1000.json", SAMPLE_JSONLD)

        with patch("sys.argv", ["prog", str(input_dir), str(output_dir), "-m", "prov", "-w", "1"]):
            main()

        captured = capsys.readouterr()
        assert "Found 1 provenance ZIP files" in captured.out
        assert "Success: 1" in captured.out
