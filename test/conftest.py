# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

import pytest
import redis


QLEVER_IMAGE = "adfreiburg/qlever:commit-5c6a72a"
QLEVER_ACCESS_TOKEN = "qlever_test_token"


def _wait_for_endpoint(url: str, timeout: int = 60) -> None:
    separator = "&" if "?" in url else "?"
    start = time.time()
    while time.time() - start < timeout:
        try:
            req = Request(
                f"{url}{separator}query=ASK+%7B+%3Fs+%3Fp+%3Fo+%7D",
                headers={"Accept": "application/sparql-results+json"},
            )
            urlopen(req, timeout=2)
            return
        except (URLError, OSError):
            time.sleep(1)
    raise RuntimeError(f"Endpoint {url} not ready within {timeout}s")


def _wait_for_redis(host: str, port: int, timeout: int = 30) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            client = redis.Redis(host=host, port=port)
            client.ping()
            return
        except redis.ConnectionError:
            time.sleep(0.5)
    raise RuntimeError(f"Redis {host}:{port} not ready within {timeout}s")


def _remove_container(name: str) -> None:
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@pytest.fixture(scope="session")
def redis_service():
    container_name = "oc-meta-test-redis"
    host_port = 6381

    _remove_container(container_name)

    subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container_name,
            "-p", f"{host_port}:6379",
            "redis:7-alpine",
        ],
        check=True,
        capture_output=True,
    )

    _wait_for_redis("localhost", host_port)
    yield {"host": "localhost", "port": host_port}

    _remove_container(container_name)


@pytest.fixture(scope="session")
def qlever_data_endpoint():
    container_name = "oc-meta-test-qlever"
    http_port = 8805

    _remove_container(container_name)

    with tempfile.TemporaryDirectory() as data_dir:
        input_file = Path(data_dir) / "input.nt"
        input_file.write_text(
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        )

        subprocess.run(
            [
                "docker", "run", "--rm",
                "--user", "root",
                "-v", f"{data_dir}:/data",
                "--entrypoint", "qlever-index",
                QLEVER_IMAGE,
                "-i", "/data/index",
                "-f", "/data/input.nt",
                "-F", "nt",
            ],
            check=True,
            capture_output=True,
        )

        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", container_name,
                "--user", "root",
                "-p", f"{http_port}:7001",
                "-v", f"{data_dir}:/data",
                "--entrypoint", "qlever-server",
                QLEVER_IMAGE,
                "-i", "/data/index",
                "-j", "2",
                "-p", "7001",
                "-m", "1G",
                "-a", QLEVER_ACCESS_TOKEN,
                "-n",
            ],
            check=True,
            capture_output=True,
        )

        _wait_for_endpoint(f"http://localhost:{http_port}?access-token={QLEVER_ACCESS_TOKEN}")
        yield f"http://127.0.0.1:{http_port}?access-token={QLEVER_ACCESS_TOKEN}"

        _remove_container(container_name)


@pytest.fixture(scope="session")
def qlever_prov_endpoint():
    container_name = "oc-meta-test-qlever-prov"
    http_port = 8806

    _remove_container(container_name)

    with tempfile.TemporaryDirectory() as data_dir:
        input_file = Path(data_dir) / "input.nt"
        input_file.write_text(
            "<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n"
        )

        subprocess.run(
            [
                "docker", "run", "--rm",
                "--user", "root",
                "-v", f"{data_dir}:/data",
                "--entrypoint", "qlever-index",
                QLEVER_IMAGE,
                "-i", "/data/index",
                "-f", "/data/input.nt",
                "-F", "nt",
            ],
            check=True,
            capture_output=True,
        )

        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", container_name,
                "--user", "root",
                "-p", f"{http_port}:7001",
                "-v", f"{data_dir}:/data",
                "--entrypoint", "qlever-server",
                QLEVER_IMAGE,
                "-i", "/data/index",
                "-j", "2",
                "-p", "7001",
                "-m", "1G",
                "-a", QLEVER_ACCESS_TOKEN,
                "-n",
            ],
            check=True,
            capture_output=True,
        )

        _wait_for_endpoint(f"http://localhost:{http_port}?access-token={QLEVER_ACCESS_TOKEN}")
        yield f"http://127.0.0.1:{http_port}?access-token={QLEVER_ACCESS_TOKEN}"

        _remove_container(container_name)


@pytest.fixture(scope="session", autouse=True)
def test_databases(redis_service, qlever_data_endpoint, qlever_prov_endpoint):
    return {
        "redis": redis_service,
        "data_endpoint": qlever_data_endpoint,
        "prov_endpoint": qlever_prov_endpoint,
    }
