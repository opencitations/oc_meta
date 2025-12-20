#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022-2025 OpenCitations
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

"""Integration test for database unavailability."""

import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml
from sparqlite import SPARQLClient as OriginalSPARQLClient
from test.test_utils import (
    REDIS_CACHE_DB,
    SERVER,
    VIRTUOSO_CONTAINER,
    reset_redis_counters,
    reset_server,
    wait_for_virtuoso,
)

from oc_meta.run.meta_process import MetaProcess

BASE_DIR = os.path.join("test", "meta_process")
SHORT_TIMEOUT = 3
SHORT_MAX_RETRIES = 1
SHORT_BACKOFF = 0.1


def short_timeout_sparql_client(*args, **kwargs):
    """Wrapper that forces short timeout and minimal retries for SPARQLClient."""
    kwargs["timeout"] = SHORT_TIMEOUT
    kwargs["max_retries"] = SHORT_MAX_RETRIES
    kwargs["backoff_factor"] = SHORT_BACKOFF
    return OriginalSPARQLClient(*args, **kwargs)


class TestDatabaseUnavailability(unittest.TestCase):
    """Test that database unavailability is handled correctly."""

    @classmethod
    def setUpClass(cls):
        print("[DEBUG] setUpClass: waiting for Virtuoso...")
        if not wait_for_virtuoso(SERVER, max_wait=30):
            raise TimeoutError("Virtuoso not ready")
        print("[DEBUG] setUpClass: Virtuoso ready")

    def setUp(self):
        print("[DEBUG] setUp: creating temp dir...")
        self.temp_dir = tempfile.mkdtemp()
        print("[DEBUG] setUp: resetting server...")
        reset_server()
        print("[DEBUG] setUp: resetting redis...")
        reset_redis_counters()
        print("[DEBUG] setUp: done")

    def tearDown(self):
        print("[DEBUG] tearDown: start")
        reset_redis_counters()
        if hasattr(self, "temp_dir") and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        subprocess.run(["docker", "start", VIRTUOSO_CONTAINER], capture_output=True, check=False)
        wait_for_virtuoso(SERVER, max_wait=30)
        print("[DEBUG] tearDown: done")

    @patch("oc_meta.lib.finder.SPARQLClient", side_effect=short_timeout_sparql_client)
    @patch("sparqlite.SPARQLClient", side_effect=short_timeout_sparql_client)
    def test_virtuoso_unavailable_prevents_cache_update(self, mock_sparqlite, mock_finder):
        """When Virtuoso is offline, processing fails and file is NOT cached."""
        print("[DEBUG] test: loading config...")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_3.yaml")
        with open(meta_config_path, encoding="utf-8") as f:
            settings = yaml.full_load(f)

        settings.update({
            "redis_cache_db": REDIS_CACHE_DB,
            "ts_upload_cache": os.path.join(self.temp_dir, "cache.json"),
            "ts_failed_queries": os.path.join(self.temp_dir, "failed.txt"),
            "ts_stop_file": os.path.join(self.temp_dir, ".stop"),
        })

        print("[DEBUG] test: creating MetaProcess...")
        meta_process = MetaProcess(settings=settings, meta_config_path=meta_config_path)
        print("[DEBUG] test: preparing folders...")
        files_to_process = meta_process.prepare_folders()
        self.assertGreater(len(files_to_process), 0, "No input files found")

        filename = files_to_process[0]
        print(f"[DEBUG] test: file to process: {filename}")

        print("[DEBUG] test: stopping Virtuoso...")
        subprocess.run(["docker", "stop", VIRTUOSO_CONTAINER], capture_output=True, check=True)
        print("[DEBUG] test: Virtuoso stopped")

        try:
            print("[DEBUG] test: calling curate_and_create...")
            result = meta_process.curate_and_create(
                filename,
                meta_process.cache_path,
                meta_process.errors_path,
                resp_agents_only=False,
                settings=settings,
                meta_config_path=meta_config_path,
            )
            print(f"[DEBUG] test: result = {result}")

            self.assertNotEqual(result[0]["message"], "success", "Should fail when database unavailable")

            if os.path.exists(meta_process.cache_path):
                with open(meta_process.cache_path) as f:
                    self.assertNotIn(filename, f.read(), "File should NOT be cached when upload fails")

        finally:
            print("[DEBUG] test: restarting Virtuoso...")
            subprocess.run(["docker", "start", VIRTUOSO_CONTAINER], capture_output=True, check=True)
            wait_for_virtuoso(SERVER, max_wait=30)
            print("[DEBUG] test: done")


if __name__ == "__main__":
    unittest.main()
