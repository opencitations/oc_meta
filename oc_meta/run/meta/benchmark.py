#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Benchmark tool for OpenCitations Meta processing pipeline.

This module provides utilities to measure the performance of the complete
Meta processing workflow: CSV reading → curation → RDF creation → triplestore upload.
"""

import argparse
import glob
import json
import os
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import psutil
import redis
import yaml
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.prov.prov_set import ProvSet
from oc_ocdm.storer import Storer
from oc_ocdm.support.reporter import Reporter

from oc_meta.core.creator import Creator
from oc_meta.core.curator import Curator
from oc_meta.lib import file_manager
from oc_meta.run.meta.generate_benchmark_data import BenchmarkDataGenerator
from oc_meta.run.upload.cache_manager import CacheManager
from oc_meta.run.upload.on_triplestore import upload_sparql_updates


class BenchmarkTimer:
    """Context manager for timing code blocks and collecting metrics."""

    def __init__(self, name: str):
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None
        self.start_memory: Optional[int] = None
        self.end_memory: Optional[int] = None
        self.peak_memory: Optional[int] = None

    def __enter__(self):
        self.start_time = time.time()
        process = psutil.Process()
        self.start_memory = process.memory_info().rss
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        process = psutil.Process()
        self.end_memory = process.memory_info().rss
        self.peak_memory = max(self.start_memory, self.end_memory)

    def to_dict(self) -> Dict[str, Any]:
        """Convert timing data to dictionary."""
        return {
            "name": self.name,
            "duration_seconds": round(self.duration, 3) if self.duration else None,
            "start_memory_mb": round(self.start_memory / 1024 / 1024, 2) if self.start_memory else None,
            "end_memory_mb": round(self.end_memory / 1024 / 1024, 2) if self.end_memory else None,
            "peak_memory_mb": round(self.peak_memory / 1024 / 1024, 2) if self.peak_memory else None,
        }


class MetaBenchmark:
    """
    Benchmark runner for OpenCitations Meta processing pipeline.

    Measures end-to-end performance from CSV input to triplestore upload,
    with detailed timing breakdown for each processing phase.
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.timers: List[BenchmarkTimer] = []
        self.metrics: Dict[str, Any] = {}

        self.base_iri = self.config["base_iri"]
        self.resp_agent = self.config["resp_agent"]
        self.supplier_prefix = self.config["supplier_prefix"]

        self.ts_data = self.config["triplestore_url"]
        self.ts_prov = self.config["provenance_triplestore_url"]
        self.redis_host = self.config["redis_host"]
        self.redis_port = self.config["redis_port"]
        self.redis_db = self.config["redis_db"]
        self.cache_db = self.config["cache_db"]

        self.input_dir = file_manager.normalize_path(self.config["input_csv_dir"])
        self.output_dir = file_manager.normalize_path(self.config.get("output_rdf_dir", ""))

        self.counter_handler = RedisCounterHandler(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db
        )

    def _load_config(self) -> Dict[str, Any]:
        """Load benchmark configuration from YAML file."""
        with open(self.config_path) as f:
            return yaml.safe_load(f)

    def cleanup_databases(self):
        """Clean up test databases and generated files after benchmark execution."""
        print("\n[Cleanup] Resetting test databases...")

        with BenchmarkTimer("cleanup") as timer:
            self._reset_virtuoso(self.ts_data)
            self._reset_virtuoso(self.ts_prov)
            self._flush_redis()
            self._delete_output_files()
            self._delete_input_file(self._generated_csv)

        self.timers.append(timer)
        print(f"[Cleanup] Completed in {timer.duration:.2f}s")

    def _reset_virtuoso(self, endpoint: str):
        """Reset Virtuoso triplestore using RDF_GLOBAL_RESET."""
        parsed = urlparse(endpoint)
        port = parsed.port or 8890

        if port == 8805:
            container_name = "oc-meta-test-virtuoso"
        elif port == 8806:
            container_name = "oc-meta-test-virtuoso-prov"
        else:
            print(f"  - Warning: Unknown test container port {port}, skipping reset")
            return

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                result = subprocess.run([
                    "docker", "exec", container_name,
                    "isql", "1111", "dba", "dba",
                    "exec=RDF_GLOBAL_RESET();"
                ], check=True, capture_output=True, text=True, timeout=30)
                print(f"  - Reset Virtuoso ({container_name})")
                return
            except subprocess.CalledProcessError as e:
                if attempt < max_attempts - 1:
                    wait_time = 2 ** attempt
                    print(f"  - Warning: Failed to reset {container_name} (attempt {attempt + 1}/{max_attempts}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  - Warning: Failed to reset {container_name} after {max_attempts} attempts: {e}")
            except subprocess.TimeoutExpired:
                if attempt < max_attempts - 1:
                    wait_time = 2 ** attempt
                    print(f"  - Warning: Timeout resetting {container_name} (attempt {attempt + 1}/{max_attempts}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  - Warning: Timeout resetting {container_name} after {max_attempts} attempts")

    def _flush_redis(self):
        """Flush Redis databases used for counters and cache."""
        try:
            r = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
            r.flushdb()
            print(f"  - Flushed Redis db={self.redis_db} (counters)")

            r_cache = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.cache_db)
            r_cache.flushdb()
            print(f"  - Flushed Redis db={self.cache_db} (cache)")
        except Exception as e:
            print(f"  - Warning: Failed to flush Redis: {e}")

    def _delete_output_files(self):
        """Delete generated output files including RDF files."""
        output_path = Path(self.output_dir)
        if output_path.exists():
            try:
                shutil.rmtree(output_path)
                output_path.mkdir(parents=True)
                print(f"  - Deleted output files in {self.output_dir}")
            except Exception as e:
                print(f"  - Warning: Failed to delete output files: {e}")

        rdf_output_dir = self.config.get("output_rdf_dir")
        if rdf_output_dir:
            rdf_pattern = f"{rdf_output_dir}*"
            for rdf_dir in glob.glob(rdf_pattern):
                if os.path.isdir(rdf_dir) and rdf_dir != self.output_dir:
                    try:
                        shutil.rmtree(rdf_dir)
                        print(f"  - Deleted RDF directory: {rdf_dir}")
                    except Exception as e:
                        print(f"  - Warning: Failed to delete {rdf_dir}: {e}")

    def _delete_input_file(self, csv_path: str):
        """Delete generated input CSV file."""
        os.remove(csv_path)
        print(f"  - Deleted generated input file: {os.path.basename(csv_path)}")

    def run_benchmark(self, size: Optional[int] = None, seed: int = 42) -> Dict[str, Any]:
        """
        Run complete benchmark on all CSV files in input directory.

        Args:
            size: If specified, generate test data with N records before running benchmark
            seed: Random seed for reproducible data generation (default: 42)

        Returns:
            Dictionary with benchmark results and metrics
        """
        print(f"\n{'='*60}")
        print(f"OpenCitations Meta Benchmark")
        print(f"{'='*60}")
        print(f"Input directory: {self.input_dir}")
        print(f"Config: {self.config_path}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"{'='*60}\n")

        self._generated_csv = None
        if size:
            csv_filename = f"benchmark_{size}.csv"
            csv_path = os.path.join(self.input_dir, csv_filename)

            print(f"Generating {size} test records (seed={seed})...")
            os.makedirs(self.input_dir, exist_ok=True)
            generator = BenchmarkDataGenerator(size=size, output_path=csv_path, seed=seed)
            generator.generate()
            print()

            input_csv = csv_path
            self._generated_csv = csv_path
            print(f"Processing generated file: {csv_filename}\n")
        else:
            csv_files = [f for f in os.listdir(self.input_dir) if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV files found in {self.input_dir}")

            input_csv = os.path.join(self.input_dir, csv_files[0])
            print(f"Processing file: {csv_files[0]}\n")

        overall_timer = BenchmarkTimer("total_processing")
        overall_timer.__enter__()

        try:
            data = self._read_csv(input_csv)
            curator_data = self._run_curation(data)
            graphset, prov = self._run_creation(curator_data)
            self._run_storage_and_upload(graphset, prov)

            overall_timer.__exit__(None, None, None)
            self.timers.append(overall_timer)

            self._calculate_metrics(len(data))
            self._print_summary()

            return self._generate_report()

        except Exception as e:
            overall_timer.__exit__(type(e), e, None)
            self.timers.append(overall_timer)
            print(f"\n[ERROR] Benchmark failed: {e}")
            raise

    def _read_csv(self, csv_path: str) -> List[Dict[str, str]]:
        """Read CSV file and return data."""
        print("[Phase 1/4] Reading CSV...")
        with BenchmarkTimer("csv_reading") as timer:
            data = file_manager.get_csv_data(csv_path)

        self.timers.append(timer)
        self.metrics["input_records"] = len(data)
        print(f"  - Read {len(data)} records in {timer.duration:.2f}s")
        return data

    def _run_curation(self, data: List[Dict[str, str]]) -> Curator:
        """Run curation phase."""
        print("[Phase 2/4] Running curation (validation, disambiguation)...")
        with BenchmarkTimer("curation") as timer:
            curator = Curator(
                data=data,
                ts=self.ts_data,
                prov_config=None,
                counter_handler=self.counter_handler,
                base_iri=self.base_iri,
                prefix=self.supplier_prefix,
                settings=self.config,
                meta_config_path=self.config_path
            )
            curator.curator()

        self.timers.append(timer)
        self.metrics["curated_records"] = len(curator.data)
        print(f"  - Curated {len(curator.data)} records in {timer.duration:.2f}s")
        return curator

    def _run_creation(self, curator: Curator):
        """Run RDF creation phase."""
        print("[Phase 3/4] Creating RDF entities...")
        with BenchmarkTimer("rdf_creation") as timer:
            creator = Creator(
                data=curator.data,
                endpoint=self.ts_data,
                base_iri=self.base_iri,
                counter_handler=self.counter_handler,
                supplier_prefix=self.supplier_prefix,
                resp_agent=self.resp_agent,
                ra_index=curator.index_id_ra,
                br_index=curator.index_id_br,
                re_index_csv=curator.re_index,
                ar_index_csv=curator.ar_index,
                vi_index=curator.VolIss,
                preexisting_entities=curator.preexisting_entities,
                everything_everywhere_allatonce=curator.everything_everywhere_allatonce,
                settings=self.config,
                meta_config_path=self.config_path
            )
            creator.creator()

            prov = ProvSet(
                creator.setgraph,
                self.base_iri,
                wanted_label=False,
                supplier_prefix=self.supplier_prefix,
                custom_counter_handler=self.counter_handler
            )
            prov.generate_provenance()

        self.timers.append(timer)
        self.metrics["entities_created"] = len(creator.setgraph.res_to_entity)
        print(f"  - Created {self.metrics['entities_created']} entities in {timer.duration:.2f}s")
        return creator.setgraph, prov

    def _run_storage_and_upload(self, graphset, prov):
        """Run parallel storage and triplestore upload phase."""
        data_update_dir = os.path.join(self.output_dir, "data_updates")
        prov_update_dir = os.path.join(self.output_dir, "prov_updates")
        os.makedirs(data_update_dir, exist_ok=True)
        os.makedirs(prov_update_dir, exist_ok=True)

        data_upload_folder = os.path.join(data_update_dir, "to_be_uploaded")
        prov_upload_folder = os.path.join(prov_update_dir, "to_be_uploaded")

        print("[Phase 4/4] Running parallel SPARQL generation + upload...")
        with BenchmarkTimer("storage_and_upload") as timer:
            repok = Reporter(print_sentences=False)
            reperr = Reporter(print_sentences=True, prefix="[Storer: ERROR] ")
            storer_data = Storer(graphset, repok=repok, reperr=reperr)
            storer_prov = Storer(prov, repok=repok, reperr=reperr)

            cache_file_data = os.path.join(self.output_dir, "ts_data_cache.json")
            cache_file_prov = os.path.join(self.output_dir, "ts_prov_cache.json")

            cache_manager_data = CacheManager(
                cache_file_data,
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=self.cache_db
            )
            cache_manager_prov = CacheManager(
                cache_file_prov,
                redis_host=self.redis_host,
                redis_port=self.redis_port,
                redis_db=self.cache_db
            )

            with ThreadPoolExecutor(max_workers=4) as executor:
                rdf_futures = []

                if self.config.get("generate_rdf_files", True):
                    rdf_futures.append(executor.submit(
                        storer_data.store_all,
                        base_dir=self.output_dir,
                        base_iri=self.base_iri,
                        context_path=self.config.get("context_path", "https://w3id.org/oc/corpus/context.json"),
                        process_id=None
                    ))
                    rdf_futures.append(executor.submit(
                        storer_prov.store_all,
                        base_dir=self.output_dir,
                        base_iri=self.base_iri,
                        context_path=self.config.get("context_path", "https://w3id.org/oc/corpus/context.json"),
                        process_id=None
                    ))

                sparql_data_future = executor.submit(
                    storer_data.upload_all,
                    triplestore_url=self.ts_data,
                    base_dir=data_update_dir,
                    batch_size=10,
                    save_queries=True
                )
                sparql_prov_future = executor.submit(
                    storer_prov.upload_all,
                    triplestore_url=self.ts_prov,
                    base_dir=prov_update_dir,
                    batch_size=10,
                    save_queries=True
                )

                sparql_data_future.result()
                sparql_prov_future.result()

                upload_data_future = executor.submit(
                    upload_sparql_updates,
                    endpoint=self.ts_data,
                    folder=data_upload_folder,
                    batch_size=10,
                    cache_manager=cache_manager_data
                )
                upload_prov_future = executor.submit(
                    upload_sparql_updates,
                    endpoint=self.ts_prov,
                    folder=prov_upload_folder,
                    batch_size=10,
                    cache_manager=cache_manager_prov
                )

                upload_data_future.result()
                upload_prov_future.result()

                for future in rdf_futures:
                    future.result()

            cache_manager_data._save_to_json()
            cache_manager_prov._save_to_json()

            import atexit
            atexit.unregister(cache_manager_data._cleanup)
            atexit.unregister(cache_manager_prov._cleanup)

        self.timers.append(timer)

        num_data_files = len([f for f in os.listdir(data_upload_folder) if f.endswith('.sparql')])
        num_prov_files = len([f for f in os.listdir(prov_upload_folder) if f.endswith('.sparql')])

        self.metrics["sparql_files_data"] = num_data_files
        self.metrics["sparql_files_prov"] = num_prov_files
        print(f"  - Generated and uploaded {num_data_files} data + {num_prov_files} prov queries in {timer.duration:.2f}s")

    def _calculate_metrics(self, total_records: int):
        """Calculate summary metrics."""
        total_time = next((t.duration for t in self.timers if t.name == "total_processing"), 0)
        self.metrics["throughput_records_per_sec"] = round(total_records / total_time, 2) if total_time > 0 else 0
        self.metrics["total_duration_seconds"] = round(total_time, 3)

    def _print_summary(self):
        """Print benchmark summary to console."""
        print(f"\n{'='*60}")
        print("Benchmark Summary")
        print(f"{'='*60}")
        print(f"Total Duration: {self.metrics['total_duration_seconds']}s")
        print(f"Throughput: {self.metrics['throughput_records_per_sec']} records/sec")
        print(f"Input Records: {self.metrics['input_records']}")
        print(f"Entities Created: {self.metrics['entities_created']}")
        print(f"SPARQL Files: {self.metrics['sparql_files_data']} (data) + {self.metrics['sparql_files_prov']} (prov)")
        print(f"{'='*60}\n")

    def _generate_report(self) -> Dict[str, Any]:
        """Generate complete benchmark report."""
        return {
            "timestamp": datetime.now().isoformat(),
            "config_path": self.config_path,
            "metrics": self.metrics,
            "phases": [t.to_dict() for t in self.timers],
        }

    def save_report(self, report: Dict[str, Any], output_path: str):
        """Save benchmark report to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"[Report] Saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark OpenCitations Meta processing pipeline"
    )
    parser.add_argument(
        "-c", "--config",
        required=True,
        help="Path to benchmark configuration file (YAML)"
    )
    parser.add_argument(
        "-o", "--output",
        default="benchmark_report.json",
        help="Output JSON report file path (default: benchmark_report.json)"
    )
    parser.add_argument(
        "--size",
        type=int,
        default=None,
        help="Generate test data with N records. If not specified, use existing CSV files"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data generation (default: 42)"
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip automatic database cleanup after benchmark"
    )

    args = parser.parse_args()

    benchmark = MetaBenchmark(args.config)

    try:
        report = benchmark.run_benchmark(size=args.size, seed=args.seed)
        benchmark.save_report(report, args.output)
    finally:
        if not args.no_cleanup:
            benchmark.cleanup_databases()
        else:
            print("\n[Info] Skipping cleanup (--no-cleanup flag set)")


if __name__ == "__main__":
    main()
