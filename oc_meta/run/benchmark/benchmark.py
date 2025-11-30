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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import redis
import yaml

from oc_meta.lib import file_manager
from oc_meta.lib.timer import ProcessTimer
from oc_meta.run.benchmark.generate_benchmark_data import BenchmarkDataGenerator
from oc_meta.run.benchmark.plotting import plot_benchmark_results, plot_single_run_results
from oc_meta.run.benchmark.preload_high_author_data import (
    generate_atlas_paper_csv, generate_atlas_update_csv, preload_data)
from oc_meta.run.benchmark.statistics import BenchmarkStatistics
from oc_meta.run.meta_process import MetaProcess


class MetaBenchmark:
    """
    Benchmark runner for OpenCitations Meta processing pipeline.

    Measures end-to-end performance from CSV input to triplestore upload,
    with detailed timing breakdown for each processing phase.
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self._generated_csvs = []

        self.ts_data = self.config["triplestore_url"]
        self.ts_prov = self.config["provenance_triplestore_url"]
        self.redis_host = self.config["redis_host"]
        self.redis_port = self.config["redis_port"]
        self.redis_db = self.config["redis_db"]
        self.cache_db = self.config["cache_db"]

        self.input_dir = file_manager.normalize_path(self.config["input_csv_dir"])
        self.output_dir = file_manager.normalize_path(self.config["output_rdf_dir"])

    def _load_config(self) -> Dict[str, Any]:
        """Load benchmark configuration from YAML file."""
        with open(self.config_path) as f:
            return yaml.safe_load(f)

    def cleanup_databases(self):
        """Clean up test databases and generated files after benchmark execution."""
        print("\n[Cleanup] Resetting test databases...")

        self._reset_virtuoso(self.ts_data)
        self._reset_virtuoso(self.ts_prov)
        self._flush_redis()
        self._delete_output_files()
        for csv in self._generated_csvs:
            self._delete_input_file(csv)
        self._delete_time_agnostic_config()

        print(f"[Cleanup] Completed")

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

        rdf_output_dir = self.config["output_rdf_dir"]
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
        if os.path.exists(csv_path):
            os.remove(csv_path)
            print(f"  - Deleted generated input file: {os.path.basename(csv_path)}")

    def _delete_time_agnostic_config(self):
        """Delete auto-generated time_agnostic_library_config.json file."""
        config_dir = os.path.dirname(self.config_path)
        time_agnostic_config = os.path.join(config_dir, "time_agnostic_library_config.json")
        if os.path.exists(time_agnostic_config):
            os.remove(time_agnostic_config)
            print(f"  - Deleted time_agnostic_library_config.json")

    def _execute_single_run(self, input_csv: str, run_number: Optional[int] = None) -> Dict[str, Any]:
        """
        Execute a single benchmark run on the given CSV file.

        Args:
            input_csv: Path to input CSV file
            run_number: Optional run number for progress display

        Returns:
            Dictionary with benchmark results and metrics for this run
        """
        run_prefix = f"[Run {run_number}] " if run_number is not None else ""

        try:
            timer = ProcessTimer(enabled=True)
            meta_process = MetaProcess(
                settings=self.config,
                meta_config_path=self.config_path,
                timer=timer
            )

            filename = os.path.basename(input_csv)
            result = meta_process.curate_and_create(
                filename=filename,
                cache_path=os.path.join(self.output_dir, "benchmark_cache.txt"),
                errors_path=os.path.join(self.output_dir, "benchmark_errors.txt"),
                resp_agents_only=False,
                settings=self.config,
                meta_config_path=self.config_path
            )

            if result[0]["message"] != "success":
                raise Exception(f"Processing failed: {result[0]['message']}")

            report = timer.get_report()
            report["timestamp"] = datetime.now().isoformat()
            report["config_path"] = self.config_path

            if run_number is not None:
                print(f"{run_prefix}Completed in {report['metrics']['total_duration_seconds']:.2f}s")

            return report

        except Exception as e:
            print(f"\n{run_prefix}[ERROR] Benchmark failed: {e}")
            raise

    def run_benchmark(
        self,
        sizes: Optional[Union[int, List[int]]] = None,
        seed: int = 42,
        runs: int = 1,
        fresh_data: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete benchmark on CSV files with optional multiple runs and statistical analysis.

        Args:
            sizes: If specified, generate test data with N records (single int) or list of sizes for scalability analysis
            seed: Random seed for reproducible data generation (default: 42)
            runs: Number of times to execute the benchmark per size (default: 1)
            fresh_data: If True, generate new data for each run (default: False)

        Returns:
            Dictionary with benchmark results. If runs > 1, includes statistical analysis.
            If multiple sizes provided, returns scalability analysis across sizes.
        """
        print(f"\n{'='*60}")
        print(f"OpenCitations Meta Benchmark")
        print(f"{'='*60}")
        print(f"Input directory: {self.input_dir}")
        print(f"Config: {self.config_path}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"Runs: {runs}")
        print(f"Fresh data per run: {fresh_data}")
        print(f"{'='*60}\n")

        if isinstance(sizes, list):
            return self._run_multi_size_benchmark(sizes, seed, runs, fresh_data)
        else:
            return self._run_single_size_benchmark(sizes, seed, runs, fresh_data)

    def _run_multi_size_benchmark(
        self,
        sizes: List[int],
        seed: int,
        runs: int,
        fresh_data: bool
    ) -> Dict[str, Any]:
        """Run benchmark for multiple sizes and generate scalability analysis."""
        print(f"Scalability analysis across {len(sizes)} sizes: {sizes}")
        print(f"{'='*60}\n")

        per_size_results = []

        for size_idx, size in enumerate(sizes):
            print(f"\n{'#'*60}")
            print(f"Size {size_idx + 1}/{len(sizes)}: {size} records")
            print(f"{'#'*60}\n")

            size_result = self._run_single_size_benchmark(size, seed, runs, fresh_data)

            per_size_results.append({
                "size": size,
                "runs": size_result.get("runs", [size_result]),
                "statistics": size_result.get("statistics", {})
            })

            if size_idx < len(sizes) - 1:
                self.cleanup_databases()

        print(f"\n{'='*60}")
        print("Scalability Analysis Summary")
        print(f"{'='*60}")

        scalability_metrics = self._compute_scalability_metrics(per_size_results)

        for size_data in per_size_results:
            size = size_data["size"]
            if size_data["statistics"]:
                mean_time = size_data["statistics"]["total_duration_seconds"]["mean"]
                mean_throughput = size_data["statistics"]["throughput_records_per_sec"]["mean"]
            else:
                mean_time = size_data["runs"][0]["metrics"]["total_duration_seconds"]
                mean_throughput = size_data["runs"][0]["metrics"]["throughput_records_per_sec"]

            print(f"Size {size:>4}: {mean_time:>6.2f}s | {mean_throughput:>6.2f} rec/s")

        print(f"{'='*60}\n")

        return {
            "timestamp": datetime.now().isoformat(),
            "config_path": self.config_path,
            "config": {
                "sizes": sizes,
                "runs": runs,
                "fresh_data": fresh_data,
                "seed": seed
            },
            "per_size_results": per_size_results,
            "scalability": scalability_metrics
        }

    def _compute_scalability_metrics(self, per_size_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute aggregate scalability metrics across sizes."""
        mean_duration_by_size = {}
        throughput_by_size = {}

        for size_data in per_size_results:
            size = size_data["size"]
            if size_data["statistics"]:
                mean_duration_by_size[size] = size_data["statistics"]["total_duration_seconds"]["mean"]
                throughput_by_size[size] = size_data["statistics"]["throughput_records_per_sec"]["mean"]
            else:
                mean_duration_by_size[size] = size_data["runs"][0]["metrics"]["total_duration_seconds"]
                throughput_by_size[size] = size_data["runs"][0]["metrics"]["throughput_records_per_sec"]

        return {
            "mean_duration_by_size": mean_duration_by_size,
            "throughput_by_size": throughput_by_size
        }

    def _run_single_size_benchmark(
        self,
        sizes: Optional[int],
        seed: int,
        runs: int,
        fresh_data: bool
    ) -> Dict[str, Any]:
        """Run benchmark for a single size with multiple runs and statistical analysis."""
        all_runs = []

        for run_idx in range(runs):
            run_number = run_idx + 1

            if runs > 1:
                print(f"\n{'='*60}")
                print(f"Run {run_number}/{runs}")
                print(f"{'='*60}\n")

            current_seed = seed + run_number if fresh_data else seed
            input_csv = self._prepare_input_csv(sizes, current_seed, run_number if fresh_data else None)

            report = self._execute_single_run(input_csv, run_number if runs > 1 else None)
            all_runs.append(report)

            if fresh_data and run_idx < runs - 1:
                self.cleanup_databases()

            if runs > 1 and run_number > 1:
                current_times = [r["metrics"]["total_duration_seconds"] for r in all_runs]
                mean_time = sum(current_times) / len(current_times)
                import statistics
                std_time = statistics.stdev(current_times) if len(current_times) > 1 else 0
                print(f"Progress: Mean time so far: {mean_time:.2f}s ± {std_time:.2f}s\n")

        if runs == 1:
            return all_runs[0]

        print(f"\n{'='*60}")
        print("Statistical Analysis")
        print(f"{'='*60}")

        stats = BenchmarkStatistics.calculate_statistics(all_runs)

        print(BenchmarkStatistics.format_statistics_report(stats))
        print(f"{'='*60}\n")

        return {
            "timestamp": datetime.now().isoformat(),
            "config_path": self.config_path,
            "config": {
                "runs": runs,
                "fresh_data": fresh_data,
                "seed": seed,
                "size": sizes
            },
            "runs": all_runs,
            "statistics": stats
        }

    def _prepare_input_csv(
        self,
        size: Optional[int],
        seed: int,
        run_number: Optional[int] = None,
        partial_data: bool = False
    ) -> str:
        """
        Prepare input CSV file (generate or use existing).

        Args:
            size: If specified, generate test data with N records
            seed: Random seed for data generation
            run_number: Optional run number for unique filenames with fresh data
            partial_data: If True, generate partial records (DOI only, no venue/pages)

        Returns:
            Path to input CSV file
        """
        if size:
            parts = ["input", str(size)]
            if partial_data:
                parts.append("partial")
            if run_number is not None:
                parts.append(f"run{run_number}")
            csv_filename = "_".join(parts) + ".csv"

            csv_path = os.path.join(self.input_dir, csv_filename)

            data_type = "partial" if partial_data else "complete"
            print(f"Generating {size} {data_type} records (seed={seed})...")
            os.makedirs(self.input_dir, exist_ok=True)
            generator = BenchmarkDataGenerator(
                size=size, output_path=csv_path, seed=seed, partial_data=partial_data
            )
            generator.generate()
            print()

            self._generated_csvs.append(csv_path)
            print(f"Processing generated file: {csv_filename}\n")
            return csv_path
        else:
            csv_files = [f for f in os.listdir(self.input_dir) if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV files found in {self.input_dir}")

            input_csv = os.path.join(self.input_dir, csv_files[0])
            print(f"Processing file: {csv_files[0]}\n")
            return input_csv

    def _get_reports_dir(self) -> str:
        """Get the reports directory path (relative to benchmark module)."""
        benchmark_dir = os.path.dirname(os.path.abspath(__file__))
        reports_dir = os.path.join(benchmark_dir, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        return reports_dir

    def save_report(self, report: Dict[str, Any], prefix: str, size: Optional[int] = None):
        """Save benchmark report (JSON and PNG) in reports directory."""
        reports_dir = self._get_reports_dir()
        if size is not None:
            base_filename = f"{prefix}_{size}"
        else:
            base_filename = prefix

        json_path = os.path.join(reports_dir, f"{base_filename}.json")
        with open(json_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"[Report] Saved to {json_path}")

        png_path = os.path.join(reports_dir, f"{base_filename}.png")
        if "statistics" in report and report["statistics"]:
            runs = report.get("runs", [report])
            stats = report["statistics"]
            plot_benchmark_results(runs, stats, png_path)
        elif "update" in report:
            plot_single_run_results(report["update"], png_path)
        elif "metrics" in report:
            plot_single_run_results(report, png_path)

    def _clear_processing_cache(self):
        """Clear the processing cache file to allow reprocessing of same records."""
        cache_file = os.path.join(
            file_manager.normalize_path(self.config["base_output_dir"]),
            "cache.txt"
        )
        if os.path.exists(cache_file):
            os.remove(cache_file)
            print("[Update Scenario] Cleared processing cache")

    def run_update_scenario(self, size: int, seed: int = 42, runs: int = 1) -> Dict[str, Any]:
        """
        Run update scenario benchmark to test graph diff performance.

        This benchmark:
        1. Preloads partial data (DOI only, no venue/volume/issue/page)
        2. Processes complete data (same DOIs with additional fields)

        This forces _compute_graph_changes() to compare preexisting_graph vs entity.g
        for every entity, exercising the graph diff code path.

        Args:
            size: Number of records to generate
            seed: Random seed for reproducible data generation
            runs: Number of times to run the benchmark

        Returns:
            Dictionary with preload and update timing reports
        """
        print(f"\n{'='*60}")
        print("Update Scenario Benchmark (Graph Diff Performance)")
        print(f"{'='*60}")
        print(f"Records: {size}")
        print(f"Seed: {seed}")
        print(f"Runs: {runs}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"{'='*60}\n")

        all_update_runs = []

        for run_idx in range(runs):
            run_number = run_idx + 1

            if runs > 1:
                print(f"\n{'#'*60}")
                print(f"Run {run_number}/{runs}")
                print(f"{'#'*60}\n")

            print(f"{'='*60}")
            print("Phase 1: Generating and processing PARTIAL data")
            print(f"{'='*60}\n")

            partial_csv = self._prepare_input_csv(
                size=size, seed=seed, partial_data=True
            )
            preload_report = self._execute_single_run(partial_csv)

            self._clear_processing_cache()

            print(f"\n{'='*60}")
            print("Phase 2: Generating and processing COMPLETE data")
            print(f"{'='*60}\n")

            complete_csv = self._prepare_input_csv(
                size=size, seed=seed, partial_data=False
            )
            update_report = self._execute_single_run(complete_csv)

            all_update_runs.append(update_report)

            if run_idx < runs - 1:
                self.cleanup_databases()

        print(f"\n{'='*60}")
        print("Update Scenario Results Summary")
        print(f"{'='*60}")

        last_update = all_update_runs[-1]
        preload_prep = self._get_phase_duration(preload_report, "storage__write_files")
        update_prep = self._get_phase_duration(last_update, "storage__write_files")

        preload_total = preload_report["metrics"]["total_duration_seconds"]
        update_total = last_update["metrics"]["total_duration_seconds"]

        print(f"\nPhase 1 - Partial data (initial load):")
        print(f"  Total: {preload_total:.2f}s")
        print(f"  storage__write_files: {preload_prep:.2f}s ({100*preload_prep/preload_total:.1f}%)")

        print(f"\nPhase 2 - Complete data (with graph diff):")
        print(f"  Total: {update_total:.2f}s")
        print(f"  storage__write_files: {update_prep:.2f}s ({100*update_prep/update_total:.1f}%)")

        if preload_prep > 0:
            slowdown = update_prep / preload_prep
            print(f"\nGraph diff overhead: {slowdown:.1f}x slower preparation phase")

        print(f"{'='*60}\n")

        if runs > 1:
            print(f"\n{'='*60}")
            print("Statistical Analysis (Update Phase)")
            print(f"{'='*60}")
            stats = BenchmarkStatistics.calculate_statistics(all_update_runs)
            print(BenchmarkStatistics.format_statistics_report(stats))
            print(f"{'='*60}\n")
        else:
            stats = {}

        result = {
            "timestamp": datetime.now().isoformat(),
            "config_path": self.config_path,
            "scenario": "update",
            "config": {
                "size": size,
                "seed": seed,
                "runs": runs
            },
            "preload": preload_report,
            "update": last_update,
            "runs": all_update_runs,
            "statistics": stats,
            "summary": {
                "preload_total_seconds": preload_total,
                "preload_preparation_seconds": preload_prep,
                "update_total_seconds": update_total,
                "update_preparation_seconds": update_prep,
                "preparation_slowdown_factor": update_prep / preload_prep if preload_prep > 0 else None
            }
        }

        return result

    def _get_phase_duration(self, report: Dict[str, Any], phase_name: str) -> float:
        """Extract duration for a specific phase from a timing report."""
        for phase in report.get("phases", []):
            if phase["name"] == phase_name:
                return phase["duration_seconds"]
        return 0.0


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark OpenCitations Meta processing pipeline with statistical analysis"
    )
    parser.add_argument(
        "-c", "--config",
        required=True,
        help="Path to benchmark configuration file (YAML)"
    )
    parser.add_argument(
        "--sizes",
        type=int,
        nargs='+',
        default=None,
        help="Generate test data with N records. Can specify multiple sizes for scalability analysis (e.g., --sizes 10 50 100). If not specified, use existing CSV files"
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of times to execute the benchmark (default: 1). Multiple runs enable statistical analysis"
    )
    parser.add_argument(
        "--fresh-data",
        action="store_true",
        help="Generate new data file for each run (with different seeds). Default: reuse same data to test cache effects"
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
    parser.add_argument(
        "--preload-high-authors",
        type=int,
        default=None,
        help="Preload a BR with N authors before benchmark (e.g., 2869 for ATLAS paper)"
    )
    parser.add_argument(
        "--preload-seed",
        type=int,
        default=42,
        help="Random seed for preload data generation (default: 42)"
    )
    parser.add_argument(
        "--update-scenario",
        action="store_true",
        help="Run update scenario: preload partial data, then process complete data to trigger graph diff"
    )

    args = parser.parse_args()

    benchmark = MetaBenchmark(args.config)

    try:
        if args.update_scenario:
            if not args.sizes or len(args.sizes) != 1:
                raise ValueError("--update-scenario requires exactly one --sizes value")
            size = args.sizes[0]
            report = benchmark.run_update_scenario(size=size, seed=args.seed, runs=args.runs)
            benchmark.save_report(report, prefix="update", size=size)

        elif args.preload_high_authors:
            print(f"\n{'='*60}")
            print(f"Preloading BR with {args.preload_high_authors} authors")
            print(f"{'='*60}\n")

            preload_csv = os.path.join(benchmark.input_dir, "atlas_preload.csv")
            generate_atlas_paper_csv(preload_csv, args.preload_high_authors, args.preload_seed)

            preload_metrics = preload_data(args.config, preload_csv)

            print("\n[Preload] Complete - triplestore now contains BR with many authors")

            update_csv = os.path.join(benchmark.input_dir, "atlas_update.csv")
            generate_atlas_update_csv(update_csv)

            print("[Preload] Generated update CSV for benchmark")
            print(f"[Preload] Next benchmark run will process this BR (update scenario)\n")

            benchmark._generated_csvs.extend([preload_csv, update_csv])

            report = benchmark.run_benchmark(sizes=None, seed=args.seed, runs=args.runs, fresh_data=args.fresh_data)
            report["preload_metrics"] = preload_metrics
            benchmark.save_report(report, prefix="atlas", size=args.preload_high_authors)

        else:
            sizes_arg = args.sizes
            if sizes_arg and len(sizes_arg) == 1:
                sizes_arg = sizes_arg[0]

            report = benchmark.run_benchmark(
                sizes=sizes_arg,
                seed=args.seed,
                runs=args.runs,
                fresh_data=args.fresh_data
            )
            size = sizes_arg if isinstance(sizes_arg, int) else None
            benchmark.save_report(report, prefix="benchmark", size=size)
    finally:
        if not args.no_cleanup:
            benchmark.cleanup_databases()
        else:
            print("\n[Info] Skipping cleanup (--no-cleanup flag set)")


if __name__ == "__main__":
    main()
