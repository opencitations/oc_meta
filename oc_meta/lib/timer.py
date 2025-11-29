#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Timing and metrics collection utilities for OpenCitations Meta processing.

This module provides reusable timing infrastructure for both production
processing and benchmarking, with optional activation to avoid overhead.
"""

import time
from typing import Any, Dict, List, Optional

import psutil


class BenchmarkTimer:
    """Context manager for timing code blocks and collecting memory metrics."""

    def __init__(self, name: str, verbose: bool = False):
        self.name = name
        self.verbose = verbose
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
        if self.verbose:
            print(f"  [{self.name}] Starting...")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        process = psutil.Process()
        self.end_memory = process.memory_info().rss
        self.peak_memory = max(self.start_memory, self.end_memory)
        if self.verbose:
            print(f"  [{self.name}] Completed in {self.duration:.2f}s")

    def to_dict(self) -> Dict[str, Any]:
        """Convert timing data to dictionary."""
        return {
            "name": self.name,
            "duration_seconds": round(self.duration, 3) if self.duration else None,
            "start_memory_mb": round(self.start_memory / 1024 / 1024, 2) if self.start_memory else None,
            "end_memory_mb": round(self.end_memory / 1024 / 1024, 2) if self.end_memory else None,
            "peak_memory_mb": round(self.peak_memory / 1024 / 1024, 2) if self.peak_memory else None,
        }


class DummyTimer:
    """No-op timer for when timing is disabled."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class ProcessTimer:
    """Optional timing wrapper for MetaProcess operations."""

    def __init__(self, enabled: bool = False, verbose: bool = False):
        self.enabled = enabled
        self.verbose = verbose
        self.timers: List[BenchmarkTimer] = []
        self.metrics: Dict[str, Any] = {}

    def timer(self, name: str):
        """Create a timer context manager (or no-op if disabled)."""
        if self.enabled:
            # Don't show verbose for total_processing and sub-timers
            show_verbose = self.verbose and name not in ["total_processing", "creator_execution", "provenance_generation"]
            timer = BenchmarkTimer(name, verbose=show_verbose)
            self.timers.append(timer)
            return timer
        else:
            return DummyTimer()

    def record_metric(self, key: str, value: Any):
        """Record a metric."""
        if self.enabled:
            self.metrics[key] = value

    def record_phase(self, name: str, duration: float):
        """Record a phase with a specific duration (e.g., 0 for unused phases)."""
        if self.enabled:
            timer = BenchmarkTimer(name, verbose=False)
            timer.start_time = 0
            timer.end_time = duration
            timer.duration = duration
            timer.start_memory = 0
            timer.end_memory = 0
            timer.peak_memory = 0
            self.timers.append(timer)

    def get_report(self) -> Dict[str, Any]:
        """Generate timing report."""
        if not self.enabled:
            return {}

        total_time = next((t.duration for t in self.timers if t.name == "total_processing"), 0)
        input_records = self.metrics.get("input_records", 0)

        return {
            "metrics": {
                **self.metrics,
                "total_duration_seconds": round(total_time, 3),
                "throughput_records_per_sec": round(input_records / total_time, 2) if total_time > 0 else 0
            },
            "phases": [t.to_dict() for t in self.timers],
        }

    def print_summary(self):
        """Print timing summary to console."""
        if not self.enabled:
            return

        report = self.get_report()
        metrics = report["metrics"]

        print(f"\n{'='*60}")
        print("Timing Summary")
        print(f"{'='*60}")
        print(f"Total Duration: {metrics.get('total_duration_seconds', 0)}s")
        print(f"Throughput: {metrics.get('throughput_records_per_sec', 0)} records/sec")
        print(f"Input Records: {metrics.get('input_records', 0)}")
        print(f"Curated Records: {metrics.get('curated_records', 0)}")
        print(f"Entities Created: {metrics.get('entities_created', 0)}")
        print(f"Modified Entities: {metrics.get('modified_entities', 0)}")
        print(f"\nPhase Breakdown:")
        for phase in report["phases"]:
            if phase["name"] not in ["total_processing", "creator_execution", "provenance_generation"]:
                print(f"  {phase['name']}: {phase['duration_seconds']}s")
        print(f"{'='*60}\n")

    def print_phase_breakdown(self):
        """Print detailed phase breakdown for a single file."""
        if not self.enabled:
            return

        report = self.get_report()
        phases = report["phases"]

        print(f"\n  Phase Breakdown:")
        for phase in phases:
            if phase["name"] == "total_processing":
                continue
            name = phase["name"]
            duration = phase["duration_seconds"]
            print(f"    {name:25s} {duration:8.2f}s")

    def print_file_summary(self, filename: str):
        """Print complete summary for a single file with metrics and phases."""
        if not self.enabled:
            return

        report = self.get_report()
        metrics = report["metrics"]

        total_time = metrics.get("total_duration_seconds", 0)
        records = metrics.get("input_records", 0)
        entities = metrics.get("entities_created", 0)
        throughput = metrics.get("throughput_records_per_sec", 0)

        print(f"  ✓ Completed in {total_time:.2f}s")
        self.print_phase_breakdown()
        print(f"\n  Metrics:")
        print(f"    Records processed:    {records}")
        print(f"    Entities created:     {entities}")
        print(f"    Throughput:           {throughput:.2f} rec/s")
