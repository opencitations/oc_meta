#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Plotting utilities for benchmark visualization.

This module provides functions to generate visualizations for benchmark results,
including single-size run comparisons and multi-size scalability analysis.
"""

from typing import Any, Dict, List

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

CURATION_COLLECT_IDS_COLOR = '#FF8C00'
CURATION_REST_COLOR = '#FFE066'
RDF_CREATION_COLOR = '#C73E1D'
STORAGE_COLOR = '#6A994E'

CURATION_REST_PHASES = [
    "curation__clean_id",
    "curation__merge_duplicates",
    "curation__clean_vvi",
    "curation__clean_ra",
    "curation__finalize"
]


def get_phase_duration_by_name(run: Dict[str, Any], phase_name: str) -> float:
    """Get phase duration by name instead of index."""
    for phase in run["phases"]:
        if phase["name"] == phase_name:
            return phase["duration_seconds"] or 0
    return 0


def get_curation_total(run: Dict[str, Any]) -> float:
    """Calculate total curation time by summing sub-phases."""
    total = 0
    for phase in run["phases"]:
        if phase["name"].startswith("curation__"):
            total += phase["duration_seconds"] or 0
    return total


def get_curation_rest(run: Dict[str, Any]) -> float:
    """Calculate curation time excluding collect_identifiers."""
    return sum(get_phase_duration_by_name(run, p) for p in CURATION_REST_PHASES)


def get_storage_total(run: Dict[str, Any]) -> float:
    """Get total storage time (wall-clock)."""
    return get_phase_duration_by_name(run, "storage")


def apply_plot_style(ax, title: str, xlabel: str = None, ylabel: str = None, grid: bool = True):
    """Apply consistent styling to plot axes."""
    ax.set_title(title, fontweight='bold')
    if xlabel:
        ax.set_xlabel(xlabel, fontweight='bold')
    if ylabel:
        ax.set_ylabel(ylabel, fontweight='bold')
    if grid:
        ax.grid(True, alpha=0.3)


def extract_metric_values(runs: List[Dict[str, Any]], metric_key: str) -> List[float]:
    """Extract metric values from list of run reports."""
    return [r["metrics"][metric_key] for r in runs]


def format_bar_labels(ax, bars, values: List[float], unit: str = "s"):
    """Add value labels on top of bar chart bars."""
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{val:.1f}{unit}', ha='center', va='bottom', fontweight='bold')


def _draw_phase_breakdown(ax, collect_ids: float, curation_rest: float, rdf_time: float,
                          storage_time: float):
    """Draw bar chart for phase breakdown (shared by single/multi run plots)."""
    curation_total = collect_ids + curation_rest

    ax.bar(0, collect_ids, color=CURATION_COLLECT_IDS_COLOR, edgecolor='black', linewidth=0.5, width=0.6)
    ax.bar(0, curation_rest, bottom=collect_ids, color=CURATION_REST_COLOR, edgecolor='black', linewidth=0.5, width=0.6)
    ax.bar(1, rdf_time, color=RDF_CREATION_COLOR, edgecolor='black', linewidth=1.5, width=0.6)
    ax.bar(2, storage_time, color=STORAGE_COLOR, edgecolor='black', linewidth=1.5, width=0.6)

    ax.text(0, curation_total, f'{curation_total:.1f}s', ha='center', va='bottom', fontweight='bold')
    ax.text(1, rdf_time, f'{rdf_time:.1f}s', ha='center', va='bottom', fontweight='bold')
    ax.text(2, storage_time, f'{storage_time:.1f}s', ha='center', va='bottom', fontweight='bold')

    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(['Curation', 'RDF\ncreation', 'Storage'])

    legend_patches = [
        Rectangle((0, 0), 1, 1, facecolor=CURATION_COLLECT_IDS_COLOR, edgecolor='black', linewidth=0.5),
        Rectangle((0, 0), 1, 1, facecolor=CURATION_REST_COLOR, edgecolor='black', linewidth=0.5),
    ]
    legend_labels = ['Collect IDs', 'Curation rest']
    ax.legend(legend_patches, legend_labels, loc='upper right', fontsize=6)


def plot_scalability_analysis(per_size_results: List[Dict[str, Any]], output_path: str):
    """Generate scalability analysis visualization comparing multiple sizes."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Scalability analysis across dataset sizes', fontsize=16, fontweight='bold')

    sizes = [r["size"] for r in per_size_results]
    mean_durations = []
    mean_throughputs = []
    phase_data = {"curation": [], "rdf_creation": [], "storage_upload": []}

    for size_data in per_size_results:
        if size_data["statistics"]:
            mean_durations.append(size_data["statistics"]["total_duration_seconds"]["mean"])
            mean_throughputs.append(size_data["statistics"]["throughput_records_per_sec"]["mean"])
            # Sum all curation sub-phases
            curation_sum = (
                size_data["statistics"]["curation__collect_identifiers_duration_seconds"]["mean"] +
                sum(size_data["statistics"][f"{p}_duration_seconds"]["mean"] for p in CURATION_REST_PHASES)
            )
            phase_data["curation"].append(curation_sum)
            phase_data["rdf_creation"].append(size_data["statistics"]["rdf_creation_duration_seconds"]["mean"])
            phase_data["storage_upload"].append(size_data["statistics"].get("storage_duration_seconds", {}).get("mean", 0))
        else:
            run = size_data["runs"][0]
            mean_durations.append(run["metrics"]["total_duration_seconds"])
            mean_throughputs.append(run["metrics"]["throughput_records_per_sec"])
            phase_data["curation"].append(get_curation_total(run))
            phase_data["rdf_creation"].append(get_phase_duration_by_name(run, "rdf_creation"))
            phase_data["storage_upload"].append(get_storage_total(run))

    axes[0, 0].plot(sizes, mean_durations, marker='o', linewidth=2, markersize=8, color='#2E86AB')
    apply_plot_style(axes[0, 0], 'Total duration vs dataset size', 'Dataset size (records)', 'Duration (s)')

    axes[0, 1].plot(sizes, mean_throughputs, marker='s', linewidth=2, markersize=8, color=STORAGE_COLOR)
    apply_plot_style(axes[0, 1], 'Throughput vs dataset size', 'Dataset size (records)', 'Throughput (records/sec)')

    phase_colors = [CURATION_COLLECT_IDS_COLOR, RDF_CREATION_COLOR, STORAGE_COLOR]
    phase_labels = ['Curation', 'RDF creation', 'Storage']

    x_positions = list(range(len(sizes)))
    bottom = [0] * len(sizes)
    for idx, (phase_name, phase_values) in enumerate([
        ("curation", phase_data["curation"]),
        ("rdf_creation", phase_data["rdf_creation"]),
        ("storage_upload", phase_data["storage_upload"])
    ]):
        axes[1, 0].bar(x_positions, phase_values, bottom=bottom, label=phase_labels[idx],
                      color=phase_colors[idx], edgecolor='black', linewidth=1, width=0.6)
        bottom = [b + v for b, v in zip(bottom, phase_values)]

    axes[1, 0].set_xticks(x_positions)
    axes[1, 0].set_xticklabels([f'{s}' for s in sizes])
    apply_plot_style(axes[1, 0], 'Phase breakdown by dataset size', 'Dataset size (records)', 'Duration (s)')
    axes[1, 0].legend(loc='upper left')

    if len(sizes) > 1 and len(per_size_results[0]["runs"]) > 1:
        for size_data in per_size_results:
            size = size_data["size"]
            runs = size_data["runs"]
            durations = [r["metrics"]["total_duration_seconds"] for r in runs]
            axes[1, 1].plot([size] * len(durations), durations, 'o', alpha=0.6, markersize=6)

        apply_plot_style(axes[1, 1], 'Duration distribution across sizes', 'Dataset size (records)', 'Duration (s)')
        axes[1, 1].plot(sizes, mean_durations, 'r-', linewidth=2, label='Mean', marker='D', markersize=8)
        axes[1, 1].legend()
    else:
        bars = axes[1, 1].bar(sizes, mean_durations, color='#2E86AB', edgecolor='black', linewidth=1.5)
        format_bar_labels(axes[1, 1], bars, mean_durations, "s")
        apply_plot_style(axes[1, 1], 'Total duration by dataset size', 'Dataset size (records)', 'Duration (s)')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"[Visualization] Saved scalability analysis to {output_path}")


def plot_benchmark_results(all_runs: List[Dict[str, Any]], stats: Dict[str, Dict[str, Any]], output_path: str):
    """Generate visualization plots for benchmark results."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Benchmark results visualization', fontsize=16, fontweight='bold')

    run_numbers = list(range(1, len(all_runs) + 1))
    total_times = extract_metric_values(all_runs, "total_duration_seconds")

    axes[0, 0].plot(run_numbers, total_times, marker='o', linewidth=2, markersize=8, color='#2E86AB')
    axes[0, 0].axhline(y=stats["total_duration_seconds"]["mean"], color='#A23B72', linestyle='--', label='Mean', linewidth=1.5)
    axes[0, 0].fill_between(
        run_numbers,
        stats["total_duration_seconds"]["ci_95_lower"],
        stats["total_duration_seconds"]["ci_95_upper"],
        alpha=0.2,
        color='#A23B72',
        label='95% CI'
    )
    apply_plot_style(axes[0, 0], 'Total duration per run', 'Run number', 'Total duration (s)')
    axes[0, 0].legend()

    # Phase breakdown
    collect_ids_mean = stats["curation__collect_identifiers_duration_seconds"]["mean"]
    curation_rest_mean = sum(stats[f"{p}_duration_seconds"]["mean"] for p in CURATION_REST_PHASES)
    rdf_mean = stats["rdf_creation_duration_seconds"]["mean"]
    storage_mean = stats.get("storage_duration_seconds", {}).get("mean", 0)

    _draw_phase_breakdown(axes[0, 1], collect_ids_mean, curation_rest_mean, rdf_mean, storage_mean)
    apply_plot_style(axes[0, 1], 'Average phase duration breakdown', ylabel='Duration (s)', grid=False)
    axes[0, 1].grid(True, axis='y', alpha=0.3)

    # Box plot for phase distribution
    curation_times = [get_curation_total(r) for r in all_runs]
    rdf_times = [get_phase_duration_by_name(r, "rdf_creation") for r in all_runs]
    upload_times = [get_storage_total(r) for r in all_runs]

    boxplot_phase_names = ['Curation', 'RDF\ncreation', 'Storage']
    boxplot_colors = [CURATION_COLLECT_IDS_COLOR, RDF_CREATION_COLOR, STORAGE_COLOR]

    bp = axes[1, 0].boxplot([curation_times, rdf_times, upload_times],
                            labels=boxplot_phase_names,
                            patch_artist=True,
                            notch=True,
                            showmeans=True)

    for patch, color in zip(bp['boxes'], boxplot_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    apply_plot_style(axes[1, 0], 'Phase duration distribution (box plot)', ylabel='Duration (s)', grid=False)
    axes[1, 0].grid(True, axis='y', alpha=0.3)

    legend_elements = [
        Rectangle((0, 0), 1, 1, facecolor='gray', alpha=0.5, label='Box: 50% of data (Q1-Q3)'),
        Line2D([0], [0], color='black', linewidth=2, label='Line: median'),
        Line2D([0], [0], marker='^', color='w', markerfacecolor='green', markersize=8, label='Triangle: mean'),
        Line2D([0], [0], color='black', linewidth=1, linestyle='-', label='Whiskers: 1.5xIQR'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='white', markeredgecolor='black', markersize=6, label='Circles: outliers')
    ]
    axes[1, 0].legend(handles=legend_elements, loc='upper left', fontsize=8)

    throughputs = extract_metric_values(all_runs, "throughput_records_per_sec")
    axes[1, 1].bar(run_numbers, throughputs, color=STORAGE_COLOR, edgecolor='black', linewidth=1.5)
    axes[1, 1].axhline(y=stats["throughput_records_per_sec"]["mean"], color='#A23B72', linestyle='--', label='Mean', linewidth=1.5)
    apply_plot_style(axes[1, 1], 'Throughput per run', 'Run number', 'Throughput (records/sec)')
    axes[1, 1].legend()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"[Visualization] Saved to {output_path}")


def plot_single_run_results(run: Dict[str, Any], output_path: str):
    """Generate visualization for a single benchmark run."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle('Single run benchmark results', fontsize=16, fontweight='bold')

    collect_ids = get_phase_duration_by_name(run, "curation__collect_identifiers")
    curation_rest = get_curation_rest(run)
    rdf_time = get_phase_duration_by_name(run, "rdf_creation")
    storage_time = get_phase_duration_by_name(run, "storage")

    _draw_phase_breakdown(axes[0], collect_ids, curation_rest, rdf_time, storage_time)
    apply_plot_style(axes[0], 'Phase duration breakdown', ylabel='Duration (s)', grid=False)
    axes[0].grid(True, axis='y', alpha=0.3)

    # Throughput
    throughput = run["metrics"]["throughput_records_per_sec"]
    bar = axes[1].bar(['Throughput'], [throughput], color=STORAGE_COLOR, edgecolor='black', linewidth=1.5, width=0.4)
    format_bar_labels(axes[1], bar, [throughput], " rec/s")
    apply_plot_style(axes[1], 'Processing throughput', ylabel='Records per second', grid=False)
    axes[1].grid(True, axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"[Visualization] Saved to {output_path}")


def _get_phase_duration_from_report(report: Dict[str, Any], phase_name: str) -> float:
    """Get phase duration from a report dict by phase name."""
    for phase in report.get("phases", []):
        if phase["name"] == phase_name:
            return phase["duration_seconds"] or 0.0
    return 0.0


def _get_curation_rest_from_report(report: Dict[str, Any]) -> float:
    """Get curation rest time (excluding collect_identifiers) from report."""
    return sum(_get_phase_duration_from_report(report, p) for p in CURATION_REST_PHASES)


def plot_incremental_progress(all_reports: List[Dict[str, Any]], output_path: str):
    """Generate incremental chart showing meta_process progress with sub-phase breakdown."""
    if not all_reports:
        return

    filenames = [r["filename"] for r in all_reports]

    collect_ids_times = [_get_phase_duration_from_report(r["report"], "curation__collect_identifiers") for r in all_reports]
    curation_rest_times = [_get_curation_rest_from_report(r["report"]) for r in all_reports]
    rdf_times = [_get_phase_duration_from_report(r["report"], "rdf_creation") for r in all_reports]
    storage_times = [_get_phase_duration_from_report(r["report"], "storage") for r in all_reports]
    throughputs = [r["report"]["metrics"].get("throughput_records_per_sec", 0) for r in all_reports]

    total_times = [c + cr + r + s for c, cr, r, s in zip(collect_ids_times, curation_rest_times, rdf_times, storage_times)]

    mean_duration = sum(total_times) / len(total_times) if total_times else 0
    mean_throughput = sum(throughputs) / len(throughputs) if throughputs else 0

    _, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))

    x = np.arange(len(filenames))
    width = 0.6

    bottom = np.zeros(len(filenames))
    ax1.bar(x, collect_ids_times, width, bottom=bottom, label='Collect IDs', color=CURATION_COLLECT_IDS_COLOR, edgecolor='black', linewidth=0.3)
    bottom += np.array(collect_ids_times)
    ax1.bar(x, curation_rest_times, width, bottom=bottom, label='Curation rest', color=CURATION_REST_COLOR, edgecolor='black', linewidth=0.3)
    bottom += np.array(curation_rest_times)
    ax1.bar(x, rdf_times, width, bottom=bottom, label='RDF creation', color=RDF_CREATION_COLOR, edgecolor='black', linewidth=0.3)
    bottom += np.array(rdf_times)
    ax1.bar(x, storage_times, width, bottom=bottom, label='Storage', color=STORAGE_COLOR, edgecolor='black', linewidth=0.3)

    ax1.axhline(y=mean_duration, color='#A23B72', linestyle='--', linewidth=2, label=f'Mean ({mean_duration:.1f}s)')

    ax1.set_ylabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Processing time by phase ({len(filenames)} files processed)', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f[:20] for f in filenames], rotation=45, ha='right', fontsize=8)
    ax1.legend(loc='upper right', fontsize=8)
    ax1.grid(axis='y', alpha=0.3)

    ax2.plot(x, throughputs, marker='o', linewidth=2, markersize=6, color='#2E86AB')
    ax2.axhline(y=mean_throughput, color='#A23B72', linestyle='--', linewidth=2, label=f'Mean ({mean_throughput:.1f} rec/s)')
    ax2.set_xlabel('File index', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Throughput (records/sec)', fontsize=12, fontweight='bold')
    ax2.set_title('Processing throughput', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(x)
    ax2.set_xticklabels(range(1, len(filenames) + 1), fontsize=8)
    ax2.legend(loc='upper right')

    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()
