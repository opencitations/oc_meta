#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Plotting utilities for benchmark visualization.

This module provides functions to generate visualizations for benchmark results,
including single-size run comparisons and multi-size scalability analysis.
"""

from typing import Any, Dict, List

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle


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


def extract_phase_values(runs: List[Dict[str, Any]], phase_index: int, value_key: str = "duration_seconds") -> List[float]:
    """Extract phase values from list of run reports."""
    return [r["phases"][phase_index][value_key] for r in runs]


def format_bar_labels(ax, bars, values: List[float], unit: str = "s"):
    """Add value labels on top of bar chart bars."""
    for bar, val in zip(bars, values):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{val:.1f}{unit}', ha='center', va='bottom', fontweight='bold')


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
            phase_data["curation"].append(size_data["statistics"]["curation_duration_seconds"]["mean"])
            phase_data["rdf_creation"].append(size_data["statistics"]["rdf_creation_duration_seconds"]["mean"])
            phase_data["storage_upload"].append(size_data["statistics"]["storage_and_upload_duration_seconds"]["mean"])
        else:
            run = size_data["runs"][0]
            mean_durations.append(run["metrics"]["total_duration_seconds"])
            mean_throughputs.append(run["metrics"]["throughput_records_per_sec"])
            phase_data["curation"].append(run["phases"][0]["duration_seconds"])
            phase_data["rdf_creation"].append(run["phases"][3]["duration_seconds"])
            phase_data["storage_upload"].append(run["phases"][4]["duration_seconds"])

    axes[0, 0].plot(sizes, mean_durations, marker='o', linewidth=2, markersize=8, color='#2E86AB')
    apply_plot_style(axes[0, 0], 'Total duration vs dataset size', 'Dataset size (records)', 'Duration (s)')

    axes[0, 1].plot(sizes, mean_throughputs, marker='s', linewidth=2, markersize=8, color='#6A994E')
    apply_plot_style(axes[0, 1], 'Throughput vs dataset size', 'Dataset size (records)', 'Throughput (records/sec)')

    phase_colors = ['#F18F01', '#C73E1D', '#6A994E']
    phase_labels = ['Curation', 'RDF creation', 'Storage + upload']

    bottom = [0] * len(sizes)
    for idx, (phase_name, phase_values) in enumerate([
        ("curation", phase_data["curation"]),
        ("rdf_creation", phase_data["rdf_creation"]),
        ("storage_upload", phase_data["storage_upload"])
    ]):
        axes[1, 0].bar(sizes, phase_values, bottom=bottom, label=phase_labels[idx],
                      color=phase_colors[idx], edgecolor='black', linewidth=1)
        bottom = [b + v for b, v in zip(bottom, phase_values)]

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

    phase_names = ['Curation', 'RDF\ncreation', 'Storage\n+upload']
    phase_keys = ['curation_duration_seconds', 'rdf_creation_duration_seconds', 'storage_and_upload_duration_seconds']
    phase_means = [stats[key]["mean"] for key in phase_keys]
    colors = ['#F18F01', '#C73E1D', '#6A994E']

    bars = axes[0, 1].bar(phase_names, phase_means, color=colors, edgecolor='black', linewidth=1.5)
    format_bar_labels(axes[0, 1], bars, phase_means, "s")
    apply_plot_style(axes[0, 1], 'Average phase duration breakdown', ylabel='Duration (s)', grid=False)
    axes[0, 1].grid(True, axis='y', alpha=0.3)

    curation_times = extract_phase_values(all_runs, 0)
    rdf_times = extract_phase_values(all_runs, 3)
    upload_times = extract_phase_values(all_runs, 4)

    bp = axes[1, 0].boxplot([curation_times, rdf_times, upload_times],
                            labels=phase_names,
                            patch_artist=True,
                            notch=True,
                            showmeans=True)

    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    apply_plot_style(axes[1, 0], 'Phase duration distribution (box plot)', ylabel='Duration (s)', grid=False)
    axes[1, 0].grid(True, axis='y', alpha=0.3)

    legend_elements = [
        Rectangle((0, 0), 1, 1, facecolor='gray', alpha=0.5, label='Box: 50% of data (Q1-Q3)'),
        Line2D([0], [0], color='black', linewidth=2, label='Line: median'),
        Line2D([0], [0], marker='^', color='w', markerfacecolor='green', markersize=8, label='Triangle: mean'),
        Line2D([0], [0], color='black', linewidth=1, linestyle='-', label='Whiskers: 1.5×IQR'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='white', markeredgecolor='black', markersize=6, label='Circles: outliers')
    ]
    axes[1, 0].legend(handles=legend_elements, loc='upper left', fontsize=8)

    throughputs = extract_metric_values(all_runs, "throughput_records_per_sec")
    axes[1, 1].bar(run_numbers, throughputs, color='#6A994E', edgecolor='black', linewidth=1.5)
    axes[1, 1].axhline(y=stats["throughput_records_per_sec"]["mean"], color='#A23B72', linestyle='--', label='Mean', linewidth=1.5)
    apply_plot_style(axes[1, 1], 'Throughput per run', 'Run number', 'Throughput (records/sec)')
    axes[1, 1].legend()

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"[Visualization] Saved to {output_path}")
