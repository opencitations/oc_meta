import statistics
from typing import List, Dict, Any, Optional, Tuple

import scipy.stats


class BenchmarkStatistics:
    """Statistical analysis for benchmark results across multiple runs."""

    @staticmethod
    def calculate_statistics(runs: List[Dict[str, Any]], metrics_to_analyze: Optional[List[str]] = None) -> Dict[str, Dict[str, float]]:
        """Calculate comprehensive statistics for all numeric metrics across runs.

        Args:
            runs: List of individual run report dictionaries
            metrics_to_analyze: Optional list of metric keys to analyze. If None, analyzes all numeric metrics.

        Returns:
            Dictionary mapping metric names to their statistics:
            {
                "metric_name": {
                    "mean": float,
                    "median": float,
                    "std": float,
                    "min": float,
                    "max": float,
                    "ci_95_lower": float,
                    "ci_95_upper": float,
                    "outlier_indices": List[int]
                }
            }
        """
        if not runs:
            return {}

        all_metrics = BenchmarkStatistics._extract_numeric_metrics(runs)

        if metrics_to_analyze:
            all_metrics = {k: v for k, v in all_metrics.items() if k in metrics_to_analyze}

        stats = {}
        for metric_name, values in all_metrics.items():
            if len(values) < 2:
                continue

            stats[metric_name] = BenchmarkStatistics._calculate_metric_statistics(values)

        return stats

    @staticmethod
    def _extract_numeric_metrics(runs: List[Dict[str, Any]]) -> Dict[str, List[float]]:
        """Extract all numeric metrics from runs into lists of values."""
        metrics = {}

        for run in runs:
            if "metrics" in run:
                for key, value in run["metrics"].items():
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        if key not in metrics:
                            metrics[key] = []
                        metrics[key].append(float(value))

            if "phases" in run:
                for phase in run["phases"]:
                    phase_name = phase.get("name", "")
                    duration_key = f"{phase_name}_duration_seconds"

                    if "duration_seconds" in phase:
                        if duration_key not in metrics:
                            metrics[duration_key] = []
                        metrics[duration_key].append(float(phase["duration_seconds"]))

                    for mem_key in ["start_memory_mb", "end_memory_mb", "peak_memory_mb"]:
                        if mem_key in phase:
                            full_key = f"{phase_name}_{mem_key}"
                            if full_key not in metrics:
                                metrics[full_key] = []
                            metrics[full_key].append(float(phase[mem_key]))

        return metrics

    @staticmethod
    def _calculate_metric_statistics(values: List[float]) -> Dict[str, Any]:
        """Calculate statistics for a single metric."""
        n = len(values)

        mean_val = statistics.mean(values)
        median_val = statistics.median(values)
        std_val = statistics.stdev(values) if n > 1 else 0.0
        min_val = min(values)
        max_val = max(values)

        ci_lower, ci_upper = BenchmarkStatistics.calculate_confidence_interval(values, confidence=0.95)

        outlier_indices = BenchmarkStatistics.detect_outliers(values)

        return {
            "mean": mean_val,
            "median": median_val,
            "std": std_val,
            "min": min_val,
            "max": max_val,
            "ci_95_lower": ci_lower,
            "ci_95_upper": ci_upper,
            "outlier_indices": outlier_indices,
            "n": n
        }

    @staticmethod
    def calculate_confidence_interval(values: List[float], confidence: float = 0.95) -> Tuple[float, float]:
        """Calculate confidence interval for a list of values using t-distribution.

        Args:
            values: List of numeric values
            confidence: Confidence level (default: 0.95 for 95% CI)

        Returns:
            Tuple of (lower_bound, upper_bound)
        """
        n = len(values)
        if n < 2:
            mean_val = values[0] if values else 0.0
            return (mean_val, mean_val)

        mean_val = statistics.mean(values)
        std_val = statistics.stdev(values)

        t_critical = scipy.stats.t.ppf((1 + confidence) / 2, n - 1)
        margin_error = t_critical * (std_val / (n ** 0.5))

        return (mean_val - margin_error, mean_val + margin_error)

    @staticmethod
    def detect_outliers(values: List[float]) -> List[int]:
        """Detect outliers using IQR (Interquartile Range) method.

        Args:
            values: List of numeric values

        Returns:
            List of indices where outliers occur
        """
        if len(values) < 4:
            return []

        q1 = statistics.quantiles(values, n=4)[0]
        q3 = statistics.quantiles(values, n=4)[2]
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outlier_indices = []
        for idx, value in enumerate(values):
            if value < lower_bound or value > upper_bound:
                outlier_indices.append(idx)

        return outlier_indices

    @staticmethod
    def format_statistics_report(stats: Dict[str, Dict[str, Any]], indent: int = 2) -> str:
        """Format statistics dictionary into human-readable string.

        Args:
            stats: Statistics dictionary from calculate_statistics()
            indent: Number of spaces for indentation

        Returns:
            Formatted string report
        """
        if not stats:
            return "No statistics available"

        lines = []
        indent_str = " " * indent

        for metric_name, metric_stats in sorted(stats.items()):
            lines.append(f"\n{metric_name}:")
            lines.append(f"{indent_str}Mean:   {metric_stats['mean']:.4f}")
            lines.append(f"{indent_str}Median: {metric_stats['median']:.4f}")
            lines.append(f"{indent_str}Std:    {metric_stats['std']:.4f}")
            lines.append(f"{indent_str}Min:    {metric_stats['min']:.4f}")
            lines.append(f"{indent_str}Max:    {metric_stats['max']:.4f}")
            lines.append(f"{indent_str}95% CI: [{metric_stats['ci_95_lower']:.4f}, {metric_stats['ci_95_upper']:.4f}]")

            if metric_stats["outlier_indices"]:
                outliers_str = ", ".join(f"#{i}" for i in metric_stats["outlier_indices"])
                lines.append(f"{indent_str}Outliers: {outliers_str}")

        return "\n".join(lines)
