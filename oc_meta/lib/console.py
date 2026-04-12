# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

# -*- coding: utf-8 -*-
from datetime import timedelta
from math import ceil

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.text import Text

console = Console()


class EMATimeRemainingColumn(ProgressColumn):
    """Time remaining column blending EMA speed with overall average speed.

    Rich's default TimeRemainingColumn uses a simple windowed average that becomes
    unstable with infrequent updates. Pure EMA (α = 0.3) fixes that but over-reacts
    to speed bursts in long-running tasks, producing wildly optimistic estimates
    when recent items are fast (e.g. cache hits).

    This implementation blends:
    - Overall average speed (completed / elapsed): stable anchor
    - EMA speed: responsive to recent trends

    Final speed = _EMA_WEIGHT × EMA + (1 - _EMA_WEIGHT) × overall_average

    Skip handling:
    If task.fields contains a "processed" counter, speed is calculated based on
    processed items only (ignoring skipped items). This prevents cache hits from
    falsely inflating speed estimates. Use progress.update(task, advance=1, processed=1)
    for actual work, and progress.update(task, advance=1) for skipped items.
    """

    max_refresh = 0.5
    _SMOOTHING = 0.3
    _EMA_WEIGHT = 0.3

    def __init__(self):
        self._ema_speed: dict[int, float] = {}
        self._last_processed: dict[int, float] = {}
        self._last_time: dict[int, float] = {}
        self._start_time: dict[int, float] = {}
        super().__init__()

    def render(self, task: Task) -> Text:
        if task.finished:
            return Text("0:00:00", style="progress.remaining")
        if task.total is None or task.remaining is None:
            return Text("-:--:--", style="progress.remaining")

        current_time = task.get_time()
        task_id = task.id

        current_processed = task.fields.get("processed", task.completed)

        if task_id not in self._start_time:
            self._start_time[task_id] = current_time

        if task_id in self._last_time:
            dt = current_time - self._last_time[task_id]
            dp = current_processed - self._last_processed[task_id]

            if dt > 0 and dp > 0:
                instant_speed = dp / dt

                if task_id in self._ema_speed:
                    self._ema_speed[task_id] = (
                        self._SMOOTHING * instant_speed
                        + (1 - self._SMOOTHING) * self._ema_speed[task_id]
                    )
                else:
                    self._ema_speed[task_id] = instant_speed

                self._last_time[task_id] = current_time
                self._last_processed[task_id] = current_processed

        if task_id not in self._last_time:
            self._last_time[task_id] = current_time
            self._last_processed[task_id] = current_processed

        ema_speed = self._ema_speed.get(task_id)
        if not ema_speed:
            return Text("-:--:--", style="progress.remaining")

        elapsed = current_time - self._start_time[task_id]
        if elapsed > 0 and current_processed > 0:
            overall_speed = current_processed / elapsed
            speed = self._EMA_WEIGHT * ema_speed + (1 - self._EMA_WEIGHT) * overall_speed
        else:
            speed = ema_speed

        estimate = ceil(task.remaining / speed)
        delta = timedelta(seconds=estimate)
        return Text(str(delta), style="progress.remaining")


def create_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.completed}/{task.total}[/cyan]"),
        TimeElapsedColumn(),
        EMATimeRemainingColumn(),
        console=console,
    )


def advance_progress(
    progress: Progress,
    task_id: TaskID,
    advance: int = 1,
    processed: bool = True,
) -> None:
    """Advance progress bar, optionally marking items as actually processed.

    Args:
        progress: The Progress instance
        task_id: The task ID to advance
        advance: Number of items to advance (default: 1)
        processed: If True, count as actual work done (affects time estimate).
                   If False, count as skipped/cached (progress advances but
                   doesn't affect time estimate).
    """
    task = progress._tasks[task_id]
    current_processed = task.fields.get("processed", 0)
    if processed:
        progress.update(task_id, advance=advance, processed=current_processed + advance)
    else:
        progress.update(task_id, advance=advance, processed=current_processed)
