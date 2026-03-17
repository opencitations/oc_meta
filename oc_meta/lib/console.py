# -*- coding: utf-8 -*-
# Copyright 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
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
    """Time remaining column using Exponential Moving Average for stable estimates.

    Rich's default TimeRemainingColumn uses a simple windowed average that becomes
    unstable with infrequent updates. This implementation uses EMA (like tqdm) to
    provide smoother estimates by weighting recent observations more while retaining
    historical information.

    EMA formula: EMA_new = α × current_value + (1 - α) × EMA_previous

    With α = 0.3 (default):
    - 30% weight to the newly measured speed
    - 70% weight to the historical average (which itself contains 70% of previous
      history, creating exponential decay of older values)

    Skip handling:
    If task.fields contains a "processed" counter, speed is calculated based on
    processed items only (ignoring skipped items). This prevents cache hits from
    falsely inflating speed estimates. Use progress.update(task, advance=1, processed=1)
    for actual work, and progress.update(task, advance=1) for skipped items.
    """

    max_refresh = 0.5

    def __init__(self, smoothing: float = 0.3):
        self.smoothing = smoothing
        self._ema_speed: dict[int, float] = {}
        self._last_processed: dict[int, float] = {}
        self._last_time: dict[int, float] = {}
        super().__init__()

    def render(self, task: Task) -> Text:
        if task.finished:
            return Text("0:00:00", style="progress.remaining")
        if task.total is None or task.remaining is None:
            return Text("-:--:--", style="progress.remaining")

        current_time = task.get_time()
        task_id = task.id

        current_processed = task.fields.get("processed", task.completed)

        if task_id in self._last_time:
            dt = current_time - self._last_time[task_id]
            dp = current_processed - self._last_processed[task_id]

            if dt > 0 and dp > 0:
                instant_speed = dp / dt

                if task_id in self._ema_speed:
                    self._ema_speed[task_id] = (
                        self.smoothing * instant_speed
                        + (1 - self.smoothing) * self._ema_speed[task_id]
                    )
                else:
                    self._ema_speed[task_id] = instant_speed

                self._last_time[task_id] = current_time
                self._last_processed[task_id] = current_processed

        if task_id not in self._last_time:
            self._last_time[task_id] = current_time
            self._last_processed[task_id] = current_processed

        speed = self._ema_speed.get(task_id)
        if not speed:
            return Text("-:--:--", style="progress.remaining")

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
