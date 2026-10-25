# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import hashlib
import os
import signal
from collections.abc import Callable
from datetime import datetime, timezone
from typing import cast

import orjson

from oc_meta.core.editor import MetaEditor
from oc_meta.lib.console import create_progress
from oc_meta.lib.rdf_patch import (
    EntityFileLocator,
    load_audit_config,
    read_json_object,
    sha256,
    write_json,
)
from oc_meta.run.merge.entities import REINDEX_SENTINEL_FILENAME

Operation = dict[str, object]
PlanCheck = Callable[[dict[str, object], list[Operation], EntityFileLocator], None]
OperationStep = Callable[[MetaEditor, Operation, EntityFileLocator], None]

_stop_requested = False


def _request_stop(signum: int, frame: object) -> None:
    del signum, frame
    global _stop_requested
    _stop_requested = True


def stop_on_signals() -> None:
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)


def object_sha256(value: object) -> str:
    return hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()


def identified(operation: Operation) -> Operation:
    operation["operation_id"] = object_sha256(operation)[:20]
    return operation


def _plan_operations(plan: dict[str, object], version: int) -> list[Operation]:
    if plan["version"] != version:
        raise ValueError(
            f"Unsupported correction plan version: {plan['version']}; "
            f"expected {version}"
        )
    operations = cast(list[Operation], plan["operations"])
    for operation in operations:
        payload = dict(operation)
        operation_id = payload.pop("operation_id")
        if object_sha256(payload)[:20] != operation_id:
            raise ValueError(f"Correction plan has modified operation {operation_id}")
    if plan["operations_sha256"] != object_sha256(operations):
        raise ValueError("Correction plan operations were modified")
    return operations


def _load_progress(path: str, plan_sha256: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    progress = read_json_object(path)
    if progress["plan_sha256"] != plan_sha256:
        raise ValueError("Progress file belongs to a different correction plan")
    return set(cast(list[str], progress["completed_operations"]))


def _save_progress(path: str, plan_sha256: str, completed: set[str]) -> None:
    write_json(
        path, {"plan_sha256": plan_sha256, "completed_operations": sorted(completed)}
    )


def execute_plan(
    config_path: str,
    plan_path: str,
    resp_agent: str,
    progress_path: str,
    execution_report_path: str,
    version: int,
    description: str,
    after_reindex: str,
    check: PlanCheck,
    step: OperationStep,
) -> dict[str, object]:
    global _stop_requested
    _stop_requested = False
    config_path = os.path.abspath(config_path)
    plan_path = os.path.abspath(plan_path)
    plan = read_json_object(plan_path)
    operations = _plan_operations(plan, version)
    if plan["config_sha256"] != sha256(config_path):
        raise ValueError("The Meta configuration changed after plan generation")
    plan_sha256 = sha256(plan_path)
    completed = _load_progress(progress_path, plan_sha256)
    sentinel_path = os.path.join(os.path.dirname(plan_path), REINDEX_SENTINEL_FILENAME)
    if os.path.exists(sentinel_path) and not os.path.exists(progress_path):
        raise RuntimeError(
            f"{sentinel_path} exists. Re-index the triplestores and remove the "
            "sentinel before starting another correction plan."
        )
    pending = [
        operation
        for operation in operations
        if cast(str, operation["operation_id"]) not in completed
    ]
    config = load_audit_config(config_path)
    locator = EntityFileLocator(
        config.rdf_dir, config.dir_split, config.items_per_file, config.zip_output
    )
    check(plan, pending, locator)
    attempted = 0
    if pending:
        _save_progress(progress_path, plan_sha256, completed)
        editor = MetaEditor(config_path, resp_agent, save_queries=True)
        editor.rdf_files_only = True
        try:
            with create_progress() as progress:
                task = progress.add_task(description, total=len(operations))
                progress.advance(task, len(completed))
                for operation in pending:
                    if _stop_requested:
                        break
                    attempted += 1
                    step(editor, operation, locator)
                    completed.add(cast(str, operation["operation_id"]))
                    _save_progress(progress_path, plan_sha256, completed)
                    progress.advance(task)
        finally:
            if attempted:
                with open(sentinel_path, "w", encoding="utf-8") as stream:
                    stream.write(
                        f"{plan_path} changed local RDF files.\nRe-index the data "
                        "and provenance triplestores from the RDF files, then "
                        f"delete this file.{after_reindex}\n"
                    )
    complete = len(completed) == len(operations) and not _stop_requested
    report: dict[str, object] = {
        "plan": plan_path,
        "plan_sha256": plan_sha256,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "complete": complete,
        "planned_operations": len(operations),
        "completed_operations": len(completed),
        "reindex_sentinel": sentinel_path if os.path.exists(sentinel_path) else None,
    }
    write_json(execution_report_path, report)
    if complete and os.path.exists(progress_path):
        os.remove(progress_path)
    return report
