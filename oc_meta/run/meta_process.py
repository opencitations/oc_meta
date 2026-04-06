# SPDX-FileCopyrightText: 2019 Silvio Peroni <silvio.peroni@unibo.it>
# SPDX-FileCopyrightText: 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# SPDX-FileCopyrightText: 2021 Simone Persiani <iosonopersia@gmail.com>
# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations

import bisect
import csv
import multiprocessing
import os
import sys
import traceback
from argparse import ArgumentParser
from datetime import datetime
from sys import executable, platform
from typing import Any, Dict, Iterator, List, Optional, Tuple

import orjson
import redis
import yaml
from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.prov import ProvSet
from oc_ocdm.support.reporter import Reporter
from piccione.upload.on_triplestore import upload_sparql_updates
from rich_argparse import RichHelpFormatter
from time_agnostic_library.support import generate_config_file

from oc_meta.core.creator import Creator
from oc_meta.core.curator import Curator
from oc_meta.lib.console import console, create_progress
from oc_meta.lib.file_manager import (get_csv_data, init_cache, normalize_path,
                                      pathoo, sort_files)
from oc_meta.lib.timer import ProcessTimer
from oc_meta.run.benchmark.plotting import plot_incremental_progress


def _upload_to_triplestore(endpoint: str, folder: str, redis_host: str, redis_port: int, redis_db: int, failed_file: str, stop_file: str, description: str = "Processing files") -> None:
    """Upload SPARQL queries from folder to triplestore endpoint."""
    try:
        upload_sparql_updates(
            endpoint=endpoint,
            folder=folder,
            failed_file=failed_file,
            stop_file=stop_file,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            description=description,
            show_progress=False,
        )
    except Exception as e:
        console.print(f"[red]Upload to {endpoint} failed: {e}[/red]")
        sys.exit(1)


def _generate_queries_worker(storer: Storer, triplestore_url: str, base_dir: str) -> None:
    storer.upload_all(
        triplestore_url=triplestore_url,
        base_dir=base_dir,
        batch_size=10,
    )


def _store_rdf_worker(storer: Storer, base_dir, base_iri, context_path):
    storer.store_all(
        base_dir=base_dir,
        base_iri=base_iri,
        context_path=context_path
    )


class MetaProcess:
    def __init__(self, settings: dict, meta_config_path: str, timer: Optional[ProcessTimer] = None):
        self.settings = settings
        # Mandatory settings
        self.triplestore_url = settings["triplestore_url"]  # Main triplestore for data
        self.provenance_triplestore_url = settings["provenance_triplestore_url"]  # Separate triplestore for provenance
        self.input_csv_dir = normalize_path(settings["input_csv_dir"])
        self.base_output_dir = normalize_path(settings["base_output_dir"])
        self.resp_agent = settings["resp_agent"]
        self.info_dir = os.path.join(self.base_output_dir, "info_dir")
        self.output_csv_dir = os.path.join(self.base_output_dir, "csv")
        self.output_rdf_dir = (
            normalize_path(settings["output_rdf_dir"]) + os.sep + "rdf" + os.sep
        )
        self.cache_path = os.path.join(self.base_output_dir, "cache.txt")
        self.errors_path = os.path.join(self.base_output_dir, "errors.txt")
        self.timer = timer or ProcessTimer(enabled=False)
        # Optional settings
        self.base_iri = settings["base_iri"]
        self.normalize_titles = settings.get("normalize_titles", True)
        self.context_path = settings["context_path"]
        self.dir_split_number = settings["dir_split_number"]
        self.items_per_file = settings["items_per_file"]
        self.default_dir = settings["default_dir"]
        self.zip_output_rdf = settings["zip_output_rdf"]
        self.source = settings["source"]
        self.valid_dois_cache: dict[str, str] = (
            dict() if settings["use_doi_api_service"] else {}
        )
        supplier_prefix: str = settings["supplier_prefix"]
        self.supplier_prefix = (
            supplier_prefix if supplier_prefix.endswith("0") else f"{supplier_prefix}0"
        )
        self.silencer = settings["silencer"]
        self.rdf_files_only = settings.get("rdf_files_only", False)
        # Time-Agnostic_library integration
        self.time_agnostic_library_config = os.path.join(
            os.path.dirname(meta_config_path), "time_agnostic_library_config.json"
        )
        if not os.path.exists(self.time_agnostic_library_config):
            generate_config_file(
                config_path=self.time_agnostic_library_config,
                dataset_urls=[self.triplestore_url],
                dataset_dirs=list(),
                provenance_urls=[self.provenance_triplestore_url] if self.provenance_triplestore_url not in settings["provenance_endpoints"] else settings["provenance_endpoints"],
                provenance_dirs=list(),
                blazegraph_full_text_search=settings["blazegraph_full_text_search"],
                fuseki_full_text_search=settings["fuseki_full_text_search"],
                virtuoso_full_text_search=settings["virtuoso_full_text_search"],
                graphdb_connector_name=settings["graphdb_connector_name"],
            )

        # Redis settings
        self.redis_host = settings.get("redis_host", "localhost")
        self.redis_port = settings.get("redis_port", 6379)
        self.redis_db = settings.get("redis_db", 5)
        self.redis_cache_db = settings.get("redis_cache_db", 2)
        self.redis_client = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )

        self.workers = settings.get("workers", 1)

        self.counter_handler = RedisCounterHandler(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )

        # Triplestore upload settings
        self.ts_failed_queries = settings.get("ts_failed_queries", "failed_queries.txt")
        self.ts_stop_file = settings.get("ts_stop_file", ".stop_upload")

        self.data_update_dir = os.path.join(self.base_output_dir, "to_be_uploaded_data")
        self.prov_update_dir = os.path.join(self.base_output_dir, "to_be_uploaded_prov")

    def prepare_folders(self) -> List[str]:
        completed = init_cache(self.cache_path)
        files_in_input_csv_dir = {
            filename
            for filename in os.listdir(self.input_csv_dir)
            if filename.endswith(".csv")
        }
        files_to_be_processed = sort_files(
            list(files_in_input_csv_dir.difference(completed))
        )
        pathoo(self.output_csv_dir)
        csv.field_size_limit(128)
        return files_to_be_processed

    def curate_and_create(
        self,
        filename: str,
        cache_path: str,
        errors_path: str,
        settings: dict | None = None,
        meta_config_path: str | None = None,
        progress=None,
    ) -> Tuple[dict, str, str, str]:
        try:
            with self.timer.timer("total_processing"):
                filepath = os.path.join(self.input_csv_dir, filename)
                console.print(filepath)
                data = get_csv_data(filepath)
                self.timer.record_metric("input_records", len(data))

                min_rows_parallel = settings.get("min_rows_parallel", 1000) if settings else 1000
                curator_obj = Curator(
                    data=data,
                    ts=self.triplestore_url,
                    prov_config=self.time_agnostic_library_config,
                    counter_handler=self.counter_handler,
                    base_iri=self.base_iri,
                    prefix=self.supplier_prefix,
                    valid_dois_cache=self.valid_dois_cache,
                    settings=settings,
                    silencer=self.silencer,
                    meta_config_path=meta_config_path,
                    timer=self.timer,
                    progress=progress,
                    min_rows_parallel=min_rows_parallel,
                )
                name = f"{filename.replace('.csv', '')}_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"
                curator_obj.curator(
                    filename=name, path_csv=self.output_csv_dir
                )
                self.timer.record_metric("curated_records", len(curator_obj.data))

                with self.timer.timer("rdf_creation"):
                    local_g_size = curator_obj.finder.triple_count()
                    self.timer.record_metric("local_g_triples", local_g_size)
                    preexisting_count = len(curator_obj.preexisting_entities)
                    self.timer.record_metric("preexisting_entities_count", preexisting_count)

                    RDF_BATCH_SIZE = 50_000
                    data = curator_obj.data
                    n_batches = (len(data) + RDF_BATCH_SIZE - 1) // RDF_BATCH_SIZE
                    total_entities = 0
                    total_modified = 0

                    for batch_idx in range(n_batches):
                        batch_start = batch_idx * RDF_BATCH_SIZE
                        batch_end = min(batch_start + RDF_BATCH_SIZE, len(data))
                        batch_data = data[batch_start:batch_end]

                        if n_batches > 1:
                            console.print(
                                f"  [dim]Batch {batch_idx + 1}/{n_batches} "
                                f"({len(batch_data)} records)[/dim]"
                            )

                        creator_obj = Creator(
                            data=batch_data,
                            finder=curator_obj.finder,
                            base_iri=self.base_iri,
                            counter_handler=self.counter_handler,
                            supplier_prefix=self.supplier_prefix,
                            resp_agent=self.resp_agent,
                            ra_index=curator_obj.index_id_ra,
                            br_index=curator_obj.index_id_br,
                            re_index_csv=curator_obj.re_index,
                            ar_index_csv=curator_obj.ar_index,
                            vi_index=curator_obj.VolIss,
                            silencer=self.silencer,
                            progress=progress,
                        )
                        creator = creator_obj.creator(source=self.source)
                        total_entities += len(creator.res_to_entity)

                        prov = ProvSet(
                            creator,
                            self.base_iri,
                            wanted_label=False,
                            supplier_prefix=self.supplier_prefix,
                            custom_counter_handler=self.counter_handler,
                        )
                        modified_entities = prov.generate_provenance()
                        total_modified += len(modified_entities)

                        repok = Reporter(print_sentences=False)
                        reperr = Reporter(print_sentences=True, prefix="[Storer: ERROR] ")
                        res_storer = Storer(
                            abstract_set=creator,
                            repok=repok,
                            reperr=reperr,
                            context_map={},
                            dir_split=self.dir_split_number,
                            n_file_item=self.items_per_file,
                            default_dir=self.default_dir,
                            output_format="json-ld",
                            zip_output=self.zip_output_rdf,
                            modified_entities=modified_entities,
                        )
                        prov_storer = Storer(
                            abstract_set=prov,
                            repok=repok,
                            reperr=reperr,
                            context_map={},
                            dir_split=self.dir_split_number,
                            n_file_item=self.items_per_file,
                            output_format="json-ld",
                            zip_output=self.zip_output_rdf,
                            modified_entities=modified_entities,
                        )
                        self.store_data_and_prov(res_storer, prov_storer)
                        del creator_obj, creator, prov, res_storer, prov_storer, modified_entities

                    self.timer.record_metric("entities_created", total_entities)
                    self.timer.record_metric("modified_entities", total_modified)

            return {"message": "success"}, cache_path, errors_path, filename
        except Exception as e:
            tb = traceback.format_exc()
            template = (
                "An exception of type {0} occurred. Arguments:\n{1!r}\nTraceback:\n{2}"
            )
            message = template.format(type(e).__name__, e.args, tb)
            return {"message": message}, cache_path, errors_path, filename

    def _setup_output_directories(self) -> None:
        """Create output directories for data and provenance."""
        os.makedirs(self.data_update_dir, exist_ok=True)
        os.makedirs(self.prov_update_dir, exist_ok=True)

    def _upload_sparql_queries(self) -> None:
        """Upload SPARQL queries to triplestores in parallel."""
        data_upload_folder = os.path.join(self.data_update_dir, "to_be_uploaded")
        prov_upload_folder = os.path.join(self.prov_update_dir, "to_be_uploaded")

        # Use forkserver to avoid deadlocks when forking from a multi-threaded process.
        # Libraries like Redis and rdflib create background threads, and fork() would
        # copy locked mutexes into the child process, causing hangs.
        ctx = multiprocessing.get_context('forkserver')

        data_process = ctx.Process(
            target=_upload_to_triplestore,
            args=(
                self.triplestore_url,
                data_upload_folder,
                self.redis_host,
                self.redis_port,
                self.redis_cache_db,
                self.ts_failed_queries,
                self.ts_stop_file,
                "Uploading data SPARQL"
            )
        )

        prov_process = ctx.Process(
            target=_upload_to_triplestore,
            args=(
                self.provenance_triplestore_url,
                prov_upload_folder,
                self.redis_host,
                self.redis_port,
                self.redis_cache_db,
                self.ts_failed_queries,
                self.ts_stop_file,
                "Uploading prov SPARQL"
            )
        )

        data_process.start()
        prov_process.start()

        data_process.join()
        prov_process.join()

        if data_process.exitcode != 0:
            raise RuntimeError(f"Data upload failed with exit code {data_process.exitcode}")
        if prov_process.exitcode != 0:
            raise RuntimeError(f"Provenance upload failed with exit code {prov_process.exitcode}")

    def store_data_and_prov(
        self, res_storer: Storer, prov_storer: Storer
    ) -> None:
        """Orchestrate storage and upload."""
        self._setup_output_directories()
        self._store_and_upload(res_storer, prov_storer, self.timer)

    def _store_and_upload(
        self, res_storer: Storer, prov_storer: Storer, timer: ProcessTimer
    ) -> None:
        """Store RDF files and upload queries to triplestore with parallel execution."""
        with timer.timer("storage"):
            # Use forkserver to avoid deadlocks when forking from a multi-threaded process.
            # Libraries like Redis and rdflib create background threads, and fork() would
            # copy locked mutexes into the child process, causing hangs.
            ctx = multiprocessing.get_context('forkserver')

            data_store_process = ctx.Process(
                target=_store_rdf_worker,
                args=(res_storer, self.output_rdf_dir, self.base_iri, self.context_path)
            )
            prov_store_process = ctx.Process(
                target=_store_rdf_worker,
                args=(prov_storer, self.output_rdf_dir, self.base_iri, self.context_path)
            )
            rdf_store_processes = [data_store_process, prov_store_process]
            for p in rdf_store_processes:
                p.start()

            if not self.rdf_files_only:
                data_query_process = ctx.Process(
                    target=_generate_queries_worker,
                    args=(res_storer, self.triplestore_url, self.data_update_dir)
                )
                prov_query_process = ctx.Process(
                    target=_generate_queries_worker,
                    args=(prov_storer, self.provenance_triplestore_url, self.prov_update_dir)
                )
                data_query_process.start()
                prov_query_process.start()
                data_query_process.join()
                prov_query_process.join()

                if data_query_process.exitcode != 0:
                    raise RuntimeError(f"Data query generation failed with exit code {data_query_process.exitcode}")
                if prov_query_process.exitcode != 0:
                    raise RuntimeError(f"Prov query generation failed with exit code {prov_query_process.exitcode}")

                self._upload_sparql_queries()

            for p in rdf_store_processes:
                p.join()
                if p.exitcode != 0:
                    raise RuntimeError(f"RDF storage failed with exit code {p.exitcode}")

    def run_sparql_updates(self, endpoint: str, folder: str):
        upload_sparql_updates(
            endpoint=endpoint,
            folder=folder,
            failed_file=self.ts_failed_queries,
            stop_file=self.ts_stop_file,
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_db=self.redis_cache_db,
        )


def _save_incremental_report(all_reports: List[Dict[str, Any]], meta_config_path: str, output_path: str) -> None:
    """Save incremental timing report to JSON file."""
    aggregate_report = {
        "timestamp": datetime.now().isoformat(),
        "config_path": meta_config_path,
        "total_files_processed": len(all_reports),
        "files": all_reports,
        "aggregate": _compute_aggregate_metrics(all_reports)
    }
    with open(output_path, 'wb') as f:
        f.write(orjson.dumps(aggregate_report, option=orjson.OPT_INDENT_2))


def _get_file_peak_memory(report: Dict[str, Any]) -> float:
    """Get peak memory (MB) across all phases in a file report."""
    phases = report["report"]["phases"]
    peaks = [p["peak_memory_mb"] for p in phases if p["peak_memory_mb"]]
    return max(peaks) if peaks else 0


def _compute_aggregate_metrics(all_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute aggregate statistics across all file reports."""
    if not all_reports:
        return {}

    total_duration = sum(r["report"]["metrics"].get("total_duration_seconds", 0) for r in all_reports)
    total_records = sum(r["report"]["metrics"].get("input_records", 0) for r in all_reports)
    total_entities = sum(r["report"]["metrics"].get("entities_created", 0) for r in all_reports)

    durations = [r["report"]["metrics"].get("total_duration_seconds", 0) for r in all_reports]
    throughputs = [r["report"]["metrics"].get("throughput_records_per_sec", 0) for r in all_reports]

    file_peaks = [_get_file_peak_memory(r) for r in all_reports]
    non_zero_peaks = [p for p in file_peaks if p]

    result: Dict[str, Any] = {
        "total_files": len(all_reports),
        "total_duration_seconds": round(total_duration, 3),
        "total_records_processed": total_records,
        "total_entities_created": total_entities,
        "average_time_per_file": round(total_duration / len(all_reports), 3) if all_reports else 0,
        "average_throughput": round(sum(throughputs) / len(throughputs), 2) if throughputs else 0,
        "min_time": round(min(durations), 3) if durations else 0,
        "max_time": round(max(durations), 3) if durations else 0,
        "overall_throughput": round(total_records / total_duration, 2) if total_duration > 0 else 0,
    }
    if non_zero_peaks:
        result["peak_memory_mb"] = round(max(non_zero_peaks), 1)
        result["average_peak_memory_mb"] = round(sum(non_zero_peaks) / len(non_zero_peaks), 1)
    return result


def _print_aggregate_summary(all_reports: List[Dict[str, Any]]) -> None:
    """Print aggregate summary of all processed files."""
    aggregate = _compute_aggregate_metrics(all_reports)

    console.print(f"\n{'='*60}")
    console.print("[bold]Aggregate Timing Summary[/bold]")
    console.print(f"{'='*60}")
    console.print(f"Total Files: {aggregate['total_files']}")
    console.print(f"Total Duration: {aggregate['total_duration_seconds']}s")
    console.print(f"Total Records: {aggregate['total_records_processed']}")
    console.print(f"Total Entities: {aggregate['total_entities_created']}")
    console.print(f"Average Time/File: {aggregate['average_time_per_file']}s")
    console.print(f"Min/Max Time: {aggregate['min_time']}s / {aggregate['max_time']}s")
    console.print(f"Overall Throughput: {aggregate['overall_throughput']} rec/s")
    if "peak_memory_mb" in aggregate:
        console.print(f"Peak Memory (RSS): {aggregate['peak_memory_mb']} MB")
        console.print(f"Avg Peak Memory:   {aggregate['average_peak_memory_mb']} MB")
    console.print(f"{'='*60}\n")


def run_meta_process(
    settings: dict, meta_config_path: str, enable_timing: bool = False, timing_output: Optional[str] = None
) -> None:
    is_unix = platform in {"linux", "linux2", "darwin"}
    all_reports = []

    meta_process_setup = MetaProcess(settings=settings, meta_config_path=meta_config_path)
    files_to_be_processed = meta_process_setup.prepare_folders()

    generate_gentle_buttons(meta_process_setup.base_output_dir, meta_config_path, is_unix)

    with create_progress() as progress:
        task_id = progress.add_task("Processing files", total=len(files_to_be_processed))
        for idx, filename in enumerate(files_to_be_processed, 1):
            try:
                if os.path.exists(os.path.join(meta_process_setup.base_output_dir, ".stop")):
                    console.print("\n[yellow]Stop file detected. Halting processing.[/yellow]")
                    break

                if enable_timing:
                    console.print(f"\n[cyan][{idx}/{len(files_to_be_processed)}][/cyan] Processing {filename}...")

                on_phase_cb = None
                if enable_timing and timing_output:
                    _chart = timing_output.replace('.json', '_chart.png')
                    _reports, _fn, _cfg, _out = all_reports, filename, meta_config_path, timing_output
                    def _on_phase(timer: ProcessTimer) -> None:
                        snapshot = _reports + [{"filename": _fn, "report": timer.get_report()}]
                        _save_incremental_report(snapshot, _cfg, _out)
                        plot_incremental_progress(snapshot, _chart)
                    on_phase_cb = _on_phase

                file_timer = ProcessTimer(enabled=enable_timing, verbose=enable_timing, on_phase_complete=on_phase_cb)
                meta_process_setup.timer = file_timer

                result = meta_process_setup.curate_and_create(
                    filename,
                    meta_process_setup.cache_path,
                    meta_process_setup.errors_path,
                    settings=settings,
                    meta_config_path=meta_config_path,
                    progress=progress if enable_timing else None,
                )
                task_done(result)

                if enable_timing:
                    report = file_timer.get_report()
                    all_reports.append({
                        "filename": filename,
                        "report": report
                    })
                    file_timer.print_file_summary(filename)

            except Exception as e:
                traceback_str = traceback.format_exc()
                console.print(
                    f"[red]Error processing file {filename}: {e}\nTraceback:\n{traceback_str}[/red]"
                )
            finally:
                progress.advance(task_id)

    if not os.path.exists(os.path.join(meta_process_setup.base_output_dir, ".stop")):
        if os.path.exists(meta_process_setup.cache_path):
            os.rename(
                meta_process_setup.cache_path,
                meta_process_setup.cache_path.replace(
                    ".txt", f'_{datetime.now().strftime("%Y-%m-%dT%H_%M_%S_%f")}.txt'
                ),
            )
        if is_unix:
            delete_lock_files(base_dir=meta_process_setup.base_output_dir)

    if enable_timing and all_reports:
        _print_aggregate_summary(all_reports)
        if timing_output:
            aggregate_report = {
                "timestamp": datetime.now().isoformat(),
                "config_path": meta_config_path,
                "total_files": len(all_reports),
                "files": all_reports,
                "aggregate": _compute_aggregate_metrics(all_reports)
            }
            with open(timing_output, 'wb') as f:
                f.write(orjson.dumps(aggregate_report, option=orjson.OPT_INDENT_2))
            console.print(f"[green][Timing] Report saved to {timing_output}[/green]")


def _cache_sort_key(filename: str) -> int:
    return int(filename.replace(".csv", ""))


def task_done(task_output: tuple) -> None:
    message, cache_path, errors_path, filename = task_output
    if message["message"] == "skip":
        pass
    elif message["message"] == "success":
        if not os.path.exists(cache_path):
            with open(cache_path, "w", encoding="utf-8") as aux_file:
                aux_file.write(filename + "\n")
        else:
            with open(cache_path, "r", encoding="utf-8") as aux_file:
                cache_data = aux_file.read().splitlines()
            try:
                bisect.insort(cache_data, filename, key=_cache_sort_key)
            except ValueError:
                # Non-numeric filename (e.g. "data.csv"): append without ordering
                cache_data.append(filename)
            with open(cache_path, "w", encoding="utf-8") as aux_file:
                aux_file.write("\n".join(cache_data))
    else:
        with open(errors_path, "a", encoding="utf-8") as aux_file:
            aux_file.write(f'{filename}: {message["message"]}' + "\n")


def chunks(lst: list, n: int) -> Iterator[list]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def delete_lock_files(base_dir: str) -> None:
    for dirpath, _, filenames in os.walk(base_dir):
        for filename in filenames:
            if filename.endswith(".lock"):
                os.remove(os.path.join(dirpath, filename))


def generate_gentle_buttons(dir: str, config: str, is_unix: bool):
    if os.path.exists(os.path.join(dir, ".stop")):
        os.remove(os.path.join(dir, ".stop"))
    ext = "sh" if is_unix else "bat"
    with open(f"gently_run.{ext}", "w") as rsh:
        rsh.write(
            f'{executable} -m oc_meta.lib.stopper -t "{dir}" --remove\n{executable} -m oc_meta.run.meta_process -c {config}'
        )
    with open(f"gently_stop.{ext}", "w") as rsh:
        rsh.write(f'{executable} -m oc_meta.lib.stopper -t "{dir}" --add')


if __name__ == "__main__":  # pragma: no cover
    arg_parser = ArgumentParser(
        "meta_process.py",
        description="This script runs the OCMeta data processing workflow",
        formatter_class=RichHelpFormatter,
    )
    arg_parser.add_argument(
        "-c",
        "--config",
        dest="config",
        required=True,
        help="Configuration file directory",
    )
    arg_parser.add_argument(
        "--timing",
        action="store_true",
        help="Enable timing metrics collection and display summary at the end",
    )
    arg_parser.add_argument(
        "--timing-output",
        dest="timing_output",
        default=None,
        help="Optional path to save timing report as JSON file",
    )
    args = arg_parser.parse_args()
    with open(args.config, encoding="utf-8") as file:
        settings = yaml.full_load(file)
    run_meta_process(
        settings=settings,
        meta_config_path=args.config,
        enable_timing=args.timing,
        timing_output=args.timing_output
    )
