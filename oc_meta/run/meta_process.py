#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021 Simone Persiani <iosonopersia@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from __future__ import annotations

import csv
import glob
import json
import os
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime
from sys import executable, platform
from typing import Any, Dict, Iterator, List, Optional, Tuple

import redis
import yaml
from oc_meta.core.creator import Creator
from oc_meta.core.curator import Curator
from oc_meta.lib.file_manager import (get_csv_data, init_cache, normalize_path,
                                      pathoo, sort_files)
from oc_meta.lib.timer import ProcessTimer
from oc_meta.plugins.multiprocess.resp_agents_creator import RespAgentsCreator
from oc_meta.plugins.multiprocess.resp_agents_curator import RespAgentsCurator
from oc_meta.run.benchmark.plotting import plot_incremental_progress
from oc_meta.run.upload.on_triplestore import *
from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.prov import ProvSet
from oc_ocdm.support.reporter import Reporter
from time_agnostic_library.support import generate_config_file
from tqdm import tqdm
from virtuoso_utilities.bulk_load import bulk_load


def _store_rdf_process(args: tuple) -> None:
    """
    Worker function to execute storer.store_all() in a separate process.
    """
    storer, output_dir, base_iri, context_path = args

    storer.store_all(
        base_dir=output_dir,
        base_iri=base_iri,
        context_path=context_path,
        process_id=None
    )


def _upload_queries_process(args: tuple) -> None:
    """
    Worker function to execute storer.upload_all() in a separate process.
    """
    storer, triplestore_url, update_dir = args

    storer.upload_all(
        triplestore_url=triplestore_url,
        base_dir=update_dir,
        batch_size=10,
        save_queries=True
    )


def _upload_to_triplestore(endpoint: str, folder: str, redis_host: str, redis_port: int, redis_db: int, cache_file: str, failed_file: str, stop_file: str) -> None:
    """Upload SPARQL queries from folder to triplestore endpoint."""
    cache_manager = CacheManager(
        cache_file,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db
    )

    upload_sparql_updates(
        endpoint=endpoint,
        folder=folder,
        batch_size=10,
        cache_file=None,
        failed_file=failed_file,
        stop_file=stop_file,
        cache_manager=cache_manager,
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
        self.indexes_dir = os.path.join(self.base_output_dir, "indexes")
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
        self.valid_dois_cache = (
            dict() if bool(settings["use_doi_api_service"]) == True else None
        )
        supplier_prefix: str = settings["supplier_prefix"]
        self.supplier_prefix = (
            supplier_prefix if supplier_prefix.endswith("0") else f"{supplier_prefix}0"
        )
        self.silencer = settings["silencer"]
        self.generate_rdf_files = settings.get("generate_rdf_files", True)
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
                cache_endpoint=settings["cache_endpoint"],
                cache_update_endpoint=settings["cache_update_endpoint"],
            )

        # Redis settings
        self.redis_host = settings.get("redis_host", "localhost")
        self.redis_port = settings.get("redis_port", 6379)
        self.redis_db = settings.get("redis_db", 5)
        self.redis_cache_db = settings.get("redis_cache_db", 2)
        self.redis_client = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )

        self.counter_handler = RedisCounterHandler(
            host=self.redis_host, port=self.redis_port, db=self.redis_db
        )

        # Triplestore upload settings
        self.ts_upload_cache = settings.get("ts_upload_cache", "ts_upload_cache.json")
        self.ts_failed_queries = settings.get("ts_failed_queries", "failed_queries.txt")
        self.ts_stop_file = settings.get("ts_stop_file", ".stop_upload")

        self.data_update_dir = os.path.join(self.base_output_dir, "to_be_uploaded_data")
        self.prov_update_dir = os.path.join(self.base_output_dir, "to_be_uploaded_prov")

        self.cache_manager = CacheManager(
            json_cache_file=self.ts_upload_cache,
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_db=self.redis_cache_db,
        )

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
        for dir in [self.output_csv_dir, self.indexes_dir]:
            pathoo(dir)
        csv.field_size_limit(128)
        return files_to_be_processed

    def curate_and_create(
        self,
        filename: str,
        cache_path: str,
        errors_path: str,
        resp_agents_only: bool = False,
        settings: str | None = None,
        meta_config_path: str = None,
    ) -> Tuple[dict, str, str, str]:
        try:
            with self.timer.timer("total_processing"):
                filepath = os.path.join(self.input_csv_dir, filename)
                print(filepath)
                data = get_csv_data(filepath)
                self.timer.record_metric("input_records", len(data))

                with self.timer.timer("curation"):
                    self.info_dir = os.path.join(self.info_dir, self.supplier_prefix)
                    if resp_agents_only:
                        curator_obj = RespAgentsCurator(
                            data=data,
                            ts=self.triplestore_url,
                            prov_config=self.time_agnostic_library_config,
                            counter_handler=self.counter_handler,
                            base_iri=self.base_iri,
                            prefix=self.supplier_prefix,
                            settings=settings,
                            meta_config_path=meta_config_path,
                        )
                    else:
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
                        )
                    name = f"{filename.replace('.csv', '')}_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"
                    curator_obj.curator(
                        filename=name, path_csv=self.output_csv_dir, path_index=self.indexes_dir
                    )
                    self.timer.record_metric("curated_records", len(curator_obj.data))

                with self.timer.timer("rdf_creation"):
                    if not resp_agents_only:
                        local_g_size = len(curator_obj.everything_everywhere_allatonce)
                        self.timer.record_metric("local_g_triples", local_g_size)
                        preexisting_count = len(curator_obj.preexisting_entities)
                        self.timer.record_metric("preexisting_entities_count", preexisting_count)

                    with self.timer.timer("creator_execution"):
                        if resp_agents_only:
                            creator_obj = RespAgentsCreator(
                                data=curator_obj.data,
                                endpoint=self.triplestore_url,
                                base_iri=self.base_iri,
                                counter_handler=self.counter_handler,
                                supplier_prefix=self.supplier_prefix,
                                resp_agent=self.resp_agent,
                                ra_index=curator_obj.index_id_ra,
                                preexisting_entities=curator_obj.preexisting_entities,
                                everything_everywhere_allatonce=curator_obj.everything_everywhere_allatonce,
                                settings=settings,
                                meta_config_path=meta_config_path,
                            )
                        else:
                            creator_obj = Creator(
                                data=curator_obj.data,
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
                                silencer=settings.get("silencer", []),
                            )
                        creator = creator_obj.creator(source=self.source)
                        self.timer.record_metric("entities_created", len(creator.res_to_entity))

                    with self.timer.timer("provenance_generation"):
                        prov = ProvSet(
                            creator,
                            self.base_iri,
                            wanted_label=False,
                            supplier_prefix=self.supplier_prefix,
                            custom_counter_handler=self.counter_handler,
                        )
                        modified_entities = prov.generate_provenance()
                        self.timer.record_metric("modified_entities", len(modified_entities))

                with self.timer.timer("storage_and_upload"):
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

    def _prepare_store_rdf_tasks(self, res_storer: Storer, prov_storer: Storer) -> list:
        """Prepare RDF storage tasks if RDF file generation is enabled."""
        store_rdf_tasks = []
        if self.generate_rdf_files:
            store_rdf_tasks.append((
                res_storer,
                self.output_rdf_dir,
                self.base_iri,
                self.context_path
            ))
            store_rdf_tasks.append((
                prov_storer,
                self.output_rdf_dir,
                self.base_iri,
                self.context_path
            ))
        return store_rdf_tasks

    def _prepare_upload_queries_tasks(self, res_storer: Storer, prov_storer: Storer) -> list:
        """Prepare SPARQL query generation tasks."""
        return [
            (res_storer, self.triplestore_url, self.data_update_dir),
            (prov_storer, self.provenance_triplestore_url, self.prov_update_dir)
        ]

    def _upload_sparql_queries_parallel(self) -> None:
        """Upload SPARQL queries to triplestores in parallel."""
        data_upload_folder = os.path.join(self.data_update_dir, "to_be_uploaded")
        prov_upload_folder = os.path.join(self.prov_update_dir, "to_be_uploaded")

        with ProcessPoolExecutor(max_workers=2) as executor:
            upload_data_future = executor.submit(
                _upload_to_triplestore,
                self.triplestore_url,
                data_upload_folder,
                self.redis_host,
                self.redis_port,
                self.redis_cache_db,
                self.ts_upload_cache,
                self.ts_failed_queries,
                self.ts_stop_file
            )
            upload_prov_future = executor.submit(
                _upload_to_triplestore,
                self.provenance_triplestore_url,
                prov_upload_folder,
                self.redis_host,
                self.redis_port,
                self.redis_cache_db,
                self.ts_upload_cache,
                self.ts_failed_queries,
                self.ts_stop_file
            )
            upload_data_future.result()
            upload_prov_future.result()

    def store_data_and_prov(
        self, res_storer: Storer, prov_storer: Storer
    ) -> None:
        """Orchestrate storage and upload using appropriate strategy."""
        self._setup_output_directories()

        bulk_config = self.settings.get("virtuoso_bulk_load", {})
        if bulk_config.get("enabled", False):
            self._store_bulk_load(res_storer, prov_storer, bulk_config)
        else:
            self._store_standard(res_storer, prov_storer)

    def _store_standard(
        self, res_storer: Storer, prov_storer: Storer
    ) -> None:
        """Standard upload path using SPARQL protocol."""
        store_rdf_tasks = self._prepare_store_rdf_tasks(res_storer, prov_storer)
        upload_queries_tasks = self._prepare_upload_queries_tasks(res_storer, prov_storer)

        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = []
            for task in store_rdf_tasks:
                futures.append(executor.submit(_store_rdf_process, task))
            for task in upload_queries_tasks:
                futures.append(executor.submit(_upload_queries_process, task))
            for future in futures:
                future.result()

        self._upload_sparql_queries_parallel()

    def _store_bulk_load(
        self, res_storer: Storer, prov_storer: Storer, bulk_config: dict
    ) -> None:
        """
        Alternative upload path using Virtuoso bulk loading for INSERT queries.

        Flow:
        1. Generate separated queries (INSERTs as .nq.gz, DELETEs as .sparql) + RDF files in parallel
        2. Execute DELETE queries via SPARQL protocol
        3. Bulk load INSERT nquads to data and provenance containers in parallel
        """
        data_container = bulk_config["data_container"]
        prov_container = bulk_config["prov_container"]
        bulk_load_dir = bulk_config["bulk_load_dir"]
        data_mount_dir = bulk_config["data_mount_dir"]
        prov_mount_dir = bulk_config["prov_mount_dir"]

        data_nquads_dir = os.path.abspath(data_mount_dir)
        prov_nquads_dir = os.path.abspath(prov_mount_dir)
        os.makedirs(data_nquads_dir, exist_ok=True)
        os.makedirs(prov_nquads_dir, exist_ok=True)

        store_rdf_tasks = self._prepare_store_rdf_tasks(res_storer, prov_storer)

        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = []

            # Nquads generation (data + prov)
            futures.append(executor.submit(
                res_storer.upload_all,
                triplestore_url=self.triplestore_url,
                base_dir=self.data_update_dir,
                batch_size=10,
                prepare_bulk_load=True,
                bulk_load_dir=data_nquads_dir
            ))
            futures.append(executor.submit(
                prov_storer.upload_all,
                triplestore_url=self.provenance_triplestore_url,
                base_dir=self.prov_update_dir,
                batch_size=10,
                prepare_bulk_load=True,
                bulk_load_dir=prov_nquads_dir
            ))

            for task in store_rdf_tasks:
                futures.append(executor.submit(_store_rdf_process, task))

            for future in futures:
                future.result()

        self._upload_sparql_queries_parallel()

        self._run_virtuoso_bulk_load(
            container_name=data_container,
            bulk_load_dir=bulk_load_dir,
            nquads_host_dir=data_nquads_dir
        )
        self._run_virtuoso_bulk_load(
            container_name=prov_container,
            bulk_load_dir=bulk_load_dir,
            nquads_host_dir=prov_nquads_dir
        )

    def _run_virtuoso_bulk_load(
        self, container_name: str, bulk_load_dir: str, nquads_host_dir: str
    ) -> None:
        """
        Runs Virtuoso bulk loading using mounted volumes.

        Args:
            container_name: Docker container name (used for ISQL commands)
            bulk_load_dir: Path INSIDE container where files are mounted
            nquads_host_dir: Host directory mounted as volume (contains .nq.gz files)
        """
        nquads_files = glob.glob(os.path.join(nquads_host_dir, "*.nq.gz"))
        if not nquads_files:
            return

        virtuoso_password = os.environ.get("VIRTUOSO_PASSWORD", "dba")

        print(f"Running bulk load for {len(nquads_files)} files from {nquads_host_dir} (container: {container_name})")
        bulk_load(
            data_directory=nquads_host_dir,
            password=virtuoso_password,
            docker_container=container_name,
            container_data_directory=bulk_load_dir,
            log_level="WARNING"
        )
        print(f"Bulk load completed successfully for {container_name}")

    def run_sparql_updates(self, endpoint: str, folder: str, batch_size: int = 10):
        cache_manager = CacheManager(
            json_cache_file=self.ts_upload_cache,
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_db=self.redis_cache_db,
        )
        upload_sparql_updates(
            endpoint=endpoint,
            folder=folder,
            batch_size=batch_size,
            cache_file=self.ts_upload_cache,
            failed_file=self.ts_failed_queries,
            stop_file=self.ts_stop_file,
            cache_manager=cache_manager,
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
    with open(output_path, 'w') as f:
        json.dump(aggregate_report, f, indent=2)


def _compute_aggregate_metrics(all_reports: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute aggregate statistics across all file reports."""
    if not all_reports:
        return {}

    total_duration = sum(r["report"]["metrics"].get("total_duration_seconds", 0) for r in all_reports)
    total_records = sum(r["report"]["metrics"].get("input_records", 0) for r in all_reports)
    total_entities = sum(r["report"]["metrics"].get("entities_created", 0) for r in all_reports)

    durations = [r["report"]["metrics"].get("total_duration_seconds", 0) for r in all_reports]
    throughputs = [r["report"]["metrics"].get("throughput_records_per_sec", 0) for r in all_reports]

    return {
        "total_files": len(all_reports),
        "total_duration_seconds": round(total_duration, 3),
        "total_records_processed": total_records,
        "total_entities_created": total_entities,
        "average_time_per_file": round(total_duration / len(all_reports), 3) if all_reports else 0,
        "average_throughput": round(sum(throughputs) / len(throughputs), 2) if throughputs else 0,
        "min_time": round(min(durations), 3) if durations else 0,
        "max_time": round(max(durations), 3) if durations else 0,
        "overall_throughput": round(total_records / total_duration, 2) if total_duration > 0 else 0
    }


def _print_aggregate_summary(all_reports: List[Dict[str, Any]]) -> None:
    """Print aggregate summary of all processed files."""
    aggregate = _compute_aggregate_metrics(all_reports)

    print(f"\n{'='*60}")
    print("Aggregate Timing Summary")
    print(f"{'='*60}")
    print(f"Total Files: {aggregate['total_files']}")
    print(f"Total Duration: {aggregate['total_duration_seconds']}s")
    print(f"Total Records: {aggregate['total_records_processed']}")
    print(f"Total Entities: {aggregate['total_entities_created']}")
    print(f"Average Time/File: {aggregate['average_time_per_file']}s")
    print(f"Min/Max Time: {aggregate['min_time']}s / {aggregate['max_time']}s")
    print(f"Overall Throughput: {aggregate['overall_throughput']} rec/s")
    print(f"{'='*60}\n")


def run_meta_process(
    settings: dict, meta_config_path: str, resp_agents_only: bool = False, enable_timing: bool = False, timing_output: Optional[str] = None
) -> None:
    is_unix = platform in {"linux", "linux2", "darwin"}
    all_reports = []

    meta_process_setup = MetaProcess(settings=settings, meta_config_path=meta_config_path)
    files_to_be_processed = meta_process_setup.prepare_folders()

    generate_gentle_buttons(meta_process_setup.base_output_dir, meta_config_path, is_unix)

    with tqdm(total=len(files_to_be_processed), desc="Processing files") as progress_bar:
        for idx, filename in enumerate(files_to_be_processed, 1):
            try:
                if os.path.exists(os.path.join(meta_process_setup.base_output_dir, ".stop")):
                    print(f"\nStop file detected. Halting processing.")
                    break

                if enable_timing:
                    print(f"\n[{idx}/{len(files_to_be_processed)}] Processing {filename}...")

                file_timer = ProcessTimer(enabled=enable_timing, verbose=enable_timing)
                meta_process = MetaProcess(settings=settings, meta_config_path=meta_config_path, timer=file_timer)

                result = meta_process.curate_and_create(
                    filename,
                    meta_process.cache_path,
                    meta_process.errors_path,
                    resp_agents_only=resp_agents_only,
                    settings=settings,
                    meta_config_path=meta_config_path
                )
                task_done(result)

                if enable_timing:
                    report = file_timer.get_report()
                    all_reports.append({
                        "filename": filename,
                        "report": report
                    })

                    file_timer.print_file_summary(filename)

                    if timing_output:
                        _save_incremental_report(all_reports, meta_config_path, timing_output)
                        print(f"\n  JSON updated: {timing_output}")

                    chart_file = timing_output.replace('.json', '_chart.png') if timing_output else 'meta_process_timing_chart.png'
                    plot_incremental_progress(all_reports, chart_file)
                    print(f"  Chart updated: {chart_file}\n")

            except Exception as e:
                traceback_str = traceback.format_exc()
                print(
                    f"Error processing file {filename}: {e}\nTraceback:\n{traceback_str}"
                )
            finally:
                progress_bar.update(1)

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
            with open(timing_output, 'w') as f:
                json.dump(aggregate_report, f, indent=2)
            print(f"[Timing] Report saved to {timing_output}")


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
                cache_data.append(filename)
                try:
                    data_sorted = sorted(
                        cache_data,
                        key=lambda filename: int(filename.replace(".csv", "")),
                        reverse=False,
                    )
                except ValueError:
                    data_sorted = cache_data
            with open(cache_path, "w", encoding="utf-8") as aux_file:
                aux_file.write("\n".join(data_sorted))
    else:
        with open(errors_path, "a", encoding="utf-8") as aux_file:
            aux_file.write(f'{filename}: {message["message"]}' + "\n")


def chunks(lst: list, n: int) -> Iterator[list]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def delete_lock_files(base_dir: list) -> None:
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
        resp_agents_only=False,
        enable_timing=args.timing,
        timing_output=args.timing_output
    )
