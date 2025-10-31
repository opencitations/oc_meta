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
import os
import traceback
from argparse import ArgumentParser
from concurrent.futures import as_completed
from datetime import datetime
from itertools import cycle
from sys import executable, platform
from typing import Iterator, List, Tuple

import redis
import yaml
from oc_meta.core.creator import Creator
from oc_meta.core.curator import Curator
from oc_meta.lib.file_manager import (get_csv_data, init_cache, normalize_path,
                                      pathoo, sort_files)
from oc_meta.plugins.multiprocess.resp_agents_creator import RespAgentsCreator
from oc_meta.plugins.multiprocess.resp_agents_curator import RespAgentsCurator
from oc_meta.run.upload.on_triplestore import *
from oc_ocdm import Storer
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.prov import ProvSet
from oc_ocdm.support.reporter import Reporter
from pebble import ProcessPool
from time_agnostic_library.support import generate_config_file
from tqdm import tqdm


class MetaProcess:
    def __init__(self, settings: dict, meta_config_path: str):
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
        self.workers_number = int(settings["workers_number"])
        supplier_prefix: str = settings["supplier_prefix"]
        self.supplier_prefix = (
            supplier_prefix[:-1] if supplier_prefix.endswith("0") else supplier_prefix
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
        worker_number: int = None,
        resp_agents_only: bool = False,
        settings: str | None = None,
        meta_config_path: str = None,
    ) -> Tuple[dict, str, str, str]:
        if os.path.exists(os.path.join(self.base_output_dir, ".stop")):
            return {"message": "skip"}, cache_path, errors_path, filename
        try:
            filepath = os.path.join(self.input_csv_dir, filename)
            print(filepath)
            data = get_csv_data(filepath)
            supplier_prefix = (
                f"{self.supplier_prefix}0"
                if worker_number is None
                else f"{self.supplier_prefix}{str(worker_number)}0"
            )
            # Curator
            self.info_dir = os.path.join(self.info_dir, supplier_prefix)
            if resp_agents_only:
                curator_obj = RespAgentsCurator(
                    data=data,
                    ts=self.triplestore_url,
                    prov_config=self.time_agnostic_library_config,
                    counter_handler=self.counter_handler,
                    base_iri=self.base_iri,
                    prefix=supplier_prefix,
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
                    prefix=supplier_prefix,
                    valid_dois_cache=self.valid_dois_cache,
                    settings=settings,
                    silencer=self.silencer,
                    meta_config_path=meta_config_path,
                )
            name = f"{filename.replace('.csv', '')}_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"
            curator_obj.curator(
                filename=name, path_csv=self.output_csv_dir, path_index=self.indexes_dir
            )
            # Creator
            if resp_agents_only:
                creator_obj = RespAgentsCreator(
                    data=curator_obj.data,
                    endpoint=self.triplestore_url,
                    base_iri=self.base_iri,
                    counter_handler=self.counter_handler,
                    supplier_prefix=supplier_prefix,
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
                    endpoint=self.triplestore_url,
                    base_iri=self.base_iri,
                    counter_handler=self.counter_handler,
                    supplier_prefix=supplier_prefix,
                    resp_agent=self.resp_agent,
                    ra_index=curator_obj.index_id_ra,
                    br_index=curator_obj.index_id_br,
                    re_index_csv=curator_obj.re_index,
                    ar_index_csv=curator_obj.ar_index,
                    vi_index=curator_obj.VolIss,
                    preexisting_entities=curator_obj.preexisting_entities,
                    everything_everywhere_allatonce=curator_obj.everything_everywhere_allatonce,
                    settings=settings,
                    meta_config_path=meta_config_path,
                )
            creator = creator_obj.creator(source=self.source)
            # Provenance
            prov = ProvSet(
                creator,
                self.base_iri,
                wanted_label=False,
                supplier_prefix=supplier_prefix,
                custom_counter_handler=self.counter_handler,
            )
            modified_entities = prov.generate_provenance()
            # Storer
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
            # with suppress_stdout():
            self.store_data_and_prov(res_storer, prov_storer)
            return {"message": "success"}, cache_path, errors_path, filename
        except Exception as e:
            tb = traceback.format_exc()
            template = (
                "An exception of type {0} occurred. Arguments:\n{1!r}\nTraceback:\n{2}"
            )
            message = template.format(type(e).__name__, e.args, tb)
            return {"message": message}, cache_path, errors_path, filename

    def store_data_and_prov(
        self, res_storer: Storer, prov_storer: Storer
    ) -> None:
        os.makedirs(self.data_update_dir, exist_ok=True)
        os.makedirs(self.prov_update_dir, exist_ok=True)

        if self.generate_rdf_files:
            res_storer.store_all(
                base_dir=self.output_rdf_dir,
                base_iri=self.base_iri,
                context_path=self.context_path,
                process_id=None,
            )
            prov_storer.store_all(
                self.output_rdf_dir,
                self.base_iri,
                self.context_path,
                process_id=None
            )

        res_storer.upload_all(
            triplestore_url=self.triplestore_url,
            base_dir=self.data_update_dir,
            batch_size=10,
            save_queries=True
        )

        prov_storer.upload_all(
            triplestore_url=self.provenance_triplestore_url,
            base_dir=self.prov_update_dir,
            batch_size=10,
            save_queries=True
        )

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


def run_meta_process(
    settings: dict, meta_config_path: str, resp_agents_only: bool = False
) -> None:
    meta_process = MetaProcess(settings=settings, meta_config_path=meta_config_path)
    is_unix = platform in {"linux", "linux2", "darwin"}
    # delete_lock_files(base_dir=meta_process.base_output_dir)
    files_to_be_processed = meta_process.prepare_folders()
    max_workers = meta_process.workers_number
    if max_workers == 0:
        workers = list(range(1, os.cpu_count()))
    elif max_workers == 1:
        workers = [None]
    else:
        tens = int(str(max_workers)[:-1]) if max_workers >= 10 else 0
        multiples_of_ten = {
            i for i in range(1, max_workers + tens + 1) if int(i) % 10 == 0
        }
        workers = [
            i
            for i in range(1, max_workers + len(multiples_of_ten) + 1)
            if i not in multiples_of_ten
        ]
    generate_gentle_buttons(meta_process.base_output_dir, meta_config_path, is_unix)
    with ProcessPool(max_workers=max_workers, max_tasks=1) as executor, tqdm(
        total=len(files_to_be_processed), desc="Processing files"
    ) as progress_bar:
        futures = [
            executor.schedule(
                curate_and_create_wrapper,
                args=(file, worker, resp_agents_only, settings, meta_config_path),
            )
            for file, worker in zip(files_to_be_processed, cycle(workers))
        ]
        for future in as_completed(futures):
            try:
                result = future.result()
                task_done(result)  # Gestisci il risultato del task
            except Exception as e:
                # Gestisci l'eccezione per il task che ha sollevato un'errore
                traceback_str = traceback.format_exc()
                print(
                    f"Errore durante l'elaborazione: {e}\nTraceback:\n{traceback_str}"
                )
            finally:
                progress_bar.update(1)

    if not os.path.exists(os.path.join(meta_process.base_output_dir, ".stop")):
        if os.path.exists(meta_process.cache_path):
            os.rename(
                meta_process.cache_path,
                meta_process.cache_path.replace(
                    ".txt", f'_{datetime.now().strftime("%Y-%m-%dT%H_%M_%S_%f")}.txt'
                ),
            )
        if is_unix:
            delete_lock_files(base_dir=meta_process.base_output_dir)

    # Run SPARQL updates for the main triplestore
    meta_process.run_sparql_updates(
        endpoint=settings["triplestore_url"],
        folder=os.path.join(meta_process.data_update_dir, "to_be_uploaded"),
    )
    
    # Run SPARQL updates for the provenance triplestore
    meta_process.run_sparql_updates(
        endpoint=settings["provenance_triplestore_url"],
        folder=os.path.join(meta_process.prov_update_dir, "to_be_uploaded"),
    )


def curate_and_create_wrapper(
    file_to_be_processed, worker_number, resp_agents_only, settings, meta_config_path
):
    meta_process = MetaProcess(settings=settings, meta_config_path=meta_config_path)
    return meta_process.curate_and_create(
        file_to_be_processed,
        meta_process.cache_path,
        meta_process.errors_path,
        worker_number,
        resp_agents_only,
        settings,
        meta_config_path,
    )


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
    args = arg_parser.parse_args()
    with open(args.config, encoding="utf-8") as file:
        settings = yaml.full_load(file)
    run_meta_process(
        settings=settings, meta_config_path=args.config, resp_agents_only=False
    )
