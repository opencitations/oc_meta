#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2025, Arcangelo Massari <arcangelo.massari@unibo.it>
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

"""
Download and organize full dumps from OpenCitations Meta Virtuoso endpoints.

This script downloads complete RDF dumps from two Virtuoso endpoints:
1. Main data endpoint - containing bibliographic and entity data
2. Provenance endpoint - containing provenance information

After downloading, the script automatically converts the dumps into an organized
directory structure for better file indexing.

The script uses the virtuoso_utilities package to perform efficient dumps
and saves the output as both compressed N-Quads files and organized JSON-LD files.
"""

import argparse
import gzip
import logging
import multiprocessing
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from rdflib import ConjunctiveGraph, URIRef
from tqdm import tqdm
from virtuoso_utilities.dump_quadstore import dump_quadstore

from oc_meta.run.gen_rdf_from_export import (find_paths,
                                             store_in_file,
                                             merge_all_files_parallel,
                                             generate_unique_id)

BASE_IRI = 'https://w3id.org/oc/meta/'
DIR_SPLIT = 10000
N_FILE_ITEM = 1000
ZIP_OUTPUT = True


def setup_logging(output_dir: Path, verbose: bool = False) -> None:
    """
    Setup logging configuration.

    Args:
        output_dir: Directory where log file will be created
        verbose: If True, set logging level to DEBUG
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_file = output_dir / f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def convert_dumps_to_organized_structure(
    input_dir: Path,
    output_dir: Path,
    num_processes: int = None,
    batch_size: int = 50
) -> None:
    """
    Convert downloaded N-Quads dumps to organized directory structure using batch processing.

    Args:
        input_dir: Directory containing .nq.gz dump files
        output_dir: Directory for organized output
        num_processes: Number of parallel processes (None for auto)
        batch_size: Number of dump files to process per batch
    """
    logging.info(f"Converting dumps from {input_dir} to organized structure in {output_dir}")

    dump_files = list(input_dir.glob("*.nq.gz"))
    if not dump_files:
        logging.warning(f"No .nq.gz files found in {input_dir}")
        return

    logging.info(f"Found {len(dump_files)} dump files to process in batches of {batch_size}")

    # Process files in batches to avoid inode exhaustion
    total_batches = (len(dump_files) + batch_size - 1) // batch_size
    stop_file = str(output_dir / ".stop")

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(dump_files))
        batch_files = dump_files[start_idx:end_idx]

        logging.info(f"Processing batch {batch_idx + 1}/{total_batches} "
                    f"({len(batch_files)} files: {start_idx}-{end_idx-1})")

        with multiprocessing.Pool(processes=num_processes) as pool:
            args_list = [
                (dump_file, output_dir, BASE_IRI, DIR_SPLIT, N_FILE_ITEM, ZIP_OUTPUT)
                for dump_file in batch_files
            ]

            list(tqdm(
                pool.imap(process_dump_file, args_list),
                total=len(batch_files),
                desc=f"Batch {batch_idx + 1}/{total_batches}"
            ))

        logging.info(f"Merging files after batch {batch_idx + 1}/{total_batches}")
        merge_all_files_parallel(str(output_dir), ZIP_OUTPUT, stop_file)

        logging.info(f"Completed batch {batch_idx + 1}/{total_batches}")

    logging.info("Conversion completed")

def process_dump_file(args: tuple) -> None:
    """
    Process a single dump file and organize its contents.

    Args:
        args: Tuple of (dump_file, output_dir, base_iri, dir_split, n_file_item, zip_output)
    """
    dump_file, output_dir, base_iri, dir_split, n_file_item, zip_output = args
    unique_id = generate_unique_id()

    try:
        with gzip.open(dump_file, 'rt', encoding='utf-8') as f:
            graph = ConjunctiveGraph()
            graph.parse(f, format='nquads')

        files_data = {}

        for context in graph.contexts():
            for s, p, o in context:
                if isinstance(s, URIRef):
                    dir_path, file_path = find_paths(
                        s, str(output_dir) + os.sep, base_iri,
                        '_', dir_split, n_file_item, is_json=True
                    )

                    if file_path:
                        base_name = os.path.splitext(os.path.basename(file_path))[0]
                        new_file_name = f"{base_name}_{unique_id}"
                        file_path = os.path.join(os.path.dirname(file_path),
                                                new_file_name + ('.zip' if zip_output else '.json'))

                        if file_path not in files_data:
                            files_data[file_path] = ConjunctiveGraph()

                        files_data[file_path].add((s, p, o, context.identifier))

        for file_path, file_graph in files_data.items():
            dir_path = os.path.dirname(file_path)
            os.makedirs(dir_path, exist_ok=True)

            if zip_output and not file_path.endswith('.zip'):
                file_path = file_path.replace('.json', '.zip')

            store_in_file(file_graph, file_path, zip_output)

        logging.debug(f"Completed processing {dump_file}")

    except Exception as e:
        logging.error(f"Error processing {dump_file}: {e}")

def create_output_directories(base_output_dir: Path) -> Tuple[Path, Path]:
    """
    Create or get output directories for data and provenance dumps.
    Uses fixed names to allow resuming/skipping if already exists.

    Args:
        base_output_dir: Base directory for all outputs

    Returns:
        Tuple of (data_dir, provenance_dir)
    """
    data_dir = base_output_dir / "data_dump"
    prov_dir = base_output_dir / "provenance_dump"

    data_dir.mkdir(parents=True, exist_ok=True)
    prov_dir.mkdir(parents=True, exist_ok=True)

    return data_dir, prov_dir

def download_dump(
    host: str,
    port: int,
    user: str,
    password: str,
    output_dir: Path,
    file_length_limit: int,
    docker_container: Optional[str] = None,
    endpoint_name: str = "endpoint"
) -> bool:
    """
    Download dump from Virtuoso endpoint.

    Args:
        host: Virtuoso server hostname
        port: Virtuoso server port
        user: Database username
        password: Database password
        output_dir: Directory to save dump files
        file_length_limit: Maximum file size in bytes
        docker_container: Optional Docker container name
        endpoint_name: Name of the endpoint (for logging)

    Returns:
        True if successful, False otherwise
    """
    logging.info(f"Starting dump from {endpoint_name} ({host}:{port})")

    try:
        args = argparse.Namespace()
        args.host = host
        args.port = port
        args.user = user
        args.password = password
        args.output_dir = str(output_dir)
        args.file_length_limit = file_length_limit
        args.compression = True
        args.docker_container = docker_container

        args.isql_path = "isql"
        args.docker_isql_path = "isql"
        args.docker_path = "docker"

        success = dump_quadstore(args)

        if success:
            logging.info(f"Successfully completed dump from {endpoint_name}")
        else:
            logging.error(f"dump_quadstore returned False for {endpoint_name}")

        return success

    except Exception as e:
        logging.error(f"Error dumping from {endpoint_name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Download full dumps from OpenCitations Meta Virtuoso endpoints"
    )

    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("./virtuoso_dumps"),
        help="Base directory for output dumps (default: ./virtuoso_dumps)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--file-length-limit",
        type=int,
        default=100_000_000,  # 100MB
        help="Maximum file size in bytes (default: 100MB)"
    )
    parser.add_argument(
        "--data-host",
        default="localhost",
        help="Data endpoint hostname (default: localhost)"
    )
    parser.add_argument(
        "--data-port",
        type=int,
        default=1111,
        help="Data endpoint port (default: 1111)"
    )
    parser.add_argument(
        "--data-user",
        default="dba",
        help="Data endpoint username (default: dba)"
    )
    parser.add_argument(
        "--data-password",
        default="dba",
        help="Data endpoint password (required)"
    )
    parser.add_argument(
        "--data-docker",
        help="Docker container name for data endpoint"
    )
    parser.add_argument(
        "--prov-host",
        default="localhost",
        help="Provenance endpoint hostname (default: localhost)"
    )
    parser.add_argument(
        "--prov-port",
        type=int,
        default=1111,
        help="Provenance endpoint port (default: 1111)"
    )
    parser.add_argument(
        "--prov-user",
        default="dba",
        help="Provenance endpoint username (default: dba)"
    )
    parser.add_argument(
        "--prov-password",
        default="dba",
        help="Provenance endpoint password (required)"
    )
    parser.add_argument(
        "--prov-docker",
        help="Docker container name for provenance endpoint"
    )
    parser.add_argument(
        "--skip-data",
        action="store_true",
        help="Skip downloading data dump"
    )
    parser.add_argument(
        "--skip-provenance",
        action="store_true",
        help="Skip downloading provenance dump"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if dumps already exist"
    )

    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.output_dir, args.verbose)

    logging.info("=" * 60)
    logging.info("OpenCitations Meta Virtuoso Dump Downloader")
    logging.info("=" * 60)

    data_dir, prov_dir = create_output_directories(args.output_dir)

    success = True

    if not args.skip_data:
        existing_data_files = list(data_dir.glob("*.nq.gz"))
        if existing_data_files and not args.force:
            logging.info("\n" + "-" * 40)
            logging.info(f"DATA dump already exists with {len(existing_data_files)} files")
            logging.info(f"Skipping data download. Use --force to re-download")
            logging.info("-" * 40)
        else:
            logging.info("\n" + "-" * 40)
            logging.info("Downloading DATA dump...")
            logging.info("-" * 40)

            data_success = download_dump(
                host=args.data_host,
                port=args.data_port,
                user=args.data_user,
                password=args.data_password,
                output_dir=data_dir,
                file_length_limit=args.file_length_limit,
                docker_container=args.data_docker,
                endpoint_name="Data Endpoint"
            )

            if not data_success:
                logging.error("Failed to download data dump")
                success = False
            else:
                files = list(data_dir.glob("*.nq.gz"))
                logging.info(f"Downloaded {len(files)} data files to {data_dir}")

    if not args.skip_provenance:
        existing_prov_files = list(prov_dir.glob("*.nq.gz"))
        if existing_prov_files and not args.force:
            logging.info("\n" + "-" * 40)
            logging.info(f"PROVENANCE dump already exists with {len(existing_prov_files)} files")
            logging.info(f"Skipping provenance download. Use --force to re-download")
            logging.info("-" * 40)
        else:
            logging.info("\n" + "-" * 40)
            logging.info("Downloading PROVENANCE dump...")
            logging.info("-" * 40)

            prov_success = download_dump(
                host=args.prov_host,
                port=args.prov_port,
                user=args.prov_user,
                password=args.prov_password,
                output_dir=prov_dir,
                file_length_limit=args.file_length_limit,
                docker_container=args.prov_docker,
                endpoint_name="Provenance Endpoint"
            )

            if not prov_success:
                logging.error("Failed to download provenance dump")
                success = False
            else:
                files = list(prov_dir.glob("*.nq.gz"))
                logging.info(f"Downloaded {len(files)} provenance files to {prov_dir}")

    if success:
        logging.info("\n" + "=" * 60)
        logging.info("CONVERTING TO ORGANIZED STRUCTURE...")
        logging.info("=" * 60)

        rdf_dir = args.output_dir / "rdf"

        if not args.skip_data and data_dir.exists():
            logging.info(f"\nConverting data dumps to: {rdf_dir}")
            convert_dumps_to_organized_structure(data_dir, rdf_dir)

        if not args.skip_provenance and prov_dir.exists():
            logging.info(f"\nConverting provenance dumps to: {rdf_dir}")
            convert_dumps_to_organized_structure(prov_dir, rdf_dir)

    logging.info("\n" + "=" * 60)
    if success:
        logging.info("PROCESS COMPLETED SUCCESSFULLY")
        logging.info(f"Raw dumps saved to: {args.output_dir}")
        logging.info(f"Organized RDF data saved to: {args.output_dir / 'rdf'}")
    else:
        logging.error("PROCESS COMPLETED WITH ERRORS")
        logging.error("Please check the log for details")
    logging.info("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())