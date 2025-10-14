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
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from rdflib import Dataset, URIRef
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

INODE_THRESHOLD = 10_000_000  # 10 million free inodes trigger merge
merge_needed_flag = multiprocessing.Event()  # Shared flag for all processes

VIRTUOSO_EXCLUDED_GRAPHS = [
    "http://localhost:8890/DAV/",
    "http://www.openlinksw.com/schemas/virtrdf#",
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/ns/ldp#",
    "urn:activitystreams-owl:map",
    "urn:core:services:sparql",
]


def get_free_inodes(path: str) -> int:
    """Get the number of free inodes on the filesystem containing the given path."""
    stat = os.statvfs(path)
    return stat.f_favail  # Free inodes available to non-root users


def setup_logging(output_dir: Path, verbose: bool = False) -> None:
    """
    Setup logging configuration for all modules.
    Configures the root logger to capture logs from all imported modules.

    Args:
        output_dir: Directory where log file will be created
        verbose: If True, set logging level to DEBUG
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_file = output_dir / f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File handler - captures everything
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def convert_dumps_to_organized_structure(
    input_dir: Path,
    output_dir: Path,
    num_processes: int = None
) -> None:
    """
    Convert downloaded N-Quads dumps to organized directory structure with dynamic inode monitoring.

    Args:
        input_dir: Directory containing .nq.gz dump files
        output_dir: Directory for organized output
        num_processes: Number of parallel processes (None for auto)
    """
    logging.info(f"Converting dumps from {input_dir} to organized structure in {output_dir}")

    cache_file = output_dir / ".processed_dumps_cache.txt"
    processed_files = set()

    if cache_file.exists():
        with open(cache_file, 'r') as f:
            processed_files = set(line.strip() for line in f if line.strip())
        logging.info(f"Loaded cache with {len(processed_files)} already processed files")

    dump_files = list(input_dir.glob("*.nq.gz"))
    if not dump_files:
        logging.warning(f"No .nq.gz files found in {input_dir}")
        return

    unprocessed_files = [f for f in dump_files if str(f.absolute()) not in processed_files]

    total_files = len(dump_files)
    skipped_files = len(dump_files) - len(unprocessed_files)

    logging.info(f"Found {total_files} total dump files")
    if skipped_files > 0:
        logging.info(f"Skipping {skipped_files} already processed files")
    logging.info(f"Will process {len(unprocessed_files)} files")

    if not unprocessed_files:
        logging.info("All files have already been processed")
        return

    dump_files = unprocessed_files

    stop_file = str(output_dir / ".stop")

    free_inodes = get_free_inodes(str(output_dir))
    logging.info(f"Starting with {free_inodes:,} free inodes")

    remaining_files = [
        (dump_file, output_dir, BASE_IRI, DIR_SPLIT, N_FILE_ITEM, ZIP_OUTPUT)
        for dump_file in dump_files
    ]

    total_processed = 0
    merge_count = 0
    successfully_processed = []

    while remaining_files:
        with multiprocessing.Pool(processes=num_processes) as pool:
            files_to_retry = []
            current_batch_processed = 0

            with tqdm(total=len(remaining_files), desc=f"Processing files (merge count: {merge_count})") as pbar:
                results = pool.imap_unordered(process_dump_file, remaining_files)

                for i, (success, file_path) in enumerate(results):
                    current_batch_processed += 1
                    pbar.update(1)

                    if not success:
                        # False means this file couldn't be processed due to low inodes
                        files_to_retry.append(remaining_files[i])
                    else:
                        total_processed += 1
                        successfully_processed.append(file_path)
                        with open(cache_file, 'a') as f:
                            f.write(f"{file_path}\n")

                    if merge_needed_flag.is_set():
                        logging.info("Merge needed due to low inodes. Finishing current tasks...")
                        pool.close()
                        pool.join()

                        merge_count += 1
                        logging.info(f"Starting merge operation #{merge_count}...")
                        merge_start = time.time()
                        merge_all_files_parallel(str(output_dir), ZIP_OUTPUT, stop_file)
                        merge_time = time.time() - merge_start

                        free_inodes = get_free_inodes(str(output_dir))
                        logging.info(f"Merge #{merge_count} completed in {merge_time:.1f}s. Free inodes: {free_inodes:,}")

                        merge_needed_flag.clear()

                        files_to_retry.extend(remaining_files[current_batch_processed:])
                        break

            remaining_files = files_to_retry

            if not remaining_files:
                logging.info(f"All files processed successfully. Total: {total_processed}, Merges: {merge_count}")
                break
            else:
                logging.info(f"Retrying {len(remaining_files)} files after merge")

    logging.info("Performing final merge...")
    merge_all_files_parallel(str(output_dir), ZIP_OUTPUT, stop_file)

    logging.info("Conversion completed")

def process_dump_file(args: tuple) -> tuple:
    """
    Process a single dump file and organize its contents.
    Parses N-Quads line by line to handle individual parsing errors gracefully.

    Args:
        args: Tuple of (dump_file, output_dir, base_iri, dir_split, n_file_item, zip_output)

    Returns:
        Tuple of (success: bool, dump_file_path: str)
        - success: True if processed successfully, False if merge is needed
        - dump_file_path: Path of the processed file (for cache update)
    """
    dump_file, output_dir, base_iri, dir_split, n_file_item, zip_output = args

    free_inodes = get_free_inodes(str(output_dir))
    if free_inodes < INODE_THRESHOLD:
        logging.warning(f"Low inodes detected: {free_inodes:,} < {INODE_THRESHOLD:,}. Triggering merge.")
        merge_needed_flag.set()
        return (False, str(dump_file.absolute()))

    unique_id = generate_unique_id()

    dataset = Dataset()
    total_lines = 0
    parsed_lines = 0
    skipped_lines = 0
    excluded_lines = 0
    error_samples = []  # Store first few error examples

    with gzip.open(dump_file, 'rt', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            total_lines += 1
            line = line.strip()

            if not line or line.startswith('#'):
                continue  # Skip empty lines and comments

            try:
                # Parse single N-Quad line
                temp_dataset = Dataset()
                temp_dataset.parse(data=line, format='nquads')

                added_any = False
                for s, p, o, context in temp_dataset.quads():
                    # Check if the graph (context) is in the excluded list
                    if context and str(context) in VIRTUOSO_EXCLUDED_GRAPHS:
                        continue  # Skip quads from Virtuoso system graphs

                    dataset.add((s, p, o, context))
                    added_any = True

                if added_any:
                    parsed_lines += 1
                else:
                    excluded_lines += 1

            except Exception as line_error:
                skipped_lines += 1
                # Store first 5 error examples for logging
                if len(error_samples) < 5:
                    error_samples.append({
                        'line_num': line_num,
                        'line': line[:200] + '...' if len(line) > 200 else line,
                        'error': str(line_error)
                    })
                continue

    # Log parsing statistics
    if skipped_lines > 0 or excluded_lines > 0:
        if excluded_lines > 0:
            logging.info(f"File {dump_file}: Parsed {parsed_lines}/{total_lines} lines, "
                        f"excluded {excluded_lines} Virtuoso system lines, "
                        f"skipped {skipped_lines} problematic lines")
        else:
            logging.warning(f"File {dump_file}: Parsed {parsed_lines}/{total_lines} lines, "
                            f"skipped {skipped_lines} problematic lines")

        for sample in error_samples:
            logging.debug(f"  Line {sample['line_num']}: {sample['error']}")
            logging.debug(f"    Content: {sample['line']}")

    files_data = {}

    for s, p, o, context in dataset.quads():
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
                    files_data[file_path] = Dataset()

                files_data[file_path].add((s, p, o, context))

    for file_path, file_graph in files_data.items():
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)

        if zip_output and not file_path.endswith('.zip'):
            file_path = file_path.replace('.json', '.zip')

        store_in_file(file_graph, file_path, zip_output)

    logging.debug(f"Completed processing {dump_file}")
    return (True, str(dump_file.absolute()))

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