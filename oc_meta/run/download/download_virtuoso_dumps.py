#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2024, OpenCitations <contact@opencitations.net>
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
Download full dumps from OpenCitations Meta Virtuoso endpoints.

This script downloads complete RDF dumps from two Virtuoso endpoints:
1. Main data endpoint - containing bibliographic and entity data
2. Provenance endpoint - containing provenance information

The script uses the virtuoso_utilities package to perform efficient dumps
and saves the output as compressed N-Quads files.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from virtuoso_utilities.dump_quadstore import dump_quadstore


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


def create_output_directories(base_output_dir: Path) -> Tuple[Path, Path]:
    """
    Create output directories for data and provenance dumps.

    Args:
        base_output_dir: Base directory for all outputs

    Returns:
        Tuple of (data_dir, provenance_dir)
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    data_dir = base_output_dir / f"data_dump_{timestamp}"
    prov_dir = base_output_dir / f"provenance_dump_{timestamp}"

    data_dir.mkdir(parents=True, exist_ok=True)
    prov_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Created data dump directory: {data_dir}")
    logging.info(f"Created provenance dump directory: {prov_dir}")

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

    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.output_dir, args.verbose)

    logging.info("=" * 60)
    logging.info("OpenCitations Meta Virtuoso Dump Downloader")
    logging.info("=" * 60)

    data_dir, prov_dir = create_output_directories(args.output_dir)

    success = True

    if not args.skip_data:
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
            for f in files[:5]:  # Show first 5 files
                size_mb = f.stat().st_size / (1024 * 1024)
                logging.info(f"  - {f.name} ({size_mb:.2f} MB)")
            if len(files) > 5:
                logging.info(f"  ... and {len(files) - 5} more files")

    if not args.skip_provenance:
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
            for f in files[:5]:  # Show first 5 files
                size_mb = f.stat().st_size / (1024 * 1024)
                logging.info(f"  - {f.name} ({size_mb:.2f} MB)")
            if len(files) > 5:
                logging.info(f"  ... and {len(files) - 5} more files")

    logging.info("\n" + "=" * 60)
    if success:
        logging.info("DUMP DOWNLOAD COMPLETED SUCCESSFULLY")
        logging.info(f"All dumps saved to: {args.output_dir}")
    else:
        logging.error("DUMP DOWNLOAD COMPLETED WITH ERRORS")
        logging.error("Please check the log for details")
    logging.info("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())