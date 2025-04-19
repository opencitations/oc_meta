#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to recursively search for 'se.zip' files, extract JSON-LD content,
convert it to N-Quads format, and verify the quad count match.
"""

import argparse
import logging
import os
import zipfile
from pathlib import Path
from rdflib import ConjunctiveGraph
from tqdm import tqdm
import multiprocessing
from pebble import ProcessPool
from functools import partial

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def count_quads(graph: ConjunctiveGraph) -> int:
    """Counts the number of quads in an RDFLib graph."""
    return len(graph)

def convert_jsonld_to_nquads(jsonld_content: str) -> tuple[ConjunctiveGraph | None, str | None]:
    """
    Converts a JSON-LD string into an RDFLib graph and serializes it to N-Quads.

    Args:
        jsonld_content: The JSON-LD content as a string.

    Returns:
        A tuple containing the RDFLib graph and the N-Quads string,
        or (None, None) in case of parsing or serialization error.
    """
    graph = ConjunctiveGraph()
    try:
        graph.parse(data=jsonld_content, format='json-ld')
        nquads_content = graph.serialize(format='nquads')
        return graph, nquads_content
    except Exception as e:
        logging.error(f"Error during parsing or serialization: {e}")
        return None, None

def process_zip_wrapper(zip_path: Path, output_dir: Path, input_dir_path: Path) -> bool:
    """
    Wrapper function for process_zip_file to be used with multiprocessing pool.

    Args:
        zip_path: The path to the se.zip file.
        output_dir: The directory where the resulting .nq file will be saved.
        input_dir_path: The input directory containing the se.zip files (recursive search).

    Returns:
        True if conversion and verification are successful, False otherwise.
    """
    return process_zip_file(zip_path, output_dir, input_dir_path)

def process_zip_file(zip_path: Path, output_dir: Path, input_dir_path: Path) -> bool:
    """
    Processes a single se.zip file: extracts JSON-LD, converts to N-Quads,
    saves the output, and verifies the quad count.

    Args:
        zip_path: The path to the se.zip file.
        output_dir: The directory where the resulting .nq file will be saved.
        input_dir_path: The input directory containing the se.zip files (recursive search).

    Returns:
        True if conversion and verification are successful, False otherwise.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            json_files = [name for name in zf.namelist() if name.endswith('.json')]
            if not json_files:
                logging.warning(f"No .json file found in {zip_path}. Skipping.")
                return False
            if len(json_files) > 1:
                logging.warning(f"Multiple .json files found in {zip_path}. Using the first one: {json_files[0]}.")

            json_filename = json_files[0]
            jsonld_content = zf.read(json_filename).decode('utf-8')

        input_graph, nquads_output = convert_jsonld_to_nquads(jsonld_content)
        if input_graph is None or nquads_output is None:
            logging.error(f"Conversion failed for {zip_path}.")
            return False

        input_quad_count = count_quads(input_graph)

        try:
            relative_path = zip_path.relative_to(input_dir_path)
        except ValueError:
             relative_path = zip_path.name
             logging.warning(f"Could not calculate relative path for {zip_path} relative to the input directory. Using filename only.")


        output_filename = str(relative_path).replace(os.sep, '-')
        output_filename = Path(output_filename).with_suffix('.nq').name
        output_nq_path = output_dir / output_filename

        with open(output_nq_path, 'w', encoding='utf-8') as f:
            f.write(nquads_output)

        output_graph = ConjunctiveGraph()
        try:
            output_graph.parse(output_nq_path, format='nquads')
            output_quad_count = count_quads(output_graph)
        except Exception as e:
             logging.error(f"Error parsing the output N-Quads file {output_nq_path}: {e}")
             return False


        if input_quad_count == output_quad_count:
            # logging.info(f"Success: {zip_path} -> {output_nq_path} ({input_quad_count} quads)")
            return True
        else:
            logging.warning(f"Checksum failed for {zip_path}: Input={input_quad_count}, Output={output_quad_count}")
            return False

    except zipfile.BadZipFile:
        logging.error(f"Corrupt or invalid zip file: {zip_path}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error processing {zip_path}: {e}")
        return False

def main():
    """Main function of the script."""
    parser = argparse.ArgumentParser(description="Converts JSON-LD files from se.zip to N-Quads.")
    parser.add_argument("input_dir", type=str, help="Input directory containing se.zip files (recursive search).")
    parser.add_argument("output_dir", type=str, help="Output directory for the converted .nq files.")
    parser.add_argument("-w", "--workers", type=int, default=None, help="Number of worker processes (defaults to CPU count).")
    args = parser.parse_args()

    input_path = Path(args.input_dir).resolve()
    output_path = Path(args.output_dir).resolve()
    num_workers = args.workers if args.workers else multiprocessing.cpu_count()

    if not input_path.is_dir():
        logging.error(f"Input directory '{input_path}' does not exist or is not a directory.")
        return

    output_path.mkdir(parents=True, exist_ok=True)
    logging.info(f"Recursively searching for 'se.zip' in: {input_path}")
    logging.info(f"N-Quads files will be saved in: {output_path}")

    zip_files = list(input_path.rglob('se.zip'))

    if not zip_files:
        logging.warning(f"No 'se.zip' files found in {input_path}")
        return

    logging.info(f"Found {len(zip_files)} 'se.zip' files. Starting conversion with {num_workers} workers...")

    success_count = 0
    fail_count = 0

    task_func = partial(process_zip_wrapper, output_dir=output_path, input_dir_path=input_path)

    with ProcessPool(max_workers=num_workers) as pool:
        future = pool.map(task_func, zip_files)
        iterator = future.result()

        with tqdm(total=len(zip_files), desc="Converting se.zip", unit="file") as pbar:
            while True:
                try:
                    result = next(iterator)
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                except StopIteration:
                    break
                except Exception as e:
                    logging.error(f"An error occurred during processing: {e}")
                    fail_count += 1
                finally:
                    pbar.update(1)

    logging.info("----- Final Report -----")
    logging.info(f"Successfully processed files: {success_count}")
    logging.info(f"Failed files: {fail_count}")
    logging.info("-----------------------")

if __name__ == "__main__":
    main() 