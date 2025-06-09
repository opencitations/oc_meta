#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import csv
import heapq
import logging
import multiprocessing
import os
import shutil
import tempfile
from typing import Dict, List, Tuple

from oc_meta.lib.file_manager import get_csv_data_fast
from oc_meta.run.merge_csv_dumps import (normalize_ids_in_brackets,
                                         normalize_ids_in_field,
                                         normalize_page_field,
                                         normalize_people_field)
from pebble import ProcessPool
from tqdm import tqdm

csv.field_size_limit(1000000)  # 1MB limit instead of default 131072

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_omid_from_id_field(id_field: str) -> str:
    """Extract OMID from ID field containing multiple identifiers"""
    if not id_field:
        return ""
    
    ids = id_field.split()
    for id_item in ids:
        if id_item.startswith('omid:'):
            return id_item
    return ""


def normalize_row_data(row: Dict[str, str]) -> Dict[str, str]:
    """
    Normalize row data for consistent output
    
    This method normalizes:
    - ID field: OMID first, other IDs alphabetically sorted
    - People fields (author, editor, publisher): IDs in brackets normalized
    - Venue field: IDs in brackets normalized
    - Page field: simplified ranges where start and end pages are identical (e.g., "333-333" -> "333")
    - Other fields: basic string normalization
    """
    normalized = {}
    for key, value in row.items():
        if value is None:
            normalized_value = ""
        else:
            str_value = str(value).strip()
            
            if key == 'id':
                normalized_value = normalize_ids_in_field(str_value)
            elif key in ['author', 'editor', 'publisher']:
                normalized_value = normalize_people_field(str_value)
            elif key == 'venue':
                normalized_value = normalize_ids_in_brackets(str_value)
            elif key == 'page':
                normalized_value = normalize_page_field(str_value)
            else:
                normalized_value = str_value
                
        normalized[key] = normalized_value
    return normalized


def process_csv_file_to_temp(args: Tuple[str, str, bool]) -> Tuple[str, str, int]:
    """
    Process a single CSV file and write to temporary sorted file
    
    Args:
        args: Tuple of (file_path, temp_dir, is_new_file)
        
    Returns:
        Tuple of (original_file_path, temp_file_path, row_count)
    """
    file_path, temp_dir, is_new_file = args
    
    try:
        rows = get_csv_data_fast(file_path)
        processed_rows = []
        
        for row in rows:
            normalized_row = normalize_row_data(row)
            omid = extract_omid_from_id_field(normalized_row.get('id', ''))
            
            if omid:
                # Add priority flag for new files (0 = new/higher priority, 1 = existing/lower priority)
                priority = 0 if is_new_file else 1
                processed_rows.append((omid, priority, normalized_row))
            else:
                logger.warning(f"Row without OMID found in {file_path}: {row.get('id', '')}")
        
        # Sort by OMID, then by priority (new files first)
        processed_rows.sort(key=lambda x: (x[0], x[1]))
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, dir=temp_dir, 
                                               suffix='.csv', encoding='utf-8')
        fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
        
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()
        
        for omid, priority, row_data in processed_rows:
            writer.writerow({key: row_data.get(key, '') for key in fieldnames})
        
        temp_file.close()
        
        return file_path, temp_file.name, len(processed_rows)
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return file_path, "", 0


def merge_sorted_temp_files(temp_files_with_priority: List[Tuple[str, int]], output_dir: str, total_rows: int, rows_per_file: int = 3000) -> None:
    """
    Merge multiple sorted temporary files using external merge sort with priority handling
    
    Args:
        temp_files_with_priority: List of tuples (temp_file_path, file_priority) where lower priority values take precedence
        output_dir: Output directory for merged files
        total_rows: Exact number of rows to process (for accurate progress bar)
        rows_per_file: Number of rows per output file
    """
    os.makedirs(output_dir, exist_ok=True)
    fieldnames = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type', 'publisher', 'editor']
    
    file_readers = []
    for temp_file, file_priority in temp_files_with_priority:
        if os.path.exists(temp_file):
            f = open(temp_file, 'r', encoding='utf-8')
            reader = csv.DictReader(f)
            file_readers.append((reader, f, file_priority))
    
    if not file_readers:
        logger.warning("No temporary files to merge")
        return
    
    # Initialize heap with first row from each file
    heap = []
    for i, (reader, f, file_priority) in enumerate(file_readers):
        try:
            row = next(reader)
            omid = extract_omid_from_id_field(row.get('id', ''))
            if omid:
                # Heap priority: (omid, file_priority, file_idx, row)
                # Lower file_priority values will be processed first for same OMID
                heapq.heappush(heap, (omid, file_priority, i, row))
        except StopIteration:
            f.close()
        
    # Merge files using heap
    file_count = 0
    row_count = 0
    current_output_file = None
    current_writer = None
    seen_omids = {}  # OMID -> best_priority (to track best row seen for each OMID)
    total_rows_written = 0
    
    def open_new_output_file():
        nonlocal current_output_file, current_writer, file_count
        if current_output_file:
            current_output_file.close()
        
        file_count += 1
        output_filename = f"oc_meta_data_{file_count:03d}.csv"
        output_path = os.path.join(output_dir, output_filename)
        current_output_file = open(output_path, 'w', newline='', encoding='utf-8')
        current_writer = csv.DictWriter(current_output_file, fieldnames=fieldnames)
        current_writer.writeheader()
        return output_path
    
    current_file_path = open_new_output_file()
    logger.info(f"Starting merge - first output file: {os.path.basename(current_file_path)}")
    
    with tqdm(total=total_rows, desc="Writing output rows", unit="rows") as pbar:
        while heap:
            omid, file_priority, file_idx, row = heapq.heappop(heap)
            
            # Only write if we haven't seen this OMID or this has better priority
            if omid not in seen_omids or file_priority < seen_omids[omid]:
                # Update the best priority for this OMID
                seen_omids[omid] = file_priority
                
                # Check if we need a new output file
                if row_count >= rows_per_file:
                    current_file_path = open_new_output_file()
                    pbar.set_description(f"Writing output rows (file {file_count})")
                    row_count = 0
                
                # Write row
                current_writer.writerow({key: row.get(key, '') for key in fieldnames})
                row_count += 1
                total_rows_written += 1
                pbar.update(1)
            
            # Get next row from the same file
            reader, f, current_file_priority = file_readers[file_idx]
            try:
                next_row = next(reader)
                next_omid = extract_omid_from_id_field(next_row.get('id', ''))
                if next_omid:
                    heapq.heappush(heap, (next_omid, current_file_priority, file_idx, next_row))
            except StopIteration:
                f.close()
    
    if current_output_file:
        current_output_file.close()
    
    logger.info(f"Merge complete! Wrote {total_rows_written} unique rows across {file_count} files")


def get_all_csv_files(directory: str) -> List[str]:
    """
    Get all CSV files from a directory
    
    Args:
        directory: Directory path to search for CSV files
        
    Returns:
        List of full paths to CSV files found in the directory
    """
    if not os.path.exists(directory):
        logger.warning(f"Directory {directory} does not exist")
        return []
    
    csv_files = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            csv_files.append(os.path.join(directory, filename))
    return csv_files


def load_and_merge_csv_data_streaming(existing_dir: str, new_dir: str, output_dir: str, 
                                     rows_per_file: int = 3000, max_workers: int = None) -> None:
    """
    Load and merge CSV data using streaming approach to handle large datasets
    
    Args:
        existing_dir: Directory containing existing CSV files
        new_dir: Directory containing new CSV files (take precedence)
        output_dir: Directory to save merged CSV files
        rows_per_file: Number of rows per output file
        max_workers: Maximum number of worker processes (default: CPU count)
    """
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()
    
    existing_files = get_all_csv_files(existing_dir)
    new_files = get_all_csv_files(new_dir)
    all_files = existing_files + new_files
    
    if not all_files:
        logger.warning("No CSV files found in either directory")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    
    temp_dir = tempfile.mkdtemp(prefix="merge_temp_", dir=output_dir)
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        logger.info(f"Processing {len(all_files)} files to temporary sorted files using {max_workers} workers...")
        
        tasks = []
        for file_path in all_files:
            is_new_file = file_path in new_files
            tasks.append((file_path, temp_dir, is_new_file))
        
        temp_files_with_priority = []
        
        with ProcessPool(max_workers=max_workers) as pool:
            futures = []
            for task in tasks:
                future = pool.schedule(process_csv_file_to_temp, args=[task])
                futures.append((future, task[2]))  # Store is_new_file flag
            
            completed_files = 0
            total_rows_processed = 0
            with tqdm(total=len(all_files), desc="Processing files", unit="file") as pbar:
                for future, is_new_file in futures:
                    try:
                        file_path, temp_file, row_count = future.result()
                        if temp_file:
                            # Priority: 0 for new files (higher priority), 1 for existing files (lower priority)
                            file_priority = 0 if is_new_file else 1
                            temp_files_with_priority.append((temp_file, file_priority))
                            total_rows_processed += row_count
                        completed_files += 1
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Failed to process file: {e}")
                        pbar.update(1)
        
        logger.info(f"Processed {completed_files} files, created {len(temp_files_with_priority)} temporary files with {total_rows_processed} total rows")
        
        logger.info("Merging temporary files...")
        merge_sorted_temp_files(temp_files_with_priority, output_dir, total_rows_processed, rows_per_file)
    
    finally:
        if os.path.exists(temp_dir):
            logger.info(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)


class CSVDumpMergerLight:
    """
    Light version of CSV Dump Merger for simple merging without database verification
    
    This class processes CSV dump files by merging existing and new directories,
    normalizing the data, and outputting files with progressive names and sorted OMIDs.
    Uses streaming approach and external merge sort for memory-efficient processing
    of large datasets.
    
    Features:
    - Merges existing and new CSV files (new files take precedence)
    - Normalizes all data fields for consistency
    - Sorts output by OMID alphabetically
    - Splits output into files with configurable row count
    - Uses progressive file naming (merged_001.csv, etc.)
    - Memory-efficient streaming processing for large datasets
    - External merge sort to handle datasets larger than available RAM
    """
    
    def __init__(self, max_workers: int = None):
        """
        Initialize CSVDumpMergerLight
        
        Args:
            max_workers: Maximum number of worker processes (default: CPU count)
        """
        self.max_workers = max_workers or multiprocessing.cpu_count()
        logger.info(f"Initialized CSVDumpMergerLight with {self.max_workers} workers")
    
    def merge_dumps_light(self, existing_dir: str, new_dir: str, output_dir: str, rows_per_file: int = 3000) -> None:
        """
        Light merge of CSV dumps without database verification using streaming approach
        
        This method:
        1. Processes each CSV file and creates sorted temporary files
        2. Uses external merge sort to merge all temporary files
        3. New files take precedence over existing files for the same OMID
        4. Normalizes all data fields for consistency
        5. Outputs files with progressive names and specified row count
        6. Memory-efficient: handles datasets larger than available RAM
        
        Args:
            existing_dir: Directory containing existing CSV files
            new_dir: Directory containing new CSV files (take precedence)
            output_dir: Directory to save merged CSV files
            rows_per_file: Number of rows per output file (default: 3000)
        """
        logger.info("Starting light CSV dump merge with streaming approach...")
        logger.info(f"Existing directory: {existing_dir}")
        logger.info(f"New directory: {new_dir}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Rows per file: {rows_per_file}")
        logger.info(f"Max workers: {self.max_workers}")
        
        load_and_merge_csv_data_streaming(existing_dir, new_dir, output_dir, 
                                        rows_per_file, self.max_workers)
        
        logger.info(f"Merge complete! Output files created in {output_dir}")
        logger.info(f"Output files have {rows_per_file} rows each (except possibly the last file)")
    
    def merge_dumps_minimal_memory(self, existing_dir: str, new_dir: str, output_dir: str, rows_per_file: int = 3000) -> None:
        """
        Ultra memory-efficient merge for extremely large datasets - processes files sequentially
        
        This method is designed for cases where even the streaming approach uses too much memory.
        It processes files one at a time without multiprocessing to minimize memory usage.
        
        Args:
            existing_dir: Directory containing existing CSV files
            new_dir: Directory containing new CSV files (take precedence)
            output_dir: Directory to save merged CSV files
            rows_per_file: Number of rows per output file (default: 3000)
        """
        logger.info("Starting ultra memory-efficient CSV dump merge (sequential processing)...")
        logger.info(f"Existing directory: {existing_dir}")
        logger.info(f"New directory: {new_dir}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Rows per file: {rows_per_file}")
        
        existing_files = get_all_csv_files(existing_dir)
        new_files = get_all_csv_files(new_dir)
        all_files = existing_files + new_files
        
        if not all_files:
            logger.warning("No CSV files found in either directory")
            return
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Create temporary directory inside output directory
        temp_dir = tempfile.mkdtemp(prefix="merge_temp_minimal_", dir=output_dir)
        logger.info(f"Created temporary directory: {temp_dir}")
        
        try:
            logger.info(f"Processing {len(all_files)} files to temporary sorted files sequentially...")
            
            temp_files_with_priority = []
            total_rows = 0
            
            for i, file_path in enumerate(tqdm(all_files, desc="Processing files", unit="file")):
                is_new_file = file_path in new_files
                try:
                    _, temp_file, row_count = process_csv_file_to_temp((file_path, temp_dir, is_new_file))
                    if temp_file:
                        # Priority: 0 for new files (higher priority), 1 for existing files (lower priority)
                        file_priority = 0 if is_new_file else 1
                        temp_files_with_priority.append((temp_file, file_priority))
                        total_rows += row_count
                                            
                except Exception as e:
                    logger.error(f"Failed to process file {file_path}: {e}")
            
            logger.info(f"Processed {len(all_files)} files, created {len(temp_files_with_priority)} temporary files with {total_rows} total rows")
            
            logger.info("Merging temporary files...")
            merge_sorted_temp_files(temp_files_with_priority, output_dir, total_rows, rows_per_file)
        
        finally:
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                logger.info(f"Cleaning up temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
        
        logger.info(f"Merge complete! Output files created in {output_dir}")
        logger.info(f"Output files have {rows_per_file} rows each (except possibly the last file)")


def main():
    """Main function to run light CSV dump merger from command line"""
    parser = argparse.ArgumentParser(description='Light CSV metadata dumps merger with streaming approach (memory-efficient)')
    parser.add_argument('existing_dir', help='Directory containing existing CSV files')
    parser.add_argument('new_dir', help='Directory containing new CSV files (take precedence)')
    parser.add_argument('output_dir', help='Directory to save merged CSV files')
    parser.add_argument('--rows-per-file', type=int, default=3000, 
                       help='Number of rows per output file (default: 3000)')
    parser.add_argument('--max-workers', type=int, default=None,
                       help='Maximum number of worker processes (default: CPU count)')
    parser.add_argument('--minimal-memory', action='store_true',
                       help='Use ultra memory-efficient mode (sequential processing)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Set logging level')
    
    args = parser.parse_args()
    
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    merger = CSVDumpMergerLight(max_workers=args.max_workers)
    
    if args.minimal_memory:
        logger.info("Using minimal memory mode (sequential processing)")
        merger.merge_dumps_minimal_memory(args.existing_dir, args.new_dir, args.output_dir, args.rows_per_file)
    else:
        merger.merge_dumps_light(args.existing_dir, args.new_dir, args.output_dir, args.rows_per_file)


if __name__ == '__main__':
    main() 