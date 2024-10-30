import argparse
import os
import re
import zipfile
import logging
import logging.handlers
from datetime import datetime, timezone
from multiprocessing import cpu_count, current_process
from typing import Optional, List, Tuple
from zoneinfo import ZoneInfo

from pebble import ProcessPool
from rdflib import ConjunctiveGraph, Literal, Namespace, URIRef
from rdflib.namespace import XSD
from tqdm import tqdm

PROV = Namespace("http://www.w3.org/ns/prov#")

class TqdmToLogger:
    """
    Output stream for tqdm which will output to logger.
    """
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.last_msg = ''

    def write(self, buf):
        # Don't print empty lines
        msg = buf.strip()
        if msg and msg != self.last_msg:
            self.logger.log(self.level, msg)
            self.last_msg = msg
    
    def flush(self):
        pass

def get_process_specific_logger(log_dir: str, logger_name: str) -> logging.Logger:
    """Create a process-specific rotating logger."""
    os.makedirs(log_dir, exist_ok=True)

    process_name = current_process().name
    log_filename = f"{logger_name}_{process_name}.log"
    logger = logging.getLogger(f"{logger_name}_{process_name}")
    
    if not logger.handlers:  # Avoid adding handlers multiple times
        logger.setLevel(logging.INFO)
        
        # Create a rotating file handler
        handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(log_dir, log_filename),
            maxBytes=10*1024*1024,  # 10MB per file
            backupCount=5,  # Keep 5 backup files
            encoding='utf-8'
        )
        
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Prevent propagation to avoid duplicate logs
        logger.propagate = False
    
    return logger

class ProvenanceProcessor:
    def __init__(self, log_dir: str):
        self._snapshot_number_pattern = re.compile(r'/prov/se/(\d+)$')
        self._default_time = Literal(
            datetime(2022, 12, 20, tzinfo=timezone.utc).isoformat(),
            datatype=XSD.dateTime
        )
        self.log_dir = log_dir
        self.logger = get_process_specific_logger(log_dir, 'modifications')
        
    def _log_modification(self, entity_uri: str, mod_type: str, message: str) -> None:
        """Log a modification in real-time."""
        self.logger.info(f"Entity: {entity_uri} - {mod_type} - {message}")

    def _extract_snapshot_number(self, snapshot_uri: str) -> int:
        """Extract the snapshot number from its URI using pre-compiled regex."""
        match = self._snapshot_number_pattern.search(str(snapshot_uri))
        return int(match.group(1)) if match else 0

    def _get_entity_from_prov_graph(self, graph_uri: str) -> str:
        """Extract entity URI from its provenance graph URI."""
        return str(graph_uri).replace('/prov/', '')

    def _convert_to_utc(self, timestamp_str: str) -> datetime:
        """Convert a timestamp string to UTC datetime."""
        timestamp_str = str(timestamp_str).replace('Z', '+00:00')
        dt = datetime.fromisoformat(timestamp_str)
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Europe/Rome"))
        
        return dt.astimezone(timezone.utc)

    def _normalize_timestamp(self, literal: Literal) -> Tuple[Literal, bool]:
        """Normalize a timestamp literal to UTC timezone."""
        try:
            dt = self._convert_to_utc(literal)
            new_literal = Literal(dt.isoformat(), datatype=XSD.dateTime)
            return new_literal, str(new_literal) != str(literal)
        except Exception as e:
            self.logger.error(f"Error normalizing timestamp {literal}: {e}")
            return literal, False

    def _collect_snapshot_info(self, context: ConjunctiveGraph) -> List[dict]:
        """Collect all snapshot information in a single pass."""
        snapshots = []
        seen_uris = set()
        
        for s, p, o in context.triples((None, None, None)):
            if '/prov/se/' in str(s) and str(s) not in seen_uris:
                snapshot = {
                    'uri': s,
                    'number': self._extract_snapshot_number(s),
                    'generation_times': list(context.objects(s, PROV.generatedAtTime, unique=True)),
                    'invalidation_times': list(context.objects(s, PROV.invalidatedAtTime, unique=True))
                }
                snapshots.append(snapshot)
                seen_uris.add(str(s))
        
        return sorted(snapshots, key=lambda x: x['number'])

    def _remove_multiple_timestamps(self, context: ConjunctiveGraph, 
                                  snapshot_uri: URIRef, 
                                  predicate: URIRef, 
                                  timestamps: List[Literal]) -> None:
        """Remove all timestamps for a given predicate."""
        for ts in timestamps:
            context.remove((snapshot_uri, predicate, ts))
            self._log_modification(
                str(snapshot_uri),
                f"Removed {predicate.split('#')[-1]}",
                f"{str(ts)}"
            )

    @staticmethod
    def process_file(prov_file_path: str, log_dir: str) -> Optional[bool]:
        """Process a single provenance file with optimized operations."""
        process_logger = get_process_specific_logger(log_dir, 'processing')
        processor = ProvenanceProcessor(log_dir)
        
        try:
            with zipfile.ZipFile(prov_file_path, 'r') as zip_ref:
                g = ConjunctiveGraph()
                for filename in zip_ref.namelist():
                    with zip_ref.open(filename) as file:
                        g.parse(file, format='json-ld')
                
                modified = False
                
                for context in g.contexts():
                    context_uri = str(context.identifier)
                    if not context_uri.endswith('/prov/'):
                        continue
                    
                    entity_uri = URIRef(processor._get_entity_from_prov_graph(context_uri))
                    snapshots = processor._collect_snapshot_info(context)
                    
                    if snapshots:
                        modified |= processor._process_snapshots(context, entity_uri, snapshots)
                
                if modified:
                    with zipfile.ZipFile(prov_file_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip_out:
                        jsonld_data = g.serialize(format='json-ld', encoding='utf-8', 
                                               ensure_ascii=False, indent=None)
                        zip_out.writestr('se.json', jsonld_data)
                    
                    return True
                
                return False
                
        except Exception as e:
            process_logger.error(f"Error processing {prov_file_path}: {e}")
            return None

    def _process_snapshots(self, context: ConjunctiveGraph, entity_uri: URIRef, 
                          snapshots: List[dict]) -> bool:
        """Process all snapshots in batch operations."""
        modified = False
        
        # Process specializationOf relationships
        for snapshot in snapshots:
            if not any(context.objects(snapshot['uri'], PROV.specializationOf)):
                context.add((snapshot['uri'], PROV.specializationOf, entity_uri))
                self._log_modification(
                    str(entity_uri), 
                    "Added specializationOf",
                    str(snapshot['uri'])
                )
                modified = True
        
        # Process wasDerivedFrom relationships
        for i in range(1, len(snapshots)):
            curr_snapshot = snapshots[i]
            prev_snapshot = snapshots[i-1]
            
            if not any(context.objects(curr_snapshot['uri'], PROV.wasDerivedFrom)):
                context.add((curr_snapshot['uri'], PROV.wasDerivedFrom, prev_snapshot['uri']))
                self._log_modification(
                    str(entity_uri),
                    "Added wasDerivedFrom",
                    f"{str(curr_snapshot['uri'])} → {str(prev_snapshot['uri'])}"
                )
                modified = True
        
        # Process timestamps
        modified |= self._process_timestamps(context, snapshots)
        
        return modified

    def _process_timestamps(self, context: ConjunctiveGraph, snapshots: List[dict]) -> bool:
        """Process all timestamps in batch."""
        modified = False
        
        for i, snapshot in enumerate(snapshots):
            # Handle generation time
            modified |= self._handle_generation_time(context, snapshots, i)
            
            # Handle invalidation time
            if i < len(snapshots) - 1:
                modified |= self._handle_invalidation_time(context, snapshots, i)
        
        return modified

    def _handle_generation_time(self, context: ConjunctiveGraph, 
                              snapshots: List[dict], index: int) -> bool:
        """Handle generation time for a snapshot."""
        modified = False
        snapshot = snapshots[index]

        if len(snapshot['generation_times']) != 1:
            new_time = None
            
            if snapshot['generation_times']:
                self._remove_multiple_timestamps(
                    context, snapshot['uri'], PROV.generatedAtTime, snapshot['generation_times'])
                modified = True
            
                if index > 0:
                    prev_snapshot = snapshots[index-1]
                    if prev_snapshot['invalidation_times']:
                        new_time = prev_snapshot['invalidation_times'][0]
                    elif (prev_snapshot['generation_times'] and
                          snapshot['invalidation_times'] and 
                          len(snapshot['invalidation_times']) == 1):
                        prev_time = self._convert_to_utc(prev_snapshot['generation_times'][0])
                        curr_time = self._convert_to_utc(snapshot['invalidation_times'][0])
                        middle_time = prev_time + (curr_time - prev_time) / 2
                        new_time = Literal(middle_time.isoformat(), datatype=XSD.dateTime)
                else:
                    new_time = self._default_time
                
                if new_time:
                    context.add((snapshot['uri'], PROV.generatedAtTime, new_time))
                    self._log_modification(
                        str(snapshot['uri']),
                        "Added generatedAtTime",
                        f"{str(new_time)}"
                    )
                    modified = True
        
        return modified

    def _handle_invalidation_time(self, context: ConjunctiveGraph, 
                                snapshots: List[dict], index: int) -> bool:
        """Handle invalidation time for a snapshot."""
        modified = False
        snapshot = snapshots[index]
        next_snapshot = snapshots[index + 1]

        if len(snapshot['invalidation_times']) != 1:
            if snapshot['invalidation_times']:
                self._remove_multiple_timestamps(
                    context, snapshot['uri'], PROV.invalidatedAtTime, 
                    snapshot['invalidation_times']
                )
                modified = True

            new_time = None
            if next_snapshot['generation_times']:
                if len(next_snapshot['generation_times']) == 1:
                    new_time = next_snapshot['generation_times'][0]
                else:
                    earliest_time = min(
                        self._convert_to_utc(ts) 
                        for ts in next_snapshot['generation_times']
                    )
                    new_time = Literal(earliest_time.isoformat(), datatype=XSD.dateTime)

            if new_time:
                context.add((snapshot['uri'], PROV.invalidatedAtTime, new_time))
                self._log_modification(
                    str(snapshot['uri']),
                    "Added invalidatedAtTime",
                    f"{str(new_time)}"
                )
                modified = True
        
        return modified

def process_chunk(args):
    file_chunk, log_dir = args
    results = []
    for file in file_chunk:
        result = ProvenanceProcessor.process_file(file, log_dir)
        if result:
            results.append(file)
    return results

def main():
    parser = argparse.ArgumentParser(description="Fix provenance files in parallel")
    parser.add_argument('input_dir', type=str, help="Directory containing provenance files")
    parser.add_argument('--processes', type=int, default=cpu_count(),
                       help="Number of parallel processes (default: number of CPU cores)")
    parser.add_argument('--log-dir', type=str, default='logs',
                       help="Directory for log files (default: logs)")
    args = parser.parse_args()
    
    os.makedirs(args.log_dir, exist_ok=True)
    main_logger = get_process_specific_logger(args.log_dir, 'main')
    
    main_logger.info("Starting provenance fix process")
    
    prov_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(args.input_dir)
        for file in files
        if file.endswith('se.zip')
    ]

    chunk_size = 100
    file_chunks = [prov_files[i:i + chunk_size] 
                  for i in range(0, len(prov_files), chunk_size)]
    
    # Prepare chunks with log_dir
    chunk_args = [(chunk, args.log_dir) for chunk in file_chunks]

    modified_files = []

    process_logger = get_process_specific_logger(args.log_dir, 'processing')
    tqdm_output = TqdmToLogger(process_logger)

    with ProcessPool(max_workers=args.processes, max_tasks=1) as pool:
        future = pool.map(process_chunk, chunk_args)
        iterator = future.result()

        with tqdm(
            total=len(file_chunks), 
            desc="Fixing provenance files", 
            file=tqdm_output,
            mininterval=5.0,
            maxinterval=10.0) as pbar:
            try:
                while True:
                    try:
                        result = next(iterator)
                        modified_files.extend(result)
                        pbar.update(1)
                    except StopIteration:
                        break
                    except TimeoutError as error:
                        main_logger.error(f"Chunk processing timed out: {error}")
                    except Exception as error:
                        main_logger.error(f"Error processing chunk: {error}")
            except KeyboardInterrupt:
                main_logger.warning("Processing interrupted by user")
                future.cancel()
                raise

    main_logger.info(f"Process completed. Total files modified: {len(modified_files)}")
    print(f"\nProcessing complete. Check logs in {args.log_dir} directory.")
    print(f"Total files modified: {len(modified_files)}")

if __name__ == "__main__":
    main()