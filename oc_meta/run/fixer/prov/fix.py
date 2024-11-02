import argparse
import logging
import logging.handlers
import os
import re
import zipfile
from datetime import datetime, timezone
from multiprocessing import cpu_count, current_process
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from pebble import ProcessPool
from rdflib import ConjunctiveGraph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD
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
    """Create a process-specific logger with timestamped filename."""
    os.makedirs(log_dir, exist_ok=True)

    process_name = current_process().name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"{logger_name}_{process_name}_{timestamp}.log"
    logger = logging.getLogger(f"{logger_name}_{process_name}_{timestamp}")
    
    if not logger.handlers:  # Avoid adding handlers multiple times
        logger.setLevel(logging.INFO)
        
        # Create a regular file handler with timestamp in filename
        handler = logging.FileHandler(
            filename=os.path.join(log_dir, log_filename),
            encoding='utf-8',
            mode='a'  # append mode, though it's a new file anyway
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
        base_uri = None
        
        for s, p, o in context.triples((None, None, None)):
            if '/prov/se/' in str(s) and str(s) not in seen_uris:
                if base_uri is None:
                    base_uri = str(s).rsplit('/se/', 1)[0]
                
                snapshot = {
                    'uri': s,
                    'number': self._extract_snapshot_number(s),
                    'generation_times': list(context.objects(s, PROV.generatedAtTime, unique=True)),
                    'invalidation_times': list(context.objects(s, PROV.invalidatedAtTime, unique=True))
                }
                snapshots.append(snapshot)
                seen_uris.add(str(s))
        
        sorted_snapshots = sorted(snapshots, key=lambda x: x['number'])
        
        if len(sorted_snapshots) >= 2:
            # Identify and fill missing snapshots
            filled_snapshots = self._fill_missing_snapshots(context, sorted_snapshots, base_uri)
            return filled_snapshots
            
        return sorted_snapshots

    def _fill_missing_snapshots(self, context: ConjunctiveGraph, 
                                snapshots: List[dict], base_uri: str) -> List[dict]:
        """Fill in missing snapshots in the sequence."""
        if not snapshots:
            return snapshots
            
        filled_snapshots = []
        max_num = max(s['number'] for s in snapshots)
        min_num = min(s['number'] for s in snapshots)
        existing_numbers = {s['number'] for s in snapshots}
        existing_snapshots = {s['number']: s for s in snapshots}
        
        for i in range(min_num, max_num + 1):
            if i in existing_numbers:
                filled_snapshots.append(existing_snapshots[i])
            else:
                # Create missing snapshot
                missing_uri = URIRef(f"{base_uri}/se/{i}")
                
                # Find the first previous available snapshot
                prev_num = i - 1
                while prev_num >= min_num and prev_num not in existing_numbers:
                    prev_num -= 1
                prev_snapshot = existing_snapshots.get(prev_num)
                
                # Find the first next available snapshot
                next_num = i + 1
                while next_num <= max_num and next_num not in existing_numbers:
                    next_num += 1
                next_snapshot = existing_snapshots.get(next_num)
                
                missing_snapshot = self._create_missing_snapshot(
                    context, missing_uri, i, 
                    prev_snapshot, next_snapshot
                )
                
                # Add the missing snapshot to existing_snapshots and existing_numbers
                existing_snapshots[i] = missing_snapshot
                existing_numbers.add(i)
                filled_snapshots.append(missing_snapshot)
                    
        return sorted(filled_snapshots, key=lambda x: x['number'])

    def _create_missing_snapshot(self, context: ConjunctiveGraph, 
                            missing_uri: URIRef, number: int,
                            prev_snapshot: Optional[dict], 
                            next_snapshot: Optional[dict]) -> dict:
        """Create a missing snapshot with basic information."""
        entity_uri = URIRef(self._get_entity_from_prov_graph(str(missing_uri).split('se')[0]))
        
        # Add basic triples for the missing snapshot
        context.add((missing_uri, RDF.type, PROV.Entity))
        context.add((missing_uri, PROV.specializationOf, entity_uri))
        
        # Add wasDerivedFrom if we have a previous snapshot
        if prev_snapshot:
            context.add((missing_uri, PROV.wasDerivedFrom, prev_snapshot['uri']))
        
        generation_time = None
        invalidation_time = None
        
        # First check if it is the first snapshot and lacks generation time
        if not prev_snapshot and number == 1:
            generation_time = self._default_time
        else:
            # If we have previous snapshot, generation time is its invalidation time
            if prev_snapshot and prev_snapshot['invalidation_times']:
                if number == prev_snapshot['number'] + 1:
                    generation_time = prev_snapshot['invalidation_times'][0]
            
            # If we have next snapshot, invalidation time is its generation time
            if next_snapshot and next_snapshot['generation_times']:
                if number == next_snapshot['number'] - 1:
                    invalidation_time = next_snapshot['generation_times'][0]
                    
            # If both times are missing, or one of them, we need to compute intermediate times
            if not generation_time or not invalidation_time:
                start_time = None
                end_time = None
                
                # Find temporal reference points
                if prev_snapshot:
                    if prev_snapshot['invalidation_times']:
                        start_time = self._convert_to_utc(prev_snapshot['invalidation_times'][0])
                    elif prev_snapshot['generation_times']:
                        start_time = self._convert_to_utc(prev_snapshot['generation_times'][0])
                        
                if next_snapshot:
                    if next_snapshot['generation_times']:
                        end_time = self._convert_to_utc(next_snapshot['generation_times'][0])
                    elif next_snapshot['invalidation_times']:
                        end_time = self._convert_to_utc(next_snapshot['invalidation_times'][0])
                
                if start_time and end_time:
                    # Calculate how many intervals are between existing snapshots
                    total_missing = next_snapshot['number'] - prev_snapshot['number'] - 1
                    position = number - prev_snapshot['number']
                    
                    # Uniformly distribute missing times
                    interval = (end_time - start_time) / (total_missing + 1)
                    intermediate_time = start_time + (interval * position)
                    
                    # Use computed times only if we lack the real ones
                    if not generation_time:
                        generation_time = Literal(intermediate_time.isoformat(), datatype=XSD.dateTime)
                    if not invalidation_time:
                        next_time = intermediate_time + interval
                        invalidation_time = Literal(next_time.isoformat(), datatype=XSD.dateTime)
                        
        # Add timestamps
        if generation_time:
            context.add((missing_uri, PROV.generatedAtTime, generation_time))
            self._log_modification(
                str(missing_uri),
                "Added generatedAtTime for missing snapshot",
                str(generation_time)
            )
            
        if invalidation_time:
            context.add((missing_uri, PROV.invalidatedAtTime, invalidation_time))
            self._log_modification(
                str(missing_uri),
                "Added invalidatedAtTime for missing snapshot",
                str(invalidation_time)
            )
            
        self._log_modification(
            str(missing_uri),
            "Created missing snapshot",
            f"number {number}"
        )
        
        return {
            'uri': missing_uri,
            'number': number,
            'generation_times': [generation_time] if generation_time else [],
            'invalidation_times': [invalidation_time] if invalidation_time else []
        }

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

        # Handle multiple descriptions for each snapshot
        for i, snapshot in enumerate(snapshots):
            is_first = (i == 0)
            is_last = (i == len(snapshots) - 1)
            modified |= self._handle_multiple_descriptions(context, snapshot['uri'], is_first, is_last)

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
            
            # Check if current snapshot is a merge snapshot
            curr_desc = list(context.objects(curr_snapshot['uri'], 
                                        URIRef("http://purl.org/dc/terms/description")))
            is_merge = any("has been merged with" in str(desc) for desc in curr_desc)
            
            derived_from = list(context.objects(curr_snapshot['uri'], PROV.wasDerivedFrom))

            if is_merge:
                # For merge snapshots, ensure there's at least the wasDerivedFrom link to the previous snapshot
                if not derived_from or prev_snapshot['uri'] not in derived_from:
                    context.add((curr_snapshot['uri'], PROV.wasDerivedFrom, prev_snapshot['uri']))
                    self._log_modification(
                        str(entity_uri),
                        "Added merge wasDerivedFrom",
                        f"{str(curr_snapshot['uri'])} → {str(prev_snapshot['uri'])}"
                    )
                    modified = True
            else:
                # For non-merge snapshots, ensure exactly one wasDerivedFrom link to previous snapshot
                if len(derived_from) != 1 or derived_from[0] != prev_snapshot['uri']:
                    # Remove any existing wasDerivedFrom relations
                    for df in derived_from:
                        context.remove((curr_snapshot['uri'], PROV.wasDerivedFrom, df))
                    
                    # Add the correct wasDerivedFrom relation
                    context.add((curr_snapshot['uri'], PROV.wasDerivedFrom, prev_snapshot['uri']))
                    self._log_modification(
                        str(entity_uri),
                        "Fixed wasDerivedFrom",
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

    def _get_earliest_timestamp(self, timestamps: List[Literal]) -> Literal:
        """Return the earliest timestamp from a list of timestamps."""
        earliest = min(
            self._convert_to_utc(ts) 
            for ts in timestamps
        )
        return Literal(earliest.isoformat(), datatype=XSD.dateTime)

    def _handle_generation_time(self, context: ConjunctiveGraph, 
                            snapshots: List[dict], index: int) -> bool:
        """Handle generation time for a snapshot."""
        modified = False
        snapshot = snapshots[index]

        if len(snapshot['generation_times']) > 1:
            # Se ci sono multipli timestamp, mantieni solo il più vecchio
            earliest_time = self._get_earliest_timestamp(snapshot['generation_times'])
            
            # Rimuovi tutti i timestamp esistenti
            self._remove_multiple_timestamps(
                context, snapshot['uri'], PROV.generatedAtTime, snapshot['generation_times']
            )
            
            # Aggiungi il timestamp più vecchio
            context.add((snapshot['uri'], PROV.generatedAtTime, earliest_time))
            self._log_modification(
                str(snapshot['uri']),
                "Kept earliest generatedAtTime",
                f"{str(earliest_time)}"
            )
            modified = True
        elif len(snapshot['generation_times']) == 0:
            new_time = None
            
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

        if len(snapshot['invalidation_times']) > 1:
            # Se ci sono multipli timestamp, mantieni solo il più vecchio
            earliest_time = self._get_earliest_timestamp(snapshot['invalidation_times'])
            
            # Rimuovi tutti i timestamp esistenti
            self._remove_multiple_timestamps(
                context, snapshot['uri'], PROV.invalidatedAtTime, 
                snapshot['invalidation_times']
            )
            
            # Aggiungi il timestamp più vecchio
            context.add((snapshot['uri'], PROV.invalidatedAtTime, earliest_time))
            self._log_modification(
                str(snapshot['uri']),
                "Kept earliest invalidatedAtTime",
                f"{str(earliest_time)}"
            )
            modified = True
        elif len(snapshot['invalidation_times']) == 0:
            new_time = None
            if next_snapshot['generation_times']:
                if len(next_snapshot['generation_times']) == 1:
                    new_time = next_snapshot['generation_times'][0]
                else:
                    earliest_time = self._get_earliest_timestamp(next_snapshot['generation_times'])
                    new_time = earliest_time

            if new_time:
                context.add((snapshot['uri'], PROV.invalidatedAtTime, new_time))
                self._log_modification(
                    str(snapshot['uri']),
                    "Added invalidatedAtTime",
                    f"{str(new_time)}"
                )
                modified = True
        
        return modified

    def _handle_multiple_descriptions(self, context: ConjunctiveGraph, snapshot_uri: URIRef, 
                                is_first_snapshot: bool, is_last_snapshot: bool) -> bool:
        """Handle cases where a snapshot has multiple descriptions.
        
        Args:
            context (ConjunctiveGraph): The RDF graph context
            snapshot_uri (URIRef): The URI of the snapshot to process
            is_first_snapshot (bool): Whether this is the first snapshot in the sequence
            is_last_snapshot (bool): Whether this is the last snapshot in the sequence
            
        Returns:
            bool: True if modifications were made, False otherwise
        """
        modified = False
        descriptions = list(context.objects(snapshot_uri, URIRef("http://purl.org/dc/terms/description")))
        
        if len(descriptions) <= 1:
            return modified
            
        # Check for merge descriptions
        merge_descriptions = [desc for desc in descriptions if "has been merged with" in str(desc)]

        if merge_descriptions and not is_first_snapshot:
            # For merge snapshots (not being the first one), keep all merge descriptions
            # and remove any non-merge descriptions
            for desc in descriptions:
                if "has been merged with" not in str(desc):
                    context.remove((snapshot_uri, URIRef("http://purl.org/dc/terms/description"), desc))
                    self._log_modification(
                        str(snapshot_uri),
                        "Removed non-merge description from merge snapshot",
                        str(desc)
                    )
                    modified = True
        else:
            # For non-merge cases, apply lifecycle-based priority
            selected_desc = None
            
            if is_first_snapshot:
                # First snapshot must be creation
                creation_descs = [desc for desc in descriptions if "has been created" in str(desc)]
                if creation_descs:
                    selected_desc = creation_descs[0]
            elif is_last_snapshot:
                # Last snapshot could be deletion
                deletion_descs = [desc for desc in descriptions if "has been deleted" in str(desc)]
                if deletion_descs:
                    selected_desc = deletion_descs[0]
            
            # If no specific lifecycle description was selected, default to modification
            if not selected_desc:
                modification_descs = [desc for desc in descriptions if "has been modified" in str(desc)]
                if modification_descs:
                    selected_desc = modification_descs[0]
            
            if selected_desc:
                # Remove all descriptions except the selected one
                for desc in descriptions:
                    if desc != selected_desc:
                        context.remove((snapshot_uri, URIRef("http://purl.org/dc/terms/description"), desc))
                        self._log_modification(
                            str(snapshot_uri),
                            "Removed duplicate description based on lifecycle position",
                            str(desc)
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
