import argparse
import os
import re
import zipfile
from collections import defaultdict
from datetime import UTC, datetime
from multiprocessing import Pool, cpu_count
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

from rdflib import ConjunctiveGraph, Literal, Namespace, URIRef
from rdflib.namespace import XSD
from tqdm import tqdm

PROV = Namespace("http://www.w3.org/ns/prov#")

@dataclass
class SnapshotInfo:
    uri: URIRef
    number: int
    generation_times: List[Literal]
    invalidation_times: List[Literal]

class ProvenanceProcessor:
    def __init__(self):
        self._snapshot_number_pattern = re.compile(r'/prov/se/(\d+)$')
        self._default_time = Literal(
            datetime(2022, 12, 20, tzinfo=UTC).isoformat(),
            datatype=XSD.dateTime
        )
        self.modifications = defaultdict(lambda: defaultdict(list))
        
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
        
        return dt.astimezone(UTC)

    def _normalize_timestamp(self, literal: Literal) -> Tuple[Literal, bool]:
        """Normalize a timestamp literal to UTC timezone."""
        try:
            dt = self._convert_to_utc(literal)
            new_literal = Literal(dt.isoformat(), datatype=XSD.dateTime)
            return new_literal, str(new_literal) != str(literal)
        except Exception as e:
            print(f"Error normalizing timestamp {literal}: {e}")
            return literal, False

    def _collect_snapshot_info(self, context: ConjunctiveGraph) -> List[SnapshotInfo]:
        """Collect all snapshot information in a single pass."""
        snapshots = []
        seen_uris: Set[str] = set()
        
        for s, p, o in context.triples((None, None, None)):
            if '/prov/se/' in str(s) and str(s) not in seen_uris:
                generation_times = list(context.objects(s, PROV.generatedAtTime, unique=True))
                invalidation_times = list(context.objects(s, PROV.invalidatedAtTime, unique=True))
                
                snapshot = SnapshotInfo(
                    uri=s,
                    number=self._extract_snapshot_number(s),
                    generation_times=generation_times,
                    invalidation_times=invalidation_times
                )
                
                snapshots.append(snapshot)
                seen_uris.add(str(s))
        
        return sorted(snapshots, key=lambda x: x.number)

    def _remove_multiple_timestamps(self, context: ConjunctiveGraph, 
                                  snapshot_uri: URIRef, 
                                  predicate: URIRef, 
                                  timestamps: List[Literal]) -> None:
        """Rimuove tutti i timestamp esistenti per un dato predicato."""
        for ts in timestamps:
            context.remove((snapshot_uri, predicate, ts))
            self.modifications[str(snapshot_uri)][f"Removed {predicate.split('#')[-1]}"].append(
                f"{str(snapshot_uri)}: {str(ts)}")

    def process_file(self, prov_file_path: str) -> Optional[Tuple[str, Dict]]:
        """Process a single provenance file with optimized operations."""
        try:
            with zipfile.ZipFile(prov_file_path, 'r') as zip_ref:
                g = ConjunctiveGraph()
                
                # Parse all files in a single operation
                for filename in zip_ref.namelist():
                    with zip_ref.open(filename) as file:
                        g.parse(file, format='json-ld')
                
                modified = False
                
                # Process each context
                for context in g.contexts():
                    context_uri = str(context.identifier)
                    if not context_uri.endswith('/prov/'):
                        continue
                    
                    entity_uri = URIRef(self._get_entity_from_prov_graph(context_uri))
                    
                    # Collect all snapshot info in a single pass
                    snapshots = self._collect_snapshot_info(context)
                    if not snapshots:
                        continue
                    
                    # Batch process modifications
                    modified |= self._process_snapshots(context, entity_uri, snapshots)
                
                if modified:
                    # Save modifications in a single operation
                    with zipfile.ZipFile(prov_file_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip_out:
                        jsonld_data = g.serialize(format='json-ld', encoding='utf-8', 
                                               ensure_ascii=False, indent=None)
                        zip_out.writestr('se.json', jsonld_data)
                    
                    return str(prov_file_path), dict(self.modifications)
                
        except Exception as e:
            print(f"Error processing {prov_file_path}: {e}")
        
        return None

    def _process_snapshots(self, context: ConjunctiveGraph, entity_uri: URIRef, 
                          snapshots: List[SnapshotInfo]) -> bool:
        """Process all snapshots in batch operations."""
        modified = False
        mods = self.modifications[str(entity_uri)]
        
        # Process specializationOf relationships
        for snapshot in snapshots:
            if not any(context.objects(snapshot.uri, PROV.specializationOf)):
                context.add((snapshot.uri, PROV.specializationOf, entity_uri))
                mods["Added specializationOf"].append(str(snapshot.uri))
                modified = True
        
        # Process wasDerivedFrom relationships
        for i in range(1, len(snapshots)):
            curr_snapshot = snapshots[i]
            prev_snapshot = snapshots[i-1]
            
            if not any(context.objects(curr_snapshot.uri, PROV.wasDerivedFrom)):
                context.add((curr_snapshot.uri, PROV.wasDerivedFrom, prev_snapshot.uri))
                mods["Added wasDerivedFrom"].append(
                    f"{str(curr_snapshot.uri)} → {str(prev_snapshot.uri)}")
                modified = True
        
        # Process timestamps
        modified |= self._process_timestamps(context, snapshots)
        
        return modified

    def _process_timestamps(self, context: ConjunctiveGraph, 
                          snapshots: List[SnapshotInfo]) -> bool:
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
                              snapshots: List[SnapshotInfo], index: int) -> bool:
        """Handle generation time for a snapshot."""
        modified = False
        snapshot = snapshots[index]

        # Se ci sono timestamp multipli o nessun timestamp, li gestiamo
        if len(snapshot.generation_times) != 1:
            new_time = None
            
            # Rimuovi tutti i timestamp esistenti se ce ne sono
            if snapshot.generation_times:
                self._remove_multiple_timestamps(
                    context, snapshot.uri, PROV.generatedAtTime, snapshot.generation_times)
                modified = True
            
                if index > 0:
                    prev_snapshot = snapshots[index-1]
                    if prev_snapshot.invalidation_times:
                        new_time = prev_snapshot.invalidation_times[0]
                    elif (prev_snapshot.generation_times and
                        snapshot.invalidation_times and len(snapshot.invalidation_times) == 1):
                        # Calculate intermediate time
                        prev_time = self._convert_to_utc(prev_snapshot.generation_times[0])
                        curr_time = self._convert_to_utc(snapshot.invalidation_times[0])
                        middle_time = prev_time + (curr_time - prev_time) / 2
                        new_time = Literal(middle_time.isoformat(), datatype=XSD.dateTime)
                else:
                    new_time = self._default_time
                
                if new_time:
                    context.add((snapshot.uri, PROV.generatedAtTime, new_time))
                    self.modifications[str(snapshot.uri)]["Added generatedAtTime"].append(
                        f"{str(snapshot.uri)}: {str(new_time)}")
                    modified = True
        
        return modified

    def _handle_invalidation_time(self, context: ConjunctiveGraph, 
                                    snapshots: List[SnapshotInfo], index: int) -> bool:
            """Handle invalidation time for a snapshot."""
            modified = False
            snapshot = snapshots[index]
            next_snapshot = snapshots[index + 1]

            # Gestisci timestamp multipli o mancanti
            if len(snapshot.invalidation_times) != 1:
                # Rimuovi tutti i timestamp esistenti se ce ne sono
                if snapshot.invalidation_times:
                    self._remove_multiple_timestamps(
                        context, snapshot.uri, PROV.invalidatedAtTime, snapshot.invalidation_times)
                    modified = True

                new_time = None
                if next_snapshot.generation_times:
                    if len(next_snapshot.generation_times) == 1:
                        # Caso semplice: usa l'unico generation time disponibile
                        new_time = next_snapshot.generation_times[0]
                    else:
                        # Caso con timestamp multipli: usa il più vecchio come punto di invalidazione
                        earliest_time = min(
                            self._convert_to_utc(ts) 
                            for ts in next_snapshot.generation_times
                        )
                        new_time = Literal(earliest_time.isoformat(), datatype=XSD.dateTime)

                if new_time:
                    context.add((snapshot.uri, PROV.invalidatedAtTime, new_time))
                    self.modifications[str(snapshot.uri)]["Added invalidatedAtTime"].append(
                        f"{str(snapshot.uri)}: {str(new_time)}")
                    modified = True
            
            return modified

def main():
    parser = argparse.ArgumentParser(description="Fix provenance files in parallel")
    parser.add_argument('input_dir', type=str, help="Directory containing provenance files")
    parser.add_argument('--processes', type=int, default=cpu_count(),
                       help="Number of parallel processes (default: number of CPU cores)")
    args = parser.parse_args()
    
    prov_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(args.input_dir)
        for file in files
        if file.endswith('se.zip')
    ]
    
    processor = ProvenanceProcessor()
    
    with Pool(processes=args.processes) as pool:
        results = list(tqdm(
            pool.imap_unordered(processor.process_file, prov_files),
            total=len(prov_files),
            desc="Fixing provenance files"
        ))
    
    # Generate report
    print("\nProvenance Fix Report")
    print("=" * 80)
    
    modified_files = sum(1 for result in results if result is not None)
    
    if modified_files == 0:
        print("\nNo modifications were necessary in any file.")
    else:
        for result in results:
            if result:
                file_path, entity_mods = result
                if any(mod_list for mods in entity_mods.values() for mod_list in mods.values()):
                    print(f"\nFile: {file_path}")
                    print("-" * 80)
                    
                    for entity_uri, modifications in entity_mods.items():
                        if any(mod_list for mod_list in modifications.values()):
                            print(f"\nEntity: {entity_uri}")
                            for mod_type, mod_list in modifications.items():
                                if mod_list:
                                    print(f"\n  {mod_type}:")
                                    for mod in mod_list:
                                        print(f"    - {mod}")
        
        print(f"\nTotal files modified: {modified_files}")

if __name__ == "__main__":
    main()