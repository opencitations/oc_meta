import argparse
import os
import re
import zipfile
from datetime import UTC, datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path

from rdflib import ConjunctiveGraph, Literal, URIRef
from rdflib.namespace import XSD
from tqdm import tqdm

PROV = URIRef("http://www.w3.org/ns/prov#")

def extract_snapshot_number(snapshot_uri):
    """Extract the snapshot number from its URI."""
    match = re.search(r'/prov/se/(\d+)$', str(snapshot_uri))
    return int(match.group(1)) if match else 0

def get_entity_from_prov_graph(graph_uri):
    """Extract entity URI from its provenance graph URI."""
    return str(graph_uri).replace('/prov/', '')

def fix_provenance_file(args):
    """Process a single provenance file."""
    prov_file_path, output_dir = args
    
    try:
        # Create output directory structure
        output_path = Path(output_dir) / Path(prov_file_path).relative_to(Path(prov_file_path).parent.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        modified = False
        with zipfile.ZipFile(prov_file_path, 'r') as zip_ref:
            g = ConjunctiveGraph()
            
            # Parse all files in the zip
            for filename in zip_ref.namelist():
                with zip_ref.open(filename) as file:
                    g.parse(file, format='json-ld')
            
            # Process each context (provenance graph) in the ConjunctiveGraph
            for context in g.contexts():
                context_uri = str(context.identifier)
                if not context_uri.endswith('/prov/'):
                    continue
                
                # Get the entity URI from the provenance graph URI
                entity_uri = URIRef(get_entity_from_prov_graph(context_uri))
                
                # Get all snapshots for this entity
                snapshots = []
                for s in context.subjects(predicate=None, object=None, unique=True):
                    if '/prov/se/' in str(s):
                        snapshots.append(s)
                
                if not snapshots:
                    continue
                
                # Sort snapshots by number
                snapshots.sort(key=extract_snapshot_number)
                
                # Fix specializationOf
                for snapshot in snapshots:
                    if (snapshot, PROV.specializationOf, entity_uri) not in context:
                        context.add((snapshot, PROV.specializationOf, entity_uri))
                        modified = True
                
                # Fix wasDerivedFrom chain
                for i, snapshot in enumerate(snapshots[1:], 1):
                    derived_from = list(context.objects(snapshot, PROV.wasDerivedFrom))
                    if not derived_from:
                        context.add((snapshot, PROV.wasDerivedFrom, snapshots[i-1]))
                        modified = True
                
                # Fix generation and invalidation times
                for i, snapshot in enumerate(snapshots):
                    # Check generation time
                    gen_times = list(context.objects(snapshot, PROV.generatedAtTime))
                    
                    if not gen_times or len(gen_times) > 1:
                        # If previous snapshot exists, use its invalidation time
                        if i > 0:
                            prev_invalidation = list(context.objects(snapshots[i-1], PROV.invalidatedAtTime))
                            if prev_invalidation and len(prev_invalidation) == 1:
                                # Remove existing generation times if any
                                for gt in gen_times:
                                    context.remove((snapshot, PROV.generatedAtTime, gt))
                                # Add new generation time
                                context.add((snapshot, PROV.generatedAtTime, prev_invalidation[0]))
                                modified = True
                        else:
                            # For first snapshot, if no valid generation time exists
                            if not gen_times:
                                # Use a default timestamp
                                default_time = Literal(datetime(year=2022, month=12, day=20, hour=0, minute=0, second=0, tzinfo=UTC).isoformat(), datatype=XSD.dateTime)
                                context.add((snapshot, PROV.generatedAtTime, default_time))
                                modified = True
                    
                    # Check invalidation time
                    if i < len(snapshots) - 1:  # All except last snapshot
                        invalid_times = list(context.objects(snapshot, PROV.invalidatedAtTime))
                        next_gen_time = list(context.objects(snapshots[i+1], PROV.generatedAtTime))
                        
                        if (not invalid_times or len(invalid_times) > 1) and next_gen_time and len(next_gen_time) == 1:
                            # Remove existing invalidation times if any
                            for it in invalid_times:
                                context.remove((snapshot, PROV.invalidatedAtTime, it))
                            # Add new invalidation time
                            context.add((snapshot, PROV.invalidatedAtTime, next_gen_time[0]))
                            modified = True
            
            if modified:
                # Save the modified graph
                with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip_out:
                    # Save as JSON-LD
                    jsonld_data = g.serialize(format='json-ld')
                    zip_out.writestr('provenance.json', jsonld_data)
                
                return str(prov_file_path)
    
    except Exception as e:
        tqdm.write(f"Error processing {prov_file_path}: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Fix provenance files in parallel")
    parser.add_argument('input_dir', type=str, help="Directory containing provenance files")
    parser.add_argument('output_dir', type=str, help="Directory for fixed provenance files")
    parser.add_argument('--processes', type=int, default=cpu_count(),
                       help="Number of parallel processes (default: number of CPU cores)")
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Collect all provenance files
    prov_files = []
    for root, _, files in os.walk(args.input_dir):
        for file in files:
            if file.endswith('se.zip'):
                prov_files.append(os.path.join(root, file))
    
    # Process files in parallel with progress bar
    with Pool(processes=args.processes) as pool:
        tasks = [(f, args.output_dir) for f in prov_files]
        
        # Process files with progress bar
        fixed_files = list(tqdm(
            pool.imap_unordered(fix_provenance_file, tasks),
            total=len(tasks),
            desc="Fixing provenance files"
        ))
        
        # Report results
        fixed_files = [f for f in fixed_files if f is not None]
        print(f"\nFixed {len(fixed_files)} files")

if __name__ == "__main__":
    main()