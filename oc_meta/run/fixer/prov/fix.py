import argparse
import os
import re
import zipfile
from collections import defaultdict
from datetime import UTC, datetime
from multiprocessing import Pool, cpu_count

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
    prov_file_path = args
    
    try:
        # Dictionary to store modifications for each entity
        entity_modifications = {}
        
        with zipfile.ZipFile(prov_file_path, 'r') as zip_ref:
            g = ConjunctiveGraph()
            
            # Parse all files in the zip
            for filename in zip_ref.namelist():
                with zip_ref.open(filename) as file:
                    g.parse(file, format='json-ld')
            
            modified = False
            # Process each context (provenance graph) in the ConjunctiveGraph
            for context in g.contexts():
                context_uri = str(context.identifier)
                if not context_uri.endswith('/prov/'):
                    continue
                
                # Get the entity URI from the provenance graph URI
                entity_uri = URIRef(get_entity_from_prov_graph(context_uri))
                entity_modifications[str(entity_uri)] = defaultdict(list)
                
                # Get all snapshots for this entity
                snapshots = []
                for s in context.subjects(predicate=None, object=None, unique=True):
                    if '/prov/se/' in str(s):
                        snapshots.append(s)
                
                if not snapshots:
                    continue
                
                # Sort snapshots by number
                snapshots.sort(key=extract_snapshot_number)
                mods = entity_modifications[str(entity_uri)]
                
                # Fix specializationOf
                for snapshot in snapshots:
                    if (snapshot, PROV.specializationOf, entity_uri) not in context:
                        context.add((snapshot, PROV.specializationOf, entity_uri))
                        mods["Added specializationOf"].append(str(snapshot))
                        modified = True
                
                # Fix wasDerivedFrom chain
                for i, snapshot in enumerate(snapshots[1:], 1):
                    derived_from = list(context.objects(snapshot, PROV.wasDerivedFrom))
                    if not derived_from:
                        context.add((snapshot, PROV.wasDerivedFrom, snapshots[i-1]))
                        mods["Added wasDerivedFrom"].append(f"{str(snapshot)} → {str(snapshots[i-1])}")
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
                                    mods["Removed generatedAtTime"].append(f"{str(snapshot)}: {str(gt)}")
                                # Add new generation time
                                context.add((snapshot, PROV.generatedAtTime, prev_invalidation[0]))
                                mods["Added generatedAtTime"].append(f"{str(snapshot)}: {str(prev_invalidation[0])}")
                                modified = True
                        else:
                            # For first snapshot, if no valid generation time exists
                            if not gen_times:
                                default_time = Literal(datetime(year=2022, month=12, day=20, hour=0, minute=0, second=0, tzinfo=UTC).isoformat(), datatype=XSD.dateTime)
                                context.add((snapshot, PROV.generatedAtTime, default_time))
                                mods["Added generatedAtTime"].append(f"{str(snapshot)}: {str(default_time)}")
                                modified = True
                    
                    # Check invalidation time
                    if i < len(snapshots) - 1:  # All except last snapshot
                        invalid_times = list(context.objects(snapshot, PROV.invalidatedAtTime))
                        next_gen_time = list(context.objects(snapshots[i+1], PROV.generatedAtTime))
                        
                        if (not invalid_times or len(invalid_times) > 1) and next_gen_time and len(next_gen_time) == 1:
                            # Remove existing invalidation times if any
                            for it in invalid_times:
                                context.remove((snapshot, PROV.invalidatedAtTime, it))
                                mods["Removed invalidatedAtTime"].append(f"{str(snapshot)}: {str(it)}")
                            # Add new invalidation time
                            context.add((snapshot, PROV.invalidatedAtTime, next_gen_time[0]))
                            mods["Added invalidatedAtTime"].append(f"{str(snapshot)}: {str(next_gen_time[0])}")
                            modified = True
            
            if modified:
                # Save the modified graph back to the same file
                with zipfile.ZipFile(prov_file_path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zip_out:
                    # Save as JSON-LD with UTF-8 encoding and ensure_ascii=False
                    jsonld_data = g.serialize(format='json-ld', encoding='utf-8', ensure_ascii=False)
                    zip_out.writestr('provenance.json', jsonld_data)
                
                return str(prov_file_path), entity_modifications
        
        return None
    
    except Exception as e:
        tqdm.write(f"Error processing {prov_file_path}: {str(e)}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Fix provenance files in parallel")
    parser.add_argument('input_dir', type=str, help="Directory containing provenance files")
    parser.add_argument('--processes', type=int, default=cpu_count(),
                       help="Number of parallel processes (default: number of CPU cores)")
    args = parser.parse_args()
    
    # Collect all provenance files
    prov_files = []
    for root, _, files in os.walk(args.input_dir):
        for file in files:
            if file.endswith('se.zip'):
                prov_files.append(os.path.join(root, file))
    
    # Process files in parallel with progress bar
    with Pool(processes=args.processes) as pool:
        # Process files with progress bar
        results = list(tqdm(
            pool.imap_unordered(fix_provenance_file, prov_files),
            total=len(prov_files),
            desc="Fixing provenance files"
        ))
        
        # Generate detailed per-file report
        print("\nProvenance Fix Report")
        print("=" * 80)
        
        for result in results:
            if result:
                file_path, entity_mods = result
                print(f"\nFile: {file_path}")
                print("-" * 80)
                
                for entity_uri, modifications in entity_mods.items():
                    print(f"\nEntity: {entity_uri}")
                    for mod_type, mod_list in modifications.items():
                        if mod_list:  # Only print modification types that have entries
                            print(f"\n  {mod_type}:")
                            for mod in mod_list:
                                print(f"    - {mod}")

if __name__ == "__main__":
    main()