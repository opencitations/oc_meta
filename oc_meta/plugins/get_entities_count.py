from __future__ import annotations

import json
import os
import zipfile
from multiprocessing import Manager, Pool

from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data

# Directories
rdf_base_directory = 'test/endgame/output/rdf'

# SPARQL Endpoint URL
endpoint_url = "http://127.0.0.1:9999/blazegraph/sparql"

# Define the types for each category
entity_types = {
    # 'ra': 'http://xmlns.com/foaf/0.1/Agent',
    'br': 'http://purl.org/spar/fabio/Expression'
    # 're': 'http://purl.org/spar/fabio/Manifestation',
    # 'ar': 'http://purl.org/spar/pro/RoleInTime',
    # 'id': 'http://purl.org/spar/datacite/Identifier'
}

# Define the function to process a single JSON file for entity counts
def process_json_file(filepath):
    result_dict = {'ra': 0, 'br': 0, 're': 0, 'id': 0, 'ar': 0}
    entity_ids = set()
    with open(filepath, 'r', encoding='utf-8') as jsonfile:
        json_data = json.load(jsonfile)
        for item in json_data:
            if '@graph' in item:
                for node in item['@graph']:
                    if '@id' in node:
                        key = node['@id'].split('https://w3id.org/oc/meta/')[1].split('/')[0]
                        result_dict[key] += 1
                        entity_ids.add(node['@id'])
    return result_dict, entity_ids

# Define the function to process a single ZIP file for entity counts
def process_zip_file(zip_path):
    result_dict = {'ra': 0, 'br': 0, 're': 0, 'id': 0, 'ar': 0}
    entity_ids = set()
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for zip_info in zip_ref.infolist():
            if zip_info.filename.endswith('.json'):
                with zip_ref.open(zip_info) as jsonfile:
                    json_data = json.load(jsonfile)
                    for item in json_data:
                        if '@graph' in item:
                            for node in item['@graph']:
                                if '@id' in node:
                                    key = node['@id'].split('https://w3id.org/oc/meta/')[1].split('/')[0]
                                    result_dict[key] += 1
                                    entity_ids.add(node['@id']), entity_ids
    return result_dict, entity_ids

# Helper function to dispatch the correct processing function based on file type
def process_file(args):
    filepath, file_type, rdf_base_directory = args

    if file_type == 'json':
        result_dict, entity_ids = process_json_file(filepath)
    elif file_type == 'zip':
        result_dict, entity_ids = process_zip_file(filepath)

    entities_with_provenance = set()
    base_file_name = os.path.splitext(os.path.basename(filepath))[0]
    prov_file_path = os.path.join(rdf_base_directory, base_file_name, 'prov', 'se.json')
    if os.path.isfile(prov_file_path):
        with open(prov_file_path, 'r', encoding='utf-8') as prov_file:
            prov_data = json.load(prov_file)
            for entity_id in entity_ids:
                for item in prov_data:
                    for node in item['@graph']:
                        try:
                            if node['http://www.w3.org/ns/prov#specializationOf'][0]['@id'] == entity_id:
                                entities_with_provenance.add(entity_id)
                        except Exception:
                            print(node)
    if entity_ids - entities_with_provenance:
        print('oh og')
    return result_dict

# Wrapper function to process all files using multiprocessing with progress bar
def process_all_files(rdf_base_directory):
    tasks = []
    for root, dirs, files in os.walk(rdf_base_directory):
        if 'prov' in root:
            continue  # Skip any files within 'prov' folders
        for filename in files:
            if filename.endswith('.json'):
                tasks.append((os.path.join(root, filename), 'json', root))
            elif filename.endswith('.zip'):
                tasks.append((os.path.join(root, filename), 'zip', root))

    # Process files in parallel with a progress bar
    with Pool() as pool:
        manager = Manager()
        final_result_dict = manager.dict({'ra': 0, 'br': 0, 're': 0, 'id': 0, 'ar': 0})
        
        def update_result(result):
            # Merge the results safely within the manager dict context
            for key in result.keys():
                final_result_dict[key] += result[key]
            pbar.update()  # Update the progress bar
        
        pbar = tqdm(total=len(tasks), desc="Processing files")

        # Setup a list to hold the results of the apply_async calls for later retrieval if needed
        for task in tasks:
            # Append the async result object to a list to retrieve later if needed
            pool.apply_async(process_file, args=(task,), callback=update_result)

        # Close the pool and wait for the work to finish
        pool.close()
        pool.join()

        # There is no need to retrieve results since they are updated in the manager.dict via callbacks
        pbar.close()  # Close the tqdm bar after all processes are done

    return dict(final_result_dict)  # Convert the manager dict to a regular dict

if __name__ == '__main__':
    # Execute the processing of all files
    rdf_result_dict = process_all_files(rdf_base_directory)
    # print(rdf_result_dict)
    # SPARQL query to count entities of a certain type
    triplestore_dict = {key: 0 for key in entity_types.keys()}
    for entity_code, entity_type in entity_types.items():
        sparql_query = f"""
        SELECT (COUNT(DISTINCT ?entity) as ?count)
        WHERE {{
            ?entity a <{entity_type}> .
        }}
        """

        # Execute the query
        sparql = SPARQLWrapper(endpoint_url)
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results_sparql = sparql.query().convert()

        # Extract the count and store it in the dictionary
        for result in results_sparql["results"]["bindings"]:
            count = result["count"]["value"]
            triplestore_dict[entity_code] = int(count)

    # Convert sets to counts and prepare final dictionary
    final_results = dict()
    final_results['rdf'] = rdf_result_dict
    final_results['triplestore'] = triplestore_dict

    print(final_results)
