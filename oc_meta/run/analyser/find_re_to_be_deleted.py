import argparse
import json
from rdflib import URIRef
from oc_ocdm.support.support import find_paths
from time_agnostic_library.agnostic_query import VersionQuery
from tqdm import tqdm
from datetime import datetime
import multiprocessing

def process_manifestations(args):
    br_uri, rdf_dir, config_dict = args
    _, cur_file_path = find_paths(URIRef(f"{br_uri}/prov/se/1"), rdf_dir, "https://w3id.org/oc/meta/", "_", 10000, 1000, True, None)
    cur_file_path = cur_file_path.replace('.json', '.zip')
    config_dict['provenance']['file_paths'] = [cur_file_path]

    query = f'''
        SELECT DISTINCT ?re
        WHERE {{
            <{br_uri}> <http://purl.org/vocab/frbr/core#embodiment> ?re.
        }}
    '''
    agnostic_query = VersionQuery(query, None, False, config_path=None, config_dict=config_dict)
    agnostic_result, _ = agnostic_query.run_agnostic_query()
    sorted_agnostic_result = sorted(agnostic_result.items(), key=lambda x: datetime.strptime(x[0], "%Y-%m-%dT%H:%M:%S"))

    first_relevant_resources = None
    for _, resources in sorted_agnostic_result:
        if resources:
            first_relevant_resources = {res[0] for res in resources}
            break

    resources_to_delete = set()
    if first_relevant_resources:
        for _, resources in sorted_agnostic_result:
            if resources:
                current_resources = {res[0] for res in resources}
                resources_to_delete.update(current_resources - first_relevant_resources)

    return resources_to_delete

def main():
    parser = argparse.ArgumentParser(description="Remove multiple manifestations from bibliographic resources.")
    parser.add_argument("json_file", type=str, help="Path to the JSON file containing information about bibliographic resources with multiple manifestations")
    parser.add_argument("rdf_dir", type=str, help="Path to the OpenCitations Meta RDF directory")
    parser.add_argument("time_agnostic_config", type=str, help="Path to the time agnostic library configuration file")
    args = parser.parse_args()

    with open(args.json_file, 'r') as file:
        data = json.load(file)

    with open(args.time_agnostic_config, 'r') as f:
        config_dict = json.load(f)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    tasks = [(br_uri, args.rdf_dir, config_dict) for br_uri, _ in data.items()]
    results = list(tqdm(pool.imap_unordered(process_manifestations, tasks), total=len(tasks)))

    pool.close()
    pool.join()

    deletions = list()
    for result in results:
        deletions.extend(result)

    with open('re_to_be_deleted.json', 'w') as f:
        json.dump(deletions, f, indent=4)

    print(f"Results saved to 're_to_be_deleted.json'")


if __name__ == "__main__":
    main()