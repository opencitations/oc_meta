import argparse
import csv
import os
import random
import re
import time
import zipfile
from functools import partial
from multiprocessing import Pool, cpu_count

import filelock
import yaml
from oc_meta.plugins.editor import MetaEditor
from rdflib import RDF, ConjunctiveGraph, Literal, Namespace, URIRef
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm

DATACITE = "http://purl.org/spar/datacite/"
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
PROV = Namespace("http://www.w3.org/ns/prov#")
DCTERMS = Namespace("http://purl.org/dc/terms/")


def read_csv(csv_file):
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        return list(reader)


def sparql_query_with_retry(sparql, max_retries=3, initial_delay=1, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (backoff_factor**attempt)
            time.sleep(delay + random.uniform(0, 1))


def check_agent_constraints(g: ConjunctiveGraph, entity):
    issues = []

    # Check type (must be exactly one: foaf:Agent)
    types = list(g.objects(entity, RDF.type, unique=True))
    if not types:
        issues.append(f"Entity {entity} has no type")
    elif len(types) > 1:
        issues.append(f"Entity {entity} has multiple types")
    elif URIRef(FOAF + "Agent") not in types:
        issues.append(f"Entity {entity} is not a foaf:Agent")

    # Check identifiers
    identifiers = list(
        g.objects(entity, URIRef(DATACITE + "hasIdentifier"), unique=True)
    )
    if not identifiers:
        issues.append(f"Entity {entity} has no datacite:hasIdentifier")

    # Check name properties (must have at least one)
    has_name = False
    names = list(g.objects(entity, FOAF.name, unique=True))
    given_names = list(g.objects(entity, FOAF.givenName, unique=True))
    family_names = list(g.objects(entity, FOAF.familyName, unique=True))

    if names or given_names or family_names:
        has_name = True

    if not has_name:
        issues.append(
            f"Entity {entity} has no name properties (name, givenName, or familyName)"
        )

    return issues


def check_entity_sparql(sparql_endpoint, entity_uri, is_surviving):
    sparql = SPARQLWrapper(sparql_endpoint)
    has_issues = False

    # Query to check if the entity exists
    exists_query = f"""
    ASK {{
        <{entity_uri}> ?p ?o .
    }}
    """
    sparql.setQuery(exists_query)
    sparql.setReturnFormat(JSON)
    exists_results = sparql_query_with_retry(sparql)

    if exists_results["boolean"]:
        if not is_surviving:
            tqdm.write(f"Error in SPARQL: Merged entity {entity_uri} still exists")
            has_issues = True
    else:
        if is_surviving:
            tqdm.write(f"Error in SPARQL: Surviving entity {entity_uri} does not exist")
            has_issues = True
        return has_issues

    # Check type constraints
    types_query = f"""
    SELECT ?type WHERE {{
        <{entity_uri}> a ?type .
    }}
    """
    sparql.setQuery(types_query)
    sparql.setReturnFormat(JSON)
    types_results = sparql_query_with_retry(sparql)

    types = [result["type"]["value"] for result in types_results["results"]["bindings"]]
    if not types:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has no type")
        has_issues = True
    elif len(types) > 1:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has multiple types")
        has_issues = True
    elif FOAF + "Agent" not in types:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} is not a foaf:Agent")
        has_issues = True

    # Check name properties
    names_query = f"""
    SELECT ?name ?givenName ?familyName WHERE {{
        OPTIONAL {{ <{entity_uri}> <{FOAF}name> ?name }}
        OPTIONAL {{ <{entity_uri}> <{FOAF}givenName> ?givenName }}
        OPTIONAL {{ <{entity_uri}> <{FOAF}familyName> ?familyName }}
    }}
    """
    sparql.setQuery(names_query)
    sparql.setReturnFormat(JSON)
    names_results = sparql_query_with_retry(sparql)

    has_name = False
    for result in names_results["results"]["bindings"]:
        if any(key in result for key in ["name", "givenName", "familyName"]):
            has_name = True
            break

    if not has_name:
        tqdm.write(f"Error in SPARQL: Entity {entity_uri} has no name properties")
        has_issues = True

    # Check identifiers
    identifiers_query = f"""
    SELECT ?identifier WHERE {{
        <{entity_uri}> <{DATACITE}hasIdentifier> ?identifier .
    }}
    """
    sparql.setQuery(identifiers_query)
    sparql.setReturnFormat(JSON)
    identifiers_results = sparql_query_with_retry(sparql)

    identifiers = [
        result["identifier"]["value"]
        for result in identifiers_results["results"]["bindings"]
    ]
    if not identifiers:
        tqdm.write(
            f"Error in SPARQL: Entity {entity_uri} has no datacite:hasIdentifier"
        )
        has_issues = True

    return has_issues


def get_entity_triples(sparql_endpoint, entity_uri):
    sparql = SPARQLWrapper(sparql_endpoint)
    query = f"""
    SELECT ?g ?s ?p ?o
    WHERE {{
        GRAPH ?g {{
            {{
                <{entity_uri}> ?p ?o .
                BIND(<{entity_uri}> AS ?s)
            }}
            UNION
            {{
                ?s ?p <{entity_uri}> .
                BIND(<{entity_uri}> AS ?o)
            }}
        }}
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql_query_with_retry(sparql)

    triples = []
    for result in results["results"]["bindings"]:
        graph = result["g"]["value"]
        subject = URIRef(result["s"]["value"])
        predicate = URIRef(result["p"]["value"])

        obj_data = result["o"]
        if obj_data["type"] == "uri":
            obj = URIRef(obj_data["value"])
        else:
            datatype = obj_data.get("datatype")
            obj = Literal(
                obj_data["value"], datatype=URIRef(datatype) if datatype else None
            )

        triples.append((graph, subject, predicate, obj))

    return triples


def check_entity_provenance(
    entity_uri, is_surviving, prov_graph: ConjunctiveGraph, prov_file_path
):
    def extract_snapshot_number(snapshot_uri):
        match = re.search(r"/prov/se/(\d+)$", str(snapshot_uri))
        if match:
            return int(match.group(1))
        return 0

    snapshots = list(
        prov_graph.subjects(PROV.specializationOf, entity_uri, unique=True)
    )
    if len(snapshots) <= 1:
        tqdm.write(
            f"Error in provenance file {prov_file_path}: Less than two snapshots found for entity {entity_uri}"
        )
        return

    snapshots.sort(key=extract_snapshot_number)
    for i, snapshot in enumerate(snapshots):
        snapshot_number = extract_snapshot_number(snapshot)
        if snapshot_number != i + 1:
            tqdm.write(
                f"Error in provenance file {prov_file_path}: Snapshot {snapshot} has unexpected number {snapshot_number}, expected {i + 1}"
            )

        gen_time = prov_graph.value(snapshot, PROV.generatedAtTime)
        if gen_time is None:
            tqdm.write(
                f"Error in provenance file {prov_file_path}: Snapshot {snapshot} has no generation time"
            )

        if i < len(snapshots) - 1 or not is_surviving:
            invalidation_time = prov_graph.value(snapshot, PROV.invalidatedAtTime)
            if invalidation_time is None:
                tqdm.write(
                    f"Error in provenance file {prov_file_path}: Non-last snapshot {snapshot} has no invalidation time"
                )
        elif (
            is_surviving
            and prov_graph.value(snapshot, PROV.invalidatedAtTime) is not None
        ):
            tqdm.write(
                f"Error in provenance file {prov_file_path}: Last snapshot of surviving entity {snapshot} should not have invalidation time"
            )

        description = prov_graph.value(snapshot, DCTERMS.description)
        is_merge_snapshot = description and "has been merged with" in str(description)

        derived_from = list(
            prov_graph.objects(snapshot, PROV.wasDerivedFrom, unique=True)
        )
        if i == 0:  # First snapshot
            if derived_from:
                tqdm.write(
                    f"Error in provenance file {prov_file_path}: First snapshot {snapshot} should not have prov:wasDerivedFrom relation"
                )
        elif is_merge_snapshot:
            if len(derived_from) < 2:
                tqdm.write(
                    f"Error in provenance file {prov_file_path}: Merge snapshot {snapshot} should be derived from at least two snapshots"
                )
        else:  # Regular modification snapshot
            if len(derived_from) != 1:
                tqdm.write(
                    f"Error in provenance file {prov_file_path}: Regular modification snapshot {snapshot} should have exactly one prov:wasDerivedFrom relation"
                )
            else:
                previous_snapshot = snapshots[i - 1]
                if derived_from[0] != previous_snapshot:
                    tqdm.write(
                        f"Error in provenance file {prov_file_path}: Snapshot {snapshot} is not derived from the previous snapshot"
                    )


def process_file_group(args):
    file_path, entities, sparql_endpoint, query_output_dir = args

    if file_path is None:
        for entity, is_surviving, surviving_entity in entities:
            tqdm.write(f"Error: Could not find file for entity {entity}")
            has_issues = check_entity_sparql(sparql_endpoint, entity, is_surviving)
            if has_issues and not is_surviving:
                triples = get_entity_triples(sparql_endpoint, entity)
                combined_query = generate_update_query(
                    entity, surviving_entity, triples
                )
                query_file_path = os.path.join(
                    query_output_dir, f"update_{entity.split('/')[-1]}.sparql"
                )
                with open(query_file_path, "w") as f:
                    f.write(combined_query)
        return

    # Create lock files
    data_lock_file = f"{file_path}.lock"
    prov_lock_file = f"{file_path.replace('.zip', '')}/prov/se.zip.lock"

    data_lock = filelock.FileLock(data_lock_file)
    prov_lock = filelock.FileLock(prov_lock_file)

    try:
        with data_lock.acquire(timeout=60):  # Wait up to 60 seconds for the lock
            try:
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    g = ConjunctiveGraph()
                    for filename in zip_ref.namelist():
                        with zip_ref.open(filename) as file:
                            g.parse(file, format="json-ld")
            except FileNotFoundError:
                for entity, is_surviving, surviving_entity in entities:
                    tqdm.write(f"Error: File not found for entity {entity}")
                    has_issues = check_entity_sparql(
                        sparql_endpoint, entity, is_surviving
                    )
                    if has_issues and not is_surviving:
                        triples = get_entity_triples(sparql_endpoint, entity)
                        combined_query = generate_update_query(
                            entity, surviving_entity, triples
                        )
                        query_file_path = os.path.join(
                            query_output_dir, f"update_{entity.split('/')[-1]}.sparql"
                        )
                        with open(query_file_path, "w") as f:
                            f.write(combined_query)
                return

            prov_file_path = file_path.replace(".zip", "") + "/prov/se.zip"
            try:
                with prov_lock.acquire(
                    timeout=60
                ):  # Wait up to 60 seconds for the lock
                    try:
                        with zipfile.ZipFile(prov_file_path, "r") as zip_ref:
                            prov_graph = ConjunctiveGraph()
                            for filename in zip_ref.namelist():
                                with zip_ref.open(filename) as file:
                                    prov_graph.parse(file, format="json-ld")
                    except FileNotFoundError:
                        prov_graph = None
                    except zipfile.BadZipFile:
                        prov_graph = "badzip"

                    for entity, is_surviving, surviving_entity in entities:
                        if (URIRef(entity), None, None) not in g:
                            if is_surviving:
                                tqdm.write(
                                    f"Error in file {file_path}: Surviving entity {entity} does not exist"
                                )
                        else:
                            if not is_surviving:
                                tqdm.write(
                                    f"Error in file {file_path}: Merged entity {entity} still exists"
                                )
                            else:
                                agent_issues = check_agent_constraints(
                                    g, URIRef(entity)
                                )
                                for issue in agent_issues:
                                    tqdm.write(f"Error in file {file_path}: {issue}")

                        if prov_graph == None:
                            tqdm.write(
                                f"Error: Provenance file not found for entity {entity}"
                            )
                        elif prov_graph == "badzip":
                            tqdm.write(
                                f"Error: Invalid zip file for provenance of entity {entity}"
                            )
                        else:
                            check_entity_provenance(
                                URIRef(entity), is_surviving, prov_graph, prov_file_path
                            )

                        has_issues = check_entity_sparql(
                            sparql_endpoint, entity, is_surviving
                        )
                        if has_issues and not is_surviving:
                            triples = get_entity_triples(sparql_endpoint, entity)
                            combined_query = generate_update_query(
                                entity, surviving_entity, triples
                            )
                            query_file_path = os.path.join(
                                query_output_dir,
                                f"update_{entity.split('/')[-1]}.sparql",
                            )
                            with open(query_file_path, "w") as f:
                                f.write(combined_query)

            except filelock.Timeout:
                tqdm.write(
                    f"Could not acquire lock for provenance file {prov_file_path} within timeout period"
                )

    except filelock.Timeout:
        tqdm.write(
            f"Could not acquire lock for data file {file_path} within timeout period"
        )


def generate_update_query(merged_entity, surviving_entity, triples):
    delete_query = "DELETE DATA {\n"
    insert_query = "INSERT DATA {\n"

    for graph, subject, predicate, obj in triples:
        if subject == URIRef(merged_entity):
            delete_query += (
                f"  GRAPH <{graph}> {{ <{subject}> <{predicate}> {obj.n3()} }}\n"
            )
        elif obj == URIRef(merged_entity):
            delete_query += (
                f"  GRAPH <{graph}> {{ <{subject}> <{predicate}> <{obj}> }}\n"
            )
            insert_query += f"  GRAPH <{graph}> {{ <{subject}> <{predicate}> <{surviving_entity}> }}\n"

    delete_query += "}\n"
    insert_query += "}\n"

    return delete_query + "\n" + insert_query


def process_csv(args, csv_file):
    csv_path, rdf_dir, meta_config_path, sparql_endpoint, query_output_dir = args
    csv_path = os.path.join(csv_path, csv_file)
    data = read_csv(csv_path)
    tasks = []

    meta_editor = MetaEditor(meta_config_path, "")

    for row in data:
        if "Done" not in row or row["Done"] != "True":
            continue

        surviving_entity = row["surviving_entity"]
        merged_entities = row["merged_entities"].split("; ")
        all_entities = [surviving_entity] + merged_entities

        for entity in all_entities:
            file_path = meta_editor.find_file(
                rdf_dir, meta_editor.dir_split, meta_editor.n_file_item, entity, True
            )
            tasks.append(
                (
                    entity,
                    entity == surviving_entity,
                    file_path,
                    rdf_dir,
                    meta_config_path,
                    sparql_endpoint,
                    query_output_dir,
                    surviving_entity,
                )
            )
    return tasks


def main():
    parser = argparse.ArgumentParser(
        description="Check merge process success on files and SPARQL endpoint for responsible agents"
    )
    parser.add_argument(
        "csv_folder", type=str, help="Path to the folder containing CSV files"
    )
    parser.add_argument("rdf_dir", type=str, help="Path to the RDF directory")
    parser.add_argument(
        "--meta_config", type=str, required=True, help="Path to meta configuration file"
    )
    parser.add_argument(
        "--query_output",
        type=str,
        required=True,
        help="Path to the folder where SPARQL queries will be saved",
    )
    args = parser.parse_args()

    with open(args.meta_config, "r") as config_file:
        config = yaml.safe_load(config_file)

    sparql_endpoint = config["triplestore_url"]
    os.makedirs(args.query_output, exist_ok=True)

    csv_files = [f for f in os.listdir(args.csv_folder) if f.endswith(".csv")]

    # Process CSV files to gather tasks
    with Pool(processes=cpu_count()) as pool:
        process_csv_partial = partial(
            process_csv,
            (
                args.csv_folder,
                args.rdf_dir,
                args.meta_config,
                sparql_endpoint,
                args.query_output,
            ),
        )
        all_tasks_list = list(
            tqdm(
                pool.imap(process_csv_partial, csv_files),
                total=len(csv_files),
                desc="Processing CSV files",
            )
        )

    # Flatten the list of tasks
    all_tasks = [task for sublist in all_tasks_list for task in sublist]

    # Group tasks by file path
    tasks_by_file = {}
    for (
        entity,
        is_surviving,
        file_path,
        rdf_dir,
        meta_config_path,
        sparql_endpoint,
        query_output_dir,
        surviving_entity,
    ) in all_tasks:
        if file_path not in tasks_by_file:
            tasks_by_file[file_path] = []
        tasks_by_file[file_path].append((entity, is_surviving, surviving_entity))

    # Convert to list of arguments for parallel processing
    file_groups = [
        (file_path, tasks, sparql_endpoint, args.query_output)
        for file_path, tasks in tasks_by_file.items()
    ]

    # Process each file group in parallel
    with Pool(processes=cpu_count()) as pool:
        list(
            tqdm(
                pool.imap(process_file_group, file_groups),
                total=len(file_groups),
                desc="Processing files",
            )
        )


if __name__ == "__main__":
    main()
