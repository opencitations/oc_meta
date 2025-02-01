import argparse
import os
import time

from oc_meta.run.split_insert_and_delete import process_sparql_file
from oc_meta.run.upload.cache_manager import CacheManager
from oc_meta.run.upload.triplestore_connection import TriplestoreConnection
from tqdm import tqdm


def save_failed_query_file(filename, failed_file):
    with open(failed_file, "a", encoding="utf8") as failed_file:
        failed_file.write(f"{filename}\n")


def execute_sparql_update(endpoint, query):
    attempt = 0
    max_attempts = 3
    wait_time = 5  # Initial wait time in seconds

    connection = TriplestoreConnection(endpoint)

    while attempt < max_attempts:
        try:
            success = connection.execute_update(query)
            if success:
                return True
            raise Exception("Query execution failed")
        except Exception as e:
            attempt += 1
            if attempt < max_attempts:
                print(
                    f"[3] Attempt {attempt} failed. Could not execute SPARQL update due to communication problems: {e}"
                )
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= 2  # Double the wait time for the next attempt
            else:
                print(
                    f"[3] All {max_attempts} attempts failed. Could not execute SPARQL update due to communication problems: {e}"
                )
                return False


def generate_sparql_queries(quads_to_add, quads_to_remove, batch_size):
    queries = []

    if quads_to_add:
        for i in range(0, len(quads_to_add), batch_size):
            insert_query = "INSERT DATA {\n"
            batch = quads_to_add[i : i + batch_size]
            for graph in set(q[-1] for q in batch):
                insert_query += f"  GRAPH {graph} {{\n"
                for quad in batch:
                    if quad[-1] == graph:
                        insert_query += "    " + " ".join(quad[:-1]) + " .\n"
                insert_query += "  }\n"
            insert_query += "}\n"
            queries.append(insert_query)

    if quads_to_remove:
        for i in range(0, len(quads_to_remove), batch_size):
            delete_query = "DELETE DATA {\n"
            batch = quads_to_remove[i : i + batch_size]
            for graph in set(q[-1] for q in batch):
                delete_query += f"  GRAPH {graph} {{\n"
                for quad in batch:
                    if quad[-1] == graph:
                        delete_query += "    " + " ".join(quad[:-1]) + " .\n"
                delete_query += "  }\n"
            delete_query += "}\n"
            queries.append(delete_query)

    return queries


def split_queries(file_path, batch_size):
    quads_to_add, quads_to_remove = process_sparql_file(file_path)
    return generate_sparql_queries(quads_to_add, quads_to_remove, batch_size)


def remove_stop_file(stop_file):
    if os.path.exists(stop_file):
        os.remove(stop_file)
        print(f"Existing stop file {stop_file} has been removed.")


def upload_sparql_updates(
    endpoint,
    folder,
    batch_size,
    cache_file="ts_upload_cache.json",
    failed_file="failed_queries.txt",
    stop_file=".stop_upload",
    cache_manager=None,
):
    """
    Carica gli aggiornamenti SPARQL sul triplestore.

    Args:
        endpoint (str): URL dell'endpoint SPARQL
        folder (str): Cartella contenente i file SPARQL da processare
        batch_size (int): Numero di triple da includere in ogni batch
        cache_file (str, optional): File per il caching dei file processati. Default 'ts_upload_cache.json'
        failed_file (str, optional): File per registrare le query fallite. Default 'failed_queries.txt'
        stop_file (str, optional): File per interrompere il processo. Default '.stop_upload'
        cache_manager (CacheManager, optional): Instance of CacheManager to use. If None, a new one will be created.
    """
    if not os.path.exists(folder):
        return

    if cache_manager is None:
        cache_manager = CacheManager(cache_file)
    failed_files = []

    # Misura tempo scansione directory e filtraggio file
    all_files = [f for f in os.listdir(folder) if f.endswith(".sparql")]
    files_to_process = [f for f in all_files if f not in cache_manager]
    print(
        f"Found {len(files_to_process)} files to process out of {len(all_files)} total files"
    )

    for file in tqdm(files_to_process, desc="Processing files"):
        if os.path.exists(stop_file):
            print(f"\nStop file {stop_file} detected. Interrupting the process...")
            break

        file_path = os.path.join(folder, file)
        queries = split_queries(file_path, batch_size)

        if not queries:
            save_failed_query_file(file, failed_file)
            continue

        all_queries_successful = True

        for query in queries:
            success = execute_sparql_update(endpoint, query)
            if not success:
                save_failed_query_file(file, failed_file)
                all_queries_successful = False
                break

        if all_queries_successful:
            cache_manager.add(file)

    if failed_files:
        print("Files with failed queries:")
        for file in failed_files:
            print(file)


def main():
    parser = argparse.ArgumentParser(
        description="Execute SPARQL update queries on a triple store."
    )
    parser.add_argument("endpoint", type=str, help="Endpoint URL of the triple store")
    parser.add_argument(
        "folder",
        type=str,
        help="Path to the folder containing SPARQL update query files",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=10,
        help="Number of quadruples to include in a batch (default: 10)",
    )
    parser.add_argument(
        "--cache_file",
        type=str,
        default="ts_upload_cache.json",
        help="Path to cache file",
    )
    parser.add_argument(
        "--failed_file",
        type=str,
        default="failed_queries.txt",
        help="Path to failed queries file",
    )
    parser.add_argument(
        "--stop_file", type=str, default=".stop_upload", help="Path to stop file"
    )

    args = parser.parse_args()

    remove_stop_file(args.stop_file)

    upload_sparql_updates(
        args.endpoint,
        args.folder,
        args.batch_size,
        args.cache_file,
        args.failed_file,
        args.stop_file,
    )


if __name__ == "__main__":
    main()
