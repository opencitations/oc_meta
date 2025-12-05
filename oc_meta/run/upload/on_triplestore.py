import argparse
import os

from oc_meta.run.upload.cache_manager import CacheManager
from sparqlite import SPARQLClient
from tqdm import tqdm


def save_failed_query_file(filename, failed_file):
    with open(failed_file, "a", encoding="utf8") as failed_file:
        failed_file.write(f"{filename}\n")


def remove_stop_file(stop_file):
    if os.path.exists(stop_file):
        os.remove(stop_file)
        print(f"Existing stop file {stop_file} has been removed.")


def upload_sparql_updates(
    endpoint,
    folder,
    failed_file="failed_queries.txt",
    stop_file=".stop_upload",
    cache_manager=None,
    description="Processing files",
    show_progress=True,
):
    """
    Upload SPARQL updates to the triplestore.

    Args:
        endpoint: URL of the SPARQL endpoint
        folder: Folder containing SPARQL files to process
        failed_file: File to record failed queries
        stop_file: File to stop the process
        cache_manager: CacheManager instance. If None, a new one will be created.
    """
    if not os.path.exists(folder):
        return

    if cache_manager is None:
        cache_manager = CacheManager()

    all_files = [f for f in os.listdir(folder) if f.endswith(".sparql")]
    files_to_process = [f for f in all_files if f not in cache_manager]

    if not files_to_process:
        return

    iterator = tqdm(files_to_process, desc=description) if show_progress else files_to_process
    with SPARQLClient(endpoint, max_retries=3, backoff_factor=5) as client:
        for file in iterator:
            if os.path.exists(stop_file):
                print(f"\nStop file {stop_file} detected. Interrupting the process...")
                break

            file_path = os.path.join(folder, file)

            with open(file_path, "r", encoding="utf-8") as f:
                query = f.read().strip()

            if not query:
                cache_manager.add(file)
                continue

            try:
                client.update(query)
                cache_manager.add(file)
            except Exception as e:
                print(f"Failed to execute {file}: {e}")
                save_failed_query_file(file, failed_file)


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
        args.failed_file,
        args.stop_file,
    )


if __name__ == "__main__":
    main()
