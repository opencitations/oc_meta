import argparse
import os
from SPARQLWrapper import SPARQLWrapper, POST
from tqdm import tqdm

def execute_sparql_update(endpoint, query):
    try:
        sparql = SPARQLWrapper(endpoint)
        sparql.setMethod(POST)
        sparql.setQuery(query)
        response = sparql.queryAndConvert()
        return response
    except Exception as e:
        print(f"Error: {e}")
        return False

def split_queries(content):
    queries = []
    current_query = []
    in_query = False

    lines = content.split('\n')
    for line in lines:
        stripped_line = line.strip()
        if stripped_line.upper().startswith("INSERT DATA"):
            if in_query:
                queries.append("\n".join(current_query))
                current_query = []
            in_query = True
        if in_query:
            current_query.append(line)
    
    if current_query:
        queries.append("\n".join(current_query))
    
    return queries

def main():
    parser = argparse.ArgumentParser(description='Execute SPARQL update queries on a triple store.')
    parser.add_argument('endpoint', type=str, help='Endpoint URL of the triple store')
    parser.add_argument('folder', type=str, help='Path to the folder containing SPARQL update query files')
    parser.add_argument('--batch_size', type=int, default=10, help='Number of queries to include in a batch (default: 10)')

    args = parser.parse_args()

    files = [f for f in os.listdir(args.folder) if f.endswith('.txt')]

    for file in tqdm(files, desc="Processing files"):
        file_path = os.path.join(args.folder, file)
        with open(file_path, 'r', encoding='utf8') as query_file:
            content = query_file.read()
            queries = split_queries(content)  # Split queries manually but structured
            print(len(queries))
            # batch_size = args.batch_size
            # for i in range(0, len(queries), batch_size):
            #     batch_queries = ";\n".join(queries[i:i + batch_size]) + ";"  # Join batch of queries
            #     success = execute_sparql_update(args.endpoint, batch_queries)
            #     if not success:
            #         print(f"Failed to execute batch starting from query {i} in file {file}")

if __name__ == "__main__":
    main()
