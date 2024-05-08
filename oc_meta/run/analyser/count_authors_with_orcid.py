import argparse
import zipfile
from tqdm import tqdm
from multiprocessing import Pool
import rdflib
from pathlib import Path
import os


def extract_author_uris(zip_path):
    graph = rdflib.ConjunctiveGraph()
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with zip_ref.open(zip_ref.namelist()[0]) as rdf_file:
            graph.parse(rdf_file, format='json-ld')
                        
    query = """
    SELECT DISTINCT ?ra WHERE {
        ?ar <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author> ;
           <http://purl.org/spar/pro/isHeldBy> ?ra .
    }
    """
    results = graph.query(query)
    return {str(row.ra) for row in results}

def extract_identified_ras(zip_path):
    graph = rdflib.ConjunctiveGraph()
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with zip_ref.open(zip_ref.namelist()[0]) as rdf_file:
            graph.parse(rdf_file, format='json-ld')

    query = """
    SELECT DISTINCT ?ra WHERE {
        ?ra <http://purl.org/spar/datacite/hasIdentifier> ?id .
    }
    """
    results = graph.query(query)
    return {str(row.ra) for row in results}

def process_directory(directory):
    path_ar = Path(os.path.join(directory, 'ar'))
    zip_files_ar = [f for f in path_ar.rglob('*.zip') if f.name != 'se.zip']
    
    with Pool() as pool:
        authors = set.union(*list(tqdm(pool.imap(extract_author_uris, zip_files_ar), total=len(zip_files_ar), desc="Looking for authors")))
    
    path_ra = Path(os.path.join(directory, 'ra'))
    zip_files_ra = [f for f in path_ra.rglob('*.zip') if f.name != 'se.zip']

    with Pool() as pool:
        identified_ras = set.union(*list(tqdm(pool.imap(extract_identified_ras, zip_files_ra), total=len(zip_files_ra), desc="Looking for identified responsible agents")))

    total_authors = len(authors)
    identified_authors = len(authors.intersection(identified_ras))
    percentage = (identified_authors / total_authors * 100) if total_authors > 0 else 0
    
    return total_authors, identified_authors, percentage

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze RDF data within ZIP files to identify and count authors with specific roles and identifiers.")
    parser.add_argument("directory", help="Path to the directory containing ZIP files for analysis")
    args = parser.parse_args()
    
    total_authors, identified_authors, percentage = process_directory(args.directory)
    print(f"There are {total_authors} authors, of which {identified_authors} have an identifier ({percentage:.2f}%).")