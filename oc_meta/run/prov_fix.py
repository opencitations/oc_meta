#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2023 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE

import argparse
import json
import os
import zipfile
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from pebble import ProcessPool
import rdflib
from rdflib import Namespace, Literal, URIRef, ConjunctiveGraph
from rdflib.namespace import RDF, XSD
import tempfile

from tqdm import tqdm


DC = Namespace("http://purl.org/dc/terms/")
PROV = Namespace("http://www.w3.org/ns/prov#")
RESP_AGENT = URIRef("https://w3id.org/oc/meta/prov/pa/1")

def process_zip_file(zip_path):
    g = ConjunctiveGraph()
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as file:
                json_data = json.load(file)
                for graph in json_data:
                    for entity in graph["@graph"]:
                        entity_id = URIRef(entity["@id"])
                        graph_uri = URIRef(f'{entity_id}/prov/')
                        se_uri = URIRef(f"{entity_id}/prov/se/1")
                        g.add((se_uri, RDF.type, PROV.Entity, graph_uri))
                        g.add((se_uri, DC.description, Literal(f"The entity '{entity_id}' has been created."), graph_uri))
                        g.add((se_uri, PROV.generatedAtTime, Literal(datetime.utcnow(), datatype=XSD.dateTime), graph_uri))
                        g.add((se_uri, PROV.specializationOf, entity_id, graph_uri))
                        g.add((se_uri, PROV.wasAttributedTo, RESP_AGENT, graph_uri))
    graph_jsonld = g.serialize(format='json-ld')
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.json') as temp_file:
        temp_file.write(graph_jsonld)
        temp_file_path = temp_file.name
    output_zip_path = os.path.join(os.path.dirname(zip_path), os.path.splitext(os.path.basename(zip_path))[0], 'prov', 'se.zip')
    with zipfile.ZipFile(output_zip_path, 'w') as output_zip:
        output_zip.write(temp_file_path, 'se.json')
    os.remove(temp_file_path)

def find_zip_files_in_subdir(subdir, process_immediately=False):
    if process_immediately:
        if os.path.exists(subdir):
            for root, dirs, files in os.walk(subdir):
                if 'prov' not in root.split(os.sep):
                    for file in files:
                        if file.endswith('.zip'):
                            process_zip_file(os.path.join(root, file))
    else:
        zip_files = []
        if os.path.exists(subdir):
            for root, dirs, files in os.walk(subdir):
                if 'prov' not in root.split(os.sep):
                    for file in files:
                        if file.endswith('.zip'):
                            zip_files.append(os.path.join(root, file))
        return zip_files

def find_subdirs(directory):
    subdirs = []
    for dir in os.listdir(directory):
        subdirs.append(os.path.join(directory, dir))
    return subdirs

def search_zip_files(directory, show_progress):
    main_subdirs = ['ar', 'br', 'ra', 're', 'id']
    subdirs = []

    for main_subdir in main_subdirs:
        main_subdir_path = os.path.join(directory, main_subdir)
        subdirs.extend(find_subdirs(main_subdir_path))

    if show_progress:
        zip_files = []
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(find_zip_files_in_subdir, subdir) for subdir in subdirs]
            for future in futures:
                zip_files.extend(future.result())
        with ProcessPool() as pool:
            results = pool.map(process_zip_file, zip_files, chunksize=1)
            for _ in tqdm(results.result(), total=len(zip_files), desc="Processing ZIP files"):
                pass
    else:
        for subdir in subdirs:
            find_zip_files_in_subdir(subdir, process_immediately=True)

def main():
    parser = argparse.ArgumentParser(description="Process JSON-LD files within ZIP archives")
    parser.add_argument("directory", help="Directory to search for ZIP files")
    parser.add_argument("--progress", help="Show progress bar", action="store_true")
    args = parser.parse_args()
    search_zip_files(args.directory, args.progress)

if __name__ == "__main__":
    main()