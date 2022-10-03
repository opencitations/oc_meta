# ISC License (ISC)
# ==================================
# Copyright 2022 Arcangelo Massari

# Permission to use, copy, modify, and/or distribute this software for any purpose with or
# without fee is hereby granted, provided that the above copyright notice and this permission
# notice appear in all copies.

# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS
# SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
# THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
# OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.


from oc_meta.lib.file_manager import get_csv_data, write_csv
from oc_meta.plugins.crossref.crossref_processing import CrossrefProcessing
from pebble import ThreadPool, ProcessFuture
from psutil import Process
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Tuple, List
import json
import os
import requests
import requests_cache


CITING_DOI = '10.1007/978-3-540-88851-2'
COCI_REST_API = 'https://w3id.org/oc/index/coci/api/v1'
CROSSREF_REST_API = 'https://api.crossref.org'
CROSSREF_DUMP_DIR = 'D:/meta_input'
ORCID_INDEX = 'D:/pre_data/doi_orcid_index'
PUBLISHERS_FILEPATH = 'D:/pre_data/publishers.csv'


def _requests_retry_session(
    tries=3,
    status_forcelist=(500, 502, 504, 520, 521),
    session=None
) -> Session:
    session = session or requests.Session()
    retry = Retry(
        total=tries,
        read=tries,
        connect=tries,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def handle_request(url:str, cache_path:str='', timeout:int=5, error_log_dict:dict=dict()) -> json:
    if cache_path != '':
        requests_cache.install_cache(cache_path)
    try:
        data = _requests_retry_session().get(url, timeout=timeout)
        if data.status_code == 200:
            return data.json()
        else:
            error_log_dict[url] = data.status_code
    except Exception as e:
        error_log_dict[url] = str(e)

def get_citations_and_metadata(crossref_processing:CrossrefProcessing, doi:str) -> Tuple[List[dict], List[dict]]:
    references_request = COCI_REST_API + '/references/' + doi
    references_data = handle_request(url=references_request, cache_path='./examples_cache')
    if references_data is None:
        return [], []
    citations_csv = [
        {'citing_id': f"doi:{citation['citing']}", 'citing_publication_date': citation['creation'], 'cited_id': f"doi:{citation['cited']}", 'cited_publication_date': ''} 
        for citation in references_data]
    dois_to_look_metadata_for = [citation['cited_id'] for citation in citations_csv]
    dois_to_look_metadata_for.append(doi)
    metadata_requests= [CROSSREF_REST_API + '/works/' + doi for doi in dois_to_look_metadata_for]
    metadata_csv = filter(None, [handle_request(url, cache_path='./examples_cache') for url in metadata_requests])
    metadata_csv = [crossref_processing.csv_creator(item['message']) for item in metadata_csv]
    for citation in citations_csv:
        citation['cited_publication_date'] = next((el['pub_date'] for el in metadata_csv if f"doi:{citation['cited_id']}" in el['id'].split()), '')
    return citations_csv, metadata_csv

def get_csv_data_by_requirements(
        crossref_processing:CrossrefProcessing,
        file_to_be_processed:str, 
        citations_number_range:Tuple[int, int]=(5,10),
        citing_entity_type:str='journal article',
        required_types = {'journal article'}) -> Tuple[List[dict], List[dict]]:
    data = get_csv_data(file_to_be_processed)
    for row in data:
        if row['type'] == citing_entity_type or not citing_entity_type:
            ids = row['id'].split()
            first_doi = next((id.replace('doi:', '') for id in ids if id.startswith('doi:')), None)
            if first_doi:
                print(f"[examples_generator:INFO] I'm looking for citations and metadata related to DOI {first_doi}")
                citations_csv, metadata_csv = get_citations_and_metadata(crossref_processing, first_doi)
                min_citations = True if not citations_number_range[0] else len(citations_csv) >= citations_number_range[0]
                max_citations = True if not citations_number_range[1] else len(citations_csv) <= citations_number_range[1]
                if min_citations and max_citations \
                    and all([any([br_metadata['type'] != citing_entity_type if required_type == 'other' else br_metadata['type'] == required_type 
                            for br_metadata in metadata_csv]) for required_type in required_types]):
                        return citations_csv, metadata_csv
                else:
                    print('[examples_generator:INFO] The requirements have not been met')
    return [], []

def dump_citations_and_metadata_csvs(task_output:ProcessFuture) -> None:
    citations_csv, metadata_csv = task_output.result()
    if citations_csv and metadata_csv:
        write_csv('./example_citations.csv', citations_csv)
        write_csv('./example_metadata.csv', metadata_csv)
        p = Process(pid)
        p.terminate()

def initializer():
    global pid
    pid = os.getpid()

if __name__ == '__main__': # pragma: no cover
    max_workers = os.cpu_count()
    files_to_be_processed = os.listdir(CROSSREF_DUMP_DIR)
    if os.path.exists('./example_citations.csv') and os.path.exists('./example_metadata.csv'):
        os.remove('./example_citations.csv')
        os.remove('./example_metadata.csv')
    crossref_processing = CrossrefProcessing(orcid_index=ORCID_INDEX, publishers_filepath=PUBLISHERS_FILEPATH)
    with ThreadPool(max_workers=max_workers, initializer=initializer) as executor:
        for file_to_be_processed in files_to_be_processed:
            future:ProcessFuture = executor.schedule(
                function=get_csv_data_by_requirements, 
                args=(crossref_processing, os.path.join(CROSSREF_DUMP_DIR, file_to_be_processed), (9,None), 'journal article', {'journal article', 'other'})) 
            future.add_done_callback(dump_citations_and_metadata_csvs)    


