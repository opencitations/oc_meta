#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2023 Arcangelo Massari <arcangelo.massari@unibo.it>
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
# SOFTWARE.


import warnings

import pandas as pd
from tqdm import tqdm

warnings.simplefilter('error', pd.errors.DtypeWarning)

filepath = 'C:/Users/arcangelo.massari2/Downloads/icite_metadata/icite_metadata.csv'
filter = ["pmid", "doi", "title", "authors", "year", "journal", "references"]
pmid_found = set()
duplicated_pmids = list()
with open(filepath, 'r', encoding='utf8') as f:
    number_of_rows = sum(1 for _ in f) - 1
pbar = tqdm(total=number_of_rows)
dtype={'pmid': str, 'doi': str, 'title': str, 'authors': str, 'year': str, 'journal': str, 'references': str}
pmid_by_doi = dict()
count_of_multiple_pmid_by_doi = set()
try:
    with pd.read_csv(filepath, usecols=filter, chunksize=100000, dtype=dtype) as reader:
        for chunk in reader:
            chunk.fillna("", inplace=True)
            df_dict_list = chunk.to_dict("records")
            filt_values = [d for d in df_dict_list if (d.get("cited_by") or d.get("references"))]
            for item in filt_values:
                pmid = item['pmid']
                doi = item['doi']
                if doi:
                    pmid_by_doi.setdefault(doi, {'pmid': set(), 'title': ''})
                    pmid_by_doi[doi]['pmid'].add(pmid)
                    pmid_by_doi[doi]['title'] = item['title']
                    if len(pmid_by_doi[doi]) > 1:
                        count_of_multiple_pmid_by_doi.add(doi)
            pbar.update(len(chunk))
except pd.errors.DtypeWarning:
    print(item)
    raise(pd.errors.DtypeWarning)

pbar.close()
print(len(count_of_multiple_pmid_by_doi))