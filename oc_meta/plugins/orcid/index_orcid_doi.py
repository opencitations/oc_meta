#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
# Copyright 2021-2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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


import os

from bs4 import BeautifulSoup
from oc_idmanager import DOIManager
from tqdm import tqdm

from oc_meta.lib.csvmanager import CSVManager


class Index_orcid_doi:
    def __init__(self, output_path:str, threshold:int=10000, low_memory:bool=False, verbose:bool=False):
        self.file_counter = 0
        self.threshold = 10000 if not threshold else int(threshold)
        self.verbose = verbose
        if self.verbose:
            print("[INFO: CSVManager] Loading existing csv file")
        self.doimanager = DOIManager(use_api_service=False)
        self.csvstorage = CSVManager(output_path=output_path, line_threshold=threshold, low_memory=low_memory)
        # ORCIDs are extracted to skip the corresponding files at the first reading of an existing CSV.
        self.cache = self.cache = set(el.split("[")[1][:-1].strip() for _,v in self.csvstorage.data.items() for el in v)
    
    def explorer(self, summaries_path:str) -> None:
        if self.verbose:
            print("[INFO: Index_orcid_doi] Counting files to process")
        files_to_process = [os.path.join(fold,file) for fold, _, files in os.walk(summaries_path) for file in files if file.replace('.xml', '') not in self.cache]
        processed_files = len(self.cache)
        del self.cache
        if self.verbose:
            pbar = tqdm(total=len(files_to_process))
        for file in files_to_process:
            self.finder(file)
            self.file_counter += 1
            cur_file = self.file_counter + processed_files
            if self.file_counter % self.threshold == 0:
                self.csvstorage.dump_data(f'{cur_file-self.threshold+1}-{cur_file}.csv')
            if self.verbose:
                pbar.update(1)
        cur_file = self.file_counter + processed_files
        self.csvstorage.dump_data(f'{cur_file + 1 - (cur_file % self.threshold)}-{cur_file}.csv')
        if self.verbose:
            pbar.close()

    def finder(self, file:str):
        orcid = file.replace('.xml', '')[-19:]
        valid_doi = False
        if file.endswith('.xml'):
            with open(file, 'r', encoding='utf-8') as xml_file:
                xml_soup = BeautifulSoup(xml_file, 'xml')
                ids = xml_soup.findAll('common:external-id')                
                if ids:
                    for el in ids:
                        id_type = el.find('common:external-id-type')
                        rel = el.find('common:external-id-relationship')
                        if id_type and rel:
                            if id_type.get_text().lower() == 'doi' and rel.get_text().lower() == 'self':
                                doi = el.find('common:external-id-value').get_text()
                                doi = self.doimanager.normalise(doi)
                                if doi:
                                    g_name = xml_soup.find('personal-details:given-names')
                                    f_name = xml_soup.find('personal-details:family-name')
                                    if f_name:
                                        f_name = f_name.get_text()
                                        if g_name:
                                            g_name = g_name.get_text()
                                            name = f_name + ', ' + g_name
                                        else:
                                            name = f_name
                                        auto = name + ' [' + orcid + ']'
                                        valid_doi = True
                                        self.csvstorage.add_value(doi, auto)
        if not valid_doi:
            # Save file names where nothing was found, to skip them during the next run
            self.csvstorage.add_value('None', f'[{orcid}]')