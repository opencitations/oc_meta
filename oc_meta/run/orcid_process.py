#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# Copyright 2019-2020 Fabio Mariani <fabio.mariani555@gmail.com>
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
import re
from argparse import ArgumentParser

from bs4 import BeautifulSoup
from oc_ds_converter.oc_idmanager import DOIManager
from rich.console import Console

from oc_meta.lib.console import create_progress
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.master_of_regex import orcid_pattern


class IndexOrcidDoi:
    def __init__(self, output_path: str, threshold: int = 10000):
        self.file_counter = 0
        self.threshold = threshold
        self.console = Console()
        self.console.print("[cyan][INFO][/cyan] Loading existing CSV files")
        self.orcid_re = re.compile(orcid_pattern)
        self.doimanager = DOIManager(use_api_service=False)
        self.csvstorage = CSVManager(output_path=output_path)
        self.cache = self._build_cache()

    def _build_cache(self) -> set[str]:
        cache = set()
        for values in self.csvstorage.data.values():
            for value in values:
                orcid = self._extract_orcid(value)
                if orcid:
                    cache.add(orcid)
        return cache

    def _extract_orcid(self, text: str) -> str | None:
        match = self.orcid_re.search(text)
        return match.group(0) if match else None

    def explorer(self, summaries_path: str) -> None:
        self.console.print("[cyan][INFO][/cyan] Counting files to process")
        files_to_process = [
            os.path.join(fold, filename)
            for fold, _, files in os.walk(summaries_path)
            for filename in files
            if filename.endswith('.xml') and self._extract_orcid(filename) not in self.cache
        ]
        processed_files = len(self.cache)
        del self.cache
        progress = create_progress()
        with progress:
            task = progress.add_task("Processing files", total=len(files_to_process))
            for file in files_to_process:
                self._process_file(file)
                self.file_counter += 1
                if self.file_counter % self.threshold == 0:
                    start = processed_files + self.file_counter - self.threshold + 1
                    end = processed_files + self.file_counter
                    self.csvstorage.dump_data(f'{start}-{end}.csv')
                progress.advance(task)
        if self.csvstorage.data_to_store:
            start = processed_files + self.file_counter - (self.file_counter % self.threshold) + 1
            end = processed_files + self.file_counter
            self.csvstorage.dump_data(f'{start}-{end}.csv')

    def _process_file(self, file_path: str) -> None:
        orcid = self._extract_orcid(file_path)
        if not orcid:
            return
        with open(file_path, 'r', encoding='utf-8') as xml_file:
            xml_soup = BeautifulSoup(xml_file, 'xml')
        name = self._extract_name(xml_soup)
        author = f'{name} [{orcid}]' if name else f'[{orcid}]'
        valid_doi = False
        for el in xml_soup.find_all('common:external-id'):
            id_type = el.find('common:external-id-type')
            rel = el.find('common:external-id-relationship')
            if not (id_type and rel):
                continue
            if id_type.get_text().lower() != 'doi' or rel.get_text().lower() != 'self':
                continue
            doi_el = el.find('common:external-id-value')
            if not doi_el:
                continue
            doi = self.doimanager.normalise(doi_el.get_text())
            if not doi:
                continue
            valid_doi = True
            self.csvstorage.add_value(doi, author)
        if not valid_doi:
            self.csvstorage.add_value('None', f'[{orcid}]')

    def _extract_name(self, xml_soup: BeautifulSoup) -> str | None:
        family_name_el = xml_soup.find('personal-details:family-name')
        given_name_el = xml_soup.find('personal-details:given-names')
        if family_name_el and given_name_el:
            return f'{family_name_el.get_text()}, {given_name_el.get_text()}'
        if family_name_el:
            return family_name_el.get_text()
        if given_name_el:
            return given_name_el.get_text()
        return None


if __name__ == '__main__':  # pragma: no cover
    arg_parser = ArgumentParser(
        'orcid_process.py',
        description='Build a CSV index of DOIs associated with ORCIDs from XML summary files'
    )
    arg_parser.add_argument('-out', '--output', dest='output_path', required=True,
                            help='Output directory for CSV files')
    arg_parser.add_argument('-s', '--summaries', dest='summaries_path', required=True,
                            help='Directory containing ORCID XML summaries (scanned recursively)')
    arg_parser.add_argument('-t', '--threshold', dest='threshold', type=int, default=10000,
                            help='Number of files to process before saving a CSV chunk (default: 10000)')
    args = arg_parser.parse_args()
    iod = IndexOrcidDoi(output_path=args.output_path, threshold=args.threshold)
    iod.explorer(summaries_path=args.summaries_path)
