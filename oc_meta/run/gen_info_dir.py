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

from __future__ import annotations

import argparse
import os
import zipfile
import json
import re
from tqdm import tqdm
import os
from shutil import copymode, move
from tempfile import mkstemp
from typing import TYPE_CHECKING
from pebble import ProcessPool
from concurrent.futures import TimeoutError, as_completed

if TYPE_CHECKING:
    from typing import BinaryIO, Tuple, List, Dict

from oc_ocdm.counter_handler.counter_handler import CounterHandler
from oc_ocdm.support.support import is_string_empty

class FilesystemCounterHandler(CounterHandler):
    """A concrete implementation of the ``CounterHandler`` interface that persistently stores
    the counter values within the filesystem."""

    _initial_line_len: int = 3
    _trailing_char: str = " "

    def __init__(self, info_dir: str) -> None:
        """
        Constructor of the ``FilesystemCounterHandler`` class.

        :param info_dir: The path to the folder that does/will contain the counter values.
        :type info_dir: str
        :raises ValueError: if ``info_dir`` is None or an empty string.
        """
        if info_dir is None or is_string_empty(info_dir):
            raise ValueError("info_dir parameter is required!")

        if info_dir[-1] != os.sep:
            info_dir += os.sep

        self.info_dir: str = info_dir
        self.datasets_dir: str = info_dir + 'datasets' + os.sep
        self.short_names: List[str] = ["an", "ar", "be", "br", "ci", "de", "id", "pl", "ra", "re", "rp"]
        self.metadata_short_names: List[str] = ["di"]
        self.info_files: Dict[str, str] = {key: ("info_file_" + key + ".txt")
                                           for key in self.short_names}
        self.prov_files: Dict[str, str] = {key: ("prov_file_" + key + ".txt")
                                           for key in self.short_names}

    def set_counter(self, new_value: int, entity_short_name: str, prov_short_name: str = "",
                    identifier: int = 1) -> None:
        """
        It allows to set the counter value of graph and provenance entities.

        :param new_value: The new counter value to be set
        :type new_value: int
        :param entity_short_name: The short name associated either to the type of the entity itself
         or, in case of a provenance entity, to the type of the relative graph entity.
        :type entity_short_name: str
        :param prov_short_name: In case of a provenance entity, the short name associated to the type
         of the entity itself. An empty string otherwise.
        :type prov_short_name: str
        :param identifier: In case of a provenance entity, the counter value that identifies the relative
          graph entity. The integer value '1' otherwise.
        :type identifier: int
        :raises ValueError: if ``new_value`` is a negative integer or ``identifier`` is less than or equal to zero.
        :return: None
        """
        if new_value < 0:
            raise ValueError("new_value must be a non negative integer!")

        if prov_short_name == "se":
            file_path: str = self._get_prov_path(entity_short_name)
        else:
            file_path: str = self._get_info_path(entity_short_name)
        self._set_number(new_value, file_path, identifier)

    def set_counters_batch(self, updates: Dict[Tuple[str, str], Dict[int, int]]) -> None:
        """
        Updates counters in batch for multiple files.
        `updates` is a dictionary where the key is a tuple (entity_short_name, prov_short_name)
        and the value is a dictionary of line numbers to new counter values.
        """
        for (entity_short_name, prov_short_name), file_updates in updates.items():
            file_path = self._get_prov_path(entity_short_name) if prov_short_name == "se" else self._get_info_path(entity_short_name)
            self._set_numbers(file_path, file_updates)

    def read_counter(self, entity_short_name: str, prov_short_name: str = "", identifier: int = 1) -> int:
        """
        It allows to read the counter value of graph and provenance entities.

        :param entity_short_name: The short name associated either to the type of the entity itself
         or, in case of a provenance entity, to the type of the relative graph entity.
        :type entity_short_name: str
        :param prov_short_name: In case of a provenance entity, the short name associated to the type
         of the entity itself. An empty string otherwise.
        :type prov_short_name: str
        :param identifier: In case of a provenance entity, the counter value that identifies the relative
          graph entity. The integer value '1' otherwise.
        :type identifier: int
        :raises ValueError: if ``identifier`` is less than or equal to zero.
        :return: The requested counter value.
        """
        if prov_short_name == "se":
            file_path: str = self._get_prov_path(entity_short_name)
        else:
            file_path: str = self._get_info_path(entity_short_name)
        return self._read_number(file_path, identifier)[0]

    def increment_counter(self, entity_short_name: str, prov_short_name: str = "", identifier: int = 1) -> int:
        """
        It allows to increment the counter value of graph and provenance entities by one unit.

        :param entity_short_name: The short name associated either to the type of the entity itself
         or, in case of a provenance entity, to the type of the relative graph entity.
        :type entity_short_name: str
        :param prov_short_name: In case of a provenance entity, the short name associated to the type
         of the entity itself. An empty string otherwise.
        :type prov_short_name: str
        :param identifier: In case of a provenance entity, the counter value that identifies the relative
          graph entity. The integer value '1' otherwise.
        :type identifier: int
        :raises ValueError: if ``identifier`` is less than or equal to zero.
        :return: The newly-updated (already incremented) counter value.
        """
        if prov_short_name == "se":
            file_path: str = self._get_prov_path(entity_short_name)
        else:
            file_path: str = self._get_info_path(entity_short_name)
        return self._add_number(file_path, identifier)

    def _get_info_path(self, short_name: str) -> str:
        return self.info_dir + self.info_files[short_name]

    def _get_prov_path(self, short_name: str) -> str:
        return self.info_dir + self.prov_files[short_name]

    def _get_metadata_path(self, short_name: str, dataset_name: str) -> str:
        return self.datasets_dir + dataset_name + os.sep + 'metadata_' + short_name + '.txt'

    def __initialize_file_if_not_existing(self, file_path: str):
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))

        if not os.path.isfile(file_path):
            with open(file_path, 'wb') as file:
                first_line: str = self._trailing_char * (self._initial_line_len - 1) + '\n'
                file.write(first_line.encode('ascii'))

    def _read_number(self, file_path: str, line_number: int) -> Tuple[int, int]:
        if line_number <= 0:
            raise ValueError("line_number must be a positive non-zero integer number!")

        self.__initialize_file_if_not_existing(file_path)

        cur_number: int = 0
        cur_line_len: int = 0
        try:
            with open(file_path, 'rb') as file:
                cur_line_len = self._get_line_len(file)
                line_offset = (line_number - 1) * cur_line_len
                file.seek(line_offset)
                line = file.readline(cur_line_len).decode('ascii')
                cur_number = int(line.rstrip(self._trailing_char + '\n'))
        except ValueError:
            cur_number = 0
        except Exception as e:
            print(e)

        return cur_number, cur_line_len

    def _add_number(self, file_path: str, line_number: int = 1) -> int:
        if line_number <= 0:
            raise ValueError("line_number must be a positive non-zero integer number!")

        self.__initialize_file_if_not_existing(file_path)

        cur_number, cur_line_len = self._read_number(file_path, line_number)
        cur_number += 1

        cur_number_len: int = len(str(cur_number)) + 1
        if cur_number_len > cur_line_len:
            self._increase_line_len(file_path, new_length=cur_number_len)
            cur_line_len = cur_number_len

        with open(file_path, 'r+b') as file:
            line_offset: int = (line_number - 1) * cur_line_len
            file.seek(line_offset)
            line: str = str(cur_number).ljust(cur_line_len - 1, self._trailing_char) + '\n'
            file.write(line.encode('ascii'))
            file.seek(-cur_line_len, os.SEEK_CUR)
            self._fix_previous_lines(file, cur_line_len)
        return cur_number

    def _set_number(self, new_value: int, file_path: str, line_number: int = 1) -> None:
        if new_value < 0:
            raise ValueError("new_value must be a non negative integer!")

        if line_number <= 0:
            raise ValueError("line_number must be a positive non-zero integer number!")

        self.__initialize_file_if_not_existing(file_path)

        cur_line_len = self._read_number(file_path, line_number)[1]

        cur_number_len: int = len(str(new_value)) + 1
        if cur_number_len > cur_line_len:
            self._increase_line_len(file_path, new_length=cur_number_len)
            cur_line_len = cur_number_len

        with open(file_path, 'r+b') as file:
            line_offset: int = (line_number - 1) * cur_line_len
            file.seek(line_offset)
            line: str = str(new_value).ljust(cur_line_len - 1, self._trailing_char) + '\n'
            file.write(line.encode('ascii'))
            file.seek(-cur_line_len, os.SEEK_CUR)
            self._fix_previous_lines(file, cur_line_len)

    def _set_numbers(self, file_path: str, updates: Dict[int, int]) -> None:
        """
        Apply multiple counter updates to a single file.
        `updates` is a dictionary where the key is the line number (identifier)
        and the value is the new counter value.
        """
        self.__initialize_file_if_not_existing(file_path)
        with open(file_path, 'r') as file:
            lines = file.readlines()
        max_line_number = max(updates.keys())

        # Ensure the lines list is long enough
        while len(lines) < max_line_number + 1:
            lines.append("\n")  # Default counter value

        # Apply updates
        for line_number, new_value in updates.items():
            lines[line_number-1] = str(new_value).rstrip() + "\n"

        # Write updated lines back to file
        with open(file_path, 'w') as file:
            file.writelines(lines)

    @staticmethod
    def _get_line_len(file: BinaryIO) -> int:
        cur_char: str = file.read(1).decode("ascii")
        count: int = 1
        while cur_char is not None and len(cur_char) == 1 and cur_char != "\0":
            cur_char = file.read(1).decode("ascii")
            count += 1
            if cur_char == "\n":
                break

        # Undo I/O pointer updates
        file.seek(0)

        if cur_char is None:
            raise EOFError("Reached end-of-file without encountering a line separator!")
        elif cur_char == "\0":
            raise ValueError("Encountered a NULL byte!")
        else:
            return count

    def _increase_line_len(self, file_path: str, new_length: int = 0) -> None:
        if new_length <= 0:
            raise ValueError("new_length must be a positive non-zero integer number!")

        with open(file_path, 'rb') as cur_file:
            if self._get_line_len(cur_file) >= new_length:
                raise ValueError("Current line length is greater than new_length!")

        fh, abs_path = mkstemp()
        with os.fdopen(fh, 'wb') as new_file:
            with open(file_path, 'rt', encoding='ascii') as old_file:
                for line in old_file:
                    number: str = line.rstrip(self._trailing_char + '\n')
                    new_line: str = str(number).ljust(new_length - 1, self._trailing_char) + '\n'
                    new_file.write(new_line.encode('ascii'))

        # Copy the file permissions from the old file to the new file
        copymode(file_path, abs_path)

        # Replace original file
        os.remove(file_path)
        move(abs_path, file_path)

    @staticmethod
    def _is_a_valid_line(buf: bytes) -> bool:
        string: str = buf.decode("ascii")
        return (string[-1] == "\n") and ("\0" not in string[:-1])

    def _fix_previous_lines(self, file: BinaryIO, line_len: int) -> None:
        if line_len < self._initial_line_len:
            raise ValueError("line_len should be at least %d!" % self._initial_line_len)

        while file.tell() >= line_len:
            file.seek(-line_len, os.SEEK_CUR)
            buf: bytes = file.read(line_len)
            if self._is_a_valid_line(buf) or len(buf) < line_len:
                break
            else:
                file.seek(-line_len, os.SEEK_CUR)
                fixed_line: str = (self._trailing_char * (line_len - 1)) + "\n"
                file.write(fixed_line.encode("ascii"))
                file.seek(-line_len, os.SEEK_CUR)

    def set_metadata_counter(self, new_value: int, entity_short_name: str, dataset_name: str) -> None:
        """
        It allows to set the counter value of metadata entities.

        :param new_value: The new counter value to be set
        :type new_value: int
        :param entity_short_name: The short name associated either to the type of the entity itself.
        :type entity_short_name: str
        :param dataset_name: In case of a ``Dataset``, its name. Otherwise, the name of the relative dataset.
        :type dataset_name: str
        :raises ValueError: if ``new_value`` is a negative integer, ``dataset_name`` is None or
          ``entity_short_name`` is not a known metadata short name.
        :return: None
        """
        if new_value < 0:
            raise ValueError("new_value must be a non negative integer!")

        if dataset_name is None:
            raise ValueError("dataset_name must be provided!")

        if entity_short_name not in self.metadata_short_names:
            raise ValueError("entity_short_name is not a known metadata short name!")

        file_path: str = self._get_metadata_path(entity_short_name, dataset_name)
        return self._set_number(new_value, file_path, 1)

    def read_metadata_counter(self, entity_short_name: str, dataset_name: str) -> int:
        """
        It allows to read the counter value of metadata entities.

        :param entity_short_name: The short name associated either to the type of the entity itself.
        :type entity_short_name: str
        :param dataset_name: In case of a ``Dataset``, its name. Otherwise, the name of the relative dataset.
        :type dataset_name: str
        :raises ValueError: if ``dataset_name`` is None or ``entity_short_name`` is not a known metadata short name.
        :return: The requested counter value.
        """
        if dataset_name is None:
            raise ValueError("dataset_name must be provided!")

        if entity_short_name not in self.metadata_short_names:
            raise ValueError("entity_short_name is not a known metadata short name!")

        file_path: str = self._get_metadata_path(entity_short_name, dataset_name)
        return self._read_number(file_path, 1)[0]

    def increment_metadata_counter(self, entity_short_name: str, dataset_name: str) -> int:
        """
        It allows to increment the counter value of metadata entities by one unit.

        :param entity_short_name: The short name associated either to the type of the entity itself.
        :type entity_short_name: str
        :param dataset_name: In case of a ``Dataset``, its name. Otherwise, the name of the relative dataset.
        :type dataset_name: str
        :raises ValueError: if ``dataset_name`` is None or ``entity_short_name`` is not a known metadata short name.
        :return: The newly-updated (already incremented) counter value.
        """
        if dataset_name is None:
            raise ValueError("dataset_name must be provided!")

        if entity_short_name not in self.metadata_short_names:
            raise ValueError("entity_short_name is not a known metadata short name!")

        file_path: str = self._get_metadata_path(entity_short_name, dataset_name)
        return self._add_number(file_path, 1)


# Variable used in several functions
entity_regex: str = r"^(.+)/([a-z][a-z])/(0[1-9]+0)?((?:[1-9][0-9]*)|(?:\d+-\d+))$"

def get_prefix(res: URIRef) -> str:
    string_iri: str = str(res)
    return _get_match(entity_regex, 3, string_iri)

def find_max_numbered_folder(path):
    """
    Trova la sottocartella con il numero più elevato in una data cartella.
    """
    max_number = -1
    for folder in os.listdir(path):
        if folder.isdigit():
            max_number = max(max_number, int(folder))
    return max_number

def find_max_numbered_zip_file(folder_path):
    """
    Trova il file zippato con il numero più elevato prima di ".zip" all'interno di una cartella.
    """
    max_number = -1
    max_zip_file = None

    for file_name in os.listdir(folder_path):
        if file_name.endswith(".zip"):
            prefix, extension = os.path.splitext(file_name)
            if prefix.isdigit():
                number = int(prefix)
                if number > max_number:
                    max_number = number
                    max_zip_file = file_name
    return max_zip_file

def _get_match(regex: str, group: int, string: str) -> str:
    match: Match = re.match(regex, string)
    if match is not None:
        return match.group(group)
    else:
        return ""

def get_resource_number(res) -> int:
    string_iri: str = str(res)
    return int(_get_match(entity_regex, 4, string_iri))

def get_short_name(res) -> str:
    string_iri: str = str(res)
    return _get_match(entity_regex, 2, string_iri)

def process_zip_file(zip_file_path):
    batch_updates = {}
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        first_file = zip_ref.namelist()[0]
        with zip_ref.open(first_file) as entity_file:
            json_data = json.load(entity_file)
            for graph in json_data:
                for entity in graph['@graph']:
                    entity_uri = entity['@id']
                    supplier_prefix = get_prefix(entity_uri)
                    resource_number = get_resource_number(entity_uri)
                    short_name = get_short_name(entity_uri)
                    batch_key = (short_name, 'se')
                    if supplier_prefix not in batch_updates:
                        batch_updates[supplier_prefix] = dict()
                    if batch_key not in batch_updates[supplier_prefix]:
                        batch_updates[supplier_prefix][batch_key] = dict()
                    batch_updates[supplier_prefix][batch_key][resource_number] = 1
    return batch_updates

def explore_directories(root_path, output_root):
    """
    Esplora le directory e associa a ciascun supplier prefix il numero maggiore.
    """
    main_folders = ["ar", "br", "ra", "re", "id"]

    for main_folder in main_folders:
        main_folder_path = os.path.join(root_path, main_folder)
        if os.path.isdir(main_folder_path):
            for supplier_prefix in os.listdir(main_folder_path):
                supplier_path = os.path.join(main_folder_path, supplier_prefix)
                prov_counter_handler = FilesystemCounterHandler(info_dir=os.path.join(output_root, supplier_prefix, 'creator'))
                max_folder = find_max_numbered_folder(supplier_path)
                max_zip_file = find_max_numbered_zip_file(os.path.join(supplier_path, str(max_folder)))
                zip_file_path = os.path.join(supplier_path, str(max_folder), max_zip_file)
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    first_file = zip_ref.namelist()[0]
                    with zip_ref.open(first_file) as entity_file:
                        json_data = json.load(entity_file)
                        max_entity = -1
                        for graph in json_data:
                            for entity in graph['@graph']:
                                entity_uri = entity['@id']
                                resource_number = get_resource_number(entity_uri)
                                max_entity = max(max_entity, resource_number)

                counter_handler_curator = FilesystemCounterHandler(info_dir=os.path.join(output_root, supplier_prefix, 'curator'))
                counter_handler_creator = FilesystemCounterHandler(info_dir=os.path.join(output_root, supplier_prefix, 'creator'))
                counter_handler_curator.set_counter(max_entity, main_folder)
                counter_handler_creator.set_counter(max_entity, main_folder)
    
    zip_files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(root_path) 
                 for f in filenames if f.endswith('.zip') and 'prov' not in dp]

    pbar = tqdm(total=len(zip_files))
    results = []

    timeout = 30

    with ProcessPool() as pool:
        future_results = {pool.schedule(process_zip_file, args=[zip_file], timeout=timeout): zip_file 
                          for zip_file in zip_files}

        results = []
        with tqdm(total=len(zip_files)) as pbar:
            for future in as_completed(future_results):
                zip_file = future_results[future]
                try:
                    result = future.result()
                    results.append(result)
                except TimeoutError:
                    print(f"Processo eseguito oltre il timeout di {timeout} secondi per il file: {zip_file}")
                except Exception as e:
                    print(f"Errore nell'elaborazione del file {zip_file}: {e}")
                finally:
                    pbar.update(1)

    final_batch_updates = {}
    with tqdm(total=len(results), desc="Fusione dei risultati") as pbar:
        for batch in results:
            for supplier_prefix, value in batch.items():
                if supplier_prefix not in final_batch_updates:
                    final_batch_updates[supplier_prefix] = value
                else:
                    for batch_key, inner_value in value.items():
                        if batch_key in final_batch_updates[supplier_prefix]:
                            final_batch_updates[supplier_prefix][batch_key].update(inner_value)
                        else:
                            final_batch_updates[supplier_prefix][batch_key] = inner_value
            pbar.update(1)

    for supplier_prefix, value in final_batch_updates.items():
        prov_counter_handler = FilesystemCounterHandler(info_dir=os.path.join(output_root, supplier_prefix, 'creator'))
        prov_counter_handler.set_counters_batch(value)

def main():
    parser = argparse.ArgumentParser(description="Esplora le directory e trova i numeri massimi.")
    parser.add_argument("directory", type=str, help="Il percorso della directory da esplorare")
    parser.add_argument("output_directory", type=str, help="Il percorso della directory di output per salvare i file info_dir.txt")
    args = parser.parse_args()

    max_numbers = explore_directories(args.directory, args.output_directory)

if __name__ == "__main__":
    main()