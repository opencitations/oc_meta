#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


from __future__ import annotations

import csv
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Set
from zipfile import ZIP_DEFLATED, ZipFile

from _collections_abc import dict_keys
from bs4 import BeautifulSoup
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError

from oc_meta.lib.cleaner import Cleaner


def get_csv_data(filepath:str) -> List[Dict[str, str]]:
    if not os.path.splitext(filepath)[1].endswith('.csv'):
        return list()
    field_size_changed = False
    cur_field_size = 128
    data = list()
    while not data:
        try:
            with open(filepath, 'r', encoding='utf8') as data_initial:
                valid_data = (Cleaner(line).normalize_spaces().replace('\0','') for line in data_initial)
                data = list(csv.DictReader(valid_data, delimiter=','))
        except csv.Error:
            cur_field_size *= 2
            csv.field_size_limit(cur_field_size)
            field_size_changed = True
    if field_size_changed:
        csv.field_size_limit(128)
    return data

def pathoo(path):
    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

def write_csv(path:str, datalist:List[dict], fieldnames:list|dict_keys|None=None, method:str='w') -> None:
    if datalist:
        fieldnames = datalist[0].keys() if fieldnames is None else fieldnames
        pathoo(path)
        file_exists = os.path.isfile(path)
        with open(path, method, newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(f=output_file, fieldnames=fieldnames, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            if method == 'w' or (method == 'a' and not file_exists):
                dict_writer.writeheader()
            dict_writer.writerows(datalist)

def normalize_path(path:str) -> str:
    normal_path = path.replace('\\', '/').replace('/', os.sep)
    return normal_path

def init_cache(cache_filepath:str|None) -> Set[str]:
    completed = set()
    if cache_filepath:
        if not os.path.exists(cache_filepath):
            pathoo(cache_filepath)
        else:
            with open(cache_filepath, 'r', encoding='utf-8') as cache_file:
                completed = {line.rstrip('\n') for line in cache_file}
    return completed

@contextmanager
def suppress_stdout():
    with open(os.devnull, 'w') as devnull: #pragma: no cover
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout

def sort_files(files_to_be_processed:list) -> list:
    if all(filename.replace('.csv', '').isdigit() for filename in files_to_be_processed):
        files_to_be_processed = sorted(files_to_be_processed, key=lambda filename: int(filename.replace('.csv', '')))
    elif all(filename.split('_')[-1].replace('.csv', '').isdigit() for filename in files_to_be_processed):
        files_to_be_processed = sorted(files_to_be_processed, key=lambda filename: int(filename.split('_')[-1].replace('.csv', '')))
    return files_to_be_processed

def zipdir(path, ziph):
    for root, _, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file),
                       os.path.relpath(os.path.join(root, file),
                                       os.path.join(path, '..')))

def zipit(dir_list:list, zip_name:str) -> None:
    zipf = ZipFile(file=zip_name, mode='w', compression=ZIP_DEFLATED, allowZip64=True)
    for dir in dir_list:
        zipdir(dir, zipf)
    zipf.close()

def zip_files_in_dir(src_dir:str, dst_dir:str, replace_files:bool=False) -> None:
    '''
    This method zips files individually in all directories starting from a specified root directory. 
    In other words, this function does not zip the entire folder but individual files 
    while maintaining the folder hierarchy in the specified output directory.

    :params src_dir: the source directory
    :type src_dir: str
    :params dst_dir: the destination directory
    :type dst_dir: str
    :params replace_files: True if you want to replace the original unzipped files with their zipped versions. The dafult value is False
    :type replace_files: bool
    :returns: None
    '''
    for dirpath, _, filenames in os.walk(src_dir):
        for filename in filenames:
            src_path = os.path.join(dirpath, filename)
            dst_path = os.path.join(
                dst_dir, 
                str(Path(src_path).parent)
                    .replace(f'{src_dir}{os.sep}', ''))
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)
            _, ext = os.path.splitext(filename)
            zip_path = os.path.join(dst_path, filename).replace(ext, '.zip')
            with ZipFile(file=zip_path, mode='w', compression=ZIP_DEFLATED, allowZip64=True) as zipf:
                zipf.write(src_path, arcname=filename)
            if replace_files:
                os.remove(src_path)

def unzip_files_in_dir(src_dir:str, dst_dir:str, replace_files:bool=False) -> None:
    '''
    This method unzips zipped files individually in all directories starting from a specified root directory. 
    In other words, this function does not unzip the entire folder but individual files 
    while maintaining the folder hierarchy in the specified output directory.

    :params src_dir: the source directory
    :type src_dir: str
    :params dst_dir: the destination directory
    :type dst_dir: str
    :params replace_files: True if you want to replace the original zipped files with their unzipped versions, defaults to [False]
    :type replace_files: bool
    :returns: None
    '''
    for dirpath, _, filenames in os.walk(src_dir):
        for filename in filenames:
            if os.path.splitext(filename)[1] == '.zip':
                src_path = os.path.join(dirpath, filename)
                dst_path = os.path.join(
                    dst_dir, 
                    str(Path(src_path).parent)
                        .replace(f'{src_dir}{os.sep}', ''))
                if not os.path.exists(dst_path):
                    os.makedirs(dst_path)
                with ZipFile(file=os.path.join(dst_path, filename), mode='r') as zipf:
                    zipf.extractall(dst_path)
                if replace_files:
                    os.remove(src_path)

def read_zipped_json(filepath:str) -> dict|None:
    '''
    This method reads a zipped json file.

    :params filepath: the zipped json file path
    :type src_dir: str
    :returns: dict -- It returns the json file as a dictionary
    '''
    with ZipFile(filepath, 'r') as zipf:
        for filename in zipf.namelist(): 
            with zipf.open(filename) as f:
                json_data = f.read()
                json_dict = json.loads(json_data.decode("utf-8"))
                return json_dict

def call_api(url:str, headers:str, r_format:str="json") -> dict|None:
    tentative = 3
    while tentative:
        tentative -= 1
        try:
            r = get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return json.loads(r.text) if r_format == "json" else BeautifulSoup(r.text, 'xml')
            elif r.status_code == 404:
                return None
        except ReadTimeout:
            # Do nothing, just try again
            pass
        except ConnectionError:
            # Sleep 5 seconds, then try again
            sleep(5)
    return None

def rm_tmp_csv_files(base_dir:str) -> None:
    for filename in os.listdir(base_dir):
        number = filename.split('_')[0]
        date = datetime.strptime(filename.split('_')[1].replace('.csv', ''), '%Y-%m-%dT%H-%M-%S')
        for other_filename in os.listdir(base_dir):
            other_number = other_filename.split('_')[0]
            other_date = datetime.strptime(other_filename.split('_')[1].replace('.csv', ''), '%Y-%m-%dT%H-%M-%S')
            if number == other_number and filename != other_filename:
                if date < other_date:
                    os.remove(os.path.join(base_dir, filename))
                elif other_date < date:
                    os.remove(os.path.join(base_dir, other_filename))