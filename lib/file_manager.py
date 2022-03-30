import csv
import os
import sys
from contextlib import contextmanager
from meta.lib.cleaner import Cleaner
from typing import List, Dict, Set



def get_data(filepath:str) -> List[Dict[str, str]]:
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
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

def write_csv(path:str, datalist:List[dict], fieldnames:list=None) -> None:
    fieldnames = datalist[0].keys() if fieldnames is None else fieldnames
    pathoo(path)
    with open(path, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(f=output_file, fieldnames=fieldnames, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        dict_writer.writeheader()
        dict_writer.writerows(datalist)

def normalize_path(path:str) -> str:
    normal_path = path.replace('\\', '/').replace('/', os.sep)
    return normal_path

def init_cache(cache_filepath:str) -> Set[str]:
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
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout