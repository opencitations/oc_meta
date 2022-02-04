import csv
import os
from typing import List
from meta.lib.cleaner import Cleaner


def get_data(filepath:str) -> List[dict]:
    field_size_changed = False
    cur_field_size = 128
    data = list()
    while not data:
        try:
            data_initial = open(filepath, 'r', encoding='utf8')
            valid_data = (Cleaner.normalize_spaces(line).replace('\0','') for line in data_initial)
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

def write_csv(path:str, datalist:List[dict], fieldnames:list=None, mode:str='w') -> None:
    fieldnames = datalist[0].keys() if fieldnames is None else fieldnames
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(path, mode, newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        dict_writer.writeheader()
        dict_writer.writerows(datalist)

def normalize_path(path:str) -> str:
    normal_path = path.replace('\\', '/').replace('/', os.sep)
    return normal_path
