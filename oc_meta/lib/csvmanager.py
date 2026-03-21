#!/usr/bin/python

# Copyright (C) 2019 Silvio Peroni <essepuntato@gmail.com>
# Copyright (C) 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
# SPDX-FileCopyrightText: 2022-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from csv import DictReader, writer
from os import mkdir, sep, walk
from os.path import exists, join
from typing import Dict


class CSVManager(object):
    def __init__(self, output_path: str | None = None):
        self._output_path = output_path
        self.data: Dict[str, set] = {}
        self.data_to_store: list[list[str]] = []
        if output_path is not None:
            self.__init_output_dir()
            self.__load_csv()

    @property
    def output_path(self) -> str:
        if self._output_path is None:
            raise ValueError("output_path is not set")
        return self._output_path

    def __init_output_dir(self) -> None:
        if not exists(self.output_path):
            mkdir(self.output_path)

    def dump_data(self, file_name: str) -> None:
        path = join(self.output_path, file_name)
        if not exists(path):
            with open(path, 'w', encoding='utf-8', newline='') as f:
                f.write('"id","value"\n')
        with open(path, 'a', encoding='utf-8', newline='') as f:
            csv_writer = writer(f, delimiter=',')
            for el in self.data_to_store:
                csv_writer.writerow([el[0].replace('"', '""'), el[1].replace('"', '""')])
        self.data_to_store = list()

    def get_value(self, id_string):
        '''
        It returns the set of values associated to the input 'id_string',
        or None if 'id_string' is not included in the CSV.
        '''
        if id_string in self.data:
            return set(self.data[id_string])

    def add_value(self, id_string, value):
        '''
        It adds the value specified in the set of values associated to 'id_string'.
        If the object was created with the option of storing also the data in a CSV
        ('store_new' = True, default behaviour), then it also add new data in the CSV.
        '''
        self.data.setdefault(id_string, set())
        if value not in self.data[id_string]:
            self.data[id_string].add(value)
            self.data_to_store.append([id_string, value])

    def __load_csv(self) -> None:
        for cur_dir, _, cur_files in walk(self.output_path):
            for cur_file in cur_files:
                if cur_file.endswith('.csv'):
                    file_path = cur_dir + sep + cur_file
                    with open(file_path, 'r', encoding='utf-8') as f:
                        reader = DictReader(f)
                        for row in reader:
                            self.data.setdefault(row['id'], set())
                            self.data[row['id']].add(row['value'])