#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
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


from pathlib import Path
from zipfile import ZipFile
import json
import os


class JsonArchiveManager():
    def __init__(self):
        pass
    
    def compress_json_files_in_dir(self, src_dir:str, dst_dir:str) -> None:
        '''
        This method zips json files individually in all directories starting from a specified root directory. 
        In other words, this function does not zip the entire folder but individual files 
        while maintaining the folder hierarchy in the specified output directory.

        :params src_dir: the source directory
        :type src_dir: str
        :params dst_dir: the destination directory
        :type dst_dir: str
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
                with ZipFile(os.path.join(dst_path, filename) + '.zip', 'w') as zipf:
                    zipf.write(src_path, arcname=filename)
    
    def read_zipped_json(self, filepath:str) -> dict:
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

