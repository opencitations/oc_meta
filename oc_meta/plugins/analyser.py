#!python
# Copyright 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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
from typing import Dict, List

import os
import re
from datetime import datetime
from dateutil.parser import parse
from functools import cmp_to_key

from tqdm import tqdm

from oc_meta.lib.file_manager import get_csv_data, write_csv
from oc_meta.lib.master_of_regex import name_and_ids


class OCMetaAnalyser:
    def __init__(self, csv_dump_path: str):
        self.csv_dump_path = csv_dump_path
    
    def merge_rows_by_id(self, output_dir: str) -> None:
        ids_by_csv = dict()
        for filename in os.listdir(self.csv_dump_path):
            csv_data = get_csv_data(os.path.join(self.csv_dump_path, filename))
            for i, row in enumerate(csv_data):
                metaid = [identifier for identifier in row['id'].split() if identifier.split(':')[0] == 'meta'][0]
                ids_by_csv.setdefault(metaid, dict())
                ids_by_csv[metaid].setdefault(filename, set())
                ids_by_csv[metaid][filename].add(i)
        storer = dict()
        for metaid, filenames in ids_by_csv.items():
            if len(filenames) > 1:
                sorted_filenames = sorted([name for name in filenames], key=cmp_to_key(self.sort_csv_filenames))
                to_be_overritten = sorted_filenames[:-1]
                latest_file = sorted_filenames[-1]
                for filename in to_be_overritten:
                    storer.setdefault(filename, set())
                    storer[filename].update(ids_by_csv[metaid][filename])
                storer.setdefault(latest_file, set())
            else:
                storer.setdefault(list(filenames.keys())[0], set())
        for filename, rows in storer.items():
            old_data = get_csv_data(os.path.join(self.csv_dump_path, filename))
            new_data = [row for i, row in enumerate(old_data) if i not in rows]
            write_csv(
                path=os.path.join(output_dir, filename), 
                datalist=new_data, 
                fieldnames=['id', 'title', 'pub_date', 'page', 'type', 'author', 'editor', 'publisher', 'volume', 'venue', 'issue'], 
                method='w')
            
    @staticmethod
    def sort_csv_filenames(file_1, file_2) -> str:
        file_1_date = datetime.strptime(file_1.split('_')[1].replace('.csv', ''), '%Y-%m-%dT%H-%M-%S')
        file_2_date = datetime.strptime(file_2.split('_')[1].replace('.csv', ''), '%Y-%m-%dT%H-%M-%S')
        if file_1_date > file_2_date:
            return 1
        elif file_1_date < file_2_date:
            return -1
        elif file_1_date == file_2_date:
            if int(file_1.split('_')[0]) > int(file_2.split('_')[0]):
                return 1
            else:
                return -1

    def explore_csv_dump(self, analyser: callable) -> None|int|dict:
        global_output = None
        filenames = os.listdir(self.csv_dump_path)
        pbar = tqdm(total=len(filenames))
        for i, filename in enumerate(filenames):
            csv_data = get_csv_data(os.path.join(self.csv_dump_path, filename))
            local_output = analyser(csv_data)
            if i == 0:
                if isinstance(local_output, int):
                    global_output = 0
                elif isinstance(local_output, dict):
                    global_output = dict()
            if isinstance(local_output, int):
                global_output += local_output
            elif isinstance(local_output, dict):
                for k,v in local_output.items():
                    if k in global_output:
                        for i_k, _ in v.items():
                            if i_k in global_output[k]:
                                if isinstance(global_output[k][i_k], set):
                                    global_output[k][i_k].update(local_output[k][i_k])
                            else:
                                global_output[k][i_k] = local_output[k][i_k]
                    else:
                        global_output[k] = local_output[k]                    
            pbar.update()
        pbar.close()
        return global_output
    

class OCMetaCounter(OCMetaAnalyser):
    def __init__(self, csv_dump_path: str):
        super(OCMetaCounter, self).__init__(csv_dump_path)
    
    def get_top(self, what: str, by_what: str, number: int|None = 10) -> dict:
        counter_func = getattr(self, f'count_{what}_by_{by_what}')
        all_data = self.explore_csv_dump(counter_func)
        all_data_sorted: list = sorted(all_data, key=lambda k: len(all_data[k][by_what]), reverse=True)
        top_n = all_data_sorted[:number-1] if number is not None else all_data_sorted
        all_top_n = [(k, v) for k, v in all_data.items() if k in top_n]
        for tuple_k_v in all_top_n:
            tuple_k_v[1]['total'] = len(tuple_k_v[1][by_what])
        return sorted(all_top_n, key=lambda x: top_n.index(x[0]))
    
    def count(self, what: str) -> int:
        counter_func = getattr(self, f'count_{what}')
        return self.explore_csv_dump(counter_func)

    def count_authors(self, csv_data: List[dict]) -> int:
        count = 0
        for row in csv_data:
            count += len(list(filter(None, row['author'].split('; '))))
        return count

    def count_editors(self, csv_data: List[dict]) -> int:
        count = 0
        for row in csv_data:
            count += len(list(filter(None, row['editor'].split('; '))))
        return count

    def count_bibliographic_resources(self, csv_data: List[dict]) -> int:
        pass
    
    def count_publishers_by_venue(self, csv_data: List[dict]) -> Dict[str, Dict[str, set|str]]:
        publishers_by_venue = dict()
        for row in csv_data:
            publisher_name_and_ids = re.search(name_and_ids, row['publisher'])
            venue_name_and_ids = re.search(name_and_ids, row['venue'])
            if publisher_name_and_ids and venue_name_and_ids:
                publisher_name = publisher_name_and_ids.group(1)
                publisher_metaid = [identifier for identifier in publisher_name_and_ids.group(2).split() if identifier.split(':')[0] == 'meta'][0]
                venue_metaid = [identifier for identifier in venue_name_and_ids.group(2).split() if identifier.split(':')[0] == 'meta'][0]
                publishers_by_venue.setdefault(publisher_metaid, {'name': publisher_name, 'venue': set()})
                publishers_by_venue[publisher_metaid]['venue'].add(venue_metaid)
        return publishers_by_venue

    def count_publishers_by_publication(self, csv_data: List[dict]) -> Dict[str, Dict[str, set|str]]:
        publishers_by_publication = dict()
        for row in csv_data:
            publishers_name_and_ids = re.search(name_and_ids, row['publisher'])
            if publishers_name_and_ids:
                publishers_name = publishers_name_and_ids.group(1)
                publisher_metaid = [identifier for identifier in publishers_name_and_ids.group(2).split() if identifier.split(':')[0] == 'meta'][0]
                row_metaid = [identifier for identifier in row['id'].split() if identifier.split(':')[0] == 'meta'][0]
                publishers_by_publication.setdefault(publisher_metaid, {'name': publishers_name, 'publication': set()})
                publishers_by_publication[publisher_metaid]['publication'].add(row_metaid)
        return publishers_by_publication

    def count_venues_by_publication(self, csv_data: List[dict]) -> Dict[str, Dict[str, set|str]]:
        venues_by_publication = dict()
        for row in csv_data:
            venue_name_and_ids = re.search(name_and_ids, row['venue'])
            if venue_name_and_ids:
                venue_name = venue_name_and_ids.group(1)
                venue_metaid = [identifier for identifier in venue_name_and_ids.group(2).split() if identifier.split(':')[0] == 'meta'][0]
                row_metaid = [identifier for identifier in row['id'].split() if identifier.split(':')[0] == 'meta'][0]
                venues_by_publication.setdefault(venue_metaid, {'name': venue_name, 'publication': set()})
                venues_by_publication[venue_metaid]['publication'].add(row_metaid)
        return venues_by_publication

    def count_years_by_publication(self, csv_data: List[dict]) -> Dict[str, Dict[str, set]]:
        years_by_publication = dict()
        for row in csv_data:
            pub_date = row['pub_date']
            if pub_date:
                year = datetime.strftime(parse(pub_date), '%Y')
                row_metaid = [identifier for identifier in row['id'].split() if identifier.split(':')[0] == 'meta'][0]
                years_by_publication.setdefault(year, {'publication': set()})
                years_by_publication[year]['publication'].add(row_metaid)
        return years_by_publication

    def count_types_by_publication(self, csv_data: List[dict]) -> Dict[str, Dict[str, set|str]]:
        types_by_publication = dict()
        for row in csv_data:
            br_type = row['type']
            if br_type:
                row_metaid = [identifier for identifier in row['id'].split() if identifier.split(':')[0] == 'meta'][0]
                types_by_publication.setdefault(br_type, {'publication': set()})
                types_by_publication[br_type]['publication'].add(row_metaid)
        return types_by_publication