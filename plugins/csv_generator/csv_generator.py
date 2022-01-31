import os
import re
from meta.lib.master_of_regex import *
from meta.lib.finder import ResourceFinder
from typing import Tuple


class Counter:
    def __init__(self, counter_dir:str):
        self.ar = 0
        self.br = 0
        self.id = 0
        self.ra = 0
        self.re = 0
        self.set_counters(counter_dir)
    
    def set_counters(self, counter_dir:str):
        for entity_type in {'ar', 'br', 'id', 'ra', 're'}:
            filepath = os.path.join(counter_dir, 'creator', f'info_file_{entity_type}.txt')
            number = self.read_number(filepath)
            setattr(self, entity_type, number)

    @staticmethod
    def read_number(file_path: str) -> Tuple[int, int]:
        with open(file_path, "rb") as f:
            number = int(f.readlines()[0].decode('ascii').strip())
            return number

class CSVGenerator:
    def __init__(self, endpoint:str, base_iri:str, counter_dir:str):
        self.counters = Counter(counter_dir)
        self.finder = ResourceFinder(endpoint, base_iri)
    
    def generate_csv(self, base_iri:str, prefix:str):
        counter = 1
        number_of_entities = getattr(self.counters, 'br')
        output = list()
        while counter <= number_of_entities:
            row = dict()
            metaid = prefix + str(counter)
            res = f'{base_iri}/br/{metaid}'
            res_exists = self.finder.check_existence(res)
            if res_exists:
                br_info = self.finder.retrieve_br_info_from_meta(metaid)
                br_title_and_ids = self.finder.retrieve_br_from_meta(metaid)
                # Pub_date, Volume, Issue, Type
                row.update(br_info)
                # Id
                ids = [id[1] for id in br_title_and_ids[1] if br_title_and_ids[1]]
                row['id'] = f'meta:br/{metaid} ' + ' '.join(ids)
                # Title
                row['title'] = br_title_and_ids[0]
                # Authors and Editors
                row['author'] = self.get_resp_agents(metaid, 'author')
                row['editor'] = self.get_resp_agents(metaid, 'editor')
                # Page
                row['page'] = br_info['page'][1] if row['page'] else ''
                # Venue
                if br_info['venue']:
                    venue_metaid = re.search(ids_inside_square_brackets, br_info['venue']).group(1).replace('meta:br/', '')
                    venue_title_and_ids = self.finder.retrieve_br_from_meta(venue_metaid)
                    venue_title = venue_title_and_ids[0]
                    venue_ids = '[' + ' '.join([id[1] for id in venue_title_and_ids[1]]) + ']'
                    row['venue'] = venue_title + ' ' + venue_ids
                # Publisher
                row = {k:v if v else '' for k,v in row.items()}
                print(row)
                output.append(row)
            counter += 1
    
    def get_resp_agents(self, metaid:str, column:str) -> str:
        resp_agents = self.finder.retrieve_ra_sequence_from_br_meta(metaid, column)
        output = ''
        if resp_agents:
            full_resp_agents = list()
            for item in resp_agents:
                for _, resp_agent in item.items():
                    author_name = resp_agent[0]
                    ids = [f'meta:ra/{resp_agent[2]}']
                    ids.extend([id[1] for id in resp_agent[1]])
                    author_ids = '[' + ' '.join(ids) + ']'
                    full_resp_agent =  author_name + ' ' + author_ids
                    full_resp_agents.append(full_resp_agent)
            output = '; '.join(full_resp_agents)
        return output

    


# counter = Counter()
# finder = ResourceFinder(ENDPOINT)
# result = finder.check_existence('https://w3id.org/oc/meta/ar/0601')
# print(result)

