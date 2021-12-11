import os, json
from bs4 import BeautifulSoup
from meta.lib.csvmanager import CSVManager
from meta.lib.id_manager.doimanager import DOIManager
from argparse import ArgumentParser
from tqdm import tqdm


class Index_orcid_doi:

    def __init__(self, csv_path:str, treshold:int=10000):
        if not os.path.exists(os.path.dirname(csv_path)):
            os.makedirs(os.path.dirname(csv_path))
        self.file_counter = 0
        self.treshold = treshold
        self.doimanager = DOIManager(use_api_service=False)
        self.csvstorage = CSVManager(csv_path)
        self.cache = set(el.split(" [")[1][:-1] for _,v in self.csvstorage.data.items() for el in v)
    
    def explorer(self, summaries_path:str, verbose:bool=False) -> None:
        if verbose:
            # file_count = sum([len(files) for _, _, files in os.walk(summaries_path)])
            file_count = 9747030
            pbar = tqdm(total=file_count) 
        for fold, _, files in os.walk(summaries_path):
            for file in files:
                self.finder(fold, file)
                if self.file_counter == self.treshold:
                    self.csvstorage.dump_data()
                    self.file_counter = 0
                if verbose:
                    pbar.update(1)
                self.file_counter += 1
        self.csvstorage.dump_data()          
        if verbose:
            pbar.close()

    def finder(self, fold:str, file:str):
        orcid = file.replace('.xml', '')
        if file.endswith('.xml') and orcid not in self.cache:
            xml_file = open(os.path.join(fold, file), 'r', encoding='utf-8')
            xml_soup = BeautifulSoup(xml_file, 'xml')
            g_name = xml_soup.find('personal-details:given-names')
            f_name = xml_soup.find('personal-details:family-name')
            if f_name:
                f_name = f_name.get_text()
                if g_name:
                    g_name = g_name.get_text()
                    name = f_name + ', ' + g_name
                else:
                    name = f_name

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
                                    auto = name + ' [' + orcid + ']'
                                    self.csvstorage.add_value(doi, auto)

if __name__ == '__main__':
    arg_parser = ArgumentParser('index_orcid_doi.py', description='This script builds a csv index of DOIs associated'
                                                                  ' with ORCIDs, starting from XML files containing'
                                                                  ' ORCID data.')

    arg_parser.add_argument('-c', '--csv', dest='csv_path', required=True,
                            help='The output CSV file path.')
    arg_parser.add_argument('-s', '--summaries', dest='summaries_path', required=True,
                            help='The folder path containing orcid summaries, subfolder will be considered too.')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time.')

    args = arg_parser.parse_args()

    iOd = Index_orcid_doi(csv_path = args.csv_path)

    iOd.explorer(summaries_path=args.summaries_path, verbose=args.verbose)