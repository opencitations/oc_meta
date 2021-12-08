import os, json
from bs4 import BeautifulSoup
from meta.lib.csvmanager import CSVManager
from meta.lib.id_manager.doimanager import DOIManager
from argparse import ArgumentParser
from tqdm import tqdm


class Index_orcid_doi:

    def __init__(self, csv_path:str, cache_path:str):
        if not os.path.exists(os.path.dirname(csv_path)):
            os.makedirs(os.path.dirname(csv_path))
        if cache_path:
            if not os.path.exists(os.path.dirname(cache_path)):
                os.makedirs(os.path.dirname(cache_path))
            self.cache = set(json.load(open(cache_path, 'r', encoding='utf-8'))) if os.path.exists(cache_path) else set()
            self.cache_counter = 0
        self.cache_path = cache_path
        self.doimanager = DOIManager(use_api_service=False)
        self.csvstorage = CSVManager(csv_path)
    
    def explorer(self, summaries_path:str, verbose:bool=False) -> None:
        if verbose:
            file_count = sum([len(files) for _, _, files in os.walk(summaries_path)])
            pbar = tqdm(total=file_count) 
        for fold, _, files in os.walk(summaries_path):
            for file in files:
                if self.cache_path:
                    self.finder_with_cache(fold, file)
                else:
                    self.finder(fold, file)
                if verbose:
                    pbar.update(1)            
        if verbose:
            pbar.close()

    def finder_with_cache(self, fold:str, file:str):
        orcid = file.replace('.xml', '')
        if orcid not in self.cache:
            self.finder(fold, file)
            self.cache.add(orcid)
            self.cache_counter += 1
        if self.cache_counter == 10000:
            self.cache_counter = 0
            json.dump(list(self.cache), open(self.cache_path, 'w', encoding='utf-8'))

    def finder(self, fold:str, file:str):
        if file.endswith('.xml'):
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
                                    orcid = file.replace('.xml', '')
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
    arg_parser.add_argument('-ca', '--cache', dest='cache_path', required=False,
                            help='The path to save the cache and/or from where to read the existing cache.')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time.')

    args = arg_parser.parse_args()

    iOd = Index_orcid_doi(csv_path = args.csv_path, cache_path=args.cache_path)

    iOd.explorer(summaries_path=args.summaries_path, verbose=args.verbose)