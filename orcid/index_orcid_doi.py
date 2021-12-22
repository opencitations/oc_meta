import os
from bs4 import BeautifulSoup
from meta.lib.csvmanager import CSVManager
from meta.lib.id_manager.doimanager import DOIManager
from argparse import ArgumentParser
from tqdm import tqdm


class Index_orcid_doi:
    
    def __init__(self, output_path:str, threshold:int=10000, low_memory:bool=False, verbose:bool=False):
        self.file_counter = 0
        self.threshold = 10000 if not threshold else int(threshold)
        self.verbose = verbose
        if self.verbose:
            print("[INFO:CSVManager] Loading existing csv file")
        self.doimanager = DOIManager(use_api_service=False)
        self.csvstorage = CSVManager(output_path=output_path, line_threshold=threshold, low_memory=low_memory)
        # ORCIDs are extracted to skip the corresponding files at the first reading of an existing CSV.
        self.cache = self.cache = set(el.split("[")[1][:-1].strip() for _,v in self.csvstorage.data.items() for el in v)
    
    def explorer(self, summaries_path:str) -> None:
        if self.verbose:
            print("[INFO:Index_orcid_doi] Counting files to process")
        files_to_process = [os.path.join(fold,file) for fold, _, files in os.walk(summaries_path) for file in files if file.replace('.xml', '') not in self.cache]
        processed_files = len(self.cache)
        del self.cache
        if self.verbose:
            pbar = tqdm(total=len(files_to_process))
        for file in files_to_process:
            self.finder(file)
            self.file_counter += 1
            cur_file = self.file_counter + processed_files
            if self.file_counter % self.threshold == 0:
                self.csvstorage.dump_data(f'{cur_file-self.threshold+1}-{cur_file}.csv')
            if self.verbose:
                pbar.update(1)
        cur_file = self.file_counter + processed_files
        self.csvstorage.dump_data(f'{cur_file + 1 - (cur_file % self.threshold)}-{cur_file}.csv')
        if self.verbose:
            pbar.close()

    def finder(self, file:str):
        orcid = file.replace('.xml', '')[-19:]
        valid_doi = False
        if file.endswith('.xml'):
            xml_file = open(file, 'r', encoding='utf-8')
            xml_soup = BeautifulSoup(xml_file, 'xml')
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
                                g_name = xml_soup.find('personal-details:given-names')
                                f_name = xml_soup.find('personal-details:family-name')
                                if f_name:
                                    f_name = f_name.get_text()
                                    if g_name:
                                        g_name = g_name.get_text()
                                        name = f_name + ', ' + g_name
                                    else:
                                        name = f_name
                                    auto = name + ' [' + orcid + ']'
                                    valid_doi = True
                                    self.csvstorage.add_value(doi, auto)
        if not valid_doi:
            # Save file names where nothing was found, to skip them during the next run
            self.csvstorage.add_value('None', f'[{orcid}]')


if __name__ == '__main__':
    arg_parser = ArgumentParser('index_orcid_doi.py', description='This script builds a csv index of DOIs associated'
                                                                  ' with ORCIDs, starting from XML files containing'
                                                                  ' ORCID data.')
    arg_parser.add_argument('-o', '--output', dest='output_path', required=True,
                            help='The output CSV file path.')
    arg_parser.add_argument('-s', '--summaries', dest='summaries_path', required=True,
                            help='The folder path containing orcid summaries, subfolder will be considered too.')
    arg_parser.add_argument('-t', '--threshold', dest='threshold', required=False,
                            help='Number of files to save after.')
    arg_parser.add_argument('-lm', '--low-memory', dest='low_memory', required=False, action='store_true',
                            help='Specify this argument if the available RAM is insufficient to accomplish the task. Warning: the processing time will increase.')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time.')
    args = arg_parser.parse_args()
    iOd = Index_orcid_doi(output_path = args.output_path, threshold=args.threshold, verbose=args.verbose)
    iOd.explorer(summaries_path=args.summaries_path)