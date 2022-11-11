from os import makedirs
import os.path
from os.path import exists
import csv
import pandas as pd
from oc_meta.preprocessing.base import Preprocessing
from tqdm import tqdm
import sys


class NIHPreProcessing(Preprocessing):
    """This class aims at pre-processing iCite Database Snapshots (NIH Open
    Citation Collection + ICite Metadata), available at:
    https://nih.figshare.com/search?q=iCite+Database+Snapshot. In particular,
    NIHPreProcessing splits the original CSV file in many lighter CSV files,
    each one containing the number of entities specified in input by the user"""
    def __init__(self, input_dir, output_dir, interval, filter=None):
        self._req_type = ".csv"
        self._input_dir = input_dir
        self._output_dir = output_dir
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        if filter:
            self._filter = filter
        else:
            self._filter = ["pmid","doi","title","authors","year","journal", "cited_by","references"]
        super(NIHPreProcessing, self).__init__()

    def split_input(self):
        maxInt = sys.maxsize
        while True:
            # decrease the maxInt value by factor 10
            # as long as the OverflowError occurs.
            try:
                csv.field_size_limit(maxInt)
                break
            except OverflowError:
                maxInt = int(maxInt / 10)

        all_files = self.get_all_files(self._input_dir, self._req_type)
        count = 0
        lines = []
        headers = self._filter
        for file_idx, file in enumerate(all_files):
            if isinstance(file, list):
                file = file[0]
            if file:
                #df = pd.DataFrame()
                iter_csv = pd.read_csv(file, usecols=self._filter, chunksize=1000, engine='python')
                try:
                    for chunk in tqdm(iter_csv):
                        try:
                            f = pd.concat([chunk], ignore_index=True)
                            f.fillna("", inplace=True)
                            #df = pd.read_csv(file, usecols=self._filter, low_memory=True, dtype={'pmid': str, 'doi': str, 'title': str, 'authors': str, 'year': int, 'journal': str, 'is_research_article': bool, 'citation_count':int, 'field_citation_rate': float, 'expected_citations_per_year': float, 'citations_per_year':float, 'relative_citation_ratio': float, 'nih_percentile': int, 'human': float, 'animal': float, 'molecular_cellular': float, 'x_coord':float,	'y_coord': float, 'apt': float, 'is_clinical': bool, 'cited_by_clin': str, 'cited_by': str, 'references':str, 'provisional': str})
                            #df.fillna("", inplace=True)
                            f1 = f.values.tolist()

                            for line in tqdm(f1):
                                try:
                                    count += 1
                                    lines.append(line)
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        lines = self.splitted_to_file(
                                            count, self._interval, self._output_dir, lines, headers
                                    )
                                except:
                                    print("error with line:", line)
                        except:
                            print("error with chunk:", chunk)
                except:
                    print("error with pd.read_csv")

        if len(lines) > 0:
            self.splitted_to_file(count, self._interval, self._output_dir, lines, headers)


    def splitted_to_file(self, cur_n, target_n, out_dir, data, headers=None):
        if int(cur_n) != 0 and int(cur_n) % int(target_n) == 0:
            # to be logged: print("Processed lines:", cur_n, ". Reduced csv nr.", cur_n // target_n)
            filename = "CSVFile_" + str(cur_n // target_n) + self._req_type
            with (
                open(os.path.join(out_dir, filename), "w", encoding="utf8", newline="")
            ) as f_out:
                writer = csv.writer(f_out)
                writer.writerow(headers)
                writer.writerows(data)
                lines = []
            return lines
        else:
            # to be logged: print("Processed lines:", cur_n)
            filename = "CSVFile_" + "Rem" + self._req_type
            with (
                open(os.path.join(out_dir, filename), "w", encoding="utf8", newline="")
            ) as f_out:
                writer = csv.writer(f_out)
                writer.writerow(headers)
                writer.writerows(data)
            return