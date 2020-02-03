from crossref.crossrefProcessing import *
import os
from argparse import ArgumentParser


def preprocess(crossref_json_dir, orcid_doi_filepath, wanted_doi_filepath, clean_csv_dir):
    for filename in os.listdir(crossref_json_dir):
        if filename.endswith(".json"):
            json_file = os.path.join(crossref_json_dir, filename)
            crossref_csv = crossrefProcessing(orcid_doi_filepath, wanted_doi_filepath)
            new_filename = filename.replace(".json", ".csv")
            filepath = os.path.join(clean_csv_dir, new_filename)
            pathoo(filepath)
            data = crossref_csv.csv_creator(json_file)
            with open(filepath, 'w', newline='', encoding="utf-8") as output_file:
                dict_writer = csv.DictWriter(output_file, data[0].keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                dict_writer.writeheader()
                dict_writer.writerows(data)

def pathoo(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

if __name__ == "__main__":
    arg_parser = ArgumentParser("preprocess.py", description="This script allows one to validate and retrieve citationd data "
                                                      "associated to an OCI (Open Citation Identifier).")

    arg_parser.add_argument("-c", "--csv", dest="csv_path", required=True,
                            help="The output CSV file path.")
    arg_parser.add_argument("-s", "--summaries", dest="summaries_path", required=True,
                            help="The folder path containing orcid sumamries, subfolder will be considered too.")

    args = arg_parser.parse_args()

    iOd = preprocess(args.csv_path)

    iOd.finder(args.summaries_path)

