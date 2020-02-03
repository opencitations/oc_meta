import os
from bs4 import BeautifulSoup
from meta.lib.csvmanager import CSVManager
from meta.lib.id_manager.doimanager import DOIManager
from argparse import ArgumentParser


class index_orcid_doi:

    def __init__(self, csv_path, valid_doi):
        if not os.path.exists(os.path.dirname(csv_path)):
            os.makedirs(os.path.dirname(csv_path))
        if not os.path.exists(os.path.dirname(valid_doi)):
            os.makedirs(os.path.dirname(valid_doi))
        self.doi_index = CSVManager(valid_doi)
        self.doimanager = DOIManager(valid_doi=self.doi_index)
        self.csvstorage = CSVManager(csv_path)

    def finder (self, summaries_path):
        for fold, dirs, files in os.walk(summaries_path):
            for file in files:
                if file.endswith('.xml'):
                    xml_file = open(os.path.join(fold, file), 'r')
                    xml_soup = BeautifulSoup(xml_file, 'xml')
                    g_name = xml_soup.find('personal-details:given-names')
                    f_name = xml_soup.find('personal-details:family-name')
                    if f_name:
                        if g_name:
                            g_name = g_name.get_text()
                        f_name = f_name.get_text()
                        name = f_name + ", " + g_name
                        ids = xml_soup.findAll('common:external-id')
                        if ids:
                            for el in ids:
                                type = el.find('common:external-id-type')
                                rel = el.find('common:external-id-relationship')
                                if type and rel:
                                    if type.get_text().lower() == "doi" and rel.get_text().lower() == "self":
                                        doi = el.find('common:external-id-value').get_text()
                                        if self.doimanager.is_valid(doi):
                                            doi = self.doimanager.normalise(doi)
                                            if doi:
                                                orcid = file.replace(".xml", "")
                                                auto = name + " [" + orcid + "]"
                                                self.csvstorage.add_value(doi, auto)


if __name__ == "__main__":
    arg_parser = ArgumentParser("index_orcid_doi.py", description="This script builds a csv index of DOIs associated with ORCIDs, "
                                                                  "starting from XML files containing ORCID data.")


    arg_parser.add_argument("-c", "--csv", dest="csv_path", required=True,
                            help="The output CSV file path.")
    arg_parser.add_argument("-s", "--summaries", dest="summaries_path", required=True,
                            help="The folder path containing orcid summaries, subfolder will be considered too.")
    arg_parser.add_argument("-v", "--valid", dest="valid_doi", required=True,
                            help="Filepath of CSv containing valid DOIs.")

    args = arg_parser.parse_args()

    iOd = index_orcid_doi(args.csv_path, args.valid_doi)

    iOd.finder(args.summaries_path)




#index_orcid_doi("C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\summaries", "orcid.csv")