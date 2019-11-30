import os
from bs4 import BeautifulSoup
from scripts.csvmanager import CSVManager
from scripts.id_manager.doimanager import DOIManager


class index_orcid_doi:

    def __init__(self, summaries_path, csv_path):
        self.doi_index = CSVManager("doi_index.csv")
        self.doimanager = DOIManager(valid_doi=self.doi_index)
        self.csvstorage = CSVManager(csv_path)
        self.index = dict()
        self.finder(summaries_path)
        for x in self.index:
            self.csvstorage.add_value(x, "; ".join(self.index[x]))

    def finder (self, summaries_path):
        x = 0
        for fold, dirs, files in os.walk(summaries_path):
            for file in files:
                if x > 10:
                    return
                print(x)
                if file.endswith('.xml'):
                    xml_file = open(os.path.join(fold, file), 'r', encoding="utf-8")
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
                                                x +=1
                                                auto = name + " [" + orcid + "]"
                                                if doi in self.index:
                                                    if auto not in self.index[doi]:
                                                        self.index[doi].append(auto)
                                                else:
                                                    self.index[doi] = list()
                                                    self.index[doi].append(auto)
