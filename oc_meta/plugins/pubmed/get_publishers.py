import re

import requests
from lxml import etree


class ExtractPublisherDOI(object):
    def __init__(self, pref_info_dict):
        self.description = "class aimed at extracting publishers' names exploiting the DOI "
        if pref_info_dict:
            self._prefix_to_data_dict = pref_info_dict
        else:
            self._prefix_to_data_dict = dict()

    def get_last_map_ver(self):
        return self._prefix_to_data_dict

    def add_prefix_pub_data(self,prefix):
        if prefix not in self._prefix_to_data_dict.keys():
            pref_to_publisher = dict()
            req_url = "https://api.crossref.org/prefixes/" + prefix

            try:
                req = requests.get(url=req_url)
                req_status_code = req.status_code
                if req_status_code == 200:
                    req_data = req.json()
                    pref_to_publisher["name"] = req_data["message"]["name"]
                    extended_member_code = req_data["message"]["member"]
                    reduced_member_code = (re.findall("(\d+)", extended_member_code))[0]
                    pref_to_publisher["crossref_member"] = reduced_member_code
                    pref_to_publisher["from"] = "Crossref"
                else:
                    pref_to_publisher["name"] = "unidentified"
                    pref_to_publisher["crossref_member"] = "not found"
                    pref_to_publisher["from"] = "not found"


                self._prefix_to_data_dict[prefix] = pref_to_publisher

            except requests.ConnectionError:
                print("failed to connect to crossref for", prefix)
                quit()
        return self._prefix_to_data_dict


    def search_in_datacite(self,doi):
        publisher = dict()
        datacite_req_url = "https://api.datacite.org/dois/" + doi

        try:
            req = requests.get(url=datacite_req_url)

            req_status_code = req.status_code
            if req_status_code == 200:
                req_data = req.json()
                publisher["name"] = req_data["data"]["attributes"]["publisher"]
                publisher["prefix"] = doi.split('/')[0]

        except requests.ConnectionError:
            print("failed to connect to datacite for", doi)

        return publisher


    def search_in_medra(self, doi):
        publisher = dict()
        medra_req_url = "https://api.medra.org/metadata/" + doi

        try:
            req = requests.get(url=medra_req_url)

            req_status_code = req.status_code
            if req_status_code == 200:
                tree = etree.XML(req.content)
                publisher_xpath = tree.xpath('//x:PublisherName',
                                             namespaces={'x': 'http://www.editeur.org/onix/DOIMetadata/2.0'})
                if len(publisher_xpath) == 0:
                    return publisher
                publisher["name"] = publisher_xpath[0].text
                publisher["prefix"] = doi.split('/')[0]

        except requests.ConnectionError:
            print("failed to connect to crossref for", doi)

        return publisher


    def search_for_cnki(self,doi):
        publisher = dict()
        datacite_req_url = "https://doi.org/api/handles/" + doi

        try:
            req = requests.get(url=datacite_req_url)

            req_status_code = req.status_code
            if req_status_code == 200:
                req_data = req.json()
                if 'values' in req_data.keys() and 'data' in req_data['values'][0].keys():
                    if 'www.cnki.net' in req_data['values'][0]['data']['value']:
                        publisher["name"] = 'CNKI Publisher (unspecified)'
                        publisher["prefix"] = doi.split('/')[0]

        except requests.ConnectionError:
            print("failed to connect to doi for", doi)

        return publisher


    def add_extra_publisher(self, publisher, agency):
        self._prefix_to_data_dict[publisher["prefix"]] = {
            'name': publisher['name'],
            'from': agency
        }


    def search_for_publisher_in_other_agencies(self,doi):
        publisher = self.search_in_datacite(doi)
        if 'name' in publisher.keys():
            self.add_extra_publisher(publisher, 'datacite')
            return self._prefix_to_data_dict
        publisher = self.search_in_medra(doi)
        if 'name' in publisher.keys():
            self.add_extra_publisher(publisher, 'medra')
            return self._prefix_to_data_dict
        publisher = self.search_for_cnki(doi)
        if 'name' in publisher.keys():
            self.add_extra_publisher(publisher, 'doi')
            return self._prefix_to_data_dict


    """
    extract_publishers_valid(row, publisher_data, prefix_to_member_code_dict, external_data_dict) manages the 
    addition of unprocessed publishers’ dictionaries to publisher_data and the update 
    of the values related to the number of either valid or invalid addressed or received 
    citations, in the case a dictionary for a given publisher already exists. 
    In the case a publisher's prefix doesn't allow its identification in Crossref, we call the function 
    search_for_publisher_in_other_agencies(row[1], external_data_dict), in order to try to identify it in other services.
    This very last option is not included for the version for not validated citations. 
    """

    def extract_publishers_v(self,doi):
        prefix = re.findall("(^10.\d{4,9})", doi.split('/')[0])[0]

        prefix_to_data_dict = self.add_prefix_pub_data(prefix)

        if prefix_to_data_dict[prefix]["from"] == "not found":
            self.search_for_publisher_in_other_agencies(doi)
        return prefix_to_data_dict[prefix]["name"], prefix_to_data_dict
