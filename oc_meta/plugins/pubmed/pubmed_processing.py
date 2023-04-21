import html
import json
import re
import warnings
from os.path import exists

from bs4 import BeautifulSoup
from oc_idmanager.pmid import PMIDManager
from oc_idmanager.doi import DOIManager
from oc_meta.lib.master_of_regex import *
from oc_meta.plugins.ra_processor import RaProcessor
from oc_meta.plugins.pubmed.get_publishers import ExtractPublisherDOI
from oc_meta.plugins.pubmed.finder_nih import NIHResourceFinder
import pathlib
import os
import fakeredis
from oc_meta.datasource.redis import RedisDataSource

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class PubmedProcessing(RaProcessor):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath_pubmed: str = None, journals_filepath: str = None, testing:bool = True):
        super(PubmedProcessing, self).__init__(orcid_index, doi_csv)
        self.nihrf = NIHResourceFinder()
        self.doi_m = DOIManager()
        if testing:
            self.BR_redis= fakeredis.FakeStrictRedis()
            self.RA_redis= fakeredis.FakeStrictRedis()

        else:
            self.BR_redis = RedisDataSource("DB-META-BR")
            self.RA_redis = RedisDataSource("DB-META-RA")

        if not journals_filepath:
            if not exists(os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files")):
                os.makedirs(os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files"))
            self.journals_filepath = os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files",
                                                  "issn_jour_ext.json")
        else:
            self.journals_filepath = journals_filepath

        self.jour_dict = self.issn_data_recover_poci(self.journals_filepath)


        if not publishers_filepath_pubmed:
            if not exists(os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files")):
                os.makedirs(os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files"))
            self.publishers_filepath = os.path.join(pathlib.Path(__file__).parent.resolve(), "support_files",
                                                  "prefix_publishers.json")
        else:
            self.publishers_filepath = publishers_filepath_pubmed

        self.jour_dict = self.issn_data_recover_poci(self.journals_filepath)

        if os.path.exists(self.publishers_filepath):
            with open(self.publishers_filepath, "r", encoding="utf8") as fdp:
                pfp = json.load(fdp)
                if pfp:
                    self.publisher_manager = ExtractPublisherDOI(pfp)
                else:
                    self.publisher_manager = ExtractPublisherDOI({})
        else:
            self.publisher_manager = ExtractPublisherDOI({})
            with open(self.publishers_filepath, "w", encoding="utf8") as fdp:
                json.dump({}, fdp, ensure_ascii=False, indent=4)


    def issn_data_recover_poci(self, path):
        journal_issn_dict = dict()
        if not path:
            return journal_issn_dict
        if not os.path.exists(path):
            return journal_issn_dict
        else:
            with open(path, "r", encoding="utf8") as fd:
                journal_issn_dict = json.load(fd)
                return journal_issn_dict


    def issn_data_to_cache_poci(self, jtitle_issn_dict, path):
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(jtitle_issn_dict, fd, ensure_ascii=False, indent=4)

    def prefix_to_publisher_to_cache(self, pref_pub_dict, path):
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(pref_pub_dict, fd, ensure_ascii=False, indent=4)



    def csv_creator(self, item: dict) -> dict: #input: singola entità, dizionario
        row = dict()
        doi = ""
        pmid = PMIDManager().normalise(str(item['pmid']))
        if (pmid and self.doi_set and pmid in self.doi_set) or (pmid and not self.doi_set):
            # create empty row
            keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                    'publisher', 'editor']
            for k in keys:
                row[k] = ''

            attributes = item

            # row['type']
            row['type'] = 'journal article'

            # row['id']
            ids_list = list()
            ids_list.append(str('pmid:' + pmid))
            if attributes.get('doi'):
                doi = str(DOIManager().normalise(attributes.get('doi'), include_prefix=False))
                if doi:
                    doi_w_pref = "doi:"+doi
                    if self.BR_redis.get(doi_w_pref):
                        ids_list.append(doi_w_pref)
                    elif self.doi_m.is_valid(doi):
                        ids_list.append(doi_w_pref)


            row['id'] = ' '.join(ids_list)

            # row['title']
            pub_title = ""
            if attributes.get("title"):
                p_title = attributes.get("title")
                soup = BeautifulSoup(p_title, 'html.parser')
                title_soup = soup.get_text().replace('\n', '')
                title_soup_space_replaced = ' '.join(title_soup.split())
                title_soup_strip = title_soup_space_replaced.strip()
                clean_tit = html.unescape(title_soup_strip)
                pub_title = clean_tit if clean_tit else p_title

            row['title'] = pub_title

            agents_list = self.add_authors_to_agent_list(attributes, [])
            authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

            # row['author']
            if attributes.get('authors'):
                row['author'] = '; '.join(authors_strings_list)

            # row['pub_date']
            dates = attributes.get("year")
            row['pub_date'] = str(dates) if dates else ""

            # row['venue']
            row['venue'] = self.get_venue_name(attributes, pmid)

            # row['volume']
            row['volume'] = ""

            # row['issue']
            row['issue'] = ""

            # row['page']
            row['page'] = "" #self.get_pubmed_pages(attributes)

            # row['publisher']
            if doi:
                row['publisher'] = self.get_publisher_name(doi)
            else:
                row['publisher'] = ""

            # row['editor']
            row['editor'] = ""

            try:
                return self.normalise_unicode(row)
            except TypeError:
                print(row)
                raise(TypeError)

    def get_pubmed_pages(self, item: dict) -> str:
        '''
        This function returns the pages interval.

        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        page_list = []
        ''' NO INFO IN DUMP: to be updated with API DATA'''
        return self.get_pages(page_list)

    def get_publisher_name(self, doi: str) -> str:
        '''
        This function aims to return a publisher's name and id. If a mapping was provided,
        it is used to find the publisher's standardized name from its id or DOI prefix.

        :params doi: the item's DOI
        :type doi: str

        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example,
        'American Medical Association (AMA) [crossref:10]'. If the id does not exist, the output
        is only the name. Finally, if there is no publisher, the output is an empty string.
        '''
        ''' NO INFO IN DUMP: to be updated with API DATA'''
        publisher_name = self.publisher_manager.extract_publishers_v(doi)
        if publisher_name[0] and publisher_name[0] != "unidentified":
            return publisher_name[0]
        else:
            return ""

    def save_updated_pref_publishers_map(self):
        upd_dict = self.publisher_manager.get_last_map_ver()
        self.prefix_to_publisher_to_cache(upd_dict, self.publishers_filepath)


    def get_venue_name(self, item: dict, id: str) -> str:
        '''
        This method deals with generating the venue's name, followed by id in square brackets, separated by spaces.
        HTML tags are deleted and HTML entities escaped. In addition, any ISBN and ISSN are validated.
        Finally, the square brackets in the venue name are replaced by round brackets to avoid conflicts with the ids enclosures.

        :params item: the item's dictionary
        :type item: dict
        :params row: a CSV row
        :type row: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'Nutrition & Food Science
         [issn:0034-6659]'. If the id does not exist, the output is only the name. Finally, if there is no venue,
         the output is an empty string.
         '''

        short_n = item.get('journal') if item.get('journal') else ""
        venids_list = []
        cont_title = short_n
        if short_n:
            if short_n not in self.jour_dict.keys():
                self.jour_dict[short_n] = {"extended": "", "issn": []}
            if not self.jour_dict[short_n].get("extended") or not self.jour_dict[short_n].get("issn"):
                if id:
                    api_response = self.nihrf._call_api(id)
                    if api_response:
                        if not self.jour_dict[short_n].get("extended"):
                            self.jour_dict[short_n]["extended"] = self.nihrf._get_extended_j_title(api_response)
                        if not self.jour_dict[short_n].get("issn"):
                            issn_dict_list_valid = [x for x in self.nihrf._get_issn(api_response) if x]
                            self.jour_dict[short_n]["issn"] = issn_dict_list_valid
                    self.issn_data_to_cache_poci(self.jour_dict, self.journals_filepath)

            if short_n in self.jour_dict.keys():
                jt_data = self.jour_dict.get(short_n)
                if jt_data.get("issn"):
                    venids_list = [x for x in jt_data.get("issn") if x.startswith("issn:")]
                    venids_list_integration = ["issn:"+x for x in jt_data.get("issn") if not x.startswith("issn:")]
                    venids_list.extend(venids_list_integration)
                extended_jt = jt_data.get("extended") if jt_data.get("extended") else short_n
                cont_title = extended_jt

        # use abbreviated journal title if no mapping was provided
        cont_title = cont_title.replace('\n', '')
        ven_soup = BeautifulSoup(cont_title, 'html.parser')
        ventit = html.unescape(ven_soup.get_text())
        ambiguous_brackets = re.search('\[\s*((?:[^\s]+:[^\s]+)?(?:\s+[^\s]+:[^\s]+)*)\s*\]', ventit)
        if ambiguous_brackets:
            match = ambiguous_brackets.group(1)
            open_bracket = ventit.find(match) - 1
            close_bracket = ventit.find(match) + len(match)
            ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
            ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
            cont_title = ventit

            # IDS
        if venids_list:
            name_and_id = cont_title + ' [' + ' '.join(venids_list) + ']' if cont_title else '[' + ' '.join(venids_list) + ']'
        else:
            name_and_id = cont_title

        return name_and_id

    def add_authors_to_agent_list(self, item: dict, ag_list: list) -> list:
        '''
        This function returns the the agents list updated with the authors dictionaries, in the correct format.

        :params item: the item's dictionary (attributes), ag_list: the agent list
        :type item: dict, ag_list: list

        :returns: listthe agents list updated with the authors dictionaries, in the correct format.
        '''
        agent_list = ag_list
        if item.get("authors"):
            multi_space = re.compile(r"\s+")
            authors_string = str(item.get("authors")).strip()
            authors_split_list = [a.strip() for a in authors_string.split(",") if a]
            for author in authors_split_list:

                agent = {}
                agent["role"] = "author"
                agent["name"] = author
                missing_names = [x for x in ["family", "given", "name"] if x not in agent]
                for mn in missing_names:
                    agent[mn] = ""
                agent_list.append(agent)
        return agent_list

    def add_editors_to_agent_list(self, item: dict, ag_list: list) -> list:
        '''
        This function returns the the agents list updated with the editors dictionaries, in the correct format.

        :params item: the item's dictionary (attributes), ag_list: the agent list
        :type item: dict, ag_list: list

        :returns: listthe agents list updated with the authors dictionaries, in the correct format.
        '''

        agent_list = ag_list

        ''' NO INFO IN DUMP: to be updated with API DATA'''
        return agent_list