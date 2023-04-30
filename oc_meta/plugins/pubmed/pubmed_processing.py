import html
import json
import os
import pathlib
import re
import warnings
from os.path import exists
from typing import Dict, List, Tuple

import fakeredis
from bs4 import BeautifulSoup
from oc_idmanager.doi import DOIManager
from oc_idmanager.orcid import ORCIDManager
from oc_idmanager.pmid import PMIDManager

from oc_meta.datasource.redis import RedisDataSource
from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.master_of_regex import *
from oc_meta.plugins.pubmed.finder_nih import NIHResourceFinder
from oc_meta.plugins.pubmed.get_publishers import ExtractPublisherDOI
from oc_meta.plugins.ra_processor import RaProcessor

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

    def csv_creator(self, item: dict) -> dict:
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
                doi = DOIManager().normalise(attributes.get('doi'), include_prefix=False)
                if doi:
                    doi_w_pref = "doi:"+doi
                    if self.BR_redis.get(doi_w_pref):
                        ids_list.append(doi_w_pref)
                    elif self.doi_m.is_valid(doi):
                        ids_list.append(doi_w_pref)
                    else:
                        doi = ''


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
                try:
                    row['publisher'] = self.get_publisher_name(doi)
                except IndexError:
                    print(doi, type(doi), row, item)
                    raise(IndexError)
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

    def find_homonyms(self, lst):
        homonyms_dict = dict()
        multi_space = re.compile(r"\s+")
        extend_pattern = r"[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}(?:\s|$)"
        for d in lst:
            if d.get('name'):
                name = d.get('name')
                author = name.replace(".", " ")
                author = multi_space.sub(" ", author).strip()
                re_extended = re.findall(extend_pattern, author)
                extended = [(s.strip()).lower() for s in re_extended]
                d_hom_set = set()
                for i in extended:
                    dicts_to_check = [dct for dct in lst if dct.get('name') and dct != d]
                    homonyms = [dct.get('name') for dct in dicts_to_check if
                                i in [(s.strip()).lower() for s in re.findall(extend_pattern, dct.get('name'))]]
                    for n in homonyms:
                        d_hom_set.add(n)
                if d_hom_set:
                    homonyms_dict[d.get('name')] = list(d_hom_set)

        return homonyms_dict

    def get_agents_strings_list(self, doi: str, agents_list: List[dict]) -> Tuple[list, list]:
        homonyms_dict = self.find_homonyms(agents_list)
        hom_w_orcid = set()
        authors_strings_list = list()
        editors_string_list = list()
        dict_orcid = None
        multi_space = re.compile(r"\s+")
        inits_pattern = r"([A-Z]|[ÄŐŰÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽÑ]){1}(?:\s|$)"
        extend_pattern = r"[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}(?:\s|$)"

        if not all('orcid' in agent or 'ORCID' in agent for agent in agents_list) and doi:
            dict_orcid = self.orcid_finder(doi)
        agents_list = [
            {k: Cleaner(v).remove_unwanted_characters() if k in {'family', 'given', 'name'} and v is not None
            else v for k, v in
            agent_dict.items()} for agent_dict in agents_list]
        for agent in agents_list:
            cur_role = agent['role']
            f_name = None
            g_name = None
            name = None
            agent_string = None
            if agent.get('family') and agent.get('given'):
                f_name = agent['family']
                g_name = agent['given']
                agent_string = f_name + ', ' + g_name
            elif agent.get('name'):
                name = agent['name']
                f_name = name.split(",")[0].strip() if "," in name else None
                g_name = name.split(",")[-1].strip() if "," in name else None

                if f_name and g_name:
                    agent_string = f_name + ', ' + g_name


            if agent_string is None:
                if agent.get('family') and not agent.get('given'):
                    if g_name:
                        agent_string = agent['family'] + ', ' + g_name
                    else:
                        agent_string = agent['family'] + ', '
                elif agent.get('given') and not agent.get('family'):
                    if f_name:
                        agent_string = f_name + ', ' + agent['given']
                    else:
                        agent_string = ', ' + agent['given']
                elif agent.get('name'):
                    agent_string = agent.get('name')

            orcid = None
            if 'orcid' in agent:
                if isinstance(agent['orcid'], list):
                    orcid = str(agent['orcid'][0])
                else:
                    orcid = str(agent['orcid'])
            elif 'ORCID' in agent:
                if isinstance(agent['ORCID'], list):
                    orcid = str(agent['ORCID'][0])
                else:
                    orcid = str(agent['ORCID'])
            if orcid:
                orcid_manager = ORCIDManager(data=dict(), use_api_service=False)
                orcid = orcid_manager.normalise(orcid, include_prefix=False)
                orcid = orcid if orcid_manager.check_digit(orcid) else None

            elif dict_orcid and f_name:
                for ori in dict_orcid:
                    orc_n: List[str] = dict_orcid[ori].split(', ')
                    orc_f = orc_n[0].lower()
                    orc_g = orc_n[1] if len(orc_n) == 2 else None
                    if f_name.lower() in orc_f.lower() or orc_f.lower() in f_name.lower():
                        if g_name and orc_g:
                            # If there are several authors with the same surname
                            if len([person for person in agents_list if 'family' in person if person['family'] if
                                    person['family'].lower() in orc_f.lower() or orc_f.lower() in person[
                                        'family'].lower()]) > 1:
                                # If there are several authors with the same surname and the same given names' initials
                                if len([person for person in agents_list if 'given' in person if person['given'] if
                                        person['given'][0].lower() == orc_g[0].lower()]) > 1:
                                    homonyms_list = [person for person in agents_list if 'given' in person if
                                                     person['given'] if person['given'].lower() == orc_g.lower()]
                                    # If there are homonyms
                                    if len(homonyms_list) > 1:
                                        # If such homonyms have different roles from the current role
                                        if [person for person in homonyms_list if person['role'] != cur_role]:
                                            if orc_g.lower() == g_name.lower():
                                                orcid = ori
                                    else:
                                        if orc_g.lower() == g_name.lower():
                                            orcid = ori
                                elif orc_g[0].lower() == g_name[0].lower():
                                    orcid = ori
                            # If there is a person whose given name is equal to the family name of the current person (a common situation for cjk names)
                            elif any([person for person in agents_list if 'given' in person if person['given'] if
                                      person['given'].lower() == f_name.lower()]):
                                if orc_g.lower() == g_name.lower():
                                    orcid = ori
                            else:
                                orcid = ori
                        else:
                            orcid = ori

            # If the ra name can't be clearly divided in given name and surname
            elif dict_orcid and name:
                for ori in dict_orcid:
                    try_match = True
                    if name in homonyms_dict.keys():
                        get_best_affinity = self.compute_affinity(dict_orcid[ori], homonyms_dict.keys())
                        if name != get_best_affinity:
                            try_match = False
                    if try_match:
                        orc_n: List[str] = dict_orcid[ori].split(', ')
                        orc_f = orc_n[0].lower()
                        orc_g = orc_n[1] if len(orc_n) == 2 else None

                        author = name.replace(".", " ")
                        author = multi_space.sub(" ", author).strip()
                        re_inits = re.findall(inits_pattern, author)
                        re_extended = re.findall(extend_pattern, author)
                        initials = [(x.strip()).lower() for x in re_inits]
                        extended = [(s.strip()).lower() for s in re_extended]
                        author_dict = {"initials": initials, "extended": extended}

                        surname_match = True if [x for x in author_dict["extended"] if x in orc_f.split()] else False
                        if orc_g:
                            name_match_all = True if [x for x in author_dict["extended"] if x in orc_g.split()] else False
                            name_match_init = True if [x for x in author_dict["initials"] if any(
                                        element.startswith(x) and element not in author_dict["extended"] for
                                                element in orc_g.split())] else False
                        else:
                            name_match_all = False
                            name_match_init = False
                        matches = (surname_match and (name_match_all or name_match_init))

                        if matches:
                            # managing cases in which a name string was already retrieved but the one
                            # provided by the mapping is better
                            f_name = orc_f
                            if not g_name:
                                g_name = orc_g
                            elif g_name:
                                if len(g_name.strip()) < len(orc_g.strip()):
                                    g_name = orc_g
                            orcid = ori

                    if agent_string is None:
                        if f_name and g_name:
                            agent_string = f_name + ', ' + g_name
                        elif f_name and not g_name:
                            agent_string = f_name + ', '
                        elif g_name and not f_name:
                            agent_string = ', ' + g_name
                    elif agent_string == agent.get('name') and f_name and g_name:
                        agent_string = f_name + ', ' + g_name


            if agent_string and orcid:
                agent_string = self.uppercase_initials(agent_string)
                if agent_string not in hom_w_orcid:
                    hom_w_orcid.add(agent_string)
                    agent_string += ' [' + 'orcid:' + str(orcid) + ']'

            if agent_string:
                agent_string = self.uppercase_initials(agent_string)

                if agent['role'] == 'author':
                    authors_strings_list.append(agent_string)
                elif agent['role'] == 'editor':
                    editors_string_list.append(agent_string)

        return authors_strings_list, editors_string_list


    def compute_affinity(self, s, lst):
        s = s.replace(r"\s+", " ")
        s = s.replace(r"\n+", " ")
        name = s.lower()
        agent = name.replace(".", " ")
        agent = agent.replace(",", " ")
        agent = agent.strip()
        agent_name_parts = agent.split()
        extended = [x for x in agent_name_parts if len(x) > 1]
        initials = [x for x in agent_name_parts if len(x) == 1]

        target_agent_dict = {"initials": initials, "extended": extended}

        report_dicts = {}
        for ag in lst:
            name = ag.lower()
            name = name.replace(r"\s+", " ")
            name = name.replace(r"\n+", " ")
            agent = name.replace(".", " ")
            agent = agent.strip()
            agent_name_parts = agent.split()
            ag_extended = [x for x in agent_name_parts if len(x) > 1]
            ag_initials = [x for x in agent_name_parts if len(x) == 1]

            copy_ext_target = [x for x in extended]
            copy_init_target = [x for x in initials]
            copy_ag_ext = [x for x in ag_extended]
            copy_ag_init = [x for x in ag_initials]

            ext_matched = 0
            init_matched = 0

            for i in ag_extended:
                if i in copy_ext_target:
                    ext_matched += 1
                    copy_ext_target.remove(i)
                    copy_ag_ext.remove(i)

            for ii in ag_initials:
                if ii in copy_init_target:
                    init_matched += 1
                    copy_init_target.remove(ii)
                    copy_ag_init.remove(ii)

            # check the remaining unpaired
            # check first if the extra initials in the ra name can be paired with the remaining extended names
            init_compatible = 0

            if copy_ag_init and copy_ext_target:
                remaining_ag_initials = [x for x in copy_ag_init]
                remaining_tar_extended = [x for x in copy_ext_target]

                for ri in remaining_ag_initials:
                    if ri in copy_ag_init:
                        for re in remaining_tar_extended:
                            if re in copy_ext_target:
                                if re.startswith(ri):
                                    copy_ag_init.remove(ri)
                                    copy_ext_target.remove(re)
                                    init_compatible += 1
                                    break

            # check if the remaining initials of the target name are compatible with the remaining extended names of the ra
            ext_compatible = 0

            if copy_ag_ext and copy_init_target:
                remaining_tar_initials = [x for x in copy_init_target]
                remaining_ag_extended = [x for x in copy_ag_ext]

                for ri in remaining_tar_initials:
                    if ri in copy_init_target:
                        for re in remaining_ag_extended:
                            if re in copy_ag_ext:
                                if re.startswith(ri):
                                    copy_ag_ext.remove(re)
                                    copy_init_target.remove(ri)
                                    ext_compatible += 1
                                    break
            ext_not_compatible = len(copy_ag_ext)
            init_not_compatible = len(copy_ag_init)

            cur_agent_dict = {
                "ext_matched": ext_matched,
                "init_matched": init_matched,
                "ext_compatible": ext_compatible,
                "init_compatible": init_compatible,
                "ext_not_compatible": ext_not_compatible,
                "init_not_compatible": init_not_compatible,
            }

            report_dicts[ag] = cur_agent_dict
        best_match_name = self.get_best_match(target_agent_dict, report_dicts)
        return best_match_name



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



    def get_best_match(self, target_agent_dict, report_dicts):
        if max([v.get("ext_matched") for k,v in report_dicts.items()]) == 0:
            return ""
        elif max([v.get("ext_matched") for k,v in report_dicts.items()]) == 1:
            min_comp_dict = {k:v for k,v in report_dicts.items() if v.get("ext_matched") ==1 and (
                    (v.get("init_matched") >= 1 or v.get("ext_compatible")>=1 or v.get("init_compatible")>=1)
                    and
                    (v.get("ext_not_compatible")<= 1 and v.get("init_not_compatible")<= 1)
            )}
            if not min_comp_dict:
                return ""



        len_target_init = len(target_agent_dict["initials"])
        len_target_ext = len(target_agent_dict["extended"])
        if len_target_init + len_target_ext >= 1:

            # Case 1: There is a perfect match with no exceedings: return it
            complete_matches = {k: v for k, v in report_dicts.items() if
                                v["ext_matched"] == len_target_ext and v["init_matched"] == len_target_init and v[
                                    "init_not_compatible"] == 0 and v["ext_not_compatible"] == 0}
            if complete_matches:
                for k in complete_matches.keys():
                    return k
            # Case 2: There is a complete match with all the extended names and the initials of the target are compatible
            match_all_extended_comp_ext = {k: v for k, v in report_dicts.items() if v["ext_matched"] == len_target_ext and (
                        v["init_matched"] + v["ext_compatible"] == len_target_init) and v["init_not_compatible"] == 0 and v[
                                               "ext_not_compatible"] == 0}
            if match_all_extended_comp_ext:
                if len(match_all_extended_comp_ext) == 1:
                    for k in match_all_extended_comp_ext.keys():
                        return k
                else:
                    return [k for k, v in match_all_extended_comp_ext.items() if
                            v["init_matched"] == max([v["init_matched"] for v in match_all_extended_comp_ext.values()])][0]

            # Case 3: Get max extended names match + compatible extended/initials
            max_comp_exc_ext = max([v["ext_matched"] for v in report_dicts.values()])
            match_max_extended_comp_init = {k: v for k, v in report_dicts.items() if
                                            v["ext_matched"] == max_comp_exc_ext and (
                                                        v["ext_matched"] + v["init_compatible"] == len_target_ext) and (
                                                        v["init_matched"] + v["ext_compatible"] == len_target_init) and v[
                                                "init_not_compatible"] == 0 and v["ext_not_compatible"] == 0}
            if match_max_extended_comp_init:
                if len(match_max_extended_comp_init) == 1:
                    for k in match_max_extended_comp_init.keys():
                        return k
                else:
                    return [k for k, v in match_max_extended_comp_init.items() if
                            v["init_matched"] == max([v["init_matched"] for v in match_max_extended_comp_init.values()])][0]

            # Case 4 (suboptimal cases), get best compatibility
            scores_dict = dict()

            for k, v in report_dicts.items():
                score = 0

                p_match_ext = 0
                if len_target_ext:
                    p_match_ext = v["ext_matched"] / len_target_ext
                    if p_match_ext < 1:
                        if v["init_compatible"]:
                            p_match_ext = (v["init_compatible"] * 0.2 + v["ext_matched"]) / len_target_ext

                p_match_init = 0
                if len_target_init:
                    p_match_init = v["init_matched"] / len_target_init
                    if p_match_init < 1:
                        if v["ext_compatible"]:
                            p_match_init = (v["ext_compatible"] * 0.7 + v["init_matched"]) / len_target_init

                total_len_name_parts_target = len_target_ext + len_target_init
                if v["ext_not_compatible"]:
                    p_inc_ext = v["ext_not_compatible"] * 0.7 / total_len_name_parts_target
                else:
                    p_inc_ext = 0
                if v["init_not_compatible"]:
                    p_inc_init = v["init_not_compatible"] * 0.2 / total_len_name_parts_target
                else:
                    p_inc_init = 0
                score = p_match_ext + p_match_init - p_inc_init - p_inc_ext
                scores_dict[k] = score
            result = [k for k, v in scores_dict.items() if v == max(scores_dict.values())]
            if len(result) == 1:
                return result[0]
            else:
                return ""
        return ""