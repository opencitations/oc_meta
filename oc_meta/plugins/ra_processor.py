#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright 2022 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import re
import unicodedata
from csv import DictReader
from typing import Dict, List, Tuple
import json

from oc_idmanager import ISBNManager, ISSNManager, ORCIDManager

from oc_meta.lib.cleaner import Cleaner
from oc_meta.lib.csvmanager import CSVManager
from oc_meta.lib.master_of_regex import orcid_pattern


class RaProcessor(object):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath: str = None):
        self.doi_set = CSVManager.load_csv_column_as_set(doi_csv, 'id') if doi_csv else None
        self.publishers_mapping = self.load_publishers_mapping(publishers_filepath) if publishers_filepath else None
        orcid_index = orcid_index if orcid_index else None
        self.orcid_index = CSVManager(orcid_index)

    def get_agents_strings_list(self, doi: str, agents_list: List[dict]) -> Tuple[list, list]:
        homonyms_dict = self.find_homonyms(agents_list)
        hom_w_orcid = set()
        authors_strings_list = list()
        editors_string_list = list()
        dict_orcid = None
        multi_space = re.compile(r"\s+")
        inits_pattern = r"([A-Z]|[ÄŐŰÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽÑ]){1}(?:\s|$)"
        extend_pattern = r"[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}(?:\s|$)"

        if not all('orcid' in agent or 'ORCID' in agent for agent in agents_list):
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

            # Check homonyms in cases in which it is not possoble to assess which parts of the name
            # belong to the family name and which ones to the given name
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
                        name_match_all = True if [x for x in author_dict["extended"] if x in orc_g.split()] else False
                        name_match_init = True if [x for x in author_dict["initials"] if any(
                                    element.startswith(x) and element not in author_dict["extended"] for
                                               element in orc_g.split())] else False
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


            if agent_string and orcid: # Schulz, Heide N
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

    def orcid_finder(self, doi:str) -> dict:
        found = dict()
        doi = doi.lower()
        people:List[str] = self.orcid_index.get_value(doi)
        if people:
            for person in people:
                orcid = re.search(orcid_pattern, person).group(0)
                name:str = person[:person.find(orcid)-1]
                found[orcid] = name.strip().lower()
        return found

    def get_pages(self, pages_list:list) -> str:
        '''
        This function returns the pages interval. 

        :params pages_list: a list of pages
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        roman_letters = {'I', 'V', 'X', 'L', 'C', 'D', 'M'}
        clean_pages_list = list()
        for page in pages_list:
            # e.g. 583-584
            if all(c.isdigit() for c in page):
                clean_pages_list.append(page)
            # e.g. G27. It is a born digital document. PeerJ uses this approach, where G27 identifies the whole document, since it has no pages.
            elif len(pages_list) == 1:
                clean_pages_list.append(page)
            # e.g. iv-vii. This syntax is used in the prefaces.
            elif all(c.upper() in roman_letters for c in page):
                clean_pages_list.append(page)
            # 583b-584. It is an error. The b must be removed.
            elif any(c.isdigit() for c in page):
                page_without_letters = ''.join([c for c in page if c.isdigit()])
                clean_pages_list.append(page_without_letters)
        if clean_pages_list:
            if len(clean_pages_list) == 1:
                clean_pages_list.append(clean_pages_list[0])
            return '-'.join(clean_pages_list)
        return ''


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

    def get_best_match(self, target_agent_dict, report_dicts):

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

    @staticmethod
    def normalise_unicode(metadata: dict) -> dict:
        return {k:unicodedata.normalize('NFKC', v) for k, v in metadata.items()}

    @staticmethod
    def id_worker(field, ids:list, func) -> None:
        if isinstance(field, list):
            for i in field:
                func(str(i), ids)
        else:
            id = str(field)
            func(id, ids)

    @staticmethod
    def load_publishers_mapping(publishers_filepath) -> dict:
        publishers_mapping: Dict[str, Dict[str, set]] = dict()
        with open(publishers_filepath, 'r', encoding='utf-8') as f:
            data = DictReader(f)
            for row in data:
                id = row['id']
                publishers_mapping.setdefault(id, dict())
                publishers_mapping[id]['name'] = row['name']
                publishers_mapping[id].setdefault('prefixes', set()).add(row['prefix'])
        return publishers_mapping

    
    @staticmethod
    def issn_worker(issnid:str, ids:list):
        issn_manager = ISSNManager()
        issnid = issn_manager.normalise(issnid, include_prefix=False)
        if issn_manager.check_digit(issnid) and f'issn:{issnid}' not in ids:
            ids.append('issn:' + issnid)

    @staticmethod
    def isbn_worker(isbnid, ids:list):
        isbn_manager = ISBNManager()
        isbnid = isbn_manager.normalise(isbnid, include_prefix=False)
        if isbn_manager.check_digit(isbnid) and f'isbn:{isbnid}' not in ids:
            ids.append('isbn:' + isbnid)

    @staticmethod
    def uppercase_initials(inp_str: str):
        upper_word_list = []
        words_list = inp_str.split()
        for w in words_list:
            upper_word_list.append(w[0].upper() + w[1:]) if len(w)>1 else upper_word_list.append(w[0].upper())
        upper_str = " ".join(upper_word_list)
        return upper_str