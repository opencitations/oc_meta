#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Giuseppe Grieco <giuseppe.grieco3@unibo.it>, Arianna Moretti <arianna.moretti4@unibo.it>, Elia Rizzetto <elia.rizzetto@studio.unibo.it>, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import re
from datetime import datetime
from re import match, sub
from time import sleep
from urllib.parse import quote

from bs4 import BeautifulSoup
from requests import ReadTimeout, get
from requests.exceptions import ConnectionError

from oc_idmanager import *
from oc_idmanager.base import IdentifierManager


class PMIDManager(IdentifierManager):
    """This class implements an identifier manager for pmid identifier"""

    def __init__(self, data={}, use_api_service=True):
        """PMID manager constructor."""
        super(PMIDManager, self).__init__()
        self._api = "https://pubmed.ncbi.nlm.nih.gov/"
        self._use_api_service = use_api_service
        self._p = "pmid:"
        self._data = data
        self._im = ISSNManager()
        #regex
        self._doi_regex = r"(?<=^AID\s-\s).*\[doi\]\s*\n"
        self._pmid_regex = r"(?<=PMID-\s)[1-9]\d*"
        self._title_regex = r"(?<=^TI\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._author_regex = r"(?<=^FAU\s-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._date_regex = r"DP\s+-\s+(\d{4}(\s?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))?(\s?((3[0-1])|([1-2][0-9])|([0]?[1-9])))?)"
        self._issn_regex = r"(?<=^IS\s{2}-\s)[0-9]{4}-[0-9]{3}[0-9X]"
        self._journal_regex = r"(?<=^JT\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._volume_regex = r"(?<=^VI\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._issue_regex = r"(?<=^IP\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._page_regex = r"(?<=^PG\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._type_regex = r"(?<=^PT\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._publisher_regex = r"(?<=^PB\s{2}-\s)(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"
        self._editor_regex = r"((?<=^FED\s-\s)|(?<=^ED\s{2}-\s))(.+?)*(\n\s{6}(.+?)*)*(?=(?:\n[A-Z]{2,4}\s{,2}-\s*|$))"

    def is_valid(self, pmid, get_extra_info=False):
        pmid = self.normalise(pmid, include_prefix=True)

        if pmid is None:
            return False
        else:
            if pmid not in self._data or self._data[pmid] is None:
                if get_extra_info:
                    info = self.exists(pmid, get_extra_info=True)
                    self._data[pmid] = info[1]
                    return (info[0] and self.syntax_ok(pmid)), info[1]
                self._data[pmid] = dict()
                self._data[pmid]["valid"] = True if (self.exists(pmid) and self.syntax_ok(pmid)) else False
                return self._data[pmid].get("valid")

            if get_extra_info:
                return self._data[pmid].get("valid"), self._data[pmid]
            return self._data[pmid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        id_string = str(id_string)
        try:
            pmid_string = sub("^0+", "", sub("\0+", "", (sub("[^\d+]", "", id_string))))
            return "%s%s" % (self._p if include_prefix else "", pmid_string)
        except:
            # Any error in processing the PMID will return None
            return None

    def syntax_ok(self, id_string):
        if not id_string.startswith(self._p):
            id_string = self._p + id_string
        return True if match("^pmid:[1-9]\d*$", id_string) else False

    def exists(self, pmid_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        if self._use_api_service:
            pmid = self.normalise(pmid_full)
            if pmid is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(
                            self._api + quote(pmid) + "/?format=pubmed",
                            headers=self._headers,
                            timeout=30,
                        )
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            soup = BeautifulSoup(r.text, features="lxml")
                            txt_obj = str(soup.find(id="article-details"))
                            match_pmid = re.finditer(self._pmid_regex, txt_obj, re.MULTILINE)
                            for matchNum_pmid, match_p in enumerate(match_pmid, start=1):
                                m_pmid = match_p.group()
                                if m_pmid:
                                    if get_extra_info:
                                        return True, self.extra_info(txt_obj)
                                    return True
                        elif r.status_code == 404:
                            if get_extra_info:
                                return False, {"valid": False}
                            return False

                    except ReadTimeout:
                        # Do nothing, just try again
                        pass
                    except ConnectionError:
                        # Sleep 5 seconds, then try again
                        sleep(5)
                valid_bool = False
            else:
                if get_extra_info:
                    return False, {"valid": False}
                return False
        if get_extra_info:
            return valid_bool, {"valid": valid_bool}
        return valid_bool

    def extra_info(self, api_response, choose_api=None, info_dict={}):
        result = {}
        result["valid"] = True

        try:
            title = ""
            match_title = re.finditer(self._title_regex, api_response, re.MULTILINE)
            for matchNum_tit, match_tit in enumerate(match_title, start=1):
                m_title = match_tit.group()
                if m_title:
                    ts = re.sub("\s+", " ", m_title)
                    t = re.sub("\n", " ", ts)
                    norm_title = t.strip()
                    if norm_title is not None:
                        title = norm_title
                        break
        except:
            title = ""

        result["title"] = title

        try:
            authors = set()
            fa_aut = re.finditer(self._author_regex, api_response, re.MULTILINE)
            for matchNum_aut, match_au in enumerate(fa_aut, start=1):
                m_aut = match_au.group()
                if m_aut:
                    fau = re.sub("\s+", " ", m_aut)
                    nlfau = re.sub("\n", " ", fau)
                    norm_fau = nlfau.strip()
                    if norm_fau is not None:
                        authors.add(norm_fau)
            authorsList = list(authors)
        except:
            authorsList = []

        result["author"] = authorsList

        try:
            date = re.search(self._date_regex,
                api_response,
                re.IGNORECASE,
            ).group(1)
            re_search = re.search(
                "(\d{4})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+((3[0-1])|([1-2][0-9])|([0]?[1-9]))",
                date,
                re.IGNORECASE,
            )
            if re_search is not None:
                src = re_search.group(0)
                datetime_object = datetime.strptime(src, "%Y %b %d")
                pmid_date = datetime.strftime(datetime_object, "%Y-%m-%d")
            else:
                re_search = re.search(
                    "(\d{4})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
                    date,
                    re.IGNORECASE,
                )
                if re_search is not None:
                    src = re_search.group(0)
                    datetime_object = datetime.strptime(src, "%Y %b")
                    pmid_date = datetime.strftime(datetime_object, "%Y-%m")
                else:
                    re_search = re.search("(\d{4})", date)
                    if re_search is not None:
                        src = re.search("(\d{4})", date).group(0)
                        datetime_object = datetime.strptime(src, "%Y")
                        pmid_date = datetime.strftime(datetime_object, "%Y")
                    else:
                        pmid_date = ""
        except:
            pmid_date = ""
        result["pub_date"] = pmid_date

        try:
            issnset = set()
            fa_issn = re.finditer(self._issn_regex, api_response, re.MULTILINE)
            for matchNum_issn, match_issn in enumerate(fa_issn, start=1):
                m_issn = match_issn.group()
                if m_issn:
                    norm_issn = self._im.normalise(m_issn, include_prefix=True)
                    if norm_issn is not None:
                        issnset.add(norm_issn)
            issnlist = list(issnset)
        except:
            issnlist = []

        # CONTINUA DA QUI

        try:
            jur_title = ""
            fa_jur_title = re.finditer(self._journal_regex, api_response, re.MULTILINE)
            for matchNum_title, match_tit in enumerate(fa_jur_title, start=1):
                m_title = match_tit.group()
                if m_title:
                    s_jt = re.sub("\s+", " ", m_title)
                    n_jt = re.sub("\n", " ", s_jt)
                    norm_jour = n_jt.strip()
                    if norm_jour is not None:
                        jur_title = norm_jour
                        break
        except:
            jur_title = ""

        result["venue"] = (
            f'{jur_title} {[x for x in issnlist]}' if jur_title else str(issnlist).replace(",", "")).replace("'", "")

        try:
            volume = ""
            fa_volume = re.finditer(self._volume_regex, api_response, re.MULTILINE)
            for matchNum_volume, match_vol in enumerate(fa_volume, start=1):
                m_vol = match_vol.group()
                if m_vol:
                    vol = re.sub("\s+", " ", m_vol)
                    norm_volume = vol.strip()
                    if norm_volume is not None:
                        volume = norm_volume
                        break
        except:
            volume = ""

        result["volume"] = volume

        try:
            issue = ""
            fa_issue = re.finditer(self._issue_regex, api_response, re.MULTILINE)
            for matchNum_issue, match_issue in enumerate(fa_issue, start=1):
                m_issue = match_issue.group()
                if m_issue:
                    s_issue = re.sub("\s+", " ", m_issue)
                    n_issue = re.sub("\n", " ", s_issue)
                    norm_issue = n_issue.strip()
                    if norm_issue is not None:
                        issue = norm_issue
                        break
        except:
            issue = ""

        result["issue"] = issue

        try:
            pag = ""
            fa_pag = re.finditer(self._page_regex, api_response, re.MULTILINE)
            for matchNum_pag, match_pag in enumerate(fa_pag, start=1):
                m_pag = match_pag.group()
                if m_pag:
                    s_pg = re.sub("\s+", " ", m_pag)
                    n_pg = re.sub("\n", " ", s_pg)
                    norm_pag = n_pg.strip()
                    if norm_pag is not None:
                        pag = norm_pag
                        break
        except:
            pag = ""

        result["page"] = pag

        try:
            pub_types = set()
            types = re.finditer(self._type_regex, api_response, re.MULTILINE)
            for matchNum_types, match_types in enumerate(types, start=1):
                m_type = match_types.group()
                if m_type:
                    s_ty = re.sub("\s+", " ", m_type)
                    b_ty = re.sub("\n", " ", s_ty)
                    norm_type = b_ty.strip().lower()
                    if norm_type is not None:
                        pub_types.add(norm_type)
            typeslist = list(pub_types)
        except:
            typeslist = []

        result["type"] = typeslist

        try:
            publisher = set()
            publishers = re.finditer(self._publisher_regex, api_response, re.MULTILINE)
            for matchNum_publishers, match_publishers in enumerate(publishers, start=1):
                m_publishers = match_publishers.group()
                if m_publishers:
                    s_pbs = re.sub("\s+", " ", m_publishers)
                    n_pbs = re.sub("\n", " ", s_pbs)
                    norm_pbs = n_pbs.strip()
                    if norm_pbs is not None:
                        publisher.add(norm_pbs)
            publisherlist = list(publisher)
        except:
            publisherlist = []

        result["publisher"] = publisherlist

        try:
            editor = set()
            editors = re.finditer(self._editor_regex, api_response, re.MULTILINE)
            for matchNum_editors, match_editors in enumerate(editors, start=1):
                m_editors = match_editors.group()
                if m_editors:
                    s_ed = re.sub("\s+", " ", m_editors)
                    n_ed = re.sub("\n", " ", s_ed)
                    norm_ed = n_ed.strip()
                    if norm_ed is not None:
                        editor.add(norm_ed)
            editorlist = list(editor)
        except:
            editorlist = []

        result["editor"] = editorlist

        doi = ""
        try:
            map_doi = re.finditer(self._doi_regex, api_response, re.MULTILINE)
            for matchNum_doi, match_doi in enumerate(map_doi, start=1):
                m_doi = match_doi.group()
                if m_doi:
                    id = re.sub("\s+", " ", m_doi)
                    n_id = re.sub("\n", " ", id)
                    n_id_strip = n_id.strip()

                    if n_id_strip.endswith('[doi]'):
                        n_id_strip = n_id_strip[:-5]
                    dm = DOIManager()
                    norm_id = dm.normalise(n_id_strip)
                    if norm_id is not None:
                        doi = norm_id
                        break
                    else:
                        doi = ""
        except:
            doi = ""

        result["doi"] = doi

        return result
