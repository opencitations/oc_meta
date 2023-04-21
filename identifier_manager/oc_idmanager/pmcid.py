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


from json import loads
from re import match, sub
from time import sleep
from urllib.parse import quote, unquote

from requests import ReadTimeout, get
from requests.exceptions import ConnectionError

from oc_idmanager.base import IdentifierManager


class PMCIDManager(IdentifierManager):
    """This class implements an identifier manager for PMCID identifier"""

    def __init__(self, data={}, use_api_service=True):
        """PMCID manager constructor."""
        super(PMCIDManager, self).__init__()
        self._api = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
        self._use_api_service = use_api_service
        self._p = "pmcid:"
        self._data = data

        # If there's a need to obtain more metadata from a PMCID, consider using Entrez (aka E-Utilities) API (
        # https://eutils.ncbi.nlm.nih.gov/entrez/eutils/), which of course works with different parameters and
        # returns different responses.
        # The ID Converter API only provides alternative IDs (doi, pmid) for the work associated to the queried pmcid.

    def is_valid(self, pmcid, get_extra_info=False):

        pmcid = self.normalise(pmcid, include_prefix=True)

        if pmcid is None:
            return False
        else:
            if pmcid not in self._data or self._data[pmcid] is None:
                if get_extra_info:
                    info = self.exists(pmcid, get_extra_info=True)
                    self._data[pmcid] = info[1]
                    return (info[0] and self.syntax_ok(pmcid)), info[1]
                self._data[pmcid] = dict()
                self._data[pmcid]["valid"] = True if (self.exists(pmcid) and self.syntax_ok(pmcid)) else False
                return self._data[pmcid].get("valid")
            if get_extra_info:
                return self._data[pmcid].get("valid"), self._data[pmcid]
            return self._data[pmcid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                pmcid_string = id_string[len(self._p):]
            else:
                pmcid_string = id_string

            pmcid_string = sub(
                "\0+", "", sub("\s+", "", unquote(id_string[id_string.index("PMC"):]))
            )
            return "%s%s" % (
                self._p if include_prefix else "",
                pmcid_string.strip(),
            )
        except:
            # Any error in processing the DOI will return None
            return None

    def syntax_ok(self, id_string):

        if not id_string.startswith("pmcid:"):
            id_string = self._p + id_string
        return True if match(r"^pmcid:PMC[1-9]\d+(\.\d{1,2})?$", id_string) else False

    def exists(self, pmcid_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        if self._use_api_service:
            pmcid = self.normalise(pmcid_full)
            if pmcid is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        parameters = {
                            'ids': quote(pmcid),
                            'format': 'json',
                            'idtype': 'pmcid'
                        }

                        r = get(self._api, params=parameters, headers=self._headers, timeout=30)
                        if r.status_code == 200:
                            r.encoding = "utf-8"
                            json_res = loads(r.text)
                            if get_extra_info:
                                extra_info_result = {}
                                try:
                                    result = True if not json_res['records'][0].get('status') =='error' else False
                                    extra_info_result['valid'] = result
                                    return result, extra_info_result
                                except KeyError:
                                    extra_info_result["valid"] = False
                                    return False, extra_info_result
                            try:
                                return True if not json_res['records'][0].get('status') =='error' else False

                            except KeyError:
                                return False

                        elif 400 <= r.status_code < 500:
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
        # to be implemented
        return result
