from re import sub, match
from oc_idmanager.base import IdentifierManager
from urllib.parse import quote
from time import sleep
from requests import ReadTimeout, get
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

class JIDManager(IdentifierManager):
    """This class implements an identifier manager for jid identifier"""
    def __init__(self, data={}, use_api_service=True):
        """JID manager constructor"""
        super(JIDManager, self).__init__()
        self._api = "https://api.jstage.jst.go.jp/searchapi/"
        self._api2 = "https://www.jstage.jst.go.jp/browse/"
        self.use_api_service = use_api_service 
        self._p = "jid:"
        self._data = data

    def is_valid(self, jid, get_extra_info=False):
        jid = self.normalise(jid, include_prefix=True)

        if jid is None:
            return False
        else:
            if jid not in self._data or self._data[jid] is None:
                if get_extra_info:
                    info = self.exists(jid, get_extra_info=True)
                    self._data[jid] = info[1]
                    return (info[0] and self.syntax_ok(jid)), info[1]
                self._data[jid] = dict()
                self._data[jid]["valid"] = True if (self.exists(jid) and self.syntax_ok(jid)) else False
                return self._data[jid].get("valid")
            
            if get_extra_info:
                return self._data[jid].get("valid"), self._data[jid]
            return self._data[jid].get("valid")
    
    def normalise(self, id_string, include_prefix=False):
        try:
            if id_string.startswith(self._p):
                jid_string = id_string[len(self._p):]
            else:
                jid_string = id_string
            jid_string = sub("[^/a-z0-9]", "", jid_string.lower())
            return "%s%s" % (self._p if include_prefix else "", jid_string)
        except:
            # Any error in processing the PMID will return None
            return None
    
    def syntax_ok(self, id_string):
        if not id_string.startswith(self._p):
            id_string = self._p+id_string
        return True if match("^jid:[a-z]+([12][0-9]{3}){0,1}[a-z]*$", id_string) else False

       
    
    def exists(self, jid_full, get_extra_info=False, allow_extra_api=None):
        valid_bool = True
        if self.use_api_service:
            jid = self.normalise(jid_full)
            if jid is not None:
                tentative = 3
                while tentative:
                    tentative -= 1
                    try:
                        r = get(self._api+ "/do?service=2&cdjournal=" + quote(jid), headers=self._headers, timeout=30)
                        #fromstring() parses XML from a string directly into an Element, which is the root element of the parsed tree
                        root = ET.fromstring(r.content)
                        status = root.find(".//{http://www.w3.org/2005/Atom}status").text
                        if status =="0":
                            if get_extra_info:
                                return True, self.extra_info(r.content)
                            return True
                        elif status == "ERR_001":
                            if get_extra_info:
                                return False, {"valid": False}
                            return False
                        else:
                            tentative=3
                            while tentative:
                                tentative -=1
                                try:
                                    r = get(self._api+ "/do?service=2&cdjournal=" + quote(jid), headers=self._headers, timeout=30)
                                    #fromstring() parses XML from a string directly into an Element, which is the root element of the parsed tree
                                    root = ET.fromstring(r.content)
                                    status = root.find(".//{http://www.w3.org/2005/Atom}status").text
                                    if status =="0":
                                        if get_extra_info:
                                            return True, self.extra_info(r.content)
                                        return True
                                    elif status == "ERR_001":
                                        if get_extra_info:
                                            return False, {"valid": False}
                                        return False
                                except ReadTimeout:
                                # Do nothing, just try again
                                    pass
                                except ConnectionError:
                                # Sleep 5 seconds, then try again
                                    sleep(5)

                            #inserisci chiamata all'altra API
                            try:
                                r = get(self._api2 + quote(jid), headers=self._headers, timeout=30)
                                if r.status_code == 404:
                                    if get_extra_info:
                                        return False, {"valid": False}
                                    return False
                                elif r.status_code == 200:
                                    r.encoding = "utf-8"
                                    soup = BeautifulSoup(r.text, features="lxml")
                                    txt_obj = str(soup.find(id="page-content"))
                                    if get_extra_info:
                                        return True, self.extra_info(txt_obj)
                                    return True
                            except ReadTimeout:
                                # Do nothing, just try again
                                    pass
                            except ConnectionError:
                            # Sleep 5 seconds, then try again
                                sleep(5)

                            if get_extra_info:
                                return False, {"valid": False}
                            return False
                    except ReadTimeout:
                        # Do nothing, just try again
                        pass
                    except ConnectionError:
                        # Sleep 5 seconds, then try again
                        sleep(5)

                valid_bool=False

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
        return result
