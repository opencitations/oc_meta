#!python
# Copyright 2019, Silvio Peroni <essepuntato@gmail.com>
# Copyright 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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


from __future__ import annotations
from bs4 import BeautifulSoup
from json import loads
from requests import get, ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep


def call_api(url:str, headers:str, r_format:str="json") -> dict|None:
    tentative = 3
    while tentative:
        tentative -= 1
        try:
            r = get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return loads(r.text) if r_format == "json" else BeautifulSoup(r.text, 'xml')
            elif r.status_code == 404:
                return None
        except ReadTimeout:
            # Do nothing, just try again
            pass
        except ConnectionError:
            # Sleep 5 seconds, then try again
            sleep(5)
    return None

def extract_info(api_response:dict, choose_api:str|None=None) -> dict:
    from oc_idmanager.metadata_manager import MetadataManager
    info_dict = {'valid': True}
    metadata_manager = MetadataManager(metadata_provider=choose_api, api_response=api_response)
    info_dict.update(metadata_manager.extract_metadata())
    return info_dict