# -*- coding: utf-8 -*-
# Copyright (c) 2021, Silvio Peroni <essepuntato@gmail.com>
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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


import html
from csv import QUOTE_NONNUMERIC, DictReader, DictWriter
from json import loads
from os.path import exists
from time import sleep

from requests import get

MAX_TRY = 5
SLEEPING_TIME = 5
csv_headers = (
    "id", "name", "prefix"
)
headers = {
    "User-Agent": 
    "OpenCitations "
    "(http://opencitations.net; mailto:contact@opencitations.net)"
}


def get_via_requests(get_url):
    tentative = 0
    print("")

    while tentative < MAX_TRY:
        tentative += 1

        try:
            print("Getting data from", get_url, "- tentative:", tentative)
            r = get(get_url, headers=headers, timeout=10)
            if r.status_code == 200:
                print("\tdata downloaded")
                r.encoding = "utf-8"

                return loads(r.text)
            elif r.status_code == 404:
                return None
            else:
                print("\tdata not downloaded, trying again in ", SLEEPING_TIME, "seconds - status:", r.status_code)
                sleep(SLEEPING_TIME)
        except Exception as e:
            print("\tdata not downloaded, trying again in ", SLEEPING_TIME, "seconds - exception:", e.message)
            sleep(SLEEPING_TIME)


def get_publishers(offset):
    get_url = "https://api.crossref.org/members?rows=1000&offset=" + str(offset)
    req = get_via_requests(get_url)
    
    if req is not None:
        r_json = req.get("message")
        if r_json is not None:
            offset += 1000
            print("\tnext offset is", offset)
            total_results = int(r_json.get("total-results"))
            items = r_json.get("items")

            return items, offset, total_results
                

def process(out_path):
    pub_ids = set()

    if exists(out_path):
        with open(out_path, encoding="utf8") as f:
            csv_reader = DictReader(f, csv_headers)
            for row in csv_reader:
                pub_ids.add(row["id"])

    # cursor = "*"
    offset = 0
    tot = 10000000000
    pub_count = 0
    while offset < tot:
        result, offset, tot = get_publishers(offset)

        if result is not None:
            for publisher in result:
                pub_count += 1
                cur_id = str(publisher["id"])
                if cur_id not in pub_ids:
                    pub_ids.add(cur_id)
                    cur_name = html.unescape(publisher["primary-name"])
                    prefixes = set()
                    for prefix in publisher["prefix"]:
                        prefix_value = prefix["value"]
                        if prefix_value not in prefixes:
                            prefixes.add(prefix_value)
                            store_csv_on_file(out_path, csv_headers, {
                                "id": cur_id, "name": cur_name, "prefix": prefix_value})
    
    if pub_count == tot:
        print("\n\nAll publishers correctly downloaded")
    else:
        print("\n\nOnly %s of the total %s publishers "
              "have been downloaded" % (pub_count, tot))

def store_csv_on_file(f_path, header, json_obj):
    f_exists = exists(f_path)
    with open(f_path, "a", encoding="utf8", newline='') as f:
        dw = DictWriter(f=f, fieldnames=header, delimiter=',', quotechar='"', quoting=QUOTE_NONNUMERIC)
        if not f_exists:
            dw.writeheader()
        dw.writerow(json_obj)
