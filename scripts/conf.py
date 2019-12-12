#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2016, Silvio Peroni <essepuntato@gmail.com>
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

__author__ = 'essepuntato'

# Official configuration
base_dir = "srv\\data\\corpus\\"
base_iri = "https://w3id.org/OC/meta/"
triplestore_url = "http://localhost:9999/blazegraph/sparql"
triplestore_url_real = ""
context_path = "https://w3id.org/oc/corpus/context.json"
context_file_path = "/srv/data/corpus/context.json"
info_dir = "/srv/share/id-counter/"
temp_dir_for_rdf_loading = "/tmp/"
orcid_conf_path = "/srv/dev/script/spacin/orcid_conf.json"
reference_dir = "/srv/share/ref/todo/"
reference_dir_error = "/srv/share/ref/err/"
reference_dir_done = "/srv/share/ref/done/"
dataset_home = "http://opencitations.net/"
dir_split_number = 10000  # This must be multiple of the following one
items_per_file = 1000
default_dir = "_"
supplier_dir = {
    "101": "01110",
    "102": "01120",
    "103": "01130",
    "104": "01140",
    "105": "01150",
    "106": "01160",
    "107": "01170",
    "108": "01180",
    "109": "01190",
    "110": "01910",
    "111": "01210",
    "112": "01220",
    "113": "01230",
    "114": "01240",
    "115": "01250",
    "116": "01260",
    "117": "01270",
    "118": "01280",
    "119": "01290",
    "120": "01920",
    "121": "01310",
    "122": "01320",
    "123": "01330",
    "124": "01340",
    "125": "01350",
    "126": "01360",
    "127": "01370",
    "128": "01380",
    "129": "01390",
    "130": "01930",
}
interface = "eth0"
do_parallel = True
sharing_dir = "/srv/share/data/"