#!python
# Copyright 2023, Arcangelo Massari <arcangelo.massari@unibo.it>
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

import os
import unittest
from SPARQLWrapper import POST, SPARQLWrapper


BASE_DIR = os.path.join('test', 'endgame')
SERVER = 'http://127.0.0.1:8805/sparql'

def reset_server(server:str=SERVER) -> None:
    ts = SPARQLWrapper(server)
    ts.setQuery('DELETE WHERE { GRAPH ?g { ?s ?p ?o } }')
    ts.setMethod(POST)
    ts.query()

if __name__ == '__main__':
    unittest.main()