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
SERVER = 'http://127.0.0.1:9999/blazegraph/sparql'

def reset_server(server:str=SERVER) -> None:
    ts = SPARQLWrapper(server)
    ts.setQuery('delete{?x ?y ?z} where{?x ?y ?z}')
    ts.setMethod(POST)
    ts.query()

# class test_Endgame(unittest.TestCase):
#     def test_endgame(self):
#         reset_server()
#         # {'csv': {'ra': 9403, 'br': 10472, 're': 0, 'id': 0, 'ar': 0}, 'rdf': {'ra': 133479, 'br': 32669, 're': 21937, 'id': 58722, 'ar': 157990}, 'triplestore': {'ra': 133479, 'br': 32669, 're': 21937, 'ar': 157990, 'id': 58722}}
#         meta_config_path = os.path.join(BASE_DIR, 'endgame.yaml')
#         with open(meta_config_path, encoding='utf-8') as file:
#             settings = yaml.full_load(file)
#         # python_executable = "python" if platform == "win32" else "python3"
#         # prepare_command = ["poetry", "run", python_executable, "-m", "oc_meta.run.prepare_multiprocess", "-c", meta_config_path]
#         # result = subprocess.run(prepare_command, shell=True, check=True)
#         # if result.returncode == 0:
#         run_meta_process(settings=settings, meta_config_path=meta_config_path)
#         # else:
#         #     raise Exception("prepare_multiprocess failed")

if __name__ == '__main__':
    unittest.main()