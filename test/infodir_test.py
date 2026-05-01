# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import tempfile

import pytest
from rdflib import URIRef

from oc_meta.run.infodir.gen import explore_directories
from oc_ocdm.counter_handler.filesystem_counter_handler import FilesystemCounterHandler
from oc_ocdm.support import get_count


class TestGenInfoDir:

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.root_dir = os.path.join('test', 'gen_info_dir', 'rdf')
        self.info_dir = tempfile.mkdtemp()
        self.supplier_prefix = "0670"
        yield

    def test_explore_directories(self):
        info_dir_with_prefix = os.path.join(self.info_dir, self.supplier_prefix) + os.sep
        explore_directories(self.root_dir, self.info_dir)

        counter_handler = FilesystemCounterHandler(info_dir=info_dir_with_prefix, supplier_prefix=self.supplier_prefix)
        br_counter = counter_handler.read_counter("br", supplier_prefix="0670")
        assert br_counter == 386000

        prov_counter_101 = counter_handler.read_counter(
            entity_short_name="br",
            prov_short_name="se",
            identifier=int(get_count(URIRef("https://w3id.org/oc/meta/br/0670101"))),
            supplier_prefix="0670",
        )
        prov_counter_3 = counter_handler.read_counter(
            entity_short_name="br",
            prov_short_name="se",
            identifier=int(get_count(URIRef("https://w3id.org/oc/meta/br/06703"))),
            supplier_prefix="0670",
        )
        assert prov_counter_101 == 2
        assert prov_counter_3 == 1
