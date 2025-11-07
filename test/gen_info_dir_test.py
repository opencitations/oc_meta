import unittest
import os
from oc_meta.run.gen_info_dir import explore_directories
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.support import get_count
import shutil
import redis

class TestGenInfoDir(unittest.TestCase):
    
    def setUp(self):
        self.root_dir = os.path.join('test', 'gen_info_dir', 'rdf')
        self.redis_host = 'localhost'
        self.redis_port = 6381
        self.redis_db = 0
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.redis_client.flushdb()

    def tearDown(self):
        self.redis_client.flushdb()
        self.redis_client.close()

    def test_explore_directories(self):
        explore_directories(self.root_dir, self.redis_host, self.redis_port, self.redis_db)
        
        # Check main counters
        counter_handler = RedisCounterHandler(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        br_counter = counter_handler.read_counter("br", supplier_prefix="0670")
        self.assertEqual(br_counter, 386000)
        
        # Check provenance counters
        prov_counter_101 = counter_handler.read_counter(
            entity_short_name="br",
            prov_short_name="se",
            identifier=int(get_count('https://w3id.org/oc/meta/br/0670101')),
            supplier_prefix="0670"
        )
        prov_counter_3 = counter_handler.read_counter(
            entity_short_name="br",
            prov_short_name="se",
            identifier=int(get_count('https://w3id.org/oc/meta/br/06703')),
            supplier_prefix="0670"
        )
        self.assertEqual(prov_counter_101, 2)
        self.assertEqual(prov_counter_3, 1)

if __name__ == "__main__":
    unittest.main()