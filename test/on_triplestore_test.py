import os
import shutil
import unittest

import redis
from oc_meta.run.upload.cache_manager import CacheManager
from oc_meta.run.upload.on_triplestore import (
    execute_sparql_update,
    save_failed_query_file,
    upload_sparql_updates,
)
from SPARQLWrapper import POST, SPARQLWrapper

SERVER = "http://127.0.0.1:8805/sparql"
TEST_REDIS_DB = 2


def reset_triplestore():
    """Reset the test triplestore"""
    sparql = SPARQLWrapper(SERVER)
    sparql.setMethod(POST)
    sparql.setQuery("DELETE WHERE { GRAPH ?g { ?s ?p ?o } }")
    sparql.query()


def reset_redis():
    """Reset the test Redis database"""
    try:
        r = redis.Redis(host="localhost", port=6381, db=TEST_REDIS_DB, decode_responses=True)
        r.flushdb()
    except redis.ConnectionError:
        print("Warning: Redis not available for tests")


def save_failed_query_file(filename, failed_file):
    with open(failed_file, "a", encoding="utf8") as f:
        f.write(f"{filename}\n")


class TestCacheManager(unittest.TestCase):
    def setUp(self):
        """Setup per ogni test"""
        reset_redis()

    def tearDown(self):
        """Cleanup dopo ogni test"""
        reset_redis()

    def test_cache_initialization(self):
        """Test cache initialization from Redis."""
        # Pre-populate Redis with test data
        r = redis.Redis(host="localhost", port=6381, db=TEST_REDIS_DB, decode_responses=True)
        initial_files = ["file1.sparql", "file2.sparql"]
        r.sadd(CacheManager.REDIS_KEY, *initial_files)

        cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)

        self.assertEqual(cache_manager.get_all(), set(initial_files))

    def test_add_and_contains(self):
        """Test adding files and checking presence."""
        cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)

        test_file = "test.sparql"
        cache_manager.add(test_file)

        self.assertIn(test_file, cache_manager.processed_files)
        self.assertTrue(cache_manager._redis.sismember(CacheManager.REDIS_KEY, test_file))
        self.assertIn(test_file, cache_manager)

    def test_persistence(self):
        """Test data persistence in Redis."""
        cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)
        test_files = ["test1.sparql", "test2.sparql"]
        for file in test_files:
            cache_manager.add(file)

        # Create a new cache manager and verify it loads data from Redis
        new_cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)
        self.assertEqual(new_cache_manager.get_all(), set(test_files))

    def test_redis_required(self):
        """Test that Redis is required."""
        with self.assertRaises(RuntimeError):
            CacheManager(redis_port=9999, redis_db=TEST_REDIS_DB)


class TestOnTriplestore(unittest.TestCase):
    def setUp(self):
        """Setup per ogni test"""
        self.temp_dir = os.path.join("test", "temp_triplestore_test")
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.makedirs(self.temp_dir)

        self.failed_file = os.path.join(self.temp_dir, "failed_queries.txt")
        self.stop_file = os.path.join(self.temp_dir, ".stop_upload")

        try:
            reset_triplestore()
            reset_redis()
        except Exception as e:
            self.skipTest(f"Triplestore o Redis non disponibile: {str(e)}")

    def tearDown(self):
        """Cleanup dopo ogni test"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_cache_operations(self):
        """Test cache operations with CacheManager."""
        cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)

        test_files = {"file1.sparql", "file2.sparql"}
        for file in test_files:
            cache_manager.add(file)

        # Create new cache manager and verify content from Redis
        new_cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)
        self.assertEqual(new_cache_manager.get_all(), test_files)

    def test_failed_query_logging(self):
        """Test del logging delle query fallite"""
        test_file = "failed_test.sparql"
        save_failed_query_file(test_file, self.failed_file)

        with open(self.failed_file, "r") as f:
            content = f.read()
        self.assertIn(test_file, content)

    def test_execute_sparql_update(self):
        """Test dell'esecuzione di una query SPARQL"""
        # Test con una query valida
        valid_query = """
        INSERT DATA {
            GRAPH <http://test.graph> {
                <http://test.subject> <http://test.predicate> "test object" .
            }
        }
        """
        success = execute_sparql_update(SERVER, valid_query)
        self.assertTrue(success)

        # Test con una query non valida
        invalid_query = "INVALID SPARQL QUERY"
        success = execute_sparql_update(SERVER, invalid_query)
        self.assertFalse(success)

    def test_upload_with_stop_file(self):
        """Test upload interruption via stop file."""
        sparql_dir = os.path.join(self.temp_dir, "sparql_files")
        os.makedirs(sparql_dir)

        test_query = """
        INSERT DATA {
            GRAPH <http://test.graph> {
                <http://test.subject> <http://test.predicate> "test object" .
            }
        }
        """
        for i in range(3):
            with open(os.path.join(sparql_dir, f"test{i}.sparql"), "w") as f:
                f.write(test_query)

        with open(self.stop_file, "w") as f:
            f.write("")

        upload_sparql_updates(
            SERVER,
            sparql_dir,
            batch_size=10,
            failed_file=self.failed_file,
            stop_file=self.stop_file,
            cache_manager=CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB),
        )

        cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)
        self.assertLess(len(cache_manager.get_all()), 3)

    def test_upload_with_failures(self):
        """Test upload with failed queries."""
        sparql_dir = os.path.join(self.temp_dir, "sparql_files")
        os.makedirs(sparql_dir)

        valid_query = """
        INSERT DATA {
            GRAPH <http://test.graph> {
                <http://test.subject> <http://test.predicate> "test object" .
            }
        }
        """
        with open(os.path.join(sparql_dir, "valid.sparql"), "w") as f:
            f.write(valid_query)

        invalid_query = "INVALID SPARQL QUERY"
        with open(os.path.join(sparql_dir, "invalid.sparql"), "w") as f:
            f.write(invalid_query)

        cache_manager = CacheManager(redis_port=6381, redis_db=TEST_REDIS_DB)
        upload_sparql_updates(
            SERVER,
            sparql_dir,
            batch_size=10,
            failed_file=self.failed_file,
            stop_file=self.stop_file,
            cache_manager=cache_manager,
        )

        self.assertIn("valid.sparql", cache_manager)
        self.assertNotIn("invalid.sparql", cache_manager)

        with open(self.failed_file, "r") as f:
            failed_content = f.read()
        self.assertIn("invalid.sparql", failed_content)


if __name__ == "__main__":
    unittest.main()
