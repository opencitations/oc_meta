import json
import os
import shutil
import unittest
from unittest.mock import patch
import time

import redis
from oc_meta.run.upload.cache_manager import CacheManager
from oc_meta.run.upload.on_triplestore import (
    execute_sparql_update,
    generate_sparql_queries,
    save_failed_query_file,
    upload_sparql_updates,
)
from oc_meta.run.upload.triplestore_connection import TriplestoreConnection
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
        self.temp_dir = os.path.join("test", "temp_cache_test")
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.makedirs(self.temp_dir)

        self.cache_file = os.path.join(self.temp_dir, "test_cache.json")
        reset_redis()

    def tearDown(self):
        """Cleanup dopo ogni test"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        reset_redis()

    def test_cache_initialization(self):
        """Test dell'inizializzazione del cache"""
        # Crea un file JSON di cache esistente
        initial_files = ["file1.sparql", "file2.sparql"]
        with open(self.cache_file, "w") as f:
            json.dump(initial_files, f)

        # Inizializza CacheManager con il DB di test
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)

        # Verifica che i file siano stati caricati
        self.assertEqual(cache_manager.get_all(), set(initial_files))

        # Verifica che i file siano stati sincronizzati su Redis
        if cache_manager._redis:
            redis_files = cache_manager._redis.smembers(CacheManager.REDIS_KEY)
            self.assertEqual(redis_files, set(initial_files))

    def test_add_and_contains(self):
        """Test dell'aggiunta di file e verifica della presenza"""
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)

        # Aggiungi un file
        test_file = "test.sparql"
        cache_manager.add(test_file)

        # Verifica presenza nel cache locale
        self.assertIn(test_file, cache_manager.processed_files)

        # Verifica presenza in Redis
        if cache_manager._redis:
            self.assertTrue(
                cache_manager._redis.sismember(CacheManager.REDIS_KEY, test_file)
            )

        # Verifica operatore in
        self.assertIn(test_file, cache_manager)

    def test_persistence(self):
        """Test della persistenza dei dati"""
        # Crea e popola un cache manager
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)
        test_files = ["test1.sparql", "test2.sparql"]
        for file in test_files:
            cache_manager.add(file)

        # Forza il salvataggio
        cache_manager._cleanup()

        # Verifica il contenuto del file JSON
        with open(self.cache_file, "r") as f:
            saved_files = set(json.load(f))
        self.assertEqual(saved_files, set(test_files))

        # Crea un nuovo cache manager e verifica che carichi i dati correttamente
        new_cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)
        self.assertEqual(new_cache_manager.get_all(), set(test_files))

    @patch("redis.Redis")
    def test_redis_fallback(self, mock_redis):
        """Test del fallback su JSON quando Redis non è disponibile"""
        # Simula Redis non disponibile
        mock_redis.side_effect = redis.ConnectionError()

        # Crea cache manager
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)

        # Verifica che stia usando solo JSON
        self.assertIsNone(cache_manager._redis)

        # Verifica che le operazioni funzionino comunque
        test_file = "test.sparql"
        cache_manager.add(test_file)
        self.assertIn(test_file, cache_manager)


class TestOnTriplestore(unittest.TestCase):
    def setUp(self):
        """Setup per ogni test"""
        self.temp_dir = os.path.join("test", "temp_triplestore_test")
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.makedirs(self.temp_dir)

        self.cache_file = os.path.join(self.temp_dir, "ts_upload_cache.json")
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

    def test_generate_sparql_queries(self):
        """Test della generazione delle query SPARQL"""
        # Prepara i dati di test
        quads_to_add = [
            ("<subj1>", "<pred1>", '"obj1"', "<graph1>"),
            ("<subj2>", "<pred2>", '"obj2"', "<graph1>"),
            ("<subj3>", "<pred3>", '"obj3"', "<graph2>"),
        ]
        quads_to_remove = [("<subj4>", "<pred4>", '"obj4"', "<graph1>")]

        # Genera le query
        queries = generate_sparql_queries(quads_to_add, quads_to_remove, batch_size=2)

        # Verifica il risultato
        self.assertEqual(len(queries), 3)  # 2 INSERT (batch size 2) + 1 DELETE
        self.assertTrue(any(q.startswith("INSERT DATA {") for q in queries))
        self.assertTrue(any(q.startswith("DELETE DATA {") for q in queries))

    def test_cache_operations(self):
        """Test delle operazioni di cache con CacheManager"""
        # Inizializza cache manager con il DB di test
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)

        # Aggiungi file al cache
        test_files = {"file1.sparql", "file2.sparql"}
        for file in test_files:
            cache_manager.add(file)

        # Forza il salvataggio
        cache_manager._cleanup()

        # Verifica che il file esista
        self.assertTrue(os.path.exists(self.cache_file))

        # Crea nuovo cache manager e verifica il contenuto
        new_cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)
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
        """Test dell'interruzione dell'upload tramite stop file"""
        # Crea directory temporanea con file SPARQL
        sparql_dir = os.path.join(self.temp_dir, "sparql_files")
        os.makedirs(sparql_dir)

        # Crea alcuni file SPARQL di test
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

        # Crea il file di stop
        with open(self.stop_file, "w") as f:
            f.write("")

        # Esegui l'upload
        upload_sparql_updates(
            SERVER,
            sparql_dir,
            batch_size=10,
            cache_file=self.cache_file,
            failed_file=self.failed_file,
            stop_file=self.stop_file,
            cache_manager=CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB),
        )

        # Verifica che il cache non contenga tutti i file
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)
        self.assertLess(len(cache_manager.get_all()), 3)

    def test_upload_with_failures(self):
        """Test dell'upload con query fallite"""
        # Crea directory temporanea con file SPARQL
        sparql_dir = os.path.join(self.temp_dir, "sparql_files")
        os.makedirs(sparql_dir)

        # Crea un file SPARQL valido
        valid_query = """
        INSERT DATA {
            GRAPH <http://test.graph> {
                <http://test.subject> <http://test.predicate> "test object" .
            }
        }
        """
        with open(os.path.join(sparql_dir, "valid.sparql"), "w") as f:
            f.write(valid_query)

        # Crea un file SPARQL non valido
        invalid_query = "INVALID SPARQL QUERY"
        with open(os.path.join(sparql_dir, "invalid.sparql"), "w") as f:
            f.write(invalid_query)

        # Esegui l'upload
        cache_manager = CacheManager(self.cache_file, redis_port=6381, redis_db=TEST_REDIS_DB)
        upload_sparql_updates(
            SERVER,
            sparql_dir,
            batch_size=10,
            cache_file=self.cache_file,
            failed_file=self.failed_file,
            stop_file=self.stop_file,
            cache_manager=cache_manager,
        )

        # Verifica i risultati usando lo stesso CacheManager
        self.assertIn("valid.sparql", cache_manager)
        self.assertNotIn("invalid.sparql", cache_manager)

        # Verifica il file dei fallimenti
        with open(self.failed_file, "r") as f:
            failed_content = f.read()
        self.assertIn("invalid.sparql", failed_content)


class TestTriplestoreConnection(unittest.TestCase):
    def setUp(self):
        self.endpoint = SERVER
        self.connection = TriplestoreConnection(self.endpoint)

    def test_singleton_pattern(self):
        """Test che verifica che venga usata la stessa istanza"""
        connection2 = TriplestoreConnection(self.endpoint)
        self.assertIs(self.connection, connection2)

    def test_connection_update(self):
        """Test che verifica che la connessione possa essere aggiornata"""
        new_endpoint = "http://new.endpoint/sparql"
        connection2 = TriplestoreConnection(new_endpoint)
        self.assertEqual(connection2.sparql.endpoint, new_endpoint)

    def test_execute_update(self):
        """Test dell'esecuzione di una query"""
        query = """
        INSERT DATA {
            GRAPH <http://test.graph> {
                <http://test.subject> <http://test.predicate> "test object" .
            }
        }
        """
        success = self.connection.execute_update(query)
        self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()
