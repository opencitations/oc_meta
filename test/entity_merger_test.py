import csv
import json
import os
import re
import unittest
from shutil import rmtree

import redis
import yaml
from oc_meta.run.merge.entities import EntityMerger
from oc_meta.run.meta_editor import MetaEditor
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from oc_ocdm.graph import GraphSet
from oc_ocdm.prov.prov_set import ProvSet
from oc_ocdm.storer import Storer
from rdflib import URIRef
from SPARQLWrapper import POST, SPARQLWrapper

BASE = os.path.join("test", "merger")
OUTPUT = os.path.join(BASE, "output/")
META_CONFIG = os.path.join("test", "merger", "meta_config.yaml")
SERVER = "http://127.0.0.1:8805/sparql"
PROV_SERVER = "http://127.0.0.1:8806/sparql"

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6381
REDIS_DB = 5  # For counters
REDIS_CACHE_DB = 2  # For cache, using same test DB as on_triplestore_test.py


def reset_triplestore():
    """Reset the test triplestore graphs"""
    # Reset main triplestore
    endpoint = SPARQLWrapper(SERVER)
    for graph in [
        "https://w3id.org/oc/meta/br/",
        "https://w3id.org/oc/meta/ra/",
        "https://w3id.org/oc/meta/re/",
        "https://w3id.org/oc/meta/id/",
        "https://w3id.org/oc/meta/ar/",
    ]:
        endpoint.setQuery(f"CLEAR GRAPH <{graph}>")
        endpoint.setMethod(POST)
        endpoint.query()
        
    # Reset provenance triplestore
    prov_endpoint = SPARQLWrapper(PROV_SERVER)
    for graph in [
        "https://w3id.org/oc/meta/br/",
        "https://w3id.org/oc/meta/ra/",
        "https://w3id.org/oc/meta/re/",
        "https://w3id.org/oc/meta/id/",
        "https://w3id.org/oc/meta/ar/",
    ]:
        prov_endpoint.setQuery(f"CLEAR GRAPH <{graph}>")
        prov_endpoint.setMethod(POST)
        prov_endpoint.query()


def reset_redis_counters():
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_cache_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_CACHE_DB
    )
    redis_client.flushdb()
    redis_cache_client.flushdb()


def get_counter_handler():
    return RedisCounterHandler(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


class TestEntityMerger(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.counter_handler = get_counter_handler()

    def setUp(self):
        # Reset environment
        if os.path.exists(OUTPUT):
            rmtree(OUTPUT)
        os.makedirs(os.path.join(BASE, "csv"), exist_ok=True)
        reset_triplestore()
        reset_redis_counters()

        # Create temporary directory for cache files
        self.temp_dir = os.path.join("test", "temp_entity_merger_test")
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        os.makedirs(self.temp_dir)

        # Setup cache files
        self.cache_file = os.path.join(self.temp_dir, "ts_upload_cache.json")
        self.failed_file = os.path.join(self.temp_dir, "failed_queries.txt")
        self.stop_file = os.path.join(self.temp_dir, ".stop_upload")
        
        # Create separate directories for data and provenance update queries
        self.data_update_dir = os.path.join(self.temp_dir, "to_be_uploaded_data")
        self.prov_update_dir = os.path.join(self.temp_dir, "to_be_uploaded_prov")
        os.makedirs(self.data_update_dir, exist_ok=True)
        os.makedirs(self.prov_update_dir, exist_ok=True)

        # Update config with Redis and cache settings
        with open(META_CONFIG, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        settings.update(
            {
                "redis_host": REDIS_HOST,
                "redis_port": REDIS_PORT,
                "redis_db": REDIS_DB,
                "redis_cache_db": REDIS_CACHE_DB,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
                "provenance_triplestore_url": PROV_SERVER,
                "data_update_dir": self.data_update_dir,
                "prov_update_dir": self.prov_update_dir
            }
        )
        with open(META_CONFIG, "w", encoding="utf-8") as file:
            yaml.dump(settings, file)

        # Initialize test data
        self.setup_test_data()

        # Create merger instance
        self.merger = EntityMerger(
            meta_config=META_CONFIG,
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            entity_types=["ra", "br", "id"],
            stop_file_path="stop.out",
            workers=4,
        )

    def tearDown(self):
        if os.path.exists(os.path.join(BASE, "csv")):
            rmtree(os.path.join(BASE, "csv"))
        if os.path.exists(OUTPUT):
            rmtree(OUTPUT)
        if os.path.exists("stop.out"):
            os.remove("stop.out")
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        reset_triplestore()
        reset_redis_counters()

    def setup_test_data(self):
        """Create initial test data in triplestore"""
        # Create a GraphSet for test data
        g_set = GraphSet(
            "https://w3id.org/oc/meta/",
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )

        # Create first author entity with specific ID
        author1 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0601"),
        )
        author1.has_name("John Smith")

        # Create ORCID identifier for author1 with specific ID
        orcid_id = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0601"),
        )
        orcid_id.create_orcid("0000-0001-1234-5678")
        author1.has_identifier(orcid_id)

        # Create second author entity with specific ID
        author2 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0602"),
        )
        author2.has_name("J. Smith")

        # Create VIAF identifier for author2 with specific ID
        viaf_id = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0602"),
        )
        viaf_id.create_viaf("12345678")
        author2.has_identifier(viaf_id)

        # Create a publication with specific ID
        pub = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/0601"),
        )
        pub.has_title("Test Publication")
        pub.has_pub_date("2024-01-01")

        # Create role for first author with specific ID
        role1 = g_set.add_ar(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ar/0601"),
        )
        role1.create_author()
        role1.is_held_by(author1)
        pub.has_contributor(role1)

        # Create role for second author with specific ID
        role2 = g_set.add_ar(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ar/0602"),
        )
        role2.create_author()
        role2.is_held_by(author2)
        pub.has_contributor(role2)

        prov = ProvSet(
            g_set,
            "https://w3id.org/oc/meta/",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        prov.generate_provenance()

        rdf_output = os.path.join(OUTPUT, "rdf") + os.sep

        res_storer = Storer(
            abstract_set=g_set,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )
        prov_storer = Storer(
            abstract_set=prov,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )

        res_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        prov_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        res_storer.upload_all(
            triplestore_url=SERVER,
            base_dir=rdf_output,
            batch_size=10,
            save_queries=False,
        )

        # Create CSV file for merger
        merge_data = [
            {
                "surviving_entity": str(author1.res),
                "merged_entities": str(author2.res),
                "Done": "False",
            }
        ]
        self.write_csv("merge_test.csv", merge_data)

    def write_csv(self, filename: str, data: list):
        filepath = os.path.join(BASE, "csv", filename)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def check_sparql_query_content(self, query: str, expected_triples: dict):
        """
        Check if a SPARQL query contains expected triples in DELETE and INSERT sections.

        Args:
            query (str): The SPARQL update query string
            expected_triples (dict): Dictionary with 'delete' and 'insert' keys containing
                                   lists of triple patterns to check for
        """
        # Split query into DELETE and INSERT sections
        delete_match = re.search(
            r"DELETE DATA \{ GRAPH.*?\{(.*?)\}.*?\}", query, re.DOTALL
        )
        insert_match = re.search(
            r"INSERT DATA \{ GRAPH.*?\{(.*?)\}.*?\}", query, re.DOTALL
        )

        delete_section = delete_match.group(1) if delete_match else ""
        insert_section = insert_match.group(1) if insert_match else ""
        # Check DELETE patterns
        if "delete" in expected_triples:
            for triple in expected_triples["delete"]:
                self.assertIn(
                    triple,
                    delete_section.strip(),
                    f"Expected triple not found in DELETE section: {triple}",
                )

        # Check INSERT patterns
        if "insert" in expected_triples:
            for triple in expected_triples["insert"]:
                self.assertIn(
                    triple,
                    insert_section.strip(),
                    f"Expected triple not found in INSERT section: {triple}",
                )

    def test_get_entity_type(self):
        """Test the static method get_entity_type"""
        test_cases = [
            ("https://w3id.org/oc/meta/ra/06107", "ra"),
            ("https://w3id.org/oc/meta/br/06101", "br"),
            ("https://w3id.org/oc/meta/id/06105", "id"),
            ("https://example.com/invalid/url", None),
            ("", None),
        ]

        for url, expected in test_cases:
            with self.subTest(url=url):
                self.assertEqual(EntityMerger.get_entity_type(url), expected)

    def test_read_write_csv(self):
        """Test CSV read and write operations"""
        test_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/ra/06107",
                "merged_entities": "https://w3id.org/oc/meta/ra/06205",
                "Done": "False",
            }
        ]

        # Write test data
        test_file = os.path.join(BASE, "csv", "test.csv")
        EntityMerger.write_csv(test_file, test_data)

        # Read back and verify
        read_data = EntityMerger.read_csv(test_file)
        self.assertEqual(test_data, read_data)

    def test_count_csv_rows(self):
        """Test CSV row counting"""
        # Test with empty file
        empty_file = os.path.join(BASE, "csv", "empty.csv")
        with open(empty_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["col1", "col2"])
            writer.writeheader()
        self.assertEqual(EntityMerger.count_csv_rows(empty_file), 0)

        # Test with multiple rows
        test_file = os.path.join(BASE, "input", "0.csv")
        row_count = EntityMerger.count_csv_rows(test_file)
        self.assertEqual(row_count, 1)

    def test_process_file_with_stop_file(self):
        """Test that processing stops when stop file is present"""
        # Create stop file
        with open(self.merger.stop_file_path, "w") as f:
            f.write("")

        # Process test file
        test_file = os.path.join(BASE, "csv", "merge_test.csv")
        result = self.merger.process_file(test_file)

        # Verify the file wasn't processed (Done should still be False)
        data = EntityMerger.read_csv(test_file)
        self.assertEqual(data[0]["Done"], "False")

    def test_process_folder(self):
        """Test processing multiple files in a folder"""
        csv_folder = os.path.join(BASE, "csv")
        self.merger.process_folder(csv_folder)

        # Verify all files were processed
        for filename in ["merge_test.csv"]:
            with self.subTest(file=filename):
                data = EntityMerger.read_csv(os.path.join(csv_folder, filename))
                self.assertEqual(data[0]["Done"], "True")

    def test_process_folder_with_worker_limit(self):
        """Test processing folder with worker count > 4"""
        self.merger.workers = 5
        csv_folder = os.path.join(BASE, "csv")

        # Create a large file
        large_data = [
            {
                "surviving_entity": f"https://w3id.org/oc/meta/ra/0610{i}",
                "merged_entities": f"https://w3id.org/oc/meta/ra/0620{i}",
                "Done": "False",
            }
            for i in range(15000)
        ]  # Create more than 10000 rows
        self.write_csv("large.csv", large_data)

        self.merger.process_folder(csv_folder)

        # Verify only small files were processed
        large_file_data = EntityMerger.read_csv(os.path.join(csv_folder, "large.csv"))
        self.assertEqual(
            large_file_data[0]["Done"], "False"
        )  # Large file should be skipped

        small_file_data = EntityMerger.read_csv(
            os.path.join(csv_folder, "merge_test.csv")
        )
        self.assertEqual(
            small_file_data[0]["Done"], "True"
        )  # Small file should be processed

    def test_merge_authors_with_real_data(self):
        """Test merging two author entities with real data"""
        # Process the merge
        csv_folder = os.path.join(BASE, "csv")
        self.merger.process_folder(csv_folder)

        # Verify files structure
        rdf_path = os.path.join(OUTPUT)
        self.assertTrue(
            os.path.exists(os.path.join(rdf_path, "rdf", "ra", "060", "10000", "1000"))
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(rdf_path, "rdf", "ra", "060", "10000", "1000", "prov")
            )
        )

        # Load and verify data files
        ra_file = os.path.join(rdf_path, "rdf", "ra", "060", "10000", "1000.json")
        with open(ra_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] == "https://w3id.org/oc/meta/ra/0601":
                        # Check has both identifiers
                        identifiers = {
                            id_obj["@id"]
                            for id_obj in entity[
                                "http://purl.org/spar/datacite/hasIdentifier"
                            ]
                        }
                        self.assertEqual(len(identifiers), 2)
                        self.assertIn("https://w3id.org/oc/meta/id/0601", identifiers)
                        self.assertIn("https://w3id.org/oc/meta/id/0602", identifiers)

                        # Check name
                        self.assertEqual(
                            entity["http://xmlns.com/foaf/0.1/name"][0]["@value"],
                            "J. Smith",
                        )

                    # Check merged entity no longer exists
                    if entity["@id"] == "https://w3id.org/oc/meta/ra/0602":
                        self.fail("Merged entity should not exist")

        # Check role reassignment
        ar_file = os.path.join(rdf_path, "rdf", "ar", "060", "10000", "1000.json")
        with open(ar_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    agent = entity["http://purl.org/spar/pro/isHeldBy"][0]["@id"]
                    self.assertEqual(
                        agent,
                        "https://w3id.org/oc/meta/ra/0601",
                        "All roles should point to surviving entity",
                    )

        # Check provenance
        prov_file = os.path.join(
            rdf_path, "rdf", "ra", "060", "10000", "1000", "prov", "se.json"
        )
        found_merge_prov = False
        expected_triples = {
            "delete": [
                '<https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/name> "John Smith"'
            ],
            "insert": [
                "<https://w3id.org/oc/meta/ra/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0602>",
                '<https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/name> "J. Smith"',
            ],
        }
        with open(prov_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if (
                        "/prov/se/" in entity["@id"]
                        and "merge"
                        in entity.get("http://purl.org/dc/terms/description", [{}])[0]
                        .get("@value", "")
                        .lower()
                    ):
                        found_merge_prov = True

                        # Check provenance fields
                        self.assertIn(
                            "http://www.w3.org/ns/prov#generatedAtTime", entity
                        )
                        self.assertIn(
                            "http://www.w3.org/ns/prov#wasAttributedTo", entity
                        )
                        self.assertIn(
                            "https://w3id.org/oc/ontology/hasUpdateQuery", entity
                        )

                        # Get actual query and normalize both expected and actual
                        actual_query = entity[
                            "https://w3id.org/oc/ontology/hasUpdateQuery"
                        ][0]["@value"]
                        self.check_sparql_query_content(actual_query, expected_triples)

        self.assertTrue(found_merge_prov, "No merge provenance found")

    def test_merge_with_invalid_entity_type(self):
        """Test merging with an invalid entity type"""
        # Create test data with invalid entity type
        invalid_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/invalid/0601",
                "merged_entities": "https://w3id.org/oc/meta/invalid/0602",
                "Done": "False",
            }
        ]
        test_file = os.path.join(BASE, "csv", "invalid_type.csv")
        self.write_csv("invalid_type.csv", invalid_data)
        self.merger.process_file(test_file)
        data = EntityMerger.read_csv(test_file)
        self.assertEqual(data[0]["Done"], "False")

    def test_merge_with_nonexistent_entities(self):
        """Test merging when one or both entities don't exist"""
        # Create test data with nonexistent entities
        nonexistent_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/ra/9999",
                "merged_entities": "https://w3id.org/oc/meta/ra/9998",
                "Done": "False",
            }
        ]
        test_file = os.path.join(BASE, "csv", "nonexistent.csv")
        self.write_csv("nonexistent.csv", nonexistent_data)
        self.merger.process_file(test_file)
        data = EntityMerger.read_csv(test_file)
        self.assertEqual(data[0]["Done"], "True")

    def test_merge_multiple_entities(self):
        """Test merging multiple entities into one surviving entity"""
        # Create additional test entities
        g_set = GraphSet(
            "https://w3id.org/oc/meta/",
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )

        # Create additional authors
        author3 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0603"),
        )
        author3.has_name("John A. Smith")

        author4 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0604"),
        )
        author4.has_name("J A Smith")

        # Add identifiers
        viaf_id = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0603"),
        )
        viaf_id.create_viaf("123456789")
        author3.has_identifier(viaf_id)

        researcher_id = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0604"),
        )
        researcher_id.create_wikidata("Q12345")
        author4.has_identifier(researcher_id)

        # Create publications and roles
        pub2 = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/0602"),
        )
        pub2.has_title("Another Test Publication")

        role3 = g_set.add_ar(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ar/0603"),
        )
        role3.create_author()
        role3.is_held_by(author3)
        pub2.has_contributor(role3)

        role4 = g_set.add_ar(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ar/0604"),
        )
        role4.create_author()
        role4.is_held_by(author4)
        pub2.has_contributor(role4)

        # Store and upload
        prov = ProvSet(
            g_set,
            "https://w3id.org/oc/meta/",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        prov.generate_provenance()

        rdf_output = os.path.join(OUTPUT, "rdf") + os.sep

        res_storer = Storer(
            abstract_set=g_set,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )
        prov_storer = Storer(
            abstract_set=prov,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )

        res_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        prov_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        res_storer.upload_all(
            triplestore_url=SERVER,
            base_dir=rdf_output,
            batch_size=10,
            save_queries=False,
        )

        # Create merge data
        merge_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/ra/0601",
                "merged_entities": "https://w3id.org/oc/meta/ra/0602; https://w3id.org/oc/meta/ra/0603; https://w3id.org/oc/meta/ra/0604",
                "Done": "False",
            }
        ]
        test_file = os.path.join(BASE, "csv", "multiple_merge.csv")
        self.write_csv("multiple_merge.csv", merge_data)

        # Process the merge
        csv_folder = os.path.join(BASE, "csv")
        self.merger.process_folder(csv_folder)

        # Verify the results by checking the output files
        rdf_path = os.path.join(OUTPUT, "rdf")

        # 1. Check researcher file for surviving entity and merged data
        ra_file = os.path.join(rdf_path, "ra", "060", "10000", "1000.json")
        with open(ra_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] == "https://w3id.org/oc/meta/ra/0601":
                        # Check has all identifiers
                        identifiers = {
                            id_obj["@id"]
                            for id_obj in entity[
                                "http://purl.org/spar/datacite/hasIdentifier"
                            ]
                        }
                        self.assertEqual(len(identifiers), 4)
                        expected_ids = {
                            "https://w3id.org/oc/meta/id/0601",
                            "https://w3id.org/oc/meta/id/0602",
                            "https://w3id.org/oc/meta/id/0603",
                            "https://w3id.org/oc/meta/id/0604",
                        }
                        self.assertEqual(identifiers, expected_ids)

                        # Check name (should take the last merged name)
                        self.assertEqual(
                            entity["http://xmlns.com/foaf/0.1/name"][0]["@value"],
                            "J A Smith",
                        )

                    # Check merged entities no longer exist
                    self.assertNotIn(
                        entity["@id"],
                        [
                            "https://w3id.org/oc/meta/ra/0602",
                            "https://w3id.org/oc/meta/ra/0603",
                            "https://w3id.org/oc/meta/ra/0604",
                        ],
                    )

        # 2. Check role assignments in agent role file
        ar_file = os.path.join(rdf_path, "ar", "060", "10000", "1000.json")
        with open(ar_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if "http://purl.org/spar/pro/isHeldBy" in entity:
                        agent = entity["http://purl.org/spar/pro/isHeldBy"][0]["@id"]
                        self.assertEqual(
                            agent,
                            "https://w3id.org/oc/meta/ra/0601",
                            "All roles should point to surviving entity",
                        )

        # 3. Check provenance
        prov_file = os.path.join(
            rdf_path, "ra", "060", "10000", "1000", "prov", "se.json"
        )
        with open(prov_file) as f:
            data = json.load(f)

            # Get all provenance snapshots for surviving entity
            surviving_snapshots = []
            for graph in data:
                if graph["@id"] == "https://w3id.org/oc/meta/ra/0601/prov/":
                    for entity in graph.get("@graph", []):
                        # Skip creation snapshot
                        if (
                            "created"
                            not in entity.get(
                                "http://purl.org/dc/terms/description", [{}]
                            )[0]
                            .get("@value", "")
                            .lower()
                        ):
                            surviving_snapshots.append(entity)

            # Should have 2 merge snapshots (one partial, one final)
            self.assertEqual(
                len(surviving_snapshots), 2, "Should have exactly 2 merge snapshots"
            )

            # Verify partial merge (0601 with 0602)
            partial_merge = next(
                s
                for s in surviving_snapshots
                if "0602" in s["http://purl.org/dc/terms/description"][0]["@value"]
                and "0603" not in s["http://purl.org/dc/terms/description"][0]["@value"]
            )

            # Check partial merge metadata
            self.assertIn("http://www.w3.org/ns/prov#generatedAtTime", partial_merge)
            self.assertIn("http://www.w3.org/ns/prov#wasAttributedTo", partial_merge)
            self.assertEqual(
                partial_merge["http://www.w3.org/ns/prov#wasAttributedTo"][0]["@id"],
                "https://orcid.org/0000-0002-8420-0696",
            )

            # Check partial merge query content
            partial_query = partial_merge[
                "https://w3id.org/oc/ontology/hasUpdateQuery"
            ][0]["@value"]
            expected_partial = {
                "delete": [
                    '<https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/name> "John Smith"'
                ],
                "insert": [
                    "<https://w3id.org/oc/meta/ra/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0602>",
                    '<https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/name> "J. Smith"',
                ],
            }
            self.check_sparql_query_content(partial_query, expected_partial)

            # Verify final merge (0601 with 0602, 0603, 0604)
            final_merge = next(
                s
                for s in surviving_snapshots
                if "0602" in s["http://purl.org/dc/terms/description"][0]["@value"]
                and "0603" in s["http://purl.org/dc/terms/description"][0]["@value"]
                and "0604" in s["http://purl.org/dc/terms/description"][0]["@value"]
            )

            # Check final merge metadata
            self.assertIn("http://www.w3.org/ns/prov#generatedAtTime", final_merge)
            self.assertIn("http://www.w3.org/ns/prov#wasAttributedTo", final_merge)
            self.assertEqual(
                final_merge["http://www.w3.org/ns/prov#wasAttributedTo"][0]["@id"],
                "https://orcid.org/0000-0002-8420-0696",
            )

            # Check final merge query content
            final_query = final_merge["https://w3id.org/oc/ontology/hasUpdateQuery"][0][
                "@value"
            ]
            expected_final = {
                "delete": [
                    '<https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/name> "John Smith"'
                ],
                "insert": [
                    "<https://w3id.org/oc/meta/ra/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0603>",
                    "<https://w3id.org/oc/meta/ra/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0604>",
                    '<https://w3id.org/oc/meta/ra/0601> <http://xmlns.com/foaf/0.1/name> "J A Smith"',
                    "<https://w3id.org/oc/meta/ra/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0602>",
                ],
            }
            self.check_sparql_query_content(final_query, expected_final)

            # Verify deletion snapshots exist for merged entities
            merged_ids = ["0602", "0603", "0604"]
            for merged_id in merged_ids:
                merged_snapshots = []
                for graph in data:
                    if graph["@id"] == f"https://w3id.org/oc/meta/ra/{merged_id}/prov/":
                        for entity in graph.get("@graph", []):
                            if (
                                "deleted"
                                in entity.get(
                                    "http://purl.org/dc/terms/description", [{}]
                                )[0]
                                .get("@value", "")
                                .lower()
                            ):
                                merged_snapshots.append(entity)

                self.assertGreater(
                    len(merged_snapshots),
                    0,
                    f"No deletion snapshot found for ra/{merged_id}",
                )

                # Verify deletion queries
                for snapshot in merged_snapshots:
                    self.assertIn(
                        "https://w3id.org/oc/ontology/hasUpdateQuery", snapshot
                    )
                    delete_query = snapshot[
                        "https://w3id.org/oc/ontology/hasUpdateQuery"
                    ][0]["@value"]
                    self.assertIn(
                        f"<https://w3id.org/oc/meta/ra/{merged_id}>", delete_query
                    )
                    self.assertIn("DELETE DATA", delete_query)

    def test_merge_with_conflicting_data(self):
        """Test merging entities with conflicting information"""
        # Create test entities with conflicting data
        g_set = GraphSet(
            "https://w3id.org/oc/meta/",
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )

        # Create conflicting authors
        author5 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0605"),
        )
        author5.has_name("John Smith")
        author5.has_given_name("John")
        author5.has_family_name("Smith")

        author6 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0606"),
        )
        author6.has_name("Johnny Smith")
        author6.has_given_name("Johnny")
        author6.has_family_name("Smith")

        # Add same identifier to both (which should be impossible in real data)
        same_orcid = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0605"),
        )
        same_orcid.create_orcid("0000-0001-9999-9999")
        author5.has_identifier(same_orcid)

        same_orcid2 = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0606"),
        )
        same_orcid2.create_orcid("0000-0001-9999-9999")
        author6.has_identifier(same_orcid2)

        # Store and upload
        prov = ProvSet(
            g_set,
            "https://w3id.org/oc/meta/",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        prov.generate_provenance()

        rdf_output = os.path.join(OUTPUT, "rdf") + os.sep

        res_storer = Storer(
            abstract_set=g_set,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )
        prov_storer = Storer(
            abstract_set=prov,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )

        res_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        prov_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        res_storer.upload_all(
            triplestore_url=SERVER,
            base_dir=rdf_output,
            batch_size=10,
            save_queries=False,
        )

        # Create merge data
        merge_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/ra/0605",
                "merged_entities": "https://w3id.org/oc/meta/ra/0606",
                "Done": "False",
            }
        ]
        test_file = os.path.join(BASE, "csv", "conflicting_merge.csv")
        self.write_csv("conflicting_merge.csv", merge_data)

        # Process the merge
        csv_folder = os.path.join(BASE, "csv")
        self.merger.process_folder(csv_folder)

        # Verify the results by checking the output files
        rdf_path = os.path.join(OUTPUT, "rdf")

        # 1. Check researcher file for surviving entity and merged data
        ra_file = os.path.join(rdf_path, "ra", "060", "10000", "1000.json")
        with open(ra_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] == "https://w3id.org/oc/meta/ra/0605":
                        # Check identifiers - should only keep one instance
                        identifiers = {
                            id_obj["@id"]
                            for id_obj in entity.get(
                                "http://purl.org/spar/datacite/hasIdentifier", []
                            )
                        }
                        self.assertEqual(len(identifiers), 1)
                        self.assertEqual(
                            identifiers, {"https://w3id.org/oc/meta/id/0605"}
                        )

                        # Check name was preserved
                        self.assertEqual(
                            entity["http://xmlns.com/foaf/0.1/name"][0]["@value"],
                            "Johnny Smith",
                        )
                        self.assertEqual(
                            entity["http://xmlns.com/foaf/0.1/givenName"][0]["@value"],
                            "Johnny",
                        )
                        self.assertEqual(
                            entity["http://xmlns.com/foaf/0.1/familyName"][0]["@value"],
                            "Smith",
                        )

                    # Check merged entity does not exist in output
                    self.assertNotEqual(
                        entity["@id"], "https://w3id.org/oc/meta/ra/0606"
                    )

        # 2. Check provenance
        prov_file = os.path.join(
            rdf_path, "ra", "060", "10000", "1000", "prov", "se.json"
        )
        with open(prov_file) as f:
            data = json.load(f)

            # Find merge snapshot
            merge_snapshot = None
            for graph in data:
                if graph["@id"] == "https://w3id.org/oc/meta/ra/0605/prov/":
                    for entity in graph.get("@graph", []):
                        if (
                            "merge"
                            in entity.get("http://purl.org/dc/terms/description", [{}])[
                                0
                            ]
                            .get("@value", "")
                            .lower()
                        ):
                            merge_snapshot = entity
                            break

            self.assertIsNotNone(merge_snapshot, "No merge snapshot found")

            # Verify merge metadata
            self.assertIn("http://www.w3.org/ns/prov#generatedAtTime", merge_snapshot)
            self.assertIn("http://www.w3.org/ns/prov#wasAttributedTo", merge_snapshot)

            # Check the merge query - should not duplicate the conflicting ORCID
            merge_query = merge_snapshot["https://w3id.org/oc/ontology/hasUpdateQuery"][
                0
            ]["@value"]
            expected_triples = {
                "delete": [
                    '<https://w3id.org/oc/meta/ra/0605> <http://xmlns.com/foaf/0.1/name> "John Smith"',
                    '<https://w3id.org/oc/meta/ra/0605> <http://xmlns.com/foaf/0.1/givenName> "John"',
                ],
                "insert": [
                    '<https://w3id.org/oc/meta/ra/0605> <http://xmlns.com/foaf/0.1/name> "Johnny Smith"',
                    '<https://w3id.org/oc/meta/ra/0605> <http://xmlns.com/foaf/0.1/givenName> "Johnny"',
                ],
            }
            self.check_sparql_query_content(merge_query, expected_triples)

            # Verify deletion snapshot exists for merged entity
            delete_snapshot = None
            for graph in data:
                if graph["@id"] == "https://w3id.org/oc/meta/ra/0606/prov/":
                    for entity in graph.get("@graph", []):
                        if (
                            "deleted"
                            in entity.get("http://purl.org/dc/terms/description", [{}])[
                                0
                            ]
                            .get("@value", "")
                            .lower()
                        ):
                            delete_snapshot = entity
                            break

            self.assertIsNotNone(
                delete_snapshot, "No deletion snapshot found for merged entity"
            )

            # Verify deletion query
            delete_query = delete_snapshot[
                "https://w3id.org/oc/ontology/hasUpdateQuery"
            ][0]["@value"]
            self.assertIn("DELETE DATA", delete_query)
            self.assertIn("<https://w3id.org/oc/meta/ra/0606>", delete_query)

    def test_merge_bibliographic_resources(self):
        """Test merging two bibliographic resource entities"""
        g_set = GraphSet(
            "https://w3id.org/oc/meta/",
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )

        # Create first publication with some metadata
        pub1 = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/0603"),
        )
        pub1.has_title("Data Integration Methods")
        pub1.has_subtitle("A Comprehensive Review")
        pub1.has_pub_date("2023")

        # Create issue for pub1
        issue = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/0605"),
        )
        issue.create_issue()
        issue.has_number("4")
        pub1.is_part_of(issue)

        # Create resource embodiment for pub1
        re1 = g_set.add_re(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/re/0603"),
        )
        re1.has_starting_page("1")
        re1.has_ending_page("20")
        pub1.has_format(re1)

        # Add DOI identifier for pub1
        doi_id = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0603"),
        )
        doi_id.create_doi("10.1000/example.doi.1")
        pub1.has_identifier(doi_id)

        # Create second publication with complementary metadata
        pub2 = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/0604"),
        )
        pub2.has_title("Data Integration Methods")  # Same title
        pub2.has_pub_date("2023")  # Same year

        # Create volume for pub2
        volume = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/0606"),
        )
        volume.create_volume()
        volume.has_number("15")
        pub2.is_part_of(volume)

        # Create resource embodiment for pub2
        re2 = g_set.add_re(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/re/0604"),
        )
        re2.has_starting_page("100")
        re2.has_ending_page("120")
        pub2.has_format(re2)

        # Add ISBN identifier for pub2
        isbn_id = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0604"),
        )
        isbn_id.create_isbn("978-0-123456-47-2")
        pub2.has_identifier(isbn_id)

        # Create authors and roles
        author1 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0605"),
        )
        author1.has_name("Jane Doe")

        author2 = g_set.add_ra(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ra/0606"),
        )
        author2.has_name("John Smith")

        # Add roles for pub1
        role1 = g_set.add_ar(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ar/0605"),
        )
        role1.create_author()
        role1.is_held_by(author1)
        pub1.has_contributor(role1)

        # Add roles for pub2
        role2 = g_set.add_ar(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/ar/0606"),
        )
        role2.create_author()
        role2.is_held_by(author2)
        pub2.has_contributor(role2)

        # Store and upload
        prov = ProvSet(
            g_set,
            "https://w3id.org/oc/meta/",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        prov.generate_provenance()

        rdf_output = os.path.join(OUTPUT, "rdf") + os.sep

        res_storer = Storer(
            abstract_set=g_set,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )
        prov_storer = Storer(
            abstract_set=prov,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )

        res_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        prov_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        res_storer.upload_all(
            triplestore_url=SERVER,
            base_dir=rdf_output,
            batch_size=10,
            save_queries=False,
        )

        # Create merge data
        merge_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/br/0603",
                "merged_entities": "https://w3id.org/oc/meta/br/0604",
                "Done": "False",
            }
        ]
        test_file = os.path.join(BASE, "csv", "br_merge.csv")
        self.write_csv("br_merge.csv", merge_data)

        # Process the merge
        csv_folder = os.path.join(BASE, "csv")
        self.merger.process_folder(csv_folder)

        # Verify the results by checking the output files
        rdf_path = os.path.join(OUTPUT, "rdf")

        # 1. Check bibliographic resource file
        br_file = os.path.join(rdf_path, "br", "060", "10000", "1000.json")
        with open(br_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] == "https://w3id.org/oc/meta/br/0603":
                        # Check basic metadata
                        self.assertEqual(
                            entity["http://purl.org/dc/terms/title"][0]["@value"],
                            "Data Integration Methods",
                        )
                        self.assertEqual(
                            entity["http://purl.org/spar/fabio/hasSubtitle"][0][
                                "@value"
                            ],
                            "A Comprehensive Review",
                        )
                        self.assertEqual(
                            entity[
                                "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
                            ][0]["@value"],
                            "2023",
                        )

                        # Check part relationships
                        parts = {
                            part["@id"]
                            for part in entity["http://purl.org/vocab/frbr/core#partOf"]
                        }
                        self.assertEqual(len(parts), 1)
                        self.assertIn(
                            "https://w3id.org/oc/meta/br/0606", parts
                        )  # Volume

                        # Check formats (resource embodiments)
                        formats = {
                            fmt["@id"]
                            for fmt in entity[
                                "http://purl.org/vocab/frbr/core#embodiment"
                            ]
                        }
                        self.assertEqual(len(formats), 1)
                        self.assertIn("https://w3id.org/oc/meta/re/0603", formats)

                        # Check identifiers
                        identifiers = {
                            id_obj["@id"]
                            for id_obj in entity[
                                "http://purl.org/spar/datacite/hasIdentifier"
                            ]
                        }
                        self.assertEqual(len(identifiers), 2)
                        self.assertIn("https://w3id.org/oc/meta/id/0603", identifiers)
                        self.assertIn("https://w3id.org/oc/meta/id/0604", identifiers)

                    # Check issue metadata
                    elif entity["@id"] == "https://w3id.org/oc/meta/br/0605":
                        self.assertIn(
                            "http://purl.org/spar/fabio/JournalIssue", entity["@type"]
                        )
                        self.assertEqual(
                            entity["http://purl.org/spar/fabio/hasSequenceIdentifier"][
                                0
                            ]["@value"],
                            "4",
                        )

                    # Check volume metadata
                    elif entity["@id"] == "https://w3id.org/oc/meta/br/0606":
                        self.assertIn(
                            "http://purl.org/spar/fabio/JournalVolume", entity["@type"]
                        )
                        self.assertEqual(
                            entity["http://purl.org/spar/fabio/hasSequenceIdentifier"][
                                0
                            ]["@value"],
                            "15",
                        )

                    # Check merged entity no longer exists
                    self.assertNotEqual(
                        entity["@id"], "https://w3id.org/oc/meta/br/0604"
                    )

        # 2. Check resource embodiments
        re_file = os.path.join(rdf_path, "re", "060", "10000", "1000.json")
        with open(re_file) as f:
            data = json.load(f)
            res_embodiments = {}
            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] in [
                        "https://w3id.org/oc/meta/re/0603",
                        "https://w3id.org/oc/meta/re/0604",
                    ]:
                        res_embodiments[entity["@id"]] = {
                            "start": entity[
                                "http://prismstandard.org/namespaces/basic/2.0/startingPage"
                            ][0]["@value"],
                            "end": entity[
                                "http://prismstandard.org/namespaces/basic/2.0/endingPage"
                            ][0]["@value"],
                        }

            self.assertEqual(len(res_embodiments), 2)
            self.assertEqual(
                res_embodiments["https://w3id.org/oc/meta/re/0603"]["start"], "1"
            )
            self.assertEqual(
                res_embodiments["https://w3id.org/oc/meta/re/0603"]["end"], "20"
            )
            self.assertEqual(
                res_embodiments["https://w3id.org/oc/meta/re/0604"]["start"], "100"
            )
            self.assertEqual(
                res_embodiments["https://w3id.org/oc/meta/re/0604"]["end"], "120"
            )

        # 3. Check role assignments
        ar_file = os.path.join(rdf_path, "ar", "060", "10000", "1000.json")
        with open(ar_file) as f:
            data = json.load(f)
            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] == "https://w3id.org/oc/meta/ar/0605":
                        self.assertIn("http://purl.org/spar/pro/withRole", entity)
                        self.assertEqual(
                            entity["http://purl.org/spar/pro/withRole"][0]["@id"],
                            "http://purl.org/spar/pro/author",
                        )
                        holder = entity["http://purl.org/spar/pro/isHeldBy"][0]["@id"]
                        self.assertEqual(holder, "https://w3id.org/oc/meta/ra/0605")

        # 4. Check provenance
        prov_file = os.path.join(
            rdf_path, "br", "060", "10000", "1000", "prov", "se.json"
        )
        with open(prov_file) as f:
            data = json.load(f)

            # Find merge snapshot
            merge_snapshot = None
            for graph in data:
                if graph["@id"] == "https://w3id.org/oc/meta/br/0603/prov/":
                    for entity in graph.get("@graph", []):
                        if (
                            "merge"
                            in entity.get("http://purl.org/dc/terms/description", [{}])[
                                0
                            ]
                            .get("@value", "")
                            .lower()
                        ):
                            merge_snapshot = entity
                            break

            self.assertIsNotNone(merge_snapshot, "No merge snapshot found")

            # Check merge query content
            merge_query = merge_snapshot["https://w3id.org/oc/ontology/hasUpdateQuery"][
                0
            ]["@value"]
            expected_triples = {
                "delete": [
                    "<https://w3id.org/oc/meta/br/0603> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0605>"
                ],
                "insert": [
                    "<https://w3id.org/oc/meta/br/0603> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0604>",
                    "<https://w3id.org/oc/meta/br/0603> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0606>",
                ],
            }
            self.check_sparql_query_content(merge_query, expected_triples)

            # Verify deletion snapshot exists for merged entity
            delete_snapshot = None
            for graph in data:
                if graph["@id"] == "https://w3id.org/oc/meta/br/0604/prov/":
                    for entity in graph.get("@graph", []):
                        if (
                            "deleted"
                            in entity.get("http://purl.org/dc/terms/description", [{}])[
                                0
                            ]
                            .get("@value", "")
                            .lower()
                        ):
                            delete_snapshot = entity
                            break

                    self.assertIsNotNone(
                        delete_snapshot, "No deletion snapshot found for merged entity"
                    )

                    # Verify deletion query
                    delete_query = delete_snapshot[
                        "https://w3id.org/oc/ontology/hasUpdateQuery"
                    ][0]["@value"]
                    expected_delete_triples = {
                        "delete": [
                            '<https://w3id.org/oc/meta/br/0604> <http://purl.org/dc/terms/title> "Data Integration Methods"',
                            '<https://w3id.org/oc/meta/br/0604> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2023"',
                            "<https://w3id.org/oc/meta/br/0604> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0606>",
                            "<https://w3id.org/oc/meta/br/0604> <http://purl.org/vocab/frbr/core#embodiment> <https://w3id.org/oc/meta/re/0604>",
                            "<https://w3id.org/oc/meta/br/0604> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0604>",
                            "<https://w3id.org/oc/meta/br/0604> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>",
                            "<https://w3id.org/oc/meta/br/0604> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0606>",
                        ]
                    }
                    self.check_sparql_query_content(
                        delete_query, expected_delete_triples
                    )

                    # Check that all related entities have appropriate provenance
                    for graph in data:
                        # Check volume provenance
                        if graph["@id"] == "https://w3id.org/oc/meta/br/0606/prov/":
                            found_volume_creation = False
                            for entity in graph.get("@graph", []):
                                if (
                                    "created"
                                    in entity.get(
                                        "http://purl.org/dc/terms/description", [{}]
                                    )[0]
                                    .get("@value", "")
                                    .lower()
                                ):
                                    found_volume_creation = True
                                    self.assertIn(
                                        "http://www.w3.org/ns/prov#generatedAtTime",
                                        entity,
                                    )
                                    self.assertIn(
                                        "http://www.w3.org/ns/prov#wasAttributedTo",
                                        entity,
                                    )
                            self.assertTrue(
                                found_volume_creation,
                                "No creation provenance found for volume",
                            )

                        # Check resource embodiment provenance
                        if graph["@id"] == "https://w3id.org/oc/meta/re/0604/prov/":
                            found_re_creation = False
                            for entity in graph.get("@graph", []):
                                if (
                                    "created"
                                    in entity.get(
                                        "http://purl.org/dc/terms/description", [{}]
                                    )[0]
                                    .get("@value", "")
                                    .lower()
                                ):
                                    found_re_creation = True
                                    self.assertIn(
                                        "http://www.w3.org/ns/prov#generatedAtTime",
                                        entity,
                                    )
                                    self.assertIn(
                                        "http://www.w3.org/ns/prov#wasAttributedTo",
                                        entity,
                                    )
                            self.assertTrue(
                                found_re_creation,
                                "No creation provenance found for resource embodiment",
                            )

                    # Verify all metadata inheritance
                    # We expect the surviving entity to inherit all identifiers
                    # while maintaining its original metadata (title, subtitle, resource embodiment, issue, contributors)

                    # Check if provenance shows correct sequence of operations
                    merge_timestamps = []
                    for graph in data:
                        if graph["@id"] == "https://w3id.org/oc/meta/br/0603/prov/":
                            for entity in graph.get("@graph", []):
                                if (
                                    "merge"
                                    in entity.get(
                                        "http://purl.org/dc/terms/description", [{}]
                                    )[0]
                                    .get("@value", "")
                                    .lower()
                                ):
                                    timestamp = entity[
                                        "http://www.w3.org/ns/prov#generatedAtTime"
                                    ][0]["@value"]
                                    merge_timestamps.append(timestamp)

                    # Check timestamps are in correct order
                    self.assertEqual(
                        len(merge_timestamps),
                        1,
                        "Should have exactly one merge operation",
                    )

                    br_file = os.path.join(rdf_path, "br", "060", "10000", "1000.json")
                    with open(br_file) as f:
                        data = json.load(f)
                        volume_found = False
                        for graph in data:
                            for entity in graph.get("@graph", []):
                                if (
                                    entity["@id"] == "https://w3id.org/oc/meta/br/0606"
                                ):  # Volume
                                    volume_found = True
                                    self.assertIn(
                                        "http://purl.org/spar/fabio/JournalVolume",
                                        entity["@type"],
                                    )

                        self.assertTrue(
                            volume_found, "Volume should still exist after merge"
                        )

                    re_file = os.path.join(rdf_path, "re", "060", "10000", "1000.json")
                    with open(re_file) as f:
                        data = json.load(f)
                        re_found = False
                        for graph in data:
                            for entity in graph.get("@graph", []):
                                if (
                                    entity["@id"] == "https://w3id.org/oc/meta/re/0604"
                                ):  # RE from merged entity
                                    re_found = True
                                    self.assertEqual(
                                        entity[
                                            "http://prismstandard.org/namespaces/basic/2.0/startingPage"
                                        ][0]["@value"],
                                        "100",
                                    )

                        self.assertTrue(
                            re_found,
                            "Resource embodiment should still exist after merge",
                        )

    def test_fetch_related_entities_batch(self):
        """Test batch fetching of related entities"""
        meta_editor = MetaEditor(
            META_CONFIG, "https://orcid.org/0000-0002-8420-0696", save_queries=False
        )

        g_set = GraphSet(
            "https://w3id.org/oc/meta/",
            supplier_prefix="060",
            custom_counter_handler=self.counter_handler,
        )

        # Utilizziamo un insieme più piccolo di numeri validi per il test
        valid_numbers = [11, 12, 13, 14, 15]
        entities = {}

        # Creiamo gli autori e li memorizziamo in un dizionario per facile accesso
        for i in valid_numbers:
            ra = g_set.add_ra(
                resp_agent="https://orcid.org/0000-0002-8420-0696",
                res=URIRef(f"https://w3id.org/oc/meta/ra/060{i}"),
            )
            ra.has_name(f"Author {i}")
            entities[i] = ra

        # Creiamo le entità correlate per ogni autore
        for i in valid_numbers:
            # Creiamo l'identificatore
            identifier = g_set.add_id(
                resp_agent="https://orcid.org/0000-0002-8420-0696",
                res=URIRef(f"https://w3id.org/oc/meta/id/060{i}"),
            )
            identifier.create_orcid(f"0000-0001-{i:04d}-1111")
            entities[i].has_identifier(identifier)

            # Creiamo il ruolo
            role = g_set.add_ar(
                resp_agent="https://orcid.org/0000-0002-8420-0696",
                res=URIRef(f"https://w3id.org/oc/meta/ar/060{i}"),
            )
            role.create_author()
            role.is_held_by(entities[i])

            # Creiamo la pubblicazione
            pub = g_set.add_br(
                resp_agent="https://orcid.org/0000-0002-8420-0696",
                res=URIRef(f"https://w3id.org/oc/meta/br/060{i}"),
            )
            pub.has_title(f"Publication {i}")
            pub.has_contributor(role)

        prov = ProvSet(
            g_set,
            "https://w3id.org/oc/meta/",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        prov.generate_provenance()

        rdf_output = os.path.join(OUTPUT, "rdf") + os.sep

        res_storer = Storer(
            abstract_set=g_set,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )
        prov_storer = Storer(
            abstract_set=prov,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )

        res_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        prov_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        res_storer.upload_all(
            triplestore_url=SERVER,
            base_dir=rdf_output,
            batch_size=10,
            save_queries=False,
        )

        batch_sizes = [1, 5, 11, 25]
        for batch_size in batch_sizes:
            with self.subTest(batch_size=batch_size):
                # Test con una singola entità
                merged_entities = [f"https://w3id.org/oc/meta/ra/060{valid_numbers[0]}"]
                surviving_entities = [
                    f"https://w3id.org/oc/meta/ra/060{valid_numbers[1]}"
                ]

                related = self.merger.fetch_related_entities_batch(
                    meta_editor=meta_editor,
                    merged_entities=merged_entities,
                    surviving_entities=surviving_entities,
                    batch_size=batch_size,
                )

                expected_related = {
                    URIRef(
                        f"https://w3id.org/oc/meta/id/060{valid_numbers[0]}"
                    ),  # ID della merged
                    URIRef(
                        f"https://w3id.org/oc/meta/ar/060{valid_numbers[0]}"
                    ),  # AR della merged
                    URIRef(
                        f"https://w3id.org/oc/meta/id/060{valid_numbers[1]}"
                    ),  # AR della surviving
                }

                self.assertEqual(related, expected_related)

                # Test con multiple entità
                merged_entities = [
                    f"https://w3id.org/oc/meta/ra/060{i}" for i in valid_numbers[:3]
                ]
                surviving_entities = [
                    f"https://w3id.org/oc/meta/ra/060{valid_numbers[3]}"
                ]

                related = self.merger.fetch_related_entities_batch(
                    meta_editor=meta_editor,
                    merged_entities=merged_entities,
                    surviving_entities=surviving_entities,
                    batch_size=batch_size,
                )

                expected_related = set()
                for i in valid_numbers[:3]:  # Entità merged
                    expected_related.add(URIRef(f"https://w3id.org/oc/meta/id/060{i}"))
                    expected_related.add(URIRef(f"https://w3id.org/oc/meta/ar/060{i}"))
                expected_related.add(
                    URIRef(f"https://w3id.org/oc/meta/id/060{valid_numbers[3]}")
                )

                self.assertEqual(related, expected_related)

    def test_merge_bibliographic_resources_with_multiple_identifiers(self):
        """Test merging two bibliographic resources with different identifiers"""
        g_set = GraphSet(
            "https://w3id.org/oc/meta/",
            supplier_prefix="065",
            custom_counter_handler=self.counter_handler,
        )

        # Create first publication (surviving entity)
        pub1 = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/06501844005"),
        )
        pub1.has_title("Higgsing The Stringy Higher Spin Symmetry")
        pub1.has_pub_date("2015-10")

        # Add first identifier (DOI)
        doi_id1 = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0680503588"),
        )
        doi_id1.create_doi("10.1007/jhep10(2015)101")
        pub1.has_identifier(doi_id1)

        # Add part of relationship
        journal = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/06501844297"),
        )
        pub1.is_part_of(journal)

        # Add contributors for first publication
        for i in range(4):
            role = g_set.add_ar(
                resp_agent="https://orcid.org/0000-0002-8420-0696",
                res=URIRef(f"https://w3id.org/oc/meta/ar/0650842286{7+i}"),
            )
            role.create_author()
            pub1.has_contributor(role)

        # Create second publication (to be merged)
        pub2 = g_set.add_br(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/br/06804303923"),
        )
        pub2.has_title("Higgsing The Stringy Higher Spin Symmetry")
        pub2.has_pub_date("2015-10-01")

        # Add second identifier (additional DOI)
        doi_id2 = g_set.add_id(
            resp_agent="https://orcid.org/0000-0002-8420-0696",
            res=URIRef("https://w3id.org/oc/meta/id/0680503589"),
        )
        doi_id2.create_doi("10.3929/ethz-b-000105964")
        pub2.has_identifier(doi_id2)

        # Add contributors for second publication
        for i in range(4):
            role = g_set.add_ar(
                resp_agent="https://orcid.org/0000-0002-8420-0696",
                res=URIRef(f"https://w3id.org/oc/meta/ar/0680174860{1+i}"),
            )
            role.create_author()
            pub2.has_contributor(role)

        # Store and upload test data
        prov = ProvSet(
            g_set,
            "https://w3id.org/oc/meta/",
            wanted_label=False,
            custom_counter_handler=self.counter_handler,
        )
        prov.generate_provenance()

        rdf_output = os.path.join(OUTPUT, "rdf") + os.sep

        res_storer = Storer(
            abstract_set=g_set,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )
        prov_storer = Storer(
            abstract_set=prov,
            dir_split=10000,
            n_file_item=1000,
            output_format="json-ld",
            zip_output=False,
        )

        res_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        prov_storer.store_all(base_dir=rdf_output, base_iri="https://w3id.org/oc/meta/")
        res_storer.upload_all(
            triplestore_url=SERVER,
            base_dir=rdf_output,
            batch_size=10,
            save_queries=False,
        )

        # # Create merge data
        merge_data = [
            {
                "surviving_entity": "https://w3id.org/oc/meta/br/06501844005",
                "merged_entities": "https://w3id.org/oc/meta/br/06804303923",
                "Done": "False",
            }
        ]
        test_file = os.path.join(BASE, "csv", "br_dois_merge.csv")
        self.write_csv("br_dois_merge.csv", merge_data)

        # # Process the merge
        self.merger.process_folder(os.path.join(BASE, "csv"))

        # # Verify the results
        rdf_path = os.path.join(OUTPUT, "rdf")
        br_file = os.path.join(rdf_path, "br", "0650", "1850000", "1845000.json")

        with open(br_file) as f:
            data = json.load(f)
            surviving_entity_found = False

            for graph in data:
                for entity in graph.get("@graph", []):
                    if entity["@id"] == "https://w3id.org/oc/meta/br/06501844005":
                        surviving_entity_found = True

                        # Check identifiers - should have both DOIs
                        identifiers = {
                            id_obj["@id"]
                            for id_obj in entity[
                                "http://purl.org/spar/datacite/hasIdentifier"
                            ]
                        }
                        self.assertEqual(len(identifiers), 2)
                        expected_ids = {
                            "https://w3id.org/oc/meta/id/0680503588",
                            "https://w3id.org/oc/meta/id/0680503589",
                        }
                        self.assertEqual(identifiers, expected_ids)

                        # Check other metadata preserved
                        self.assertEqual(
                            entity["http://purl.org/dc/terms/title"][0]["@value"],
                            "Higgsing The Stringy Higher Spin Symmetry",
                        )
                        self.assertEqual(
                            entity[
                                "http://prismstandard.org/namespaces/basic/2.0/publicationDate"
                            ][0]["@value"],
                            "2015-10-01",  # Should keep original date format
                        )

                        # Check part of relationship preserved
                        self.assertEqual(
                            entity["http://purl.org/vocab/frbr/core#partOf"][0]["@id"],
                            "https://w3id.org/oc/meta/br/06501844297",
                        )

                    # Verify merged entity doesn't exist
                    self.assertNotEqual(
                        entity["@id"], "https://w3id.org/oc/meta/br/06804303923"
                    )

            self.assertTrue(
                surviving_entity_found, "Surviving entity not found in output"
            )

        # # Verify provenance
        prov_file = os.path.join(
            rdf_path, "br", "0650", "1850000", "1845000", "prov", "se.json"
        )
        with open(prov_file) as f:
            data = json.load(f)
            merge_snapshot_found = False

            for graph in data:
                if graph["@id"] == "https://w3id.org/oc/meta/br/06501844005/prov/":
                    for entity in graph.get("@graph", []):
                        if (
                            "merge"
                            in entity.get("http://purl.org/dc/terms/description", [{}])[
                                0
                            ]
                            .get("@value", "")
                            .lower()
                        ):
                            merge_snapshot_found = True

                            # Check merge query content
                            merge_query = entity[
                                "https://w3id.org/oc/ontology/hasUpdateQuery"
                            ][0]["@value"]
                            expected_triples = {
                                "insert": [
                                    "<https://w3id.org/oc/meta/br/06501844005> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0680503589>"
                                ]
                            }
                            self.check_sparql_query_content(
                                merge_query, expected_triples
                            )

            self.assertTrue(
                merge_snapshot_found, "No merge snapshot found in provenance"
            )


if __name__ == "__main__":
    unittest.main()
