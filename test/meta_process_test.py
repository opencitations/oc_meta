import csv
import json
import os
import random
import shutil
import subprocess
import sys
import time
import unittest
from datetime import datetime
from urllib.error import URLError
from zipfile import ZipFile

import redis
import yaml
from oc_meta.lib.file_manager import get_csv_data
from oc_meta.run.meta_process import run_meta_process
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from rdflib import ConjunctiveGraph, Graph, Literal, URIRef
from SPARQLWrapper import JSON, POST, XML, SPARQLExceptions, SPARQLWrapper

BASE_DIR = os.path.join("test", "meta_process")
SERVER = "http://127.0.0.1:8805/sparql"


def execute_sparql_query(endpoint, query, return_format=JSON, max_retries=3, delay=5):
    """
    Execute a SPARQL query with retry logic and better error handling.

    Args:
        endpoint (str): SPARQL endpoint URL
        query (str): SPARQL query to execute
        return_format: Query return format (JSON, XML etc)
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds

    Returns:
        Query results in specified format

    Raises:
        URLError: If connection fails after all retries
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(return_format)

    retry_count = 0
    last_error = None

    while retry_count < max_retries:
        try:
            sparql.setTimeout(30)  # Increase timeout
            return sparql.queryAndConvert()
        except Exception as e:
            last_error = e
            retry_count += 1
            if retry_count == max_retries:
                raise URLError(
                    f"Failed to connect to SPARQL endpoint after {max_retries} attempts: {str(last_error)}"
                )
            print(
                f"Connection attempt {retry_count} failed, retrying in {delay} seconds..."
            )
            time.sleep(delay)  # Increased delay between retries


def reset_redis_counters():
    redis_host = "localhost"
    redis_port = 6379
    redis_db = 5
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    redis_client.flushdb()


def reset_server(server: str = "http://127.0.0.1:8805/sparql") -> None:
    """
    Reset the SPARQL server using Virtuoso's RDF_GLOBAL_RESET() via isql.

    Args:
        server (str): SPARQL endpoint URL (kept for compatibility)
    """
    max_retries = 5
    base_delay = 2
    current_dir = os.getcwd()

    for attempt in range(max_retries):
        try:
            # Add small random delay to avoid race conditions
            time.sleep(base_delay + random.uniform(0, 1))

            # Change to virtuoso directory
            os.chdir("virtuoso-opensource/bin")

            result = subprocess.run(
                ["./isql", "1105", "dba", "dba"],
                input=b"RDF_GLOBAL_RESET();",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
            )

            # Restore original directory
            os.chdir(current_dir)

            if result.returncode == 0:
                break

            raise Exception(f"isql command failed: {result.stderr.decode()}")

        except Exception as e:
            # Ensure we restore the directory even if an error occurs
            os.chdir(current_dir)

            if attempt == max_retries - 1:
                raise URLError(
                    f"Failed to reset RDF store after {max_retries} attempts: {str(e)}"
                )
            print(f"Reset attempt {attempt + 1} failed: {str(e)}")
            continue


def delete_output_zip(base_dir: str, start_time: datetime) -> None:
    for file in os.listdir(base_dir):
        if file.startswith("meta_output") and file.endswith(".zip"):
            file_creation_time = file.split("meta_output_")[1].replace(".zip", "")
            file_creation_time = datetime.strptime(
                file_creation_time, "%Y-%m-%dT%H_%M_%S_%f"
            )
            was_created_after_time = True if file_creation_time > start_time else False
            if was_created_after_time:
                os.remove(os.path.join(base_dir, file))


class test_ProcessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup iniziale eseguito una volta per tutta la classe di test"""
        # Aspetta che Virtuoso sia pronto
        max_wait = 30  # secondi
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                # Prova una query semplice
                sparql = SPARQLWrapper(SERVER)
                sparql.setQuery("SELECT * WHERE { ?s ?p ?o } LIMIT 1")
                sparql.setReturnFormat(JSON)
                sparql.query()
                break
            except Exception:
                time.sleep(2)
        else:
            raise TimeoutError(f"Virtuoso non pronto dopo {max_wait} secondi")

    def setUp(self):
        """Setup eseguito prima di ogni test"""
        # Reset del database
        try:
            reset_server()
            reset_redis_counters()
        except Exception as e:
            self.skipTest(f"Setup fallito: {str(e)}")

    def tearDown(self):
        reset_redis_counters()

    def test_run_meta_process(self):
        output_folder = os.path.join(BASE_DIR, "output_1")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_1.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        now = datetime.now()
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, "csv")):
            for file in filenames:
                output.extend(get_csv_data(os.path.join(dirpath, file)))
        expected_output = [
            {
                "id": "doi:10.17117/na.2015.08.1067 omid:br/0601",
                "title": "",
                "author": "",
                "pub_date": "",
                "venue": "Scientometrics [issn:0138-9130 issn:1588-2861 omid:br/0603]",
                "volume": "26",
                "issue": "",
                "page": "",
                "type": "journal article",
                "publisher": "Consulting Company Ucom [crossref:6623 omid:ra/0601]",
                "editor": "Naimi, Elmehdi [orcid:0000-0002-4126-8519 omid:ra/0602]",
            },
            {
                "id": "issn:1524-4539 issn:0009-7322 omid:br/0602",
                "title": "Circulation",
                "author": "",
                "pub_date": "",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "journal",
                "publisher": "",
                "editor": "",
            },
            {
                "id": "doi:10.9799/ksfan.2012.25.1.069 omid:br/0605",
                "title": "Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment",
                "author": "Cheigh, Chan-Ick [orcid:0000-0003-2542-5788 omid:ra/0603]; Mun, Ji-Hye [omid:ra/0604]; Chung, Myong-Soo [omid:ra/0605]",
                "pub_date": "2012-03-31",
                "venue": "The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/0608]",
                "volume": "25",
                "issue": "1",
                "page": "69-76",
                "type": "journal article",
                "publisher": "The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/0606]",
                "editor": "Chung, Myong-Soo [orcid:0000-0002-9666-2513 omid:ra/0607]",
            },
            {
                "id": "doi:10.9799/ksfan.2012.25.1.077 omid:br/0606",
                "title": "Properties Of Immature Green Cherry Tomato Pickles",
                "author": "Koh, Jong-Ho [omid:ra/0608]; Shin, Hae-Hun [omid:ra/0609]; Kim, Young-Shik [orcid:0000-0001-5673-6314 omid:ra/06010]; Kook, Moo-Chang [omid:ra/06011]",
                "pub_date": "2012-03-31",
                "venue": "The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/0608]",
                "volume": "",
                "issue": "2",
                "page": "77-82",
                "type": "journal article",
                "publisher": "The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/0606]",
                "editor": "",
            },
            {
                "id": "doi:10.1097/01.rct.0000185385.35389.cd omid:br/0607",
                "title": "Comprehensive Assessment Of Lung CT Attenuation Alteration At Perfusion Defects Of Acute Pulmonary Thromboembolism With Breath-Hold SPECT-CT Fusion Images",
                "author": "Suga, Kazuyoshi [omid:ra/06012]; Kawakami, Yasuhiko [omid:ra/06013]; Iwanaga, Hideyuki [omid:ra/06014]; Hayashi, Noriko [omid:ra/06015]; Seto, Aska [omid:ra/06016]; Matsunaga, Naofumi [omid:ra/06017]",
                "pub_date": "2006-01",
                "venue": "Journal Of Computer Assisted Tomography [issn:0363-8715 omid:br/06012]",
                "volume": "30",
                "issue": "1",
                "page": "83-91",
                "type": "journal article",
                "publisher": "Ovid Technologies (Wolters Kluwer Health) [crossref:276 omid:ra/06018]",
                "editor": "",
            },
        ]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.maxDiff = None
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        self.assertEqual(output, expected_output)

    def test_run_meta_process_ids_only(self):
        output_folder = os.path.join(BASE_DIR, "output_5")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_5.yaml")
        now = datetime.now()
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        run_meta_process(settings, meta_config_path=meta_config_path)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, "csv")):
            for file in filenames:
                output.extend(get_csv_data(os.path.join(dirpath, file)))
        expected_output = [
            {
                "id": "doi:10.17117/na.2015.08.1067 omid:br/0601",
                "title": "Some Aspects Of The Evolution Of Chernozems Under The Influence Of Natural And Anthropogenic Factors",
                "author": "[orcid:0000-0002-4126-8519 omid:ra/0601]; [orcid:0000-0003-0530-4305 omid:ra/0602]",
                "pub_date": "2015-08-22",
                "venue": "[issn:1225-4339 omid:br/0602]",
                "volume": "26",
                "issue": "",
                "page": "50",
                "type": "journal article",
                "publisher": "[crossref:6623 omid:ra/0603]",
                "editor": "[orcid:0000-0002-4126-8519 omid:ra/0601]; [orcid:0000-0002-8420-0696 omid:ra/0604]",
            }
        ]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.maxDiff = None
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        self.assertEqual(output, expected_output)

    def test_run_meta_process_two_workers(self):
        output_folder = os.path.join(BASE_DIR, "output_2")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_2.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        settings["workers_number"] = 2
        now = datetime.now()
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        output = list()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, "csv")):
            for file in filenames:
                output.extend(get_csv_data(os.path.join(dirpath, file)))
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        expected_output = [
            {
                "id": "doi:10.17117/na.2015.08.1067 omid:br/06101",
                "title": "",
                "author": "",
                "pub_date": "",
                "venue": "Scientometrics [issn:0138-9130 issn:1588-2861 omid:br/06103]",
                "volume": "26",
                "issue": "",
                "page": "",
                "type": "journal article",
                "publisher": "Consulting Company Ucom [crossref:6623 omid:ra/06101]",
                "editor": "Naimi, Elmehdi [orcid:0000-0002-4126-8519 omid:ra/06102]",
            },
            {
                "id": "issn:1524-4539 issn:0009-7322 omid:br/06102",
                "title": "Circulation",
                "author": "",
                "pub_date": "",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "journal",
                "publisher": "",
                "editor": "",
            },
            {
                "id": "doi:10.9799/ksfan.2012.25.1.069 omid:br/06201",
                "title": "Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment",
                "author": "Cheigh, Chan-Ick [orcid:0000-0003-2542-5788 omid:ra/06201]; Mun, Ji-Hye [omid:ra/06202]; Chung, Myong-Soo [omid:ra/06203]",
                "pub_date": "2012-03-31",
                "venue": "The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/06204]",
                "volume": "25",
                "issue": "1",
                "page": "69-76",
                "type": "journal article",
                "publisher": "The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/06204]",
                "editor": "Chung, Myong-Soo [orcid:0000-0002-9666-2513 omid:ra/06205]",
            },
            {
                "id": "doi:10.9799/ksfan.2012.25.1.077 omid:br/06202",
                "title": "Properties Of Immature Green Cherry Tomato Pickles",
                "author": "Koh, Jong-Ho [omid:ra/06206]; Shin, Hae-Hun [omid:ra/06207]; Kim, Young-Shik [orcid:0000-0001-5673-6314 omid:ra/06208]; Kook, Moo-Chang [omid:ra/06209]",
                "pub_date": "2012-03-31",
                "venue": "The Korean Journal Of Food And Nutrition [issn:1225-4339 omid:br/06204]",
                "volume": "",
                "issue": "2",
                "page": "77-82",
                "type": "journal article",
                "publisher": "The Korean Society Of Food And Nutrition [crossref:4768 omid:ra/06204]",
                "editor": "",
            },
            {
                "id": "doi:10.1097/01.rct.0000185385.35389.cd omid:br/06203",
                "title": "Comprehensive Assessment Of Lung CT Attenuation Alteration At Perfusion Defects Of Acute Pulmonary Thromboembolism With Breath-Hold SPECT-CT Fusion Images",
                "author": "Suga, Kazuyoshi [omid:ra/062010]; Kawakami, Yasuhiko [omid:ra/062011]; Iwanaga, Hideyuki [omid:ra/062012]; Hayashi, Noriko [omid:ra/062013]; Seto, Aska [omid:ra/062014]; Matsunaga, Naofumi [omid:ra/062015]",
                "pub_date": "2006-01",
                "venue": "Journal Of Computer Assisted Tomography [issn:0363-8715 omid:br/06208]",
                "volume": "30",
                "issue": "1",
                "page": "83-91",
                "type": "journal article",
                "publisher": "Ovid Technologies (Wolters Kluwer Health) [crossref:276 omid:ra/062016]",
                "editor": "",
            },
        ]
        output = sorted(sorted(d.items()) for d in output)
        expected_output = sorted(sorted(d.items()) for d in expected_output)
        self.assertEqual(output, expected_output)

    def test_provenance(self):
        output_folder = os.path.join(BASE_DIR, "output_3")
        now = datetime.now()
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        meta_config_path = os.path.join(BASE_DIR, "meta_config_3.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        settings["input_csv_dir"] = os.path.join(BASE_DIR, "input")
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings["input_csv_dir"] = os.path.join(BASE_DIR, "input_2")
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings["input_csv_dir"] = os.path.join(BASE_DIR, "input")
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        output = dict()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, "rdf")):
            if dirpath.endswith("prov"):
                for filename in filenames:
                    if filename.endswith(".json"):
                        filepath = os.path.join(dirpath, filename)
                        with open(filepath, "r", encoding="utf-8") as f:
                            provenance = json.load(f)
                            essential_provenance = [
                                {
                                    graph: [
                                        {
                                            p: (
                                                set(
                                                    v[0]["@value"]
                                                    .split(
                                                        "INSERT DATA { GRAPH <https://w3id.org/oc/meta/br/> { "
                                                    )[1]
                                                    .split(" } }")[0]
                                                    .split(" .")
                                                )
                                                if "@value" in v[0]
                                                else v if isinstance(v, list) else v
                                            )
                                            for p, v in se.items()
                                            if p
                                            not in {
                                                "http://www.w3.org/ns/prov#generatedAtTime",
                                                "http://purl.org/dc/terms/description",
                                                "@type",
                                                "http://www.w3.org/ns/prov#hadPrimarySource",
                                                "http://www.w3.org/ns/prov#wasAttributedTo",
                                                "http://www.w3.org/ns/prov#invalidatedAtTime",
                                            }
                                        }
                                        for se in sorted(ses, key=lambda d: d["@id"])
                                    ]
                                    for graph, ses in entity.items()
                                    if graph != "@id"
                                }
                                for entity in sorted(provenance, key=lambda x: x["@id"])
                            ]
                            output[dirpath.split(os.sep)[4]] = essential_provenance
        expected_output = {
            "ar": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0601/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/ar/0601"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0602/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/ar/0602"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0603/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/ar/0603"}
                            ],
                        }
                    ]
                },
            ],
            "br": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/br/0601/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0601"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/br/0601/prov/se/2",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0601"}
                            ],
                            "http://www.w3.org/ns/prov#wasDerivedFrom": [
                                {"@id": "https://w3id.org/oc/meta/br/0601/prov/se/1"}
                            ],
                            "https://w3id.org/oc/ontology/hasUpdateQuery": {
                                "",
                                "<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0601>",
                                "<https://w3id.org/oc/meta/br/0601> <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0603>",
                                "<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0602>",
                                "<https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/pro/isDocumentContextFor> <https://w3id.org/oc/meta/ar/0603>",
                                '<https://w3id.org/oc/meta/br/0601> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2015-08-22"^^<http://www.w3.org/2001/XMLSchema#date>',
                                '<https://w3id.org/oc/meta/br/0601> <http://purl.org/dc/terms/title> "Some Aspects Of The Evolution Of Chernozems Under The Influence Of Natural And Anthropogenic Factors"^^<http://www.w3.org/2001/XMLSchema#string>',
                            },
                        },
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/br/0602/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0602"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/br/0603/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0603"}
                            ],
                        }
                    ]
                },
            ],
            "id": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/id/0601/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/id/0601"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/id/0602/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/id/0602"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/id/0603/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/id/0603"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/id/0604/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/id/0604"}
                            ],
                        }
                    ]
                },
            ],
            "ra": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0601/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/ra/0601"}
                            ],
                        }
                    ]
                },
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0602/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/ra/0602"}
                            ],
                        }
                    ]
                },
            ],
            "re": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/re/0601/prov/se/1",
                            "http://www.w3.org/ns/prov#specializationOf": [
                                {"@id": "https://w3id.org/oc/meta/re/0601"}
                            ],
                        }
                    ]
                }
            ],
        }
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        self.maxDiff = None
        self.assertEqual(output, expected_output)

    def test_run_meta_process_thread_safe(self):
        output_folder = os.path.join(BASE_DIR, "output_4")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_4.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        original_input_csv_dir = settings["input_csv_dir"]
        settings["input_csv_dir"] = os.path.join(original_input_csv_dir, "preprocess")
        now = datetime.now()
        settings["workers_number"] = 1
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        proc = subprocess.run(
            [sys.executable, "-m", "oc_meta.run.meta_process", "-c", meta_config_path],
            capture_output=True,
            text=True,
        )
        output = dict()
        for dirpath, _, filenames in os.walk(os.path.join(output_folder, "rdf")):
            if not dirpath.endswith("prov"):
                for filename in filenames:
                    if filename.endswith(".zip"):
                        with ZipFile(os.path.join(dirpath, filename)) as archive:
                            with archive.open(filename.replace(".zip", ".json")) as f:
                                rdf = json.load(f)
                                output.setdefault(dirpath.split(os.sep)[4], list())
                                rdf_sorted = [
                                    {
                                        k: sorted(
                                            [
                                                {
                                                    p: o
                                                    for p, o in p_o.items()
                                                    if p
                                                    not in {
                                                        "@type",
                                                        "http://purl.org/spar/pro/isDocumentContextFor",
                                                    }
                                                }
                                                for p_o in v
                                            ],
                                            key=lambda d: d["@id"],
                                        )
                                        for k, v in graph.items()
                                        if k == "@graph"
                                    }
                                    for graph in rdf
                                ]
                                output[dirpath.split(os.sep)[4]].extend(rdf_sorted)
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        expected_output = {
            "ar": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0604",
                            "@type": ["http://purl.org/spar/pro/RoleInTime"],
                            "http://purl.org/spar/pro/isHeldBy": [
                                {"@id": "https://w3id.org/oc/meta/ra/0604"}
                            ],
                            "http://purl.org/spar/pro/withRole": [
                                {"@id": "http://purl.org/spar/pro/publisher"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0602",
                            "@type": ["http://purl.org/spar/pro/RoleInTime"],
                            "http://purl.org/spar/pro/isHeldBy": [
                                {"@id": "https://w3id.org/oc/meta/ra/0602"}
                            ],
                            "http://purl.org/spar/pro/withRole": [
                                {"@id": "http://purl.org/spar/pro/author"}
                            ],
                            "https://w3id.org/oc/ontology/hasNext": [
                                {"@id": "https://w3id.org/oc/meta/ar/0603"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0603",
                            "@type": ["http://purl.org/spar/pro/RoleInTime"],
                            "http://purl.org/spar/pro/isHeldBy": [
                                {"@id": "https://w3id.org/oc/meta/ra/0603"}
                            ],
                            "http://purl.org/spar/pro/withRole": [
                                {"@id": "http://purl.org/spar/pro/author"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0605",
                            "@type": ["http://purl.org/spar/pro/RoleInTime"],
                            "http://purl.org/spar/pro/isHeldBy": [
                                {"@id": "https://w3id.org/oc/meta/ra/0605"}
                            ],
                            "http://purl.org/spar/pro/withRole": [
                                {"@id": "http://purl.org/spar/pro/editor"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ar/0601",
                            "@type": ["http://purl.org/spar/pro/RoleInTime"],
                            "http://purl.org/spar/pro/isHeldBy": [
                                {"@id": "https://w3id.org/oc/meta/ra/0601"}
                            ],
                            "http://purl.org/spar/pro/withRole": [
                                {"@id": "http://purl.org/spar/pro/author"}
                            ],
                            "https://w3id.org/oc/ontology/hasNext": [
                                {"@id": "https://w3id.org/oc/meta/ar/0602"}
                            ],
                        },
                    ],
                    "@id": "https://w3id.org/oc/meta/ar/",
                }
            ],
            "br": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/br/0601",
                            "@type": [
                                "http://purl.org/spar/fabio/Expression",
                                "http://purl.org/spar/fabio/JournalArticle",
                            ],
                            "http://prismstandard.org/namespaces/basic/2.0/publicationDate": [
                                {
                                    "@type": "http://www.w3.org/2001/XMLSchema#date",
                                    "@value": "2012-03-31",
                                }
                            ],
                            "http://purl.org/dc/terms/title": [
                                {
                                    "@value": "Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment"
                                }
                            ],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0601"}
                            ],
                            "http://purl.org/spar/pro/isDocumentContextFor": [
                                {"@id": "https://w3id.org/oc/meta/ar/0603"},
                                {"@id": "https://w3id.org/oc/meta/ar/0601"},
                                {"@id": "https://w3id.org/oc/meta/ar/0604"},
                                {"@id": "https://w3id.org/oc/meta/ar/0602"},
                                {"@id": "https://w3id.org/oc/meta/ar/0605"},
                            ],
                            "http://purl.org/vocab/frbr/core#embodiment": [
                                {"@id": "https://w3id.org/oc/meta/re/0601"}
                            ],
                            "http://purl.org/vocab/frbr/core#partOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0604"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/br/0604",
                            "@type": [
                                "http://purl.org/spar/fabio/JournalIssue",
                                "http://purl.org/spar/fabio/Expression",
                            ],
                            "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                                {"@value": "1"}
                            ],
                            "http://purl.org/vocab/frbr/core#partOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0603"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/br/0602",
                            "@type": [
                                "http://purl.org/spar/fabio/Expression",
                                "http://purl.org/spar/fabio/Journal",
                            ],
                            "http://purl.org/dc/terms/title": [
                                {"@value": "The Korean Journal Of Food And Nutrition"}
                            ],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0602"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/br/0603",
                            "@type": [
                                "http://purl.org/spar/fabio/Expression",
                                "http://purl.org/spar/fabio/JournalVolume",
                            ],
                            "http://purl.org/spar/fabio/hasSequenceIdentifier": [
                                {"@value": "25"}
                            ],
                            "http://purl.org/vocab/frbr/core#partOf": [
                                {"@id": "https://w3id.org/oc/meta/br/0602"}
                            ],
                        },
                    ],
                    "@id": "https://w3id.org/oc/meta/br/",
                }
            ],
            "id": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/id/0605",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/orcid"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "0000-0002-9666-2513"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0601",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/doi"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "10.9799/ksfan.2012.25.1.069"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0603",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/orcid"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "0000-0003-2542-5788"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0604",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/crossref"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "4768"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0602",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/issn"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "1225-4339"}
                            ],
                        },
                    ],
                    "@id": "https://w3id.org/oc/meta/id/",
                }
            ],
            "ra": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0605",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0605"}
                            ],
                            "http://xmlns.com/foaf/0.1/familyName": [
                                {"@value": "Chung"}
                            ],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Myong-Soo"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0602",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Mun"}],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Ji-Hye"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0604",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0604"}
                            ],
                            "http://xmlns.com/foaf/0.1/name": [
                                {"@value": "The Korean Society Of Food And Nutrition"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0603",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://xmlns.com/foaf/0.1/familyName": [
                                {"@value": "Chung"}
                            ],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Myong-Soo"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0601",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0603"}
                            ],
                            "http://xmlns.com/foaf/0.1/familyName": [
                                {"@value": "Cheigh"}
                            ],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Chan-Ick"}
                            ],
                        },
                    ],
                    "@id": "https://w3id.org/oc/meta/ra/",
                }
            ],
            "re": [
                {
                    "@graph": [
                        {
                            "@id": "https://w3id.org/oc/meta/re/0601",
                            "@type": ["http://purl.org/spar/fabio/Manifestation"],
                            "http://prismstandard.org/namespaces/basic/2.0/endingPage": [
                                {"@value": "76"}
                            ],
                            "http://prismstandard.org/namespaces/basic/2.0/startingPage": [
                                {"@value": "69"}
                            ],
                        }
                    ],
                    "@id": "https://w3id.org/oc/meta/re/",
                }
            ],
        }
        expected_output = {
            folder: [
                {
                    k: sorted(
                        [
                            {
                                p: o
                                for p, o in p_o.items()
                                if p
                                not in {
                                    "@type",
                                    "http://purl.org/spar/pro/isDocumentContextFor",
                                }
                            }
                            for p_o in v
                        ],
                        key=lambda d: d["@id"],
                    )
                    for k, v in graph.items()
                    if k == "@graph"
                }
                for graph in data
            ]
            for folder, data in expected_output.items()
        }
        self.assertEqual(output, expected_output)
        self.assertFalse(
            "Reader: ERROR" in proc.stdout or "Storer: ERROR" in proc.stdout
        )

    def test_silencer_on(self):
        output_folder = os.path.join(BASE_DIR, "output_6")
        now = datetime.now()
        meta_config_path = os.path.join(BASE_DIR, "meta_config_6.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings["input_csv_dir"] = os.path.join(
            BASE_DIR, "same_as_input_2_with_other_authors"
        )
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        query_agents = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT (COUNT (?agent) AS ?agent_count)
            WHERE {
                <https://w3id.org/oc/meta/br/0601> pro:isDocumentContextFor ?agent.
            }
        """
        result = execute_sparql_query(SERVER, query_agents)
        expected_result = {
            "head": {"link": [], "vars": ["agent_count"]},
            "results": {
                "distinct": False,
                "ordered": True,
                "bindings": [
                    {
                        "agent_count": {
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                            "type": "typed-literal",
                            "value": "3",
                        }
                    }
                ],
            },
        }
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        self.assertEqual(result, expected_result)

    def test_silencer_off(self):
        output_folder = os.path.join(BASE_DIR, "output_7")
        now = datetime.now()
        meta_config_path = os.path.join(BASE_DIR, "meta_config_7.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings["input_csv_dir"] = os.path.join(
            BASE_DIR, "same_as_input_2_with_other_authors"
        )
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        query_agents = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT (COUNT (?agent) AS ?agent_count)
            WHERE {
                <https://w3id.org/oc/meta/br/0601> pro:isDocumentContextFor ?agent.
            }
        """
        result = execute_sparql_query(SERVER, query_agents)
        expected_result = {
            "head": {"link": [], "vars": ["agent_count"]},
            "results": {
                "distinct": False,
                "ordered": True,
                "bindings": [
                    {
                        "agent_count": {
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                            "type": "typed-literal",
                            "value": "6",
                        }
                    }
                ],
            },
        }
        shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        self.assertEqual(result, expected_result)

    def test_omid_in_input_data(self):
        query_all = """
            PREFIX fabio: <http://purl.org/spar/fabio/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            CONSTRUCT {?s ?p ?o. ?id ?id_p ?id_o.}
            WHERE {
                ?s a fabio:JournalArticle;
                    ?p ?o.
                ?s datacite:hasIdentifier ?id.
                ?id ?id_p ?id_o.
            }
        """
        result = execute_sparql_query(SERVER, query_all, return_format=XML)
        output_folder = os.path.join(BASE_DIR, "output_8")
        now = datetime.now()
        meta_config_path_without_openalex = os.path.join(BASE_DIR, "meta_config_8.yaml")
        meta_config_path_with_openalex = os.path.join(BASE_DIR, "meta_config_9.yaml")
        with open(meta_config_path_without_openalex, encoding="utf-8") as file:
            settings_without_openalex = yaml.full_load(file)
        with open(meta_config_path_with_openalex, encoding="utf-8") as file:
            settings_with_openalex = yaml.full_load(file)
        run_meta_process(
            settings=settings_without_openalex,
            meta_config_path=meta_config_path_without_openalex,
        )
        run_meta_process(
            settings=settings_with_openalex,
            meta_config_path=meta_config_path_with_openalex,
        )
        query_all = """
            PREFIX fabio: <http://purl.org/spar/fabio/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            CONSTRUCT {?s ?p ?o. ?id ?id_p ?id_o.}
            WHERE {
                ?s a fabio:JournalArticle;
                    ?p ?o.
                ?s datacite:hasIdentifier ?id.
                ?id ?id_p ?id_o.
            }
        """
        result = execute_sparql_query(SERVER, query_all, return_format=XML)
        expected_result = Graph()
        expected_result.parse(
            location=os.path.join(BASE_DIR, "test_omid_in_input_data.json"),
            format="json-ld",
        )
        prov_graph = ConjunctiveGraph()
        for dirpath, dirnames, filenames in os.walk(os.path.join(output_folder, "rdf")):
            if "br" in dirpath and "prov" in dirpath:
                for filename in filenames:
                    prov_graph.parse(
                        source=os.path.join(dirpath, filename), format="json-ld"
                    )

        expected_prov_graph = ConjunctiveGraph()
        expected_prov_graph.parse(
            os.path.join(BASE_DIR, "test_omid_in_input_data_prov.json"),
            format="json-ld",
        )
        prov_graph.remove(
            (None, URIRef("http://www.w3.org/ns/prov#generatedAtTime"), None)
        )
        expected_prov_graph.remove(
            (None, URIRef("http://www.w3.org/ns/prov#generatedAtTime"), None)
        )
        prov_graph.remove(
            (None, URIRef("http://www.w3.org/ns/prov#invalidatedAtTime"), None)
        )
        expected_prov_graph.remove(
            (None, URIRef("http://www.w3.org/ns/prov#invalidatedAtTime"), None)
        )
        shutil.rmtree(output_folder)
        self.assertTrue(
            normalize_graph(result).isomorphic(normalize_graph(expected_result))
        )

    def test_publishers_sequence(self):
        output_folder = os.path.join(BASE_DIR, "output_9")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_10.yaml")
        now = datetime.now()
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        query_all = """
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            CONSTRUCT {?br ?p ?o. ?o ?op ?oo. ?oo ?oop ?ooo. ?ooo ?ooop ?oooo.}
            WHERE {
                ?id literal:hasLiteralValue "10.17117/na.2015.08.1067"^^<http://www.w3.org/2001/XMLSchema#string>;
                    datacite:usesIdentifierScheme datacite:doi;
                    ^datacite:hasIdentifier ?br.
                ?br ?p ?o.
                ?o ?op ?oo.
                ?oo ?oop ?ooo.
                ?ooo ?ooop ?oooo.
            }
        """
        result = execute_sparql_query(SERVER, query_all, return_format=XML)
        expected_result = Graph()
        expected_result.parse(
            os.path.join(BASE_DIR, "test_publishers_sequence.json"), format="json-ld"
        )
        shutil.rmtree(output_folder)
        self.assertTrue(
            normalize_graph(result).isomorphic(normalize_graph(expected_result))
        )

    def test_duplicate_omids_with_datatype(self):
        """Test to verify that identifiers are not duplicated due to datatype differences"""
        output_folder = os.path.join(BASE_DIR, "output_duplicate_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_duplicate.yaml")

        # Setup: create test data
        os.makedirs(os.path.join(BASE_DIR, "input_duplicate"), exist_ok=True)
        with open(
            os.path.join(BASE_DIR, "input_duplicate", "test.csv"), "w", encoding="utf-8"
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "title",
                    "author",
                    "pub_date",
                    "venue",
                    "volume",
                    "issue",
                    "page",
                    "type",
                    "publisher",
                    "editor",
                ]
            )
            writer.writerow(
                [
                    "issn:2543-3288 issn:2078-7685",  # Exact problematic row from production
                    "Journal of Diabetology",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "journal",
                    "Medknow [crossref:2581]",
                    "",
                ]
            )

        # Setup: Insert pre-existing identifiers and BRs in triplestore
        sparql = SPARQLWrapper(SERVER)
        sparql.setMethod(POST)
        sparql.setQuery(
            """
        INSERT DATA {
            GRAPH <https://w3id.org/oc/meta/br/> {
                <https://w3id.org/oc/meta/br/0601> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0601> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> .
                <https://w3id.org/oc/meta/br/0602> <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0602> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> .
            }
            GRAPH <https://w3id.org/oc/meta/id/> {
                <https://w3id.org/oc/meta/id/0601> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "2078-7685" ;
                                               <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
                <https://w3id.org/oc/meta/id/0602> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "2543-3288" ;
                                               <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
            }
        }
        """
        )
        sparql.query()

        # Update Redis counters to match the inserted data
        redis_handler = RedisCounterHandler(db=5)  # Use test db
        redis_handler.set_counter(
            2, "br", supplier_prefix="060"
        )  # BR counter for two BRs
        redis_handler.set_counter(
            2, "id", supplier_prefix="060"
        )  # ID counter for two IDs

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "input_csv_dir": os.path.join(BASE_DIR, "input_duplicate"),
            "base_output_dir": output_folder,
            "output_rdf_dir": output_folder,
            "resp_agent": "test",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": None,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": False,
            "zip_output_rdf": True,
            "source": None,
            "supplier_prefix": "060",
            "workers_number": 1,
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
        }

        with open(meta_config_path, "w") as f:
            yaml.dump(settings, f)

        # Run the process
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Check for errors
        errors_file = os.path.join(output_folder, "errors.txt")
        if os.path.exists(errors_file):
            with open(errors_file, "r") as f:
                errors = f.read()
                print(f"Errors found:\n{errors}")

        # Query to check for duplicates
        query = """
        SELECT DISTINCT ?id ?value
        WHERE {
            GRAPH <https://w3id.org/oc/meta/id/> {
                ?id <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> ?value ;
                    <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
                FILTER(?value IN ("2078-7685"^^<http://www.w3.org/2001/XMLSchema#string>, "2078-7685",
                                "2543-3288"^^<http://www.w3.org/2001/XMLSchema#string>, "2543-3288"))
            }
        }
        """
        result = execute_sparql_query(SERVER, query, return_format=JSON)
        # Group IDs by value to check for duplicates
        ids_by_value = {}
        for binding in result["results"]["bindings"]:
            value = binding["value"]["value"]
            id = binding["id"]["value"]
            if value not in ids_by_value:
                ids_by_value[value] = []
            ids_by_value[value].append(id)

        # Cleanup
        shutil.rmtree(output_folder, ignore_errors=True)
        shutil.rmtree(os.path.join(BASE_DIR, "input_duplicate"), ignore_errors=True)
        if os.path.exists(meta_config_path):
            os.remove(meta_config_path)

        # Check that we have both ISSNs and no duplicates
        for issn_value, ids in ids_by_value.items():
            self.assertEqual(
                len(ids), 1, f"Found multiple IDs for ISSN {issn_value}: {ids}"
            )

        self.assertEqual(
            len(ids_by_value),
            2,
            f"Expected 2 ISSNs, found {len(ids_by_value)}: {list(ids_by_value.keys())}",
        )

    def test_duplicate_omids_with_venue_datatype(self):
        """Test to verify that identifiers are not duplicated when merging previously unconnected venues"""
        output_folder = os.path.join(BASE_DIR, "output_duplicate_venue_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_duplicate_venue.yaml")

        # Setup: create test data
        os.makedirs(os.path.join(BASE_DIR, "input_duplicate_venue"), exist_ok=True)
        with open(
            os.path.join(BASE_DIR, "input_duplicate_venue", "test.csv"),
            "w",
            encoding="utf-8",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "title",
                    "author",
                    "pub_date",
                    "venue",
                    "volume",
                    "issue",
                    "page",
                    "type",
                    "publisher",
                    "editor",
                ]
            )
            writer.writerow(
                [
                    "issn:1756-1833",
                    "BMJ",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "journal",
                    "BMJ [crossref:239]",
                    "",
                ]
            )
            writer.writerow(
                [
                    "",  # id
                    "",  # title
                    "",  # author
                    "",  # pub_date
                    "BMJ [issn:0267-0623 issn:0959-8138 issn:1468-5833 issn:0007-1447]",  # venue
                    "283",  # volume
                    "",  # issue
                    "",  # page
                    "journal volume",  # type
                    "BMJ [crossref:239]",  # publisher
                    "",  # editor
                ]
            )

        # Setup: Insert pre-existing data - aggiungiamo gli identificatori iniziali
        sparql = SPARQLWrapper(SERVER)
        sparql.setMethod(POST)
        sparql.setQuery(
            """
        INSERT DATA {
            GRAPH <https://w3id.org/oc/meta/br/> {
                # First venue - BMJ with initial ISSNs
                <https://w3id.org/oc/meta/br/0601> 
                    <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0601>, <https://w3id.org/oc/meta/id/0602> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/dc/terms/title> "BMJ" .

                # Second venue
                <https://w3id.org/oc/meta/br/0602> 
                    <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0603> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/dc/terms/title> "British Medical Journal" .
            }
            GRAPH <https://w3id.org/oc/meta/id/> {
                # First venue's ISSNs
                <https://w3id.org/oc/meta/id/0601> 
                    <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "1756-1833" ;
                    <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
                <https://w3id.org/oc/meta/id/0602>
                    <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "0959-8138" ;
                    <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
                # Second venue's ISSN
                <https://w3id.org/oc/meta/id/0603> 
                    <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "0267-0623" ;
                    <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
            }
        }
        """
        )
        sparql.query()

        # Update Redis counters for the pre-existing entities
        redis_handler = RedisCounterHandler(db=5)
        redis_handler.set_counter(
            6, "br", supplier_prefix="060"
        )  # Updated to account for 6 entities (2 venues + 4 volumes)
        redis_handler.set_counter(
            3, "id", supplier_prefix="060"
        )  # Corretto: 3 IDs (1756-1833, 0959-8138, 0267-0623)

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "input_csv_dir": os.path.join(BASE_DIR, "input_duplicate_venue"),
            "base_output_dir": output_folder,
            "output_rdf_dir": output_folder,
            "resp_agent": "test",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": None,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": False,
            "zip_output_rdf": True,
            "source": None,
            "supplier_prefix": "060",
            "workers_number": 1,
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
        }

        with open(meta_config_path, "w") as f:
            yaml.dump(settings, f)

        # Run the process
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Query to check for duplicates - check all ISSNs
        query = """
        SELECT DISTINCT ?id ?value
        WHERE {
            ?id <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> ?value ;
                <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
            FILTER(STR(?value) IN ("1756-1833", "0959-8138", "0267-0623"))
        }
        """
        result = execute_sparql_query(SERVER, query, return_format=JSON)
        # Group IDs by value to check for duplicates
        ids_by_value = {}
        for binding in result["results"]["bindings"]:
            value = binding["value"]["value"]
            id = binding["id"]["value"]
            if value not in ids_by_value:
                ids_by_value[value] = []
            ids_by_value[value].append(id)

        print(json.dumps(ids_by_value, indent=4))

        # Cleanup
        shutil.rmtree(output_folder, ignore_errors=True)
        shutil.rmtree(
            os.path.join(BASE_DIR, "input_duplicate_venue"), ignore_errors=True
        )
        if os.path.exists(meta_config_path):
            os.remove(meta_config_path)

        # Check that we don't have duplicate IDs for any ISSN
        for issn_value, ids in ids_by_value.items():
            self.assertEqual(
                len(ids), 1, f"Found multiple IDs for ISSN {issn_value} in venue: {ids}"
            )

        # Verify that pre-existing IDs were reused
        self.assertTrue(
            any("0601" in id for ids in ids_by_value.values() for id in ids)
            and any("0602" in id for ids in ids_by_value.values() for id in ids),
            "Pre-existing IDs were not reused",
        )

    def test_doi_with_multiple_slashes(self):
        """Test handling of DOIs containing multiple forward slashes"""
        output_folder = os.path.join(BASE_DIR, "output_doi_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_doi.yaml")

        # Setup: create test data with problematic DOI
        os.makedirs(os.path.join(BASE_DIR, "input_doi"), exist_ok=True)
        with open(
            os.path.join(BASE_DIR, "input_doi", "test.csv"), "w", encoding="utf-8"
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "title",
                    "author",
                    "pub_date",
                    "venue",
                    "volume",
                    "issue",
                    "page",
                    "type",
                    "publisher",
                    "editor",
                ]
            )
            writer.writerow(
                [
                    "doi:10.1093/acprof:oso/9780199230723.001.0001",  # Problematic DOI with multiple slashes
                    "Test Book",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "book",
                    "",
                    "",
                ]
            )

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "input_csv_dir": os.path.join(BASE_DIR, "input_doi"),
            "base_output_dir": output_folder,
            "output_rdf_dir": output_folder,
            "resp_agent": "test",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": None,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": False,
            "zip_output_rdf": True,
            "source": None,
            "supplier_prefix": "060",
            "workers_number": 1,
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
        }

        with open(meta_config_path, "w") as f:
            yaml.dump(settings, f)

        now = datetime.now()

        # Run the process
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Query to verify DOI was processed correctly
        query = """
        SELECT ?br ?id ?value
        WHERE {
            ?id <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1093/acprof:oso/9780199230723.001.0001"^^<http://www.w3.org/2001/XMLSchema#string> ;
                <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/doi> ;
                ^<http://purl.org/spar/datacite/hasIdentifier> ?br .
        }
        """
        result = execute_sparql_query(SERVER, query, return_format=JSON)

        # Cleanup
        shutil.rmtree(output_folder, ignore_errors=True)
        shutil.rmtree(os.path.join(BASE_DIR, "input_doi"), ignore_errors=True)
        if os.path.exists(meta_config_path):
            os.remove(meta_config_path)
        delete_output_zip(".", now)

        # Verify results
        self.assertTrue(
            len(result["results"]["bindings"]) > 0,
            "DOI with multiple slashes was not processed correctly",
        )

        # Check that we got exactly one result
        self.assertEqual(
            len(result["results"]["bindings"]),
            1,
            f"Expected 1 result, got {len(result['results']['bindings'])}",
        )

    def test_volume_issue_deduplication(self):
        """Test to verify that volumes and issues are properly deduplicated"""
        output_folder = os.path.join(BASE_DIR, "output_vvi_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_vvi.yaml")

        # Setup: create test data
        os.makedirs(os.path.join(BASE_DIR, "input_vvi"), exist_ok=True)
        with open(
            os.path.join(BASE_DIR, "input_vvi", "test.csv"), "w", encoding="utf-8"
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "title",
                    "author",
                    "pub_date",
                    "venue",
                    "volume",
                    "issue",
                    "page",
                    "type",
                    "publisher",
                    "editor",
                ]
            )
            # First article in volume 1, issue 1
            writer.writerow(
                [
                    "doi:10.1234/test.1",
                    "First Article",
                    "",
                    "2023",
                    "Test Journal [issn:1756-1833]",
                    "1",
                    "1",
                    "1-10",
                    "journal article",
                    "",
                    "",
                ]
            )
            # Second article in same volume and issue
            writer.writerow(
                [
                    "doi:10.1234/test.2",
                    "Second Article",
                    "",
                    "2023",
                    "Test Journal [issn:1756-1833]",
                    "1",
                    "1",
                    "11-20",
                    "journal article",
                    "",
                    "",
                ]
            )

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "input_csv_dir": os.path.join(BASE_DIR, "input_vvi"),
            "base_output_dir": output_folder,
            "output_rdf_dir": output_folder,
            "resp_agent": "test",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": None,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": False,
            "zip_output_rdf": True,
            "source": None,
            "supplier_prefix": "060",
            "workers_number": 1,
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
        }

        with open(meta_config_path, "w") as f:
            yaml.dump(settings, f)

        # Run the process
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Query to check volume and issue structure
        query = """
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/>
        
        SELECT ?article ?volume ?issue ?seq_id
        WHERE {
            ?article a fabio:JournalArticle ;
                    frbr:partOf ?issue .
            ?issue a fabio:JournalIssue ;
                   fabio:hasSequenceIdentifier ?seq_id ;
                   frbr:partOf ?volume .
            ?volume a fabio:JournalVolume .
        }
        ORDER BY ?article
        """

        result = execute_sparql_query(SERVER, query)

        # Cleanup
        shutil.rmtree(output_folder, ignore_errors=True)
        shutil.rmtree(os.path.join(BASE_DIR, "input_vvi"), ignore_errors=True)
        if os.path.exists(meta_config_path):
            os.remove(meta_config_path)

        # Verify results
        bindings = result["results"]["bindings"]

        # Should have 2 articles
        self.assertEqual(len(bindings), 2, "Expected 2 articles")

        # Both articles should reference the same volume and issue
        first_volume = bindings[0]["volume"]["value"]
        first_issue = bindings[0]["issue"]["value"]

        for binding in bindings[1:]:
            self.assertEqual(
                binding["volume"]["value"],
                first_volume,
                "Articles reference different volumes",
            )
            self.assertEqual(
                binding["issue"]["value"],
                first_issue,
                "Articles reference different issues",
            )

    def test_volume_issue_deduplication_with_triplestore(self):
        """Test that volumes and issues are properly deduplicated when they already exist in the triplestore"""
        output_folder = os.path.join(BASE_DIR, "output_vvi_triplestore_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_vvi_triplestore.yaml")

        # Setup: Insert pre-existing venue with duplicate volumes and issues (with/without datatype)
        sparql = SPARQLWrapper(SERVER)
        sparql.setMethod(POST)
        sparql.setQuery(
            """
        INSERT DATA {
            GRAPH <https://w3id.org/oc/meta/br/> {
                # Venue
                <https://w3id.org/oc/meta/br/0601> 
                    <http://purl.org/spar/datacite/hasIdentifier> <https://w3id.org/oc/meta/id/0601> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Journal> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/dc/terms/title> "Test Journal" .
                
                # Volume 1 (without datatype)
                <https://w3id.org/oc/meta/br/0602>
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0601> ;
                    <http://purl.org/spar/fabio/hasSequenceIdentifier> "1" .

                # Volume 1 (with datatype)
                <https://w3id.org/oc/meta/br/0604>
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalVolume> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0601> ;
                    <http://purl.org/spar/fabio/hasSequenceIdentifier> "1"^^<http://www.w3.org/2001/XMLSchema#string> .
                
                # Issue 1 (without datatype)
                <https://w3id.org/oc/meta/br/0603>
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalIssue> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0602> ;
                    <http://purl.org/spar/fabio/hasSequenceIdentifier> "1" .

                # Issue 1 (with datatype)
                <https://w3id.org/oc/meta/br/0605>
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalIssue> ;
                    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> ;
                    <http://purl.org/vocab/frbr/core#partOf> <https://w3id.org/oc/meta/br/0604> ;
                    <http://purl.org/spar/fabio/hasSequenceIdentifier> "1"^^<http://www.w3.org/2001/XMLSchema#string> .
            }
            GRAPH <https://w3id.org/oc/meta/id/> {
                <https://w3id.org/oc/meta/id/0601>
                    <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "1756-1833" ;
                    <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/issn> .
            }
        }
        """
        )
        sparql.query()

        # Update Redis counters for pre-existing entities
        redis_handler = RedisCounterHandler(db=5)
        redis_handler.set_counter(
            5, "br", supplier_prefix="060"
        )  # 5 entities: venue, 2 volumes, 2 issues
        redis_handler.set_counter(
            1, "id", supplier_prefix="060"
        )  # 1 identifier for venue

        # Create test data - article that should use existing volume and issue
        os.makedirs(os.path.join(BASE_DIR, "input_vvi_triplestore"), exist_ok=True)
        with open(
            os.path.join(BASE_DIR, "input_vvi_triplestore", "test.csv"),
            "w",
            encoding="utf-8",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "title",
                    "author",
                    "pub_date",
                    "venue",
                    "volume",
                    "issue",
                    "page",
                    "type",
                    "publisher",
                    "editor",
                ]
            )
            writer.writerow(
                [
                    "doi:10.1234/test.1",
                    "Test Article",
                    "",
                    "2023",
                    "Test Journal [issn:1756-1833]",
                    "1",  # Should match existing volume
                    "1",  # Should match existing issue
                    "1-10",
                    "journal article",
                    "",
                    "",
                ]
            )

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "input_csv_dir": os.path.join(BASE_DIR, "input_vvi_triplestore"),
            "base_output_dir": output_folder,
            "output_rdf_dir": output_folder,
            "resp_agent": "test",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": None,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": False,
            "zip_output_rdf": True,
            "source": None,
            "supplier_prefix": "060",
            "workers_number": 1,
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
        }

        with open(meta_config_path, "w") as f:
            yaml.dump(settings, f)

        # Run the process
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Check if new volumes/issues were created
        to_be_uploaded_dir = os.path.join(output_folder, "rdf", "to_be_uploaded")
        new_entities_created = False
        if os.path.exists(to_be_uploaded_dir):
            for dirpath, _, filenames in os.walk(to_be_uploaded_dir):
                for f in filenames:
                    if f.endswith(".sparql"):
                        with open(os.path.join(dirpath, f)) as file:
                            content = file.read()
                            if any(
                                "JournalVolume" in line or "JournalIssue" in line
                                for line in content.splitlines()
                            ):
                                print(f"\nFound new volume/issue creation in {f}:")
                                new_entities_created = True

        # Query to get all entities and their relationships
        query = """
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX frbr: <http://purl.org/vocab/frbr/core#>
        PREFIX datacite: <http://purl.org/spar/datacite/>
        
        SELECT DISTINCT ?article ?venue ?volume ?issue ?issn
        WHERE {
            ?article a fabio:JournalArticle ;
                    frbr:partOf ?issue .
            ?issue a fabio:JournalIssue ;
                   frbr:partOf ?volume .
            ?volume a fabio:JournalVolume ;
                    frbr:partOf ?venue .
            ?venue datacite:hasIdentifier ?id .
            ?id datacite:usesIdentifierScheme datacite:issn ;
                <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> ?issn .
        }
        """

        result = execute_sparql_query(SERVER, query)

        # Cleanup
        shutil.rmtree(output_folder, ignore_errors=True)
        shutil.rmtree(
            os.path.join(BASE_DIR, "input_vvi_triplestore"), ignore_errors=True
        )
        if os.path.exists(meta_config_path):
            os.remove(meta_config_path)

        # Verify results
        bindings = result["results"]["bindings"]

        self.assertEqual(len(bindings), 1, "Expected exactly one article")

        # Get the URIs from the result
        venue_uri = bindings[0]["venue"]["value"]
        volume_uri = bindings[0]["volume"]["value"]
        issue_uri = bindings[0]["issue"]["value"]
        issn = bindings[0]["issn"]["value"]

        # Check if venue was deduplicated (should use existing venue)
        self.assertEqual(
            venue_uri,
            "https://w3id.org/oc/meta/br/0601",
            "Venue was not deduplicated correctly",
        )

        # Check if volume was deduplicated - either version is valid
        self.assertIn(
            volume_uri,
            ["https://w3id.org/oc/meta/br/0602", "https://w3id.org/oc/meta/br/0604"],
            "Volume was not deduplicated correctly - should use one of the existing volumes",
        )

        # Check if issue was deduplicated - either version is valid
        self.assertIn(
            issue_uri,
            ["https://w3id.org/oc/meta/br/0603", "https://w3id.org/oc/meta/br/0605"],
            "Issue was not deduplicated correctly - should use one of the existing issues",
        )

        # Check ISSN
        self.assertEqual(issn, "1756-1833", "ISSN does not match")

        # Verify no new volumes/issues were created
        self.assertFalse(
            new_entities_created,
            "New volumes/issues were created when they should have been deduplicated",
        )

        # # Recreate input directory and file since sono stati cancellati dal cleanup
        # os.makedirs(os.path.join(BASE_DIR, 'input_vvi_triplestore'), exist_ok=True)
        # with open(os.path.join(BASE_DIR, 'input_vvi_triplestore', 'test.csv'), 'w', encoding='utf-8') as f:
        #     writer = csv.writer(f)
        #     writer.writerow(["id", "title", "author", "pub_date", "venue", "volume", "issue", "page", "type", "publisher", "editor"])
        #     writer.writerow([
        #         "doi:10.1234/test.1",
        #         "Test Article",
        #         "",
        #         "2023",
        #         "Test Journal [issn:1756-1833]",
        #         "1",
        #         "1",
        #         "1-10",
        #         "journal article",
        #         "",
        #         ""
        #     ])

        # # Run the process again
        # run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # # Check if ANY files were created in to_be_uploaded
        # to_be_uploaded_dir = os.path.join(output_folder, 'rdf', 'to_be_uploaded')
        # files_created = False
        # if os.path.exists(to_be_uploaded_dir):
        #     for dirpath, _, filenames in os.walk(to_be_uploaded_dir):
        #         for f in filenames:
        #             if f.endswith('.sparql'):
        #                 files_created = True
        #                 print(f"\nFound unexpected file creation in second pass - {f}:")
        #                 with open(os.path.join(dirpath, f)) as file:
        #                     print(file.read())

        # # Verify no files were created in second pass
        # self.assertFalse(files_created,
        #     "Files were created in to_be_uploaded during second pass when all data should already exist")

        # # Final cleanup
        # shutil.rmtree(output_folder, ignore_errors=True)
        # shutil.rmtree(os.path.join(BASE_DIR, 'input_vvi_triplestore'), ignore_errors=True)
        # if os.path.exists(meta_config_path):
        #     os.remove(meta_config_path)

    def test_temporary_identifiers(self):
        """Test that temporary identifiers are used for deduplication but not saved, and an OMID is generated"""
        output_folder = os.path.join(BASE_DIR, "output_temp_id_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_temp.yaml")

        # Setup: create test data with only temporary identifier
        os.makedirs(os.path.join(BASE_DIR, "input_temp"), exist_ok=True)
        with open(
            os.path.join(BASE_DIR, "input_temp", "test.csv"), "w", encoding="utf-8"
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "title",
                    "author",
                    "pub_date",
                    "venue",
                    "volume",
                    "issue",
                    "page",
                    "type",
                    "publisher",
                    "editor",
                ]
            )
            writer.writerow(
                [
                    "temp:567",  # Only temporary identifier
                    "Test Article",
                    "",
                    "2023",
                    "",
                    "",
                    "",
                    "",
                    "journal article",
                    "",
                    "",
                ]
            )

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "input_csv_dir": os.path.join(BASE_DIR, "input_temp"),
            "base_output_dir": output_folder,
            "output_rdf_dir": output_folder,
            "resp_agent": "test",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": None,
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": False,
            "zip_output_rdf": True,
            "source": None,
            "supplier_prefix": "060",
            "workers_number": 1,
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
        }

        with open(meta_config_path, "w") as f:
            yaml.dump(settings, f)

        now = datetime.now()

        # Run the process
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Query to verify an OMID was generated and no temporary identifier was saved
        query = """
        PREFIX fabio: <http://purl.org/spar/fabio/>
        PREFIX datacite: <http://purl.org/spar/datacite/>
        PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
        
        SELECT ?br ?id ?value ?scheme
        WHERE {
            ?br a fabio:JournalArticle .
            OPTIONAL {
                ?br datacite:hasIdentifier ?id .
                ?id datacite:usesIdentifierScheme ?scheme ;
                    literal:hasLiteralValue ?value .
            }
        }
        """
        result = execute_sparql_query(SERVER, query, return_format=JSON)

        # Cleanup
        shutil.rmtree(output_folder, ignore_errors=True)
        shutil.rmtree(os.path.join(BASE_DIR, "input_temp"), ignore_errors=True)
        if os.path.exists(meta_config_path):
            os.remove(meta_config_path)
        delete_output_zip(".", now)

        # Verify results
        bindings = result["results"]["bindings"]

        # Should find exactly one article
        self.assertEqual(len(bindings), 1, "Expected exactly one article")

        # The article should have a br/ URI (OMID)
        br_uri = bindings[0]["br"]["value"]
        self.assertTrue(
            "br/" in br_uri,
            f"Article URI {br_uri} does not contain expected OMID pattern 'br/'",
        )

        # Should not have any saved identifiers
        self.assertNotIn(
            "id",
            bindings[0],
            "Found unexpected identifier when only temporary ID was provided",
        )


def normalize_graph(graph):
    """
    Normalizza i letterali nel grafo rimuovendo i tipi di dato espliciti.
    """
    normalized_graph = Graph()
    for subject, predicate, obj in graph:
        if isinstance(obj, Literal) and obj.datatype is not None:
            normalized_obj = Literal(obj.toPython())
            normalized_graph.add((subject, predicate, normalized_obj))
        else:
            normalized_graph.add((subject, predicate, obj))
    return normalized_graph


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
