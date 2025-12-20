import csv
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime
from test.test_utils import (PROV_SERVER, SERVER, execute_sparql_construct,
                             execute_sparql_query, reset_redis_counters,
                             reset_server, wait_for_virtuoso)

import yaml
from oc_meta.lib.file_manager import get_csv_data, write_csv
from oc_meta.run.meta_process import run_meta_process
from oc_ocdm.counter_handler.redis_counter_handler import RedisCounterHandler
from rdflib import Dataset, Graph, Literal, URIRef
from sparqlite import SPARQLClient

BASE_DIR = os.path.join("test", "meta_process")


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
        """Setup eseguito una volta per tutta la classe di test"""
        if not wait_for_virtuoso(SERVER, max_wait=30):
            raise TimeoutError("Virtuoso not ready after 30 seconds")

    def setUp(self):
        """Setup eseguito prima di ogni test"""
        # Create temporary directory for cache files
        self.temp_dir = tempfile.mkdtemp()
        self.cache_file = os.path.join(self.temp_dir, "ts_upload_cache.json")
        self.failed_file = os.path.join(self.temp_dir, "failed_queries.txt")
        self.stop_file = os.path.join(self.temp_dir, ".stop_upload")

        # Reset del database
        reset_server()
        reset_redis_counters()

    def tearDown(self):
        reset_redis_counters()
        # Remove temporary directory and its contents
        if hasattr(self, "temp_dir") and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        # Clean up bulk load files
        bulk_load_dirs = [
            "test/test_virtuoso_db/bulk_load",
            "test/test_virtuoso_db_prov/bulk_load"
        ]
        for bulk_dir in bulk_load_dirs:
            if os.path.exists(bulk_dir):
                for file in glob.glob(os.path.join(bulk_dir, "*.nq.gz")):
                    os.remove(file)
                for file in glob.glob(os.path.join(bulk_dir, "*.backup")):
                    os.remove(file)

        for i in range(1, 11):
            output_dir = os.path.join(BASE_DIR, f"output_{i}")
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)

    def test_run_meta_process(self):
        output_folder = os.path.join(BASE_DIR, "output_1")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_1.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)

        # Update settings with temporary files and Redis cache DB
        settings.update(
            {
                "redis_cache_db": 2,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )

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

        # Update settings with temporary files and Redis cache DB
        settings.update(
            {
                "redis_cache_db": 2,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )

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

    def test_provenance(self):
        # Bulk load disabled in meta_config_3.yaml
        output_folder = os.path.join(BASE_DIR, "output_3")
        now = datetime.now()
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        delete_output_zip(".", now)
        meta_config_path = os.path.join(BASE_DIR, "meta_config_3.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)

        # Update settings with temporary files and Redis cache DB
        settings.update(
            {
                "redis_cache_db": 2,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )

        reset_server()
        
        settings["input_csv_dir"] = os.path.join(BASE_DIR, "input")
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings["input_csv_dir"] = os.path.join(BASE_DIR, "input_2")
        run_meta_process(settings=settings, meta_config_path=meta_config_path)
        settings["input_csv_dir"] = os.path.join(BASE_DIR, "input")
        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        output = dict()
        
        entity_types = ['ar', 'br', 'id', 'ra', 're']
        
        for entity_type in entity_types:
            query = f"""
            SELECT ?s ?p ?o
            WHERE {{
                ?s ?p ?o .
                FILTER(REGEX(STR(?s), "https://w3id.org/oc/meta/{entity_type}/[0-9]+/prov/se/[0-9]+"))
            }}
            """

            result = execute_sparql_query(PROV_SERVER, query)

            entities = {}
            for binding in result['results']['bindings']:
                s_str = binding['s']['value']
                p_str = binding['p']['value']
                o_data = binding['o']

                if s_str not in entities:
                    entities[s_str] = {'@id': s_str, '@type': []}

                if p_str == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                    entities[s_str]['@type'].append(o_data['value'])
                else:
                    if p_str not in entities[s_str]:
                        entities[s_str][p_str] = []

                    if o_data['type'] == 'uri':
                        entities[s_str][p_str].append({'@id': o_data['value']})
                    elif o_data.get('datatype'):
                        entities[s_str][p_str].append({
                            '@value': o_data['value'],
                            '@type': o_data['datatype']
                        })
                    else:
                        entities[s_str][p_str].append({'@value': o_data['value']})
            
            # Group entities by their parent entity (e.g., br/0601/prov/se/1 -> br/0601)
            grouped_entities = {}
            for entity_id, entity_data in entities.items():
                # Extract the parent entity ID from the provenance entity ID
                parent_id = re.match(r'https://w3id.org/oc/meta/([^/]+/[0-9]+)', entity_id).group(0)
                
                if parent_id not in grouped_entities:
                    grouped_entities[parent_id] = []
                
                # Filter out properties we don't need for comparison
                filtered_entity_data = {
                    '@id': entity_data['@id'],
                }
                
                # Keep the required properties for comparison
                properties_to_keep = [
                    'http://www.w3.org/ns/prov#specializationOf',
                    'http://www.w3.org/ns/prov#wasDerivedFrom'
                ]
                
                for prop in properties_to_keep:
                    if prop in entity_data:
                        filtered_entity_data[prop] = entity_data[prop]
                
                # Handle hasUpdateQuery specially
                if 'https://w3id.org/oc/ontology/hasUpdateQuery' in entity_data:
                    # Extract the value from the hasUpdateQuery property
                    update_query_value = entity_data['https://w3id.org/oc/ontology/hasUpdateQuery'][0].get('@value', '')
                    
                    # Split the query into individual statements
                    if update_query_value:
                        # Extract the part between the INSERT DATA { GRAPH <...> { and } }
                        try:
                            query_content = update_query_value.split(
                                "INSERT DATA { GRAPH <https://w3id.org/oc/meta/br/> { "
                            )[1].split(" } }")[0]
                            
                            # Split by dot and space to get individual statements
                            statements = set(query_content.split(" ."))
                            
                            # Add to filtered entity data
                            filtered_entity_data['https://w3id.org/oc/ontology/hasUpdateQuery'] = statements
                        except IndexError:
                            # If the format is different, just use the original value
                            filtered_entity_data['https://w3id.org/oc/ontology/hasUpdateQuery'] = \
                                entity_data['https://w3id.org/oc/ontology/hasUpdateQuery']
                
                # Add this filtered entity to its parent's group
                grouped_entities[parent_id].append(filtered_entity_data)
            
            # Format the output to match the expected structure
            entity_list = []
            for parent_id, entities_list in sorted(grouped_entities.items()):
                entity_list.append({
                    '@graph': sorted(entities_list, key=lambda x: x['@id'])
                })
            
            output[entity_type] = entity_list
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
        self.maxDiff = None
        self.assertEqual(output, expected_output)

    def test_run_meta_process_thread_safe(self):
        output_folder = os.path.join(BASE_DIR, "output_4")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_4.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)
        original_input_csv_dir = settings["input_csv_dir"]
        settings["input_csv_dir"] = os.path.join(original_input_csv_dir, "preprocess")

        # Use temporary cache files to avoid corruption
        settings["ts_upload_cache"] = self.cache_file
        settings["ts_failed_queries"] = self.failed_file
        settings["ts_stop_file"] = self.stop_file

        reset_server()

        run_meta_process(settings=settings, meta_config_path=meta_config_path)

        # Create a temporary config file with updated settings for subprocess
        temp_config_path = os.path.join(self.temp_dir, "temp_meta_config.yaml")
        with open(temp_config_path, "w") as f:
            yaml.dump(settings, f)

        # Run it again to test thread safety
        proc = subprocess.run(
            [sys.executable, "-m", "oc_meta.run.meta_process", "-c", temp_config_path],
            capture_output=True,
            text=True,
        )
        
        output = dict()
        
        entity_types = ['ar', 'br', 'id', 'ra', 're']
        
        for entity_type in entity_types:
            query = f"""
            SELECT ?s ?p ?o
            WHERE {{
                ?s ?p ?o .
                FILTER(STRSTARTS(STR(?s), "https://w3id.org/oc/meta/{entity_type}/"))
            }}
            """

            result = execute_sparql_query(SERVER, query)

            entities = {}
            for binding in result['results']['bindings']:
                s_str = binding['s']['value']
                p_str = binding['p']['value']
                o_data = binding['o']

                if s_str not in entities:
                    entities[s_str] = {'@id': s_str, '@type': []}

                if p_str == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
                    entities[s_str]['@type'].append(o_data['value'])
                else:
                    if p_str not in entities[s_str]:
                        entities[s_str][p_str] = []

                    if o_data['type'] == 'uri':
                        entities[s_str][p_str].append({'@id': o_data['value']})
                    elif o_data.get('datatype'):
                        entities[s_str][p_str].append({
                            '@value': o_data['value'],
                            '@type': o_data['datatype']
                        })
                    else:
                        entities[s_str][p_str].append({'@value': o_data['value']})
            
            entity_list = list(entities.values())
            
            output[entity_type] = [
                {
                    '@graph': entity_list,
                    '@id': f"https://w3id.org/oc/meta/{entity_type}/"
                }
            ]
        
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
                                    "@value": "Nonthermal Sterilization And Shelf-life Extension Of Seafood Products By Intense Pulsed Light Treatment",
                                    "@type": "http://www.w3.org/2001/XMLSchema#string"
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
                                {"@value": "1", "@type": "http://www.w3.org/2001/XMLSchema#string"}
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
                                {"@value": "The Korean Journal Of Food And Nutrition", "@type": "http://www.w3.org/2001/XMLSchema#string"}
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
                                {"@value": "25", "@type": "http://www.w3.org/2001/XMLSchema#string"}
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
                                {"@value": "0000-0002-9666-2513", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0601",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/doi"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "10.9799/ksfan.2012.25.1.069", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0603",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/orcid"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "0000-0003-2542-5788", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0604",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/crossref"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "4768", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/id/0602",
                            "@type": ["http://purl.org/spar/datacite/Identifier"],
                            "http://purl.org/spar/datacite/usesIdentifierScheme": [
                                {"@id": "http://purl.org/spar/datacite/issn"}
                            ],
                            "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue": [
                                {"@value": "1225-4339", "@type": "http://www.w3.org/2001/XMLSchema#string"}
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
                                {"@value": "Chung", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Myong-Soo", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0602",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://xmlns.com/foaf/0.1/familyName": [{"@value": "Mun", "@type": "http://www.w3.org/2001/XMLSchema#string"}],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Ji-Hye", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0604",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0604"}
                            ],
                            "http://xmlns.com/foaf/0.1/name": [
                                {"@value": "The Korean Society Of Food And Nutrition", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0603",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://xmlns.com/foaf/0.1/familyName": [
                                {"@value": "Chung", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Myong-Soo", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        },
                        {
                            "@id": "https://w3id.org/oc/meta/ra/0601",
                            "@type": ["http://xmlns.com/foaf/0.1/Agent"],
                            "http://purl.org/spar/datacite/hasIdentifier": [
                                {"@id": "https://w3id.org/oc/meta/id/0603"}
                            ],
                            "http://xmlns.com/foaf/0.1/familyName": [
                                {"@value": "Cheigh", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                            "http://xmlns.com/foaf/0.1/givenName": [
                                {"@value": "Chan-Ick", "@type": "http://www.w3.org/2001/XMLSchema#string"}
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
                                {"@value": "76", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                            "http://prismstandard.org/namespaces/basic/2.0/startingPage": [
                                {"@value": "69", "@type": "http://www.w3.org/2001/XMLSchema#string"}
                            ],
                        }
                    ],
                    "@id": "https://w3id.org/oc/meta/re/",
                }
            ],
        }
        
        processed_output = {}
        for entity_type, entity_data in output.items():
            processed_output[entity_type] = []
            for graph_container in entity_data:
                filtered_graph = []
                for entity in graph_container['@graph']:
                    filtered_entity = {
                        '@id': entity['@id']
                    }
                    for pred, obj in entity.items():
                        if pred != '@id':  # Only exclude @id since we already added it
                            filtered_entity[pred] = obj
                    
                    if len(filtered_entity) > 1:  # Only include if it has predicates beyond @id
                        filtered_graph.append(filtered_entity)
                
                # Sort the graph by @id
                filtered_graph = sorted(filtered_graph, key=lambda x: x['@id'])
                
                processed_output[entity_type].append({
                    '@graph': filtered_graph,
                    '@id': graph_container['@id']
                })
        # For each entity type in the expected output, verify that all expected entities exist
        # with their expected properties in the actual output from the triplestore
        for entity_type, expected_graphs in expected_output.items():
            self.assertIn(entity_type, processed_output, f"Entity type {entity_type} missing from triplestore output")
            
            for expected_graph in expected_graphs:
                expected_entities = expected_graph['@graph']
                
                # Find the corresponding graph in the processed output
                actual_graph = None
                for graph in processed_output[entity_type]:
                    if graph['@id'] == expected_graph['@id']:
                        actual_graph = graph
                        break
                
                self.assertIsNotNone(actual_graph, f"Graph {expected_graph['@id']} not found in triplestore output")
                
                # For each expected entity, verify it exists with all expected properties
                for expected_entity in expected_entities:
                    entity_id = expected_entity['@id']
                    
                    # Find the entity in the actual graph
                    actual_entity = None
                    for entity in actual_graph['@graph']:
                        if entity['@id'] == entity_id:
                            actual_entity = entity
                            break
                    
                    self.assertIsNotNone(actual_entity, f"Entity {entity_id} not found in triplestore output")
                    
                    # Check that all expected predicates and objects exist
                    for pred, expected_objects in expected_entity.items():
                        if pred != '@id':
                            self.assertIn(pred, actual_entity, f"Predicate {pred} missing for entity {entity_id}")
                            
                            # For each expected object, verify it exists in the actual objects
                            for expected_obj in expected_objects:
                                found = False
                                for actual_obj in actual_entity[pred]:
                                    # Require exact matches for all objects
                                    if expected_obj == actual_obj:
                                        found = True
                                        break
                                
                                self.assertTrue(found, f"Object {expected_obj} not found for predicate {pred} of entity {entity_id}\nActual values: {actual_entity[pred]}")
                                
        
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)
        
        self.assertFalse(
            "Reader: ERROR" in proc.stdout or "Storer: ERROR" in proc.stdout
        )
        self.assertFalse(
            "Reader: ERROR" in proc.stdout or "Storer: ERROR" in proc.stdout
        )

    def test_silencer_on(self):
        output_folder = os.path.join(BASE_DIR, "output_6")
        now = datetime.now()
        meta_config_path = os.path.join(BASE_DIR, "meta_config_6.yaml")
        with open(meta_config_path, encoding="utf-8") as file:
            settings = yaml.full_load(file)

        # Update settings with temporary files and Redis cache DB
        settings.update(
            {
                "redis_cache_db": 2,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )

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
                            "type": "literal",
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

        # Update settings with temporary files and Redis cache DB
        settings.update(
            {
                "redis_cache_db": 2,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )

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
                            "type": "literal",
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
        result = execute_sparql_construct(SERVER, query_all)
        output_folder = os.path.join(BASE_DIR, "output_8")
        meta_config_path_without_openalex = os.path.join(BASE_DIR, "meta_config_8.yaml")
        meta_config_path_with_openalex = os.path.join(BASE_DIR, "meta_config_9.yaml")
        with open(meta_config_path_without_openalex, encoding="utf-8") as file:
            settings_without_openalex = yaml.full_load(file)
        with open(meta_config_path_with_openalex, encoding="utf-8") as file:
            settings_with_openalex = yaml.full_load(file)

        # Update settings with temporary files and Redis cache DB
        cache_settings = {
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
        }
        settings_without_openalex.update(cache_settings)
        settings_with_openalex.update(cache_settings)

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
        result = execute_sparql_construct(SERVER, query_all)
        expected_result = Graph()
        expected_result.parse(
            location=os.path.join(BASE_DIR, "test_omid_in_input_data.json"),
            format="json-ld",
        )
        prov_graph = Dataset()
        for dirpath, dirnames, filenames in os.walk(os.path.join(output_folder, "rdf")):
            if "br" in dirpath and "prov" in dirpath:
                for filename in filenames:
                    prov_graph.parse(
                        source=os.path.join(dirpath, filename), format="json-ld"
                    )

        expected_prov_graph = Dataset()
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

        # Update settings with temporary files and Redis cache DB
        settings.update(
            {
                "redis_cache_db": 2,
                "ts_upload_cache": self.cache_file,
                "ts_failed_queries": self.failed_file,
                "ts_stop_file": self.stop_file,
            }
        )

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
        result = execute_sparql_construct(SERVER, query_all)
        expected_result = Graph()
        expected_result.parse(
            os.path.join(BASE_DIR, "test_publishers_sequence.json"), format="json-ld"
        )
        shutil.rmtree(output_folder)
        self.assertTrue(
            normalize_graph(result).isomorphic(normalize_graph(expected_result))
        )

    def test_duplicate_omids_with_datatype(self):
        output_folder = os.path.join(BASE_DIR, "output_duplicate_test")
        meta_config_path = os.path.join(BASE_DIR, "meta_config_duplicate.yaml")

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "provenance_triplestore_url": PROV_SERVER,
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
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
        }

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
        with SPARQLClient(SERVER, timeout=60) as client:
            client.update(
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

        # Update Redis counters to match the inserted data
        redis_handler = RedisCounterHandler(port=6381, db=5)  # Use test db
        redis_handler.set_counter(
            2, "br", supplier_prefix="060"
        )  # BR counter for two BRs
        redis_handler.set_counter(
            2, "id", supplier_prefix="060"
        )  # ID counter for two IDs

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
        result = execute_sparql_query(SERVER, query)
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
        with SPARQLClient(SERVER, timeout=60) as client:
            client.update(
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

        # Update Redis counters for the pre-existing entities
        redis_handler = RedisCounterHandler(port=6381, db=5)
        redis_handler.set_counter(
            6, "br", supplier_prefix="060"
        )  # Updated to account for 6 entities (2 venues + 4 volumes)
        redis_handler.set_counter(
            3, "id", supplier_prefix="060"
        )  # Corretto: 3 IDs (1756-1833, 0959-8138, 0267-0623)

        # Create test settings
        settings = {
            "triplestore_url": SERVER,
            "provenance_triplestore_url": PROV_SERVER,
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
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
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
        result = execute_sparql_query(SERVER, query)
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
            "provenance_triplestore_url": PROV_SERVER,
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
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
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
        result = execute_sparql_query(SERVER, query)

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
            "provenance_triplestore_url": PROV_SERVER,
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
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
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
        with SPARQLClient(SERVER, timeout=60) as client:
            client.update(
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

        # Update Redis counters for pre-existing entities
        redis_handler = RedisCounterHandler(port=6381, db=5)
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
            "provenance_triplestore_url": PROV_SERVER,
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
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
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
            "provenance_triplestore_url": PROV_SERVER,
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
            "use_doi_api_service": False,
            "blazegraph_full_text_search": False,
            "virtuoso_full_text_search": True,
            "fuseki_full_text_search": False,
            "graphdb_connector_name": None,
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
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
        result = execute_sparql_query(SERVER, query)

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

    def test_temporary_identifiers_deduplication(self):
        """Test that multiple rows with the same temporary identifier are correctly deduplicated"""
        # Create test data with two rows using the same temporary identifier
        test_data = [
            {
                "id": "temp:789",
                "title": "Test Article 1",
                "author": "Smith, John [orcid:0000-0002-1234-5678]",
                "pub_date": "2020",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "journal article",
                "publisher": "",
                "editor": "",
            },
            {
                "id": "temp:789",  # Same temporary ID
                "title": "Test Article 1",  # Same title
                "author": "Smith, John [orcid:0000-0002-1234-5678]",
                "pub_date": "2020",
                "venue": "",
                "volume": "",
                "issue": "",
                "page": "",
                "type": "journal article",
                "publisher": "",
                "editor": "",
            },
        ]

        # Write test data to CSV
        input_dir = os.path.join(BASE_DIR, "input_temp_dedup")
        os.makedirs(input_dir, exist_ok=True)
        csv_path = os.path.join(input_dir, "test.csv")
        write_csv(csv_path, test_data)

        # Run meta process
        output_dir = os.path.join(BASE_DIR, "output_temp_dedup")
        os.makedirs(output_dir, exist_ok=True)
        config = {
            "input_csv_dir": input_dir,
            "base_output_dir": output_dir,
            "output_rdf_dir": output_dir,
            "triplestore_url": SERVER,
            "provenance_triplestore_url": PROV_SERVER,
            "resp_agent": "https://w3id.org/oc/meta/prov/pa/1",
            "base_iri": "https://w3id.org/oc/meta/",
            "context_path": "https://w3id.org/oc/meta/context.json",
            "supplier_prefix": "060",
            "dir_split_number": 10000,
            "items_per_file": 1000,
            "default_dir": "_",
            "rdf_output_in_chunks": True,
            "zip_output_rdf": False,
            "source": None,
            "use_doi_api_service": False,
            "silencer": [],
            "redis_host": "localhost",
            "redis_port": 6381,
            "redis_db": 5,
            "redis_cache_db": 2,
            "ts_upload_cache": self.cache_file,
            "ts_failed_queries": self.failed_file,
            "ts_stop_file": self.stop_file,
            "graphdb_connector_name": None,
            "blazegraph_full_text_search": False,
            "fuseki_full_text_search": False,
            "virtuoso_full_text_search": False,
            "provenance_endpoints": [],
            "cache_endpoint": None,
            "cache_update_endpoint": None,
            "normalize_titles": True,
        }
        config_path = os.path.join(output_dir, "config.yaml")
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        # Run the process
        run_meta_process(settings=config, meta_config_path=config_path)

        # Query the triplestore to verify:
        # 1. Only one OMID was generated for both rows
        # 2. The temporary identifier was not saved
        query = """
        SELECT DISTINCT ?br
        WHERE {
            ?br a <http://purl.org/spar/fabio/JournalArticle> .
        }
        """
        results = execute_sparql_query(SERVER, query)

        # Clean up
        shutil.rmtree(input_dir)
        shutil.rmtree(output_dir)

        # Should only be one article
        articles = [
            str(result["br"]["value"]) for result in results["results"]["bindings"]
        ]
        self.assertEqual(
            len(articles), 1, "Should only be one article after deduplication"
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
