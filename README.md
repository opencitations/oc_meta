[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
[![Run tests](https://github.com/opencitations/oc_meta/actions/workflows/run_tests.yml/badge.svg)](https://github.com/opencitations/oc_meta/actions/workflows/run_tests.yml)
[![Coverage](https://byob.yarr.is/arcangelo7/badges/opencitations-oc_meta_coverage)](https://opencitations.github.io/oc_meta/)
![PyPI](https://img.shields.io/pypi/pyversions/oc_meta?logo=python&logoColor=white&label=python&color=blue)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/opencitations/oc_meta)

# OpenCitations Meta Software

OpenCitations Meta contains bibliographic metadata associated with the documents involved in the citations stored in the [OpenCitations](https://opencitations.net/) infrastructure. The OpenCitations Meta Software performs several key functions:

1. Data curation of provided CSV files
2. Generation of RDF files compliant with the [OpenCitations Data Model](http://opencitations.net/model)
3. Provenance tracking and management
4. Data validation and fixing utilities

An example of a raw CSV input file can be found in [`example.csv`](https://github.com/opencitations/meta/blob/master/oc_meta/example.csv).

## Table of Contents

- [Meta](#meta)
- [Plugins](#plugins)
  - [Get a DOI-ORCID index](#get-a-doi-orcid-index)
  - [Get a Crossref member-name-prefix index](#get-a-crossref-member-name-prefix-index)
  - [Generate CSVs from triplestore](#generate-csvs-from-triplestore)
  - [Prepare the multiprocess](#prepare-the-multiprocess)
- [Running Tests](#running-tests)
- [Utilities](#utilities)
  - [Provenance Management](#provenance-management)
  - [Data Validation & Analysis](#data-validation--analysis)
    - [Check Redis Info](#check-redis-info)
    - [Check Processing Results](#check-processing-results)
    - [Generate Info Directory](#generate-info-directory)

## Meta

The Meta process is launched through the [`meta_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/meta_process.py) file via the prompt command:

```console
    python -m oc_meta.run.meta_process -c <PATH>
```

Where:

- -c --config : path to the configuration file.

The configuration file is a YAML file with the following keys (an example can be found in [`config/meta_config.yaml`](https://github.com/opencitations/meta/blob/master/config/meta_config.yaml)).

| Setting                     | Mandatory | Description                                                                                                   |
| --------------------------- | --------- | ------------------------------------------------------------------------------------------------------------- |
| triplestore_url             | ✓         | Endpoint URL to load the output RDF                                                                           |
| input_csv_dir               | ✓         | Directory where raw CSV files are stored                                                                      |
| base_output_dir             | ✓         | The path to the base directory to save all output files                                                       |
| resp_agent                  | ✓         | A URI string representing the provenance agent which is considered responsible for the RDF graph manipulation |
| base_iri                    | ☓         | The base URI of entities on Meta. This setting can be safely left as is                                       |
| context_path                | ☓         | URL where the namespaces and prefixes used in the OpenCitations Data Model are defined                        |
| dir_split_number            | ☓         | Number of files per folder. Must be multiple of items_per_file                                                |
| items_per_file              | ☓         | Number of items per file                                                                                      |
| supplier_prefix             | ☓         | A prefix for the sequential number in entities' URIs                                                          |
| rdf_output_in_chunks        | ☓         | If True, save all the graphset and provset in one file. If False, use the OpenCitations folder hierarchy      |
| zip_output_rdf              | ☓         | If True, output will be zipped                                                                                |
| source                      | ☓         | Data source URL                                                                                               |
| use_doi_api_service         | ☓         | If True, use the DOI API service to check if DOIs are valid                                                   |
| workers_number              | ☓         | Number of cores to use for processing                                                                         |
| blazegraph_full_text_search | ☓         | Enable Blazegraph text index for faster queries                                                               |
| fuseki_full_text_search     | ☓         | Enable Fuseki text index for faster queries                                                                   |
| virtuoso_full_text_search   | ☓         | Enable Virtuoso text index for faster queries                                                                 |
| graphdb_connector_name      | ☓         | Name of the Lucene connector for GraphDB text search                                                          |
| cache_endpoint              | ☓         | Provenance triplestore URL for caching queries                                                                |
| cache_update_endpoint       | ☓         | Write endpoint URL for cache triplestore                                                                      |
| redis_host                  | ☓         | Redis host address (default: localhost)                                                                       |
| redis_port                  | ☓         | Redis port number (default: 6379)                                                                             |
| redis_db                    | ☓         | Redis database number (default: 0)                                                                            |

## Plugins

### Get a DOI-ORCID index

[`orcid_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/orcid_process.py) generates an index between DOIs and the author's ORCIDs using the ORCID Summaries Dump (e.g. [ORCID_2019_summaries](https://orcid.figshare.com/articles/ORCID_Public_Data_File_2019/9988322)). The output is a folder containing CSV files with two columns, 'id' and 'value', where 'id' is a DOI or None, and 'value' is an ORCID. This process can be run via the following commad:

```console
    python -m oc_meta.run.orcid_process -s <PATH> -out <PATH> -t <INTEGER> -lm -v
```

Where:

- -s --summaries: ORCID summaries dump path, subfolder will be considered too.
- -out --output: a directory where the output CSV files will be store, that is, the ORCID-DOI index.
- -t --threshold: threshold after which to update the output, not mandatory. A new file will be generated each time.
- -lm --low-memory: specify this argument if the available RAM is insufficient to accomplish the task. Warning: the processing time will increase.
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

### Get a Crossref member-name-prefix index

[`crossref_publishers_extractor.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/crossref_publishers_extractor.py) generates an index between Crossref members' ids, names and DOI prefixes. The output is a CSV file with three columns, 'id', 'name', and 'prefix'.
This process can be run via the following command:

```console
    python -m oc_meta.run.crossref_publishers_extractor -o <PATH>
```

Where:

- -o --output: The output CSV file where to store relevant information.

### Generate CSVs from triplestore

This plugin generates CSVs from the Meta triplestore. You can run the [`csv_generator.py`](https://github.com/opencitations/meta/blob/master/oc_meta/plugins/csv_generator/csv_generator.py) script in the following way:

```console
    python -m oc_meta.run.csv_generator -c <PATH>
```

Where:

- -c --config : path to the configuration file.
  The configuration file is a YAML file with the following keys (an example can be found in [`config/csv_generator_config.yaml`](https://github.com/opencitations/meta/blob/master/oc_meta/config/csv_generator_config.yaml)).

| Setting          | Mandatory | Description                                                                                                                            |
| ---------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| triplestore_url  | ✓         | URL of the endpoint where the data are located                                                                                         |
| output_csv_dir   | ✓         | Directory where the output CSV files will be stored                                                                                    |
| info_dir         | ✓         | The folder where the counters of the various types of entities are stored.                                                             |
| base_iri         | ☓         | The base IRI of entities on the triplestore. This setting can be safely left as is                                                     |
| supplier_prefix  | ☓         | A prefix for the sequential number in entities' URIs. This setting can be safely left as is                                            |
| dir_split_number | ☓         | Number of files per folder. dir_split_number's value must be multiple of items_per_file's value. This setting can be safely left as is |
| items_per_file   | ☓         | Number of items per file. This setting can be safely left as is                                                                        |
| verbose          | ☓         | Show a loading bar, elapsed time and estimated time. This setting can be safely left as is                                             |

### Prepare the multiprocess

Before running Meta in multiprocess, it is necessary to prepare the input files. In particular, the CSV files must be divided by publisher, while venues and authors having an identifier must be loaded on the triplestore, in order not to generate duplicates during the multiprocess. These operations can be done by simply running the following script:

```console
    python -m oc_meta.run.prepare_multiprocess -c <PATH>
```

Where:

- -c --config : Path to the same configuration file you want to use for Meta.

Afterwards, launch Meta in multi-process by specifying the same configuration file. All the required modifications are done automatically.

## Running Tests

The test suite is automatically executed via GitHub Actions upon pushes and pull requests. The workflow handles the setup of necessary services (Redis, Virtuoso) using Docker.

To run the test suite locally, follow these steps:

1.  **Install Dependencies:** Ensure you have [Poetry](https://python-poetry.org/) and [Docker](https://www.docker.com/) installed. Then, install project dependencies:
    ```console
    poetry install
    ```
2.  **Start Services:** Use the provided script to start the required Redis and Virtuoso Docker containers:
    ```console
    chmod +x test/start-test-databases.sh
    ./test/start-test-databases.sh
    ```
    Wait for the script to confirm that the services are ready.
    (The Virtuoso SPARQL endpoint will be available at http://localhost:8805/sparql and ISQL on port 1105).
3.  **Execute Tests:** Run the tests using the following command, which also generates a coverage report:
    ```console
    poetry run coverage run --rcfile=test/coverage/.coveragerc
    ```
    To view the coverage report in the console:
    ```console
    poetry run coverage report
    ```
    To generate an HTML coverage report (saved in the `htmlcov/` directory):
    ```console
    poetry run coverage html -d htmlcov
    ```
4.  **Stop Services:** Once finished, stop the Docker containers:
    ```console
    chmod +x test/stop-test-databases.sh
    ./test/stop-test-databases.sh
    ```

## Utilities

### Provenance Management

```console
python -m oc_meta.run.fixer.prov.fix <input_dir> [--processes <num>] [--log-dir <path>]
```

Parameters:

- input_dir: Directory containing provenance files
- --processes: Number of parallel processes (default: CPU count)
- --log-dir: Directory for log files (default: logs)

### Data Validation & Analysis

#### Check Redis Info

```console
python -m oc_meta.run.check.info_dir <directory> [--redis-host <host>] [--redis-port <port>] [--redis-db <db>]
```

Parameters:

- directory: Directory to explore
- --redis-host: Redis host (default: localhost)
- --redis-port: Redis port (default: 6379)
- --redis-db: Redis database number (default: 6)

#### Check Processing Results

```console
python -m oc_meta.run.meta.check_results <directory> --root <path> --endpoint <url> [--show-missing]
```

Parameters:

- directory: Directory containing input CSV files
- --root: Root directory containing JSON-LD ZIP files
- --endpoint: SPARQL endpoint URL
- --show-missing: Show details of identifiers without associated OMIDs

#### Generate Info Directory

```console
python -m oc_meta.run.gen_info_dir <directory> [--redis-host <host>] [--redis-port <port>] [--redis-db <db>]
```

Parameters:

- directory: Directory to explore
- --redis-host: Redis host (default: localhost)
- --redis-port: Redis port (default: 6379)
- --redis-db: Redis database number (default: 6)
