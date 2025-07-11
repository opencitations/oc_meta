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

- [OpenCitations Meta Software](#opencitations-meta-software)
- [Meta Production Workflow](#meta-production-workflow)
  - [Preprocessing Input Data (Optional)](#preprocessing-input-data-optional)
  - [Main Processing](#main-processing)
  - [Manual Upload to Triplestore](#manual-upload-to-triplestore)
- [Analysing the Dataset](#analysing-the-dataset)
  - [General Statistics (SPARQL)](#general-statistics-sparql)
  - [Venue Statistics (CSV)](#venue-statistics-csv)
- [Running Tests](#running-tests)
- [Creating Releases](#creating-releases)

## Meta Production Workflow

The Meta production process involves several steps to process bibliographic metadata. An optional but recommended preprocessing step is available to optimize the input data before the main processing.

### Preprocessing Input Data (Optional)

The [`preprocess_input.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/meta/preprocess_input.py) script helps filter and optimize CSV files before they are processed by the main Meta workflow. This preprocessing step is particularly useful for large datasets as it:

1. Removes duplicate entries across all input files
2. Filters out entries that already exist in the database (using either Redis or SPARQL)
3. Splits large input files into smaller, more manageable chunks

To run the preprocessing script:

```console
# Using Redis (default)
poetry run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR> [--redis-db <DB_NUMBER>]

# Using SPARQL endpoint
poetry run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR> --storage-type sparql --sparql-endpoint <SPARQL_ENDPOINT_URL>
```

Parameters:
- `<INPUT_DIR>`: Directory containing the input CSV files to process
- `<OUTPUT_DIR>`: Directory where the filtered and optimized CSV files will be saved
- `--storage-type`: Type of storage to check IDs against (`redis` or `sparql`, default: `redis`)
- `--redis-db`: Redis database number to use if storage type is Redis (default: 10)
- `--sparql-endpoint`: SPARQL endpoint URL if storage type is set to `sparql`

The script will generate a detailed report showing:
- Total number of input rows processed
- Number of duplicate rows removed
- Number of rows with IDs that already exist in the database
- Number of rows that passed the filtering and were written to output files

#### Choosing the Right Storage Backend

- **Redis**: Faster option for ID checking with lower memory overhead. Ideal for rapid preprocessing of large datasets.
- **SPARQL**: Directly checks against the triplestore where the data will be stored. Useful when you don't have a Redis cache of existing IDs.

After preprocessing, you can use the optimized files in the output directory as input for the main Meta process.

### Main Processing

The main Meta processing is executed through the [`meta_process.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/meta_process.py) file, which orchestrates the entire data processing workflow:

```console
poetry run python -m oc_meta.run.meta_process -c <CONFIG_PATH>
```

Parameters:
- `-c --config`: Path to the configuration YAML file.

#### What Meta Process Does

The Meta process performs the following key operations:

1. **Preparation**:
   - Sets up the required directory structure
   - Initializes connections to Redis and the triplestore
   - Loads configuration settings

2. **Data Curation**:
   - Processes input CSV files containing bibliographic metadata
   - Validates and normalizes the data
   - Handles duplicate entries and invalid data

3. **RDF Creation**:
   - Converts the curated data into RDF format following the OpenCitations Data Model
   - Generates entity identifiers and establishes relationships
   - Creates provenance information for tracking data lineage

4. **Storage and Triplestore Upload**:
   - Directly generates SPARQL queries for triplestore updates
   - Loads RDF data directly into the configured triplestore via SPARQL endpoint
   - Executes necessary SPARQL updates
   - Ensures data is properly indexed for querying

#### Meta Configuration

The Meta process requires a YAML configuration file that specifies various settings for the processing workflow. Here's an example of the configuration structure with explanations:

```yaml
# Endpoint URLs for data and provenance storage
triplestore_url: "http://127.0.0.1:8805/sparql"
provenance_triplestore_url: "http://127.0.0.1:8806/sparql"

# Base IRI for RDF entities
base_iri: "https://w3id.org/oc/meta/"

# JSON-LD context file
context_path: "https://w3id.org/oc/corpus/context.json"

# Responsible agent for provenance
resp_agent: "https://w3id.org/oc/meta/prov/pa/1"

# Source information for provenance
source: "https://api.crossref.org/"

# Redis configuration for counter handling
redis_host: "localhost"
redis_port: 6379
redis_db: 0
redis_cache_db: 1

# Processing settings
supplier_prefix: "060"
workers_number: 16
dir_split_number: 10000
items_per_file: 1000
default_dir: "_"

# Output control
generate_rdf_files: false
zip_output_rdf: true
output_rdf_dir: "/path/to/output"

# Data processing options
silencer: ["author", "editor", "publisher"]
normalize_titles: true
use_doi_api_service: false
```

### Manual Upload to Triplestore

Occasionally, the automatic upload process during Meta execution might fail due to connection issues, timeout errors, or other problems. In such cases, you can use the [`on_triplestore.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/upload/on_triplestore.py) script to manually upload the generated SPARQL files to the triplestore.

#### Running the Manual Upload Script

```console
poetry run python -m oc_meta.run.upload.on_triplestore <ENDPOINT_URL> <SPARQL_FOLDER> [OPTIONS]
```

Parameters:
- `<ENDPOINT_URL>`: The SPARQL endpoint URL of the triplestore
- `<SPARQL_FOLDER>`: Path to the folder containing SPARQL update query files (.sparql)

Options:
- `--batch_size`: Number of quadruples to include in each batch (default: 10)
- `--cache_file`: Path to the cache file tracking processed files (default: "ts_upload_cache.json")
- `--failed_file`: Path to the file recording failed queries (default: "failed_queries.txt")
- `--stop_file`: Path to the stop file used to gracefully interrupt the process (default: ".stop_upload")

## Analysing the Dataset

To gather statistics on the dataset, you can use the provided analysis tools.

### General Statistics (SPARQL)

For most statistics, such as counting bibliographic resources (`--br`) or agent roles (`--ar`), the `sparql_analyser.py` script is the recommended tool. It queries the SPARQL endpoint directly.

```console
poetry run python -m oc_meta.run.analyser.sparql_analyser <SPARQL_ENDPOINT_URL> --br --ar
```

### Venue Statistics (CSV)

**Warning:** Using the SPARQL analyser for venue statistics (`--venues`) against an OpenLink Virtuoso endpoint is **not recommended**. The complex query required for venue disambiguation can exhaust Virtuoso's RAM, causing it to return partial (and thus incorrect) results. As this query is not yet optimized for Virtuoso, this count will be wrong.

For reliable venue statistics, use the `meta_analyser.py` script to process the raw CSV output files directly.

To count the disambiguated venues, run the following command:

```console
poetry run python -m oc_meta.run.analyser.meta_analyser -c <PATH_TO_CSV_DUMP> -w venues
```
The script will save the result in a file named `venues_count.txt`.

## Running Tests

The test suite is automatically executed via GitHub Actions upon pushes and pull requests. The workflow is defined in [`.github/workflows/run_tests.yml`](https://github.com/opencitations/oc_meta/blob/master/.github/workflows/run_tests.yml) and handles the setup of necessary services (Redis, Virtuoso) using Docker.

To run the test suite locally, follow these steps:

1. **Install Dependencies:** 
   Ensure you have [Poetry](https://python-poetry.org/) and [Docker](https://www.docker.com/) installed. Then, install project dependencies:
   ```console
   poetry install
   ```

2. **Start Services:** 
   Use the provided script to start the required Redis and Virtuoso Docker containers:
   ```console
   chmod +x test/start-test-databases.sh
   ./test/start-test-databases.sh
   ```
   Wait for the script to confirm that the services are ready.
   (The Virtuoso SPARQL endpoint will be available at http://localhost:8805/sparql and ISQL on port 1105.
   Redis will be available at localhost:6379, using database 0 for some tests and database 5 for most test cases including counter handling and caching).

3. **Execute Tests:** 
   Run the tests using the following command, which also generates a coverage report:
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

4. **Stop Services:** 
   Once finished, stop the Docker containers:
   ```console
   chmod +x test/stop-test-databases.sh
   ./test/stop-test-databases.sh
   ```

## Creating Releases

The project uses semantic-release for versioning and publishing releases to PyPI. To create a new release:

1. **Commit Changes:**
   Make your changes and commit them with a message that includes `[release]` to trigger the release workflow.
   For details on how to structure semantic commit messages, see the [Semantic Commits Guide](SEMANTIC_COMMITS.md).

2. **Push to Master:**
   Push your changes to the master branch. This will trigger the test workflow first.

3. **Automatic Release Process:**
   If tests pass, the release workflow will:
   - Create a new semantic version based on commit messages
   - Generate a changelog
   - Create a GitHub release
   - Build and publish the package to PyPI

The release workflow is configured in [`.github/workflows/release.yml`](https://github.com/opencitations/oc_meta/blob/master/.github/workflows/release.yml) and is triggered automatically when:
- The commit message contains `[release]`
- The tests workflow completes successfully
- The changes are on the master branch

## How to Cite

If you have used OpenCitations Meta in your research, please cite the following paper:

Arcangelo Massari, Fabio Mariani, Ivan Heibi, Silvio Peroni, David Shotton; OpenCitations Meta. *Quantitative Science Studies* 2024; 5 (1): 50–75. doi: [https://doi.org/10.1162/qss_a_00292](https://doi.org/10.1162/qss_a_00292)
