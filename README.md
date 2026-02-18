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

## Table of contents

- [OpenCitations Meta Software](#opencitations-meta-software)
- [Meta production workflow](#meta-production-workflow)
  - [Preprocessing input data (optional)](#preprocessing-input-data-optional)
  - [Main processing](#main-processing)
  - [Verifying processing results](#verifying-processing-results)
  - [Manual upload to triplestore](#manual-upload-to-triplestore)
- [Virtuoso bulk loading (performance optimization)](#virtuoso-bulk-loading-performance-optimization)
- [Analysing the dataset](#analysing-the-dataset)
  - [General statistics (SPARQL)](#general-statistics-sparql)
  - [Venue statistics (CSV)](#venue-statistics-csv)
- [Finding and merging duplicates](#finding-and-merging-duplicates)
  - [Finding duplicate identifiers from files](#finding-duplicate-identifiers-from-files)
  - [Merging duplicate entities](#merging-duplicate-entities)
- [Running tests](#running-tests)
- [Creating releases](#creating-releases)

## Meta production workflow

The Meta production process involves several steps to process bibliographic metadata. An optional but recommended preprocessing step is available to optimize the input data before the main processing.

### Preprocessing input data (optional)

The [`preprocess_input.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/meta/preprocess_input.py) script helps filter and optimize CSV files before they are processed by the main Meta workflow. This preprocessing step is particularly useful for large datasets as it:

1. Removes duplicate entries across all input files
2. Optionally filters out entries that already exist in the database (using either Redis or SPARQL)
3. Splits large input files into smaller, more manageable chunks

To run the preprocessing script:

```console
# Basic usage: only deduplicate and split files (no storage checking)
uv run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR>

# With Redis storage checking
uv run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR> --storage-type redis

# With SPARQL storage checking
uv run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR> --storage-type sparql --sparql-endpoint <SPARQL_ENDPOINT_URL>

# Custom file size and Redis settings
uv run python -m oc_meta.run.meta.preprocess_input <INPUT_DIR> <OUTPUT_DIR> \
  --rows-per-file 5000 \
  --storage-type redis \
  --redis-host 192.168.1.100 \
  --redis-port 6380 \
  --redis-db 5
```

Parameters:
- `<INPUT_DIR>`: Directory containing the input CSV files to process
- `<OUTPUT_DIR>`: Directory where the filtered and optimized CSV files will be saved
- `--rows-per-file`: Number of rows per output file (default: 3000)
- `--storage-type`: Type of storage to check IDs against (`redis` or `sparql`). If not specified, ID checking is skipped
- `--redis-host`: Redis host (default: localhost)
- `--redis-port`: Redis port (default: 6379)
- `--redis-db`: Redis database number to use if storage type is Redis (default: 10)
- `--sparql-endpoint`: SPARQL endpoint URL (required if storage type is `sparql`)

The script will generate a detailed report showing:
- Total number of input rows processed
- Number of duplicate rows removed
- Number of rows with IDs that already exist in the database (if storage checking is enabled)
- Number of rows that passed the filtering and were written to output files

### Main processing

The main Meta processing is executed through the [`meta_process.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/meta_process.py) file, which orchestrates the entire data processing workflow:

```console
uv run python -m oc_meta.run.meta_process -c <CONFIG_PATH>
```

Parameters:
- `-c --config`: Path to the configuration YAML file.

#### What Meta process does

The Meta process performs the following key operations:

1. **Preparation**:
   - sets up the required directory structure
   - initializes connections to Redis and the triplestore
   - loads configuration settings

2. **Data curation**:
   - processes input CSV files containing bibliographic metadata
   - validates and normalizes the data
   - handles duplicate entries and invalid data

3. **RDF creation**:
   - converts the curated data into RDF format following the OpenCitations Data Model
   - generates entity identifiers and establishes relationships
   - creates provenance information for tracking data lineage

4. **Storage and triplestore upload**:
   - directly generates SPARQL queries for triplestore updates
   - loads RDF data directly into the configured triplestore via SPARQL endpoint
   - executes necessary SPARQL updates
   - ensures data is properly indexed for querying

#### Meta configuration

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

### Verifying processing results

After processing your data with the Meta workflow, you can verify that all identifiers were correctly processed and have associated data in the triplestore using the [`check_results.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/meta/check_results.py) script. This verification step helps identify potential issues such as missing OMIDs, missing provenance, or identifiers with multiple OMIDs.

#### Running the verification script

```console
uv run python -m oc_meta.run.meta.check_results <CONFIG_PATH> [--output <OUTPUT_FILE>]
```

Parameters:
- `<CONFIG_PATH>`: Path to the same meta_config.yaml file used for processing
- `--output`: Optional path to save the report to a file. If not specified, results are printed to console

#### What the script checks

The verification script performs the following checks:

1. **Identifier analysis**:
   - parses all identifiers from input CSV files (id, author, editor, publisher, venue columns)
   - queries the triplestore to find associated OMIDs for each identifier

2. **OMID verification**:
   - checks if identifiers have corresponding OMIDs in the triplestore
   - identifies identifiers without any OMID (potential processing failures)
   - detects identifiers with multiple OMIDs (potential disambiguation issues)

3. **Data graph verification** (when RDF file generation is enabled):
   - verifies that data graphs exist in the generated RDF files
   - reports missing data graphs for entities that should have been created

4. **Provenance verification**:
   - checks if provenance graphs exist in the generated RDF files
   - queries the provenance triplestore to verify provenance data
   - identifies OMIDs without associated provenance information

### Manual upload to triplestore

Occasionally, the automatic upload process during Meta execution might fail due to connection issues, timeout errors, or other problems. In such cases, you can use the [`on_triplestore.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/upload/on_triplestore.py) script to manually upload the generated SPARQL files to the triplestore.

#### Running the manual upload script

```console
uv run python -m oc_meta.run.upload.on_triplestore <ENDPOINT_URL> <SPARQL_FOLDER> [OPTIONS]
```

Parameters:
- `<ENDPOINT_URL>`: The SPARQL endpoint URL of the triplestore
- `<SPARQL_FOLDER>`: Path to the folder containing SPARQL update query files (.sparql)

Options:
- `--batch_size`: Number of quadruples to include in each batch (default: 10)
- `--cache_file`: Path to the cache file tracking processed files (default: "ts_upload_cache.json")
- `--failed_file`: Path to the file recording failed queries (default: "failed_queries.txt")
- `--stop_file`: Path to the stop file used to gracefully interrupt the process (default: ".stop_upload")

## Virtuoso bulk loading (performance optimization)

For large-scale data ingestion into Virtuoso triplestores, the Meta process supports an optional bulk loading mode that significantly improves performance compared to standard SPARQL INSERT queries. This mode leverages Virtuoso's native `ld_dir`/`rdf_loader_run` mechanism for fast data loading.

### Prerequisites

Before enabling bulk loading, ensure:

1. **Docker setup**: Both data and provenance Virtuoso instances must run in Docker containers
2. **Volume mapping**: Host directories for data and provenance must be mounted as volumes into their respective containers
3. **DirsAllowed configuration**: The bulk load directory must be listed in `DirsAllowed` parameter in `virtuoso.ini`

Example Docker volume mapping:
```bash
# Data container
docker run -d \
  --name virtuoso-data \
  -v /srv/meta/data_bulk:/database/bulk_load \
  -p 8890:8890 \
  -p 1111:1111 \
  openlink/virtuoso-opensource-7:latest

# Provenance container
docker run -d \
  --name virtuoso-prov \
  -v /srv/meta/prov_bulk:/database/bulk_load \
  -p 8891:8890 \
  -p 1112:1111 \
  openlink/virtuoso-opensource-7:latest
```

Example `virtuoso.ini` configuration:
```ini
[Parameters]
DirsAllowed = ., /database, /database/bulk_load
```

### Configuration

Edit your `meta_config.yaml` to enable bulk loading:

```yaml
virtuoso_bulk_load:
  # Set to true to enable bulk loading mode
  enabled: true

  # Docker container name for the data triplestore
  data_container: "virtuoso-data"

  # Docker container name for the provenance triplestore
  prov_container: "virtuoso-prov"

  # Host directory mounted as volume in the data container
  # Files will be generated directly here (visible to both host and container)
  # This directory must be mounted in the data container as bulk_load_dir
  data_mount_dir: "/srv/meta/data_bulk"

  # Host directory mounted as volume in the provenance container
  # Files will be generated directly here (visible to both host and container)
  # This directory must be mounted in the prov container as bulk_load_dir
  prov_mount_dir: "/srv/meta/prov_bulk"

  # Path INSIDE the container where bulk load files are accessed
  # This directory must be:
  # 1. Mapped as a volume from the host to the container
  # 2. Listed in the DirsAllowed parameter in virtuoso.ini
  bulk_load_dir: "/database/bulk_load"
```

### Behavior

- **Success**: All files are loaded successfully, files remain in the mounted directories
- **Failure**: If any file fails to load, the process crashes immediately with a detailed error message
- Files remain in the mounted directories for manual inspection or retry

## Analysing the dataset

To gather statistics on the dataset, you can use the provided analysis tools.

### General statistics (SPARQL)

For most statistics, such as counting bibliographic resources (`--br`) or agent roles (`--ar`), the `sparql_analyser.py` script is the recommended tool. It queries the SPARQL endpoint directly.

```console
uv run python -m oc_meta.run.analyser.sparql_analyser <SPARQL_ENDPOINT_URL> --br --ar
```

### Venue statistics (CSV)

**Warning:** using the SPARQL analyser for venue statistics (`--venues`) against an OpenLink Virtuoso endpoint is **not recommended**. The complex query required for venue disambiguation can exhaust Virtuoso's RAM, causing it to return partial (and thus incorrect) results. As this query is not yet optimized for Virtuoso, this count will be wrong.

For reliable venue statistics, use the `meta_analyser.py` script to process the raw CSV output files directly.

To count the disambiguated venues, run the following command:

```console
uv run python -m oc_meta.run.analyser.meta_analyser -c <PATH_TO_CSV_DUMP> -w venues
```
The script will save the result in a file named `venues_count.txt`.

## Finding and merging duplicates

The OpenCitations Meta Software provides plugins to identify and merge duplicate entities in the dataset.

### Finding duplicate identifiers from files

The [`duplicated_ids_from_files.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/find/duplicated_ids_from_files.py) script scans RDF files stored in ZIP archives to find duplicate identifiers.

#### Running the script

```console
uv run python -m oc_meta.run.find.duplicated_ids_from_files <FOLDER_PATH> <CSV_PATH> [OPTIONS]
```

Parameters:
- `<FOLDER_PATH>`: Path to the folder containing the `id` subfolder with ZIP files
- `<CSV_PATH>`: Path to the output CSV file where duplicates will be saved

Options:
- `--chunk-size`: Number of ZIP files to process per chunk (default: 5000). Decrease this value if you encounter memory issues
- `--temp-dir`: Directory for temporary files (default: system temp directory). The script automatically cleans up temporary files after completion

### Grouping entities for efficient merging

Before merging duplicates, it's recommended to group related entities using the [`group_entities.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/merge/group_entities.py) script. This preprocessing step analyzes the CSV files containing merge instructions and groups interconnected entities together, enabling efficient multiprocessing during the merge phase.

#### Why group entities?

The grouping script solves two important problems:

1. **RDF relationship consistency**: entities to be merged may have relationships with other entities in the dataset. When processing merges in parallel, interconnected entities must be handled in the same process to maintain consistency.

2. **File-level conflicts**: entities sharing the same RDF file (e.g., `br/060/10000/1000.zip`) should be grouped together to minimize file lock contention during parallel processing.

The script performs:

1. **Identifies relationships**: queries the SPARQL endpoint to find all entities related to those being merged
2. **Groups by RDF connections**: uses a Union-Find algorithm to group entities that share relationships
3. **Groups by file range**: additionally groups entities that share the same RDF file path (considering supplier prefix and number ranges)
4. **Optimizes for parallelization**: combines small independent groups while keeping large interconnected groups separate
5. **Creates balanced workloads**: targets a minimum group size to ensure efficient parallel processing

While `oc_ocdm` Storer uses FileLock for safety, this grouping reduces lock contention by ensuring workers process non-overlapping file ranges.

#### Running the grouping script

```console
uv run python -m oc_meta.run.merge.group_entities <CSV_FILE> <OUTPUT_DIR> <META_CONFIG> [--min_group_size SIZE]
```

Parameters:
- `<CSV_FILE>`: Path to the CSV file containing merge instructions
- `<OUTPUT_DIR>`: Directory where grouped CSV files will be saved
- `<META_CONFIG>`: Path to the Meta configuration YAML file (reads `triplestore_url`, `dir_split_number`, `items_per_file`, `zip_output_rdf`)
- `--min_group_size`: Minimum target size for groups (default: 50)

### Merging duplicate entities

Once you have identified duplicates (and optionally grouped them), you can merge them using the [`entities.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/merge/entities.py) script. This script processes the CSV files generated by the duplicate-finding scripts and performs the actual merge operations.

#### Running the merge script

```console
uv run python -m oc_meta.run.merge.entities <CSV_FOLDER> <META_CONFIG> <RESP_AGENT> [OPTIONS]
```

Parameters:
- `<CSV_FOLDER>`: Path to the folder containing CSV files with merge instructions (use the output from `group_entities.py` for optimal parallel processing)
- `<META_CONFIG>`: Path to the Meta configuration YAML file
- `<RESP_AGENT>`: Responsible agent URI for provenance

Options:
- `--entity_types`: Types of entities to merge (default: `ra`, `br`, `id`)
- `--stop_file`: Path to the stop file for graceful interruption (default: `stop.out`)
- `--workers`: Number of parallel workers (default: 4)

## Running tests

The test suite is automatically executed via GitHub Actions upon pushes and pull requests. The workflow is defined in [`.github/workflows/run_tests.yml`](https://github.com/opencitations/oc_meta/blob/master/.github/workflows/run_tests.yml) and handles the setup of necessary services (Redis, Virtuoso) using Docker.

To run the test suite locally, follow these steps:

1. **Install dependencies:**
   Ensure you have [uv](https://docs.astral.sh/uv/) and [Docker](https://www.docker.com/) installed. Then, install project dependencies:
   ```console
   uv sync
   ```

2. **Start services:**
   Use the provided script to start the required Redis and Virtuoso Docker containers:
   ```console
   chmod +x test/start-test-databases.sh
   ./test/start-test-databases.sh
   ```
   Wait for the script to confirm that the services are ready.
   (The Virtuoso SPARQL endpoint will be available at http://localhost:8805/sparql and ISQL on port 1105.
   Redis will be available at localhost:6379, using database 0 for some tests and database 5 for most test cases including counter handling and caching).

3. **Execute tests:**
   Run the tests using the following command, which also generates a coverage report:
   ```console
   uv run coverage run --rcfile=test/coverage/.coveragerc
   ```
   To view the coverage report in the console:
   ```console
   uv run coverage report
   ```
   To generate an HTML coverage report (saved in the `htmlcov/` directory):
   ```console
   uv run coverage html -d htmlcov
   ```

4. **Stop services:**
   Once finished, stop the Docker containers:
   ```console
   chmod +x test/stop-test-databases.sh
   ./test/stop-test-databases.sh
   ```

## Creating releases

The project uses semantic-release for versioning and publishing releases to PyPI. To create a new release:

1. **Commit changes:**
   Make your changes and commit them with a message that includes `[release]` to trigger the release workflow.
   For details on how to structure semantic commit messages, see the [Semantic Commits Guide](SEMANTIC_COMMITS.md).

2. **Push to master:**
   Push your changes to the master branch. This will trigger the test workflow first.

3. **Automatic release process:**
   If tests pass, the release workflow will:
   - create a new semantic version based on commit messages
   - generate a changelog
   - create a GitHub release
   - build and publish the package to PyPI

The release workflow is configured in [`.github/workflows/release.yml`](https://github.com/opencitations/oc_meta/blob/master/.github/workflows/release.yml) and is triggered automatically when:
- The commit message contains `[release]`
- The tests workflow completes successfully
- The changes are on the master branch

## How to cite

If you have used OpenCitations Meta in your research, please cite the following paper:

Arcangelo Massari, Fabio Mariani, Ivan Heibi, Silvio Peroni, David Shotton; OpenCitations Meta. *Quantitative Science Studies* 2024; 5 (1): 50–75. doi: [https://doi.org/10.1162/qss_a_00292](https://doi.org/10.1162/qss_a_00292)