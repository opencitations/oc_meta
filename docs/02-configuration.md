<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Configuration

Meta process requires a YAML configuration file. Here's a complete reference with all available options.

## Complete example

```yaml
# Triplestore endpoints
triplestore_url: "http://127.0.0.1:8890/sparql"
provenance_triplestore_url: "http://127.0.0.1:8891/sparql"

# RDF settings
base_iri: "https://w3id.org/oc/meta/"

# Provenance
resp_agent: "https://w3id.org/oc/meta/prov/pa/1"
source: "https://api.crossref.org/"

# File organization
supplier_prefix: "060"
dir_split_number: 10000
items_per_file: 1000
default_dir: "_"

# Input/output
input_csv_dir: "/path/to/input"
output_rdf_dir: "/path/to/output"
rdf_files_only: false
zip_output_rdf: true

# Processing options
silencer: ["author", "editor", "publisher"]
normalize_titles: true
```

## Option reference

### Triplestore settings

| Option | Type | Description |
|--------|------|-------------|
| `triplestore_url` | string | SPARQL endpoint for data storage |
| `provenance_triplestore_url` | string | SPARQL endpoint for provenance storage |

### RDF settings

| Option | Type | Description |
|--------|------|-------------|
| `base_iri` | string | Base IRI for generated entity URIs |

### Provenance

| Option | Type | Description |
|--------|------|-------------|
| `resp_agent` | string | URI of the responsible agent for provenance |
| `source` | string | Primary source URI for provenance tracking |

### Counters

OMID counters are stored on the filesystem in an `info_dir` directory (derived from `base_output_dir/info_dir/<supplier_prefix>/`). Each entity type (br, ra, id, ar, re) has its own counter file that tracks the last assigned number, producing URIs like `https://w3id.org/oc/meta/br/060/1`, `br/060/2`, etc. Counter files include `info_file_br.txt`, `info_file_ar.txt`, `prov_file_br.txt`, and similar. Managed by [`oc_ocdm.FilesystemCounterHandler`](https://github.com/opencitations/oc_ocdm/blob/master/oc_ocdm/counter_handler/filesystem_counter_handler.py).

### Redis cache (optional)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `redis_host` | string | localhost | Redis server hostname |
| `redis_port` | int | 6379 | Redis server port |
| `redis_cache_db` | int | 1 | Database for upload cache |

Redis is only needed when `rdf_files_only: false` and SPARQL upload caching is desired. The **upload cache** (`redis_cache_db`) tracks which SPARQL files have already been uploaded to the triplestore. When uploading is interrupted and resumed, Meta skips files already in the cache. Managed by [`piccione.CacheManager`](https://github.com/opencitations/piccione/blob/main/src/piccione/upload/cache_manager.py).

### File organization

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `supplier_prefix` | string | - | Prefix for OMID URIs (e.g., "060") |
| `dir_split_number` | int | 10000 | Entities per subdirectory |
| `items_per_file` | int | 1000 | Entities per RDF file |
| `default_dir` | string | "_" | Directory name when no prefix exists |

The **supplier prefix** identifies which OpenCitations dataset an entity belongs to. The prefix appears in entity URIs: `https://w3id.org/oc/meta/br/060/1` where `060` identifies Meta as the source. Other prefixes used by OpenCitations include `010` (Wikidata), `020` (Crossref), and `040` (Dryad). See the [complete supplier prefix table](https://oci.opencitations.net/) for all available prefixes.

> **Note**: Some existing entities have prefixes like `0610`, `0620`, etc. (pattern `06[1-9]0`). This was used in the past for multiprocessing, where different processes worked on separate directories. This approach is now deprecated due to stability issues with Virtuoso, which does not handle parallel queries well.

These options control how RDF files are organized on disk:

```
output_rdf_dir/
└── br/                          # Entity type (br, ra, id, ar, re)
    └── 060/                     # Supplier prefix (or default_dir if none)
        ├── 10000/               # dir_split_number: entities 1-10000
        │   ├── 1000.json        # items_per_file: entities 1-1000
        │   ├── 2000.json        # entities 1001-2000
        │   └── ...
        └── 20000/               # entities 10001-20000
            ├── 11000.json
            └── ...
```

- **`dir_split_number`**: Creates subdirectories to avoid having too many files in one folder. With `dir_split_number: 10000`, entities 1-10000 go in `10000/`, entities 10001-20000 go in `20000/`, etc.

- **`items_per_file`**: Controls how many entities are stored per JSON file. With `items_per_file: 1000`, entities 1-1000 go in `1000.json`, entities 1001-2000 go in `2000.json`, etc.

- **`default_dir`**: When entities have no supplier prefix (e.g., during migration from older formats), this directory name is used instead. Typically set to `_`.

### Input/output

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `input_csv_dir` | string | - | Directory containing input CSV files |
| `output_rdf_dir` | string | - | Directory for RDF output |
| `rdf_files_only` | bool | false | Generate only RDF files without updating triplestores |
| `zip_output_rdf` | bool | true | Compress RDF files to ZIP archives |

### Processing options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `silencer` | list | [] | Fields to skip during updates |
| `normalize_titles` | bool | true | Normalize title casing |

The `silencer` option accepts a list of field names: `author`, `editor`, and `publisher`. Meta always works in addition mode (it never overwrites existing data). The silencer prevents adding new elements to an existing sequence. For example, if `silencer: ["author"]` is set and a resource already has authors, new authors from the CSV will not be added to the existing author chain.

## Generated files

When you run Meta with a config file, it automatically generates `time_agnostic_library_config.json` in the same directory. This file is used by the provenance tracking system and shouldn't be edited manually.
