<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Merge entities

The merge script processes CSV files with merge instructions and consolidates duplicate entities.

## Usage

```bash
uv run python -m oc_meta.run.merge.entities <CSV_FOLDER> <META_CONFIG> <RESP_AGENT> [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `CSV_FOLDER` | Folder with merge instruction CSVs |
| `META_CONFIG` | Path to Meta config file |
| `RESP_AGENT` | Responsible agent URI for provenance |

| Option | Default | Description |
|--------|---------|-------------|
| `--entity_types` | ra br id | Entity types to merge (space-separated) |
| `--stop_file` | stop.out | File to trigger graceful stop |

## Examples

Basic merge:

```bash
uv run python -m oc_meta.run.merge.entities \
  merges/ \
  meta_config.yaml \
  https://w3id.org/oc/meta/prov/pa/1
```

Merge only bibliographic resources:

```bash
uv run python -m oc_meta.run.merge.entities \
  merges/ \
  meta_config.yaml \
  https://w3id.org/oc/meta/prov/pa/1 \
  --entity_types br
```

## CSV input format

Each CSV file should have:

```text
surviving_entity,merged_entities
https://w3id.org/oc/meta/br/0601,https://w3id.org/oc/meta/br/0602; https://w3id.org/oc/meta/br/0603
```

`merged_entities` accepts semicolon-separated values with or without spaces around the semicolon.

Use output from [find duplicates](12-find-duplicates.md).

## What the merge does

For each row, the script:

1. **Loads the merge closure** from the triplestore: the entities named in the row, their `frbr:partOf` container chain (issue, volume, journal), and every entity that refers to any of these. Loading the entities that refer to a container that may be deleted is what keeps the merge from leaving dangling references.
2. **Copies identifiers** from merged entities to the surviving entity
3. **Fills metadata gaps** (title, date, etc.) from merged entities
4. **Merges matching containers**: an issue, volume or journal of a merged entity is merged into the survivor's container of the same type only when the two are equivalent (they share an identifier, or have the same sequence number, or the same title). Containers that disagree are kept distinct, and a level is merged only if its parent level was merged. The publisher role follows the same rule, keyed on the responsible agent behind it.
5. **Updates references** in other entities pointing to any merged entity or merged container
6. **Keeps a single author/editor chain** per role type: the surviving entity's chain wins and the merged entities' chains of that type are discarded. When the survivor has no chain of a role type, the merged entity with the most roles of that type donates its chain, so the survivor never ends up with two parallel chains nor loses the only one available. Duplicate publisher roles held by the same responsible agent are collapsed as well.
7. **Records provenance** for the merge operation
8. **Marks merged entities as merged**
9. **Writes updated RDF** back to files

The script does not upload to the triplestore. It processes a batch of merges against a single triplestore snapshot held in memory and writes the result to files; the triplestore is re-indexed from those files afterwards.

## Sequential processing

Files are processed one at a time, in alphabetical order, and each file is merged as a single batch: one triplestore snapshot is loaded into one in-memory graph, so merges that touch a shared entity (for example the same journal reached through the container cascade) see each other's changes. Nothing runs in parallel.

## Programmatic use

Merge instructions can also be passed in memory, without CSV files:

```python
from oc_meta.run.merge.entities import EntityMerger

merger = EntityMerger("meta_config.yaml", "https://w3id.org/oc/meta/prov/pa/1")
merger.process_rows(
    [
        {
            "surviving_entity": "https://w3id.org/oc/meta/br/0601",
            "merged_entities": ["https://w3id.org/oc/meta/br/0602"],
        }
    ]
)
```

`process_rows` merges the whole list as one batch and writes the RDF files, without any CSV bookkeeping.

## Graceful interruption

To stop processing cleanly:

```bash
touch stop.out
```

The script will:
1. Finish current merge operations
2. Save progress
3. Exit with status code 0

To resume, run the same command again. Already-processed files are skipped.

## Errors

If processing a CSV file fails, the command raises the error and exits unsuccessfully. Rows from the failed file remain `Done=False`, so rerunning the command retries them after the cause is fixed.

## Progress tracking

The script stores progress in the CSV `Done` column. Each file is merged as a single batch: its rows are all marked done together once the batch is written. If interrupted and resumed, files already marked as done are skipped.

For long-running merges, the log reports each phase per file (closure size, entities imported, merges applied) and a progress bar tracks the files.
