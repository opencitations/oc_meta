---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Editing entities
description: Modify, delete, or merge entities in OpenCitations Meta
---

The `meta_editor.py` script allows you to edit entities directly in the triplestore. Changes are propagated to both the RDF files and the provenance.

## Usage

```bash
uv run python -m oc_meta.run.meta_editor \
    -c <config_path> \
    -op <operation> \
    -s <subject_uri> \
    -r <your_orcid> \
    [-p <property>] \
    [-o <object>] \
    [-ot <other_uri>]
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `-c, --config` | Path to the Meta configuration file |
| `-op, --operation` | Operation to perform: `update`, `delete`, `sync`, or `merge` |
| `-s, --subject` | URI of the entity to modify |
| `-r, --resp` | Your ORCID (responsible agent for provenance) |
| `-p, --property` | Property to modify (for update/delete) |
| `-o, --object` | New value (for update) or value to remove (for delete) |
| `-ot, --other` | Second entity URI (for merge) |

## Operations

### Update a property

Change the value of a property on an entity:

```bash
uv run python -m oc_meta.run.meta_editor \
    -c meta_config.yaml \
    -op update \
    -s "https://w3id.org/oc/meta/br/0601" \
    -p has_title \
    -o "New Title" \
    -r "https://orcid.org/0000-0002-8420-0696"
```

### Delete an entity or property

Delete an entire entity:

```bash
uv run python -m oc_meta.run.meta_editor \
    -c meta_config.yaml \
    -op delete \
    -s "https://w3id.org/oc/meta/br/0601" \
    -r "https://orcid.org/0000-0002-8420-0696"
```

Delete a specific property value:

```bash
uv run python -m oc_meta.run.meta_editor \
    -c meta_config.yaml \
    -op delete \
    -s "https://w3id.org/oc/meta/br/0601" \
    -p has_identifier \
    -o "https://w3id.org/oc/meta/id/0601" \
    -r "https://orcid.org/0000-0002-8420-0696"
```

### Sync RDF with triplestore

Regenerate RDF files from triplestore data:

```bash
uv run python -m oc_meta.run.meta_editor \
    -c meta_config.yaml \
    -op sync \
    -s "https://w3id.org/oc/meta/br/0601" \
    -r "https://orcid.org/0000-0002-8420-0696"
```

### Merge two entities

Merge a duplicate entity into another:

```bash
uv run python -m oc_meta.run.meta_editor \
    -c meta_config.yaml \
    -op merge \
    -s "https://w3id.org/oc/meta/br/0601" \
    -ot "https://w3id.org/oc/meta/br/0602" \
    -r "https://orcid.org/0000-0002-8420-0696"
```

The entity specified with `-ot` is merged into the entity specified with `-s`. All references to the merged entity are updated, and the merged entity is marked as deleted.
