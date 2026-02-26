---
title: hasNext fixer
description: Detect and repair broken author and editor sequence chains
---

The hasNext fixer detects and repairs broken `oco:hasNext` chains in author and editor sequences. It uses a three-phase approach: detect anomalies, delete broken chains, and recreate correct ones.

## Anomaly detection

The first step is detecting problems in the hasNext chains.

### What it detects

| Anomaly | Description |
|---------|-------------|
| **Cycles** | Chain loops back to an earlier element |
| **Self-loops** | Element points to itself |
| **Dangling references** | Points to non-existent entity |
| **Multiple heads** | Multiple elements claim to be first |
| **Broken chains** | Gap in sequence numbering |

### Running detection

```bash
uv run python -m oc_meta.run.find.hasnext_anomalies -c <META_CONFIG> -o <OUTPUT_JSON> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --config` | - | Path to Meta config file |
| `-o, --output` | - | Output JSON file for anomalies |
| `--workers` | 4 | Number of parallel workers |

Example:

```bash
uv run python -m oc_meta.run.find.hasnext_anomalies \
  -c meta_config.yaml \
  -o hasnext_anomalies.json \
  --workers 8
```

### Output format

The output JSON contains a report with anomalies in a flat list:

```json
{
  "config": "/path/to/meta_config.yaml",
  "rdf_dir": "/path/to/rdf",
  "timestamp": "2025-01-15T10:30:00+00:00",
  "total_brs_analyzed": 50000,
  "total_anomalies": 15,
  "anomalies_by_type": {
    "cycle": 3,
    "self_loop": 5,
    "dangling_has_next": 7
  },
  "anomalies": [
    {
      "anomaly_type": "self_loop",
      "br": "https://w3id.org/oc/meta/br/060/12345",
      "role_type": "author",
      "ars_involved": [
        {"ar": "https://w3id.org/oc/meta/ar/060/456", "ra": "...", "has_next": [...]}
      ],
      "details": "AR 456 hasNext points to itself"
    }
  ]
}
```

Anomaly types include: `self_loop`, `multiple_has_next`, `dangling_has_next`, `no_start_node`, `multiple_start_nodes`, `cycle`.

## Three-phase fix workflow

### Phase 1: Generate correction plan

Analyze anomalies and create a plan:

```bash
uv run python -m oc_meta.run.patches.has_next \
  -c meta_config.yaml \
  -a hasnext_anomalies.json \
  -o fix_plan.json \
  --csv-output recreate.csv \
  --dry-run
```

This outputs:

- `fix_plan.json` - Details of chains to delete
- `recreate.csv` - CSV input for Meta to recreate correct chains

### Phase 2: Delete broken chains

Execute the deletion plan:

```bash
uv run python -m oc_meta.run.patches.has_next \
  -c meta_config.yaml \
  --execute fix_plan.json \
  -r https://w3id.org/oc/meta/prov/pa/1
```

This removes the broken agent role chains from the triplestore with proper provenance.

### Phase 3: Recreate correct chains

Run Meta on the generated CSV to create new, correct chains:

```bash
uv run python -m oc_meta.run.meta_process -c meta_config_fixer.yaml
```

Use a config file pointing to `recreate.csv` as input.

## Plan JSON format

```json
{
  "deletions": [
    {
      "br": "https://w3id.org/oc/meta/br/060/12345",
      "chain": [
        "https://w3id.org/oc/meta/ar/060/1",
        "https://w3id.org/oc/meta/ar/060/2"
      ],
      "reason": "cycle_detected"
    }
  ]
}
```

## How correct chains are determined

The script fetches author/editor ordering from external APIs based on the BR's identifiers:

1. **Crossref** (for DOIs) - tried first
2. **DataCite** (for DOIs not in Crossref)
3. **PubMed** (for PMIDs)

If no API data is available (no identifiers, API error, or empty response), the correction is marked for manual review.