<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Fix dangling agent roles

The dangling AR fixer repairs bibliographic resources whose `pro:isDocumentContextFor` value points to an agent role that is absent from the local RDF files. It rebuilds the author, editor, and publisher chains for each affected resource from Crossref, DataCite, and OpenAlex metadata.

Its scope is limited to that missing-entity condition. It does not repair an AR that is present but malformed, a standalone dangling `oco:hasNext` link, bibliographic metadata, venues, or identifiers attached to the bibliographic resource.

## Generate a plan

```bash
uv run python -m oc_meta.run.patches.fix_dangling_ars \
  -c config/meta_config.yaml \
  --dry-run \
  --report-file dangling_ar_plan.json \
  --review-file dangling_ar_review.csv \
  --cache-file dangling_ar_api_cache.sqlite \
  --mailto name@example.org
```

The dry run reads the RDF files below `output_rdf_dir/rdf`. It does not query the triplestore and does not change RDF. API responses and `404` results are retained in the SQLite cache. Use `--refresh-cache` to fetch them again. Pass `--openalex-api-key` or set `OPENALEX_API_KEY` when the execution environment requires an OpenAlex key.

The local scan has three stages:

1. Index the AR URIs present in the RDF files.
2. Find BR entities that directly reference an absent AR.
3. Load the AR, RA, and identifier entities needed only by those BR entities.

The JSON report includes the missing ARs, their provenance status, provider choices, target role lists, planned RA reuse or creation, and the local RDF state used as an execution precondition.

## Provider and identity rules

Crossref, DataCite, and OpenAlex are queried in that order. Selection happens independently for authors, editors, and publishers: the first nonempty list for each role is used. If at least one provider returns the work but every provider has an empty list for a role, the target list for that role is empty and existing local roles of that type are removed. If no provider returns the work, the BR is blocked with `no_provider_data`.

For each target agent, the fixer applies these rules:

- reuse a global RA only through an exact ORCID, Crossref member ID, or Research Organization Registry (ROR) ID;
- preserve a RA already used on the same BR when its name has one unambiguous score of at least `0.9` and no supported identifier conflicts with the provider record;
- create an RA when neither match exists, using the provider name and supported identifier when available;
- never reuse a global RA by name alone;
- never change the name or identifiers of a reused RA.

OpenAlex author IDs are not stored as RA identifiers. An ORCID returned by OpenAlex can be stored.

The existing AR is preserved when it already represents the selected agent. Other current ARs are reassigned in their deterministic local order, missing positions receive new ARs, surplus ARs are deleted, and `oco:hasNext` is rebuilt in provider order.

An identifier that resolves to more than one RA blocks the BR. The fixer also blocks a BR when its current roles have multiple contexts, missing or multiple holders, an unknown or multiple role type, a fork, multiple predecessors, a cycle, a self-loop, or a link to another role or context. Breaks attributable to the absent AR are rebuilt.

## Review by bibliographic resource

The review CSV contains one row per BR. Set `decision` to one of:

| Value | Effect |
|---|---|
| `approve` | Apply the whole BR repair |
| `reject` | Apply no change to the BR |
| empty | Apply no change to the BR |

Do not edit the other columns. Execution compares them with the JSON plan. A blocked row cannot be approved.

## Execute approved repairs

```bash
uv run python -m oc_meta.run.patches.fix_dangling_ars \
  -c config/meta_config.yaml \
  --execute dangling_ar_plan.json \
  --review-file dangling_ar_review.csv \
  --resp-agent https://orcid.org/0000-0002-8420-0696
```

Execution reads and writes the local RDF files even if the configuration would normally allow uploads. Before saving a BR, it verifies the configuration hash, plan integrity, review row, and current RDF state. A changed precondition stops the run.

For every old missing AR, execution removes the dangling BR link and uses a new AR for any restored role. Provenance is handled according to the latest local snapshot:

| Existing provenance | Action |
|---|---|
| latest snapshot active | Add a deletion snapshot |
| latest snapshot invalidated | Add no snapshot |
| no snapshot | Report the absence and add no snapshot |

Progress is saved after each BR. Once an attempted group changes RDF, `reindex_required.out` is created beside the plan. Re-index the triplestore from the RDF files and remove the sentinel before another correction or merge run.

After re-indexing, regenerate the duplicate RA CSV and run `fix_duplicate_ras` again. Do not reuse a duplicate CSV created before this repair.
