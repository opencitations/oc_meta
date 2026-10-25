<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Delete orphan agent roles

An agent role (AR) is an orphan when no bibliographic resource (BR) lists it through `pro:isDocumentContextFor`. Most orphans remain when a merge or a deletion removes a BR but leaves its roles behind. Others belong to a BR that still exists: its provenance records the link, but the data file lost it. This fixer deletes every orphan role, along with the `oco:hasNext` links that start from it. For a BR that lost its roles, it also prepares Crossref or DataCite input so that Meta can add the roles again.

## Generate the plan

```bash
uv run python -m oc_meta.run.patches.fix_orphan_ars \
  -c config/meta_config.yaml \
  --dry-run \
  --report-file orphan_roles_plan.json \
  --csv-output-dir orphan_roles_meta_input \
  --mailto you@example.org
```

The dry run reads every BR file and every AR file below `output_rdf_dir/rdf` without changing them, so keep dataset writers stopped while it runs. An AR that no BR lists becomes part of the plan, whatever the history of its former BR.

Orphans can link to each other through `oco:hasNext`, and the fixer deletes them together because no BR uses those chains. When an orphan links to a role that a BR still lists, only the link from the orphan goes away. The summary counts these links. If a listed role links to an orphan instead, the dry run stops with an error and writes no plan, because deleting the orphan would break the chain of an existing BR.

The dry run also replays the role links in the provenance of each BR. A BR lost a role when its provenance still links it to an orphan that its data file lacks. Such a BR gets the same treatment as in the fix for appended roles. The plan removes the roles that the BR now has for each lost type, and Meta adds them again from the DOI that the BR held at creation, or from its other DOIs. The CSV files go to `crossref/input.csv` and `datacite/input.csv` below the output directory, and the API responses go to a SQLite cache.

Some BRs cannot be planned. This happens when a lost role has no single role type, when a role to remove links to a role outside the plan, or when no provider returns the lost role types. The plan lists these BRs under `blocked`, and their lost roles stay in place for a later fix.

The plan groups orphans by the file that holds them, and for each role it stores a hash of its current RDF data. Each BR to reingest is a separate operation, with hashes of the BR and of the roles it loses.

## Execute the plan

```bash
uv run python -m oc_meta.run.patches.fix_orphan_ars \
  -c config/meta_config.yaml \
  --execute orphan_roles_plan.json \
  --resp-agent https://orcid.org/0000-0002-8420-0696
```

Execution first checks the CSV hashes and compares every planned BR and role with the hash in the plan. Then it removes the roles, writes the RDF and provenance files, and saves its progress after each operation. If execution stops, run the same command again to resume. The run creates `reindex_required.out` beside the plan, and it refuses to start a new plan while that file exists. Re-index the triplestores before the next correction. Once the plan is complete and the triplestores are up to date, run Meta on the two CSV directories.
