<!--
SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>

SPDX-License-Identifier: ISC
-->

# Info dir

OpenCitations Meta assigns unique identifiers (OMIDs) to each entity. The numbering is tracked on the filesystem in an `info_dir` directory, where counter files store the last assigned number for each entity type and supplier prefix.

When processing data, the system reads the current counter value, increments it, and uses that number for the new entity URI. For example, if the counter for `br` (bibliographic resource) with prefix `0610` is at 1000, the next entity will be `https://w3id.org/oc/meta/br/06101001`.

The info dir also tracks provenance snapshot counters. Each time an entity is modified, a new provenance snapshot is created with an incremental number.

An entity counter represents the highest identifier ever assigned. It is therefore the maximum identifier found either among current RDF entities or among provenance subjects. This prevents the reuse of identifiers belonging to deleted entities.

## Structure

Counter files are stored in `base_output_dir/info_dir/<supplier_prefix>/` with names following this pattern:

- Entity counters: `info_file_{short_name}.txt` → last assigned number
- Provenance counters: `prov_file_{short_name}.txt` → provenance snapshot counters

Where `short_name` is one of: `br`, `ra`, `ar`, `re`, `id`.

Line `n` of a provenance counter file contains the maximum snapshot number for entity `n`. Empty lines represent entities without a provenance counter.

## When to use these scripts

- **After importing existing data**: if you load RDF files from another source, you need to generate the info dir so that new entities don't overlap with existing ones
- **After a system failure**: if counter files are lost, you can rebuild them from the RDF files
- **To verify consistency**: check that counter files match the actual data in the RDF files

Generation and verification operate on zipped JSON-LD RDF files. Run them while no process is modifying either the RDF directory or the info dir.
