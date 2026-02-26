---
title: Info dir
description: Redis counters for entity numbering
---

OpenCitations Meta assigns unique identifiers (OMIDs) to each entity. The numbering is tracked in Redis, where counters store the last assigned number for each entity type and supplier prefix.

When processing data, the system reads the current counter value, increments it, and uses that number for the new entity URI. For example, if the counter for `br` (bibliographic resource) with prefix `0610` is at 1000, the next entity will be `https://w3id.org/oc/meta/br/06101001`.

The info dir also tracks provenance snapshot counters. Each time an entity is modified, a new provenance snapshot is created with an incremental number.

## Structure

Redis keys follow this pattern:

- Entity counters: `{short_name}:{supplier_prefix}` → last assigned number
- Provenance counters: `{short_name}:{supplier_prefix}:{entity_number}:se` → last snapshot number

Where `short_name` is one of: `br`, `ra`, `ar`, `re`, `id`.

## When to use these scripts

- **After importing existing data**: if you load RDF files from another source, you need to generate the info dir so that new entities don't overlap with existing ones
- **After a system failure**: if Redis data is lost, you can rebuild it from the RDF files
- **To verify consistency**: check that Redis counters match the actual data in the RDF files
