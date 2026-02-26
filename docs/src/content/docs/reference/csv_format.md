---
title: CSV format
description: Input CSV format and supported identifiers
---

This page documents the input CSV format and supported identifier schemas.

## CSV format

Meta expects CSV files with these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `id` | Yes | Space-separated identifiers in `schema:value` format (see [supported schemas](#supported-identifier-schemas)) |
| `title` | No | Title of the work |
| `author` | No | Semicolon-separated names in `Surname, Name [identifier]` format (see [author/editor format](#authoreditor-format)) |
| `pub_date` | No | ISO 8601 date: `YYYY-MM-DD`, `YYYY-MM`, or `YYYY` (see [date format](#date-format)) |
| `venue` | No | Container title with optional identifier in brackets (see [venue format](#venue-format)) |
| `volume` | No | Volume number |
| `issue` | No | Issue number |
| `page` | No | Page range (e.g., `50-75`) |
| `type` | No | Resource type (see [resource types](#resource-types)) |
| `publisher` | No | Publisher name with optional identifier in brackets (see [publisher format](#publisher-format)) |
| `editor` | No | Same format as `author` |

### Example

```csv
id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor
doi:10.1162/qss_a_00292,OpenCitations Meta,"Massari, Arcangelo [orcid:0000-0002-8420-0696]; Mariani, Fabio [orcid:0000-0002-8810-1564]; Heibi, Ivan [orcid:0000-0001-5366-5194]; Peroni, Silvio [orcid:0000-0003-0530-4305]; Shotton, David [orcid:0000-0001-5506-523X]",2024-01-22,Quantitative Science Studies [issn:2641-3337],5,1,50-75,journal article,MIT Press [crossref:281],
```

## Identifier format

Identifiers use the format `schema:value`:

```
doi:10.1162/qss_a_00292
pmid:38034492
orcid:0000-0002-8420-0696
issn:2641-3337
```

Multiple identifiers are separated by spaces:

```
doi:10.1162/qss_a_00292 pmid:38034492
```

## Supported identifier schemas

### Bibliographic resources

| Schema | Example | Description |
|--------|---------|-------------|
| `doi` | `doi:10.1162/qss_a_00292` | Digital Object Identifier |
| `pmid` | `pmid:38034492` | PubMed ID |
| `pmcid` | `pmcid:PMC10927410` | PubMed Central ID |
| `arxiv` | `arxiv:2302.03976` | arXiv identifier |
| `isbn` | `isbn:978-3-030-00668-6` | International Standard Book Number |
| `issn` | `issn:2641-3337` | International Standard Serial Number |
| `url` | `url:https://opencitations.net` | Web URL |
| `wikidata` | `wikidata:Q107507571` | Wikidata entity |
| `wikipedia` | `wikipedia:OpenCitations` | Wikipedia article |
| `openalex` | `openalex:W4390928828` | OpenAlex work ID |

### Responsible agents

| Schema | Example | Description |
|--------|---------|-------------|
| `orcid` | `orcid:0000-0002-8420-0696` | ORCID identifier |
| `viaf` | `viaf:309649614` | VIAF identifier |
| `crossref` | `crossref:281` | Crossref funder/member ID |
| `wikidata` | `wikidata:Q30265034` | Wikidata entity |
| `ror` | `ror:01111rn36` | Research Organization Registry |

## Author/editor format

Authors and editors use the format:

```
Surname, Given Name [identifier]
```

Multiple authors are separated by semicolons:

```
Massari, Arcangelo [orcid:0000-0002-8420-0696]; Mariani, Fabio [orcid:0000-0002-8810-1564]; Heibi, Ivan [orcid:0000-0001-5366-5194]; Peroni, Silvio [orcid:0000-0003-0530-4305]; Shotton, David [orcid:0000-0001-5506-523X]
```

The identifier in brackets is optional.

### Name parsing

The comma determines how names are interpreted:

**With comma = Person**
```
Peroni, Silvio        → Family: Peroni, Given: Silvio
Massari, A.           → Family: Massari, Given: A.
Shotton, David M.     → Family: Shotton, Given: David M.
```

**Without comma = Organization**
```
MIT Press                     → Organization name
World Health Organization     → Organization name
```

If a name has no comma, Meta treats it as an organization, not a person.

## Date format

Dates should use ISO 8601 format:

| Format | Example | Precision |
|--------|---------|-----------|
| `YYYY-MM-DD` | `2024-01-15` | Day |
| `YYYY-MM` | `2024-01` | Month |
| `YYYY` | `2024` | Year |

## Resource types

Supported values for the `type` column:

| Value | Description |
|-------|-------------|
| `journal article` | Article in a journal |
| `book` | Complete book |
| `book chapter` | Chapter in a book |
| `book part` | Other part of a book |
| `book section` | Section of a book |
| `book series` | Series of books |
| `book set` | Set of books |
| `edited book` | Book with editors |
| `reference book` | Reference work |
| `monograph` | Single-author scholarly work |
| `report` | Technical or research report |
| `report series` | Series of reports |
| `standard` | Technical standard |
| `standard series` | Series of standards |
| `journal` | Complete journal |
| `journal volume` | Volume of a journal |
| `journal issue` | Issue of a journal |
| `proceedings` | Conference proceedings |
| `proceedings article` | Article in proceedings |
| `proceedings series` | Series of proceedings |
| `reference entry` | Entry in reference work |
| `dissertation` | Thesis or dissertation |
| `peer review` | Peer review document |
| `data file` | Dataset |
| `dataset` | Dataset |
| `web content` | Web page or content |

## Venue format

Venues can include identifiers:

```
Quantitative Science Studies [issn:2641-3337]
Proceedings of the ACM/IEEE Joint Conference on Digital Libraries [isbn:978-1-4503-9822-4]
```

For book chapters, the venue is the containing book:

```
The Semantic Web: Research and Applications [isbn:978-3-642-30283-1]
```

## Publisher format

Publishers can include Crossref member IDs or ROR identifiers:

```
MIT Press [crossref:281]
Springer Nature [crossref:297]
University of Bologna [ror:01111rn36]
```

## Validation

Meta validates identifiers during curation using `oc_ds_converter.oc_idmanager`.

### DOI

- **Syntax check**: Must match `^doi:10\.(\d{4,9}|[^\s/]+(\.[^\s/]+)*)/[^\s]+$`
- **Normalization**: Removes URL prefixes (`https://doi.org/`, `http://dx.doi.org/`), converts to lowercase

### ORCID

- **Syntax check**: Must match `^orcid:([0-9]{4}-){3}[0-9]{3}[0-9X]$`
- **Checksum**: Validates using [ISO/IEC 7064:2003 MOD 11-2](https://en.wikipedia.org/wiki/ORCID#Structure)
- **Normalization**: Removes non-digit characters, uppercases X, formats as `XXXX-XXXX-XXXX-XXXX`

### ISSN

- **Syntax check**: Must match `^issn:[0-9]{4}-[0-9]{3}[0-9X]$`
- **Checksum**: Validates using [modulo 11](https://www.loc.gov/issn/basics/basics-checkdigit.html)
- **Special case**: `0000-0000` is explicitly rejected
- **Normalization**: Removes non-digit characters, uppercases X, formats as `XXXX-XXXX`

### ISBN

- **Syntax check**: ISBN-13 must match `^isbn:97[89][0-9X]{10}$`, ISBN-10 must match `^isbn:[0-9X]{10}$`
- **Checksum**: Validates [modulo 10 for ISBN-13, modulo 11 for ISBN-10](https://en.wikipedia.org/wiki/ISBN#Check_digits)
- **Normalization**: Removes non-digit characters, uppercases X

### Other identifiers

Identifiers with other schemas (PMID, arXiv, Wikidata, etc.) are accepted without validation.
