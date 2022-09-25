[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
[![Run tests](https://github.com/opencitations/oc_meta/actions/workflows/run_tests.yml/badge.svg)](https://github.com/opencitations/oc_meta/actions/workflows/run_tests.yml)
[Coverage](https://raw.githubusercontent.com/opencitations/oc_meta/master/test/coverage/coverage.svg)
![PyPI](https://img.shields.io/pypi/pyversions/oc_meta)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/opencitations/oc_meta)



# OpenCitations Meta Software

OpenCitations Meta contains bibliographic metadata associated with the documents involved in the citations stored in the [OpenCitations](https://opencitations.net/) infrastructure. The OpenCitations Meta Software performs two main actions: a data curation of the provided CSV files and the generation of new RDF files compliant with the [OpenCitations Data Model](http://opencitations.net/model).
An example of a raw CSV input file can be found in [`example.csv`](https://github.com/opencitations/meta/blob/master/oc_meta/example.csv).

## Table of Contents

- [Meta](#meta)
- [Plugins](#plugins)
    * [Get a DOI-ORCID index](#get-a-doi-orcid-index)
    * [Get a Crossref member-name-prefix index](#get-a-crossref-member-name-prefix-index)
    * [Get raw CSV files from Crossref](#get-raw-csv-files-from-crossref)
    * [Get IDs from citations](#get-ids-from-citations)
    * [Generate CSVs from triplestore](#generate-csvs-from-triplestore)
    * [Prepare the multiprocess](#prepare-the-multiprocess)

## Meta

The Meta process is launched through the [`meta_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/meta_process.py) file via the prompt command:

```console
    python -m oc_meta.run.meta_process -c <PATH>
```
Where:
- -c --config : path to the configuration file.

The configuration file is a YAML file with the following keys (an example can be found in [`config/meta_config.yaml`](https://github.com/opencitations/meta/blob/master/config/meta_config.yaml)).

| Setting                 | Mandatory | Description                                                                                                                                                                                                                                                    |
| ----------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| triplestore\_url        | ✓         | Endpoint URL to load the output RDF                                                                                                                                                                                                                            |
| input\_csv\_dir         | ✓         | Directory where raw CSV files are stored                                                                                                                                                                                                                       |
| base\_output\_dir       | ✓         | The path to the base directory to save all output files                                                                                                                                                                                                        |
| resp\_agent             | ✓         | A URI string representing the provenance agent which is considered responsible for the RDF graph manipulation                                                                                                                                                  |
| base\_iri               | ☓         | The base URI of entities on Meta. This setting can be safely left as is                                                                                                                                                                                        |
| context\_path           | ☓         | URL where the namespaces and prefixes used in the OpenCitations Data Model are defined. This setting can be safely left as is.                                                                                                                                 |
| dir\_split\_number      | ☓         | Number of files per folder. dir\_split\_number's value must be multiple of items\_per\_file's value. This parameter is useful only if you choose to return the output in json-ld                                                                               |
| items\_per\_file        | ☓         | Number of items per file. This parameter is useful only if you choose to return the output in json-ld                                                                                                                                                          |
| default\_dir            | ☓         | This value is used as the default prefix if no prefix is specified. It is a deprecated parameter, valid only for backward compatibility and can safely be ignored                                                                                              |
| supplier\_prefix        | ☓         | A prefix for the sequential number in entities’ URIs. This setting can be safely left as is                                                                                                                                                                    |
| rdf\_output\_in\_chunks | ☓         | If True, save all the graphset and provset in one file, and save all the graphset on the triplestore. If False, the graphs are saved according to the usual OpenCitations strategy (the "complex" hierarchy of folders and subfolders for each type of entity) |
| source                  | ☓         | Data source URL. This setting can be safely left as is                                                                                                                                                                                                         |
| use\_doi\_api\_service  | ☓         | If True, use the DOI API service to check if DOIs are valid                                                                                                                                                                                                    |
| workers\_number         | ☓         | Number of cores to devote to the Meta process                                                                                                                                                                                                                  |
| verbose                 | ☓         | Show a loading bar, elapsed time and estimated time. This setting can be safely left as is                                                                                                                                                                     |

## Plugins

### Get a DOI-ORCID index

[`orcid_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/orcid_process.py) generates an index between DOIs and the author's ORCIDs using the ORCID Summaries Dump (e.g. [ORCID_2019_summaries](https://orcid.figshare.com/articles/ORCID_Public_Data_File_2019/9988322)). The output is a folder containing CSV files with two columns, 'id' and 'value', where 'id' is a DOI or None, and 'value' is an ORCID. This process can be run via the following commad:

```console
    python -m oc_meta.run.orcid_process -s <PATH> -out <PATH> -t <INTEGER> -lm -v
```
Where:
- -s --summaries: ORCID summaries dump path, subfolder will be considered too.
- -out --output: a directory where the output CSV files will be store, that is, the ORCID-DOI index.
- -t --threshold: threshold after which to update the output, not mandatory. A new file will be generated each time.
- -lm --low-memory: specify this argument if the available RAM is insufficient to accomplish the task. Warning: the processing time will increase.
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

### Get a Crossref member-name-prefix index

[`crossref_publishers_extractor.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/crossref_publishers_extractor.py) generates an index between Crossref members' ids, names and DOI prefixes. The output is a CSV file with three columns, 'id', 'name', and 'prefix'.
This process can be run via the following command:

```console
    python -m oc_meta.run.crossref_publishers_extractor -o <PATH>
```

Where:
- -o --output: The output CSV file where to store relevant information.

### Get raw CSV files from Crossref

This process generates raw CSV files using JSON files from the Crossref data dump (e.g. [Crossref Works Dump - August 2019](https://figshare.com/articles/Crossref_Works_Dump_-_August_2019/9751865)), enriching them with ORCID IDs from the ORCID-DOI Index generated by [`orcid_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/orcid_process.py).
This function is launched through the [`crossref_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/crossref_process.py) file via the prompt command:

```console
    python -m oc_meta.run.crossref_process -cf <PATH> -o <PATH> -out <PATH> -w <PATH> -v
```
Where:
- -cf --crossref: Crossref JSON files directory (input files).
- -p --publishers: CSV file path containing information about publishers (id, name, prefix). This file can be generated via [`crossref_publishers_extractor.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/crossref_publishers_extractor.py).
- -o --orcid: ORCID-DOI index filepath, generated by [`orcid_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/orcid_process.py).
- -out --output: directory where CSVs will be stored.
- -w --wanted: path of a CSV file containing what DOI to process, not mandatory.     
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

As the parameters are many, you can also specify them via YAML configuration file. In this case, the process is launched via the command:
```console
    python -m oc_meta.run.crossref_process -c <PATH>
```
Where:
- -c --config : path to the configuration file.

The configuration file is a YAML file with the following keys (an example can be found in [`config/crossref_config.yaml`](https://github.com/opencitations/meta/blob/master/config/crossref_config.yaml).

| Setting               | Mandatory | Description                                                                                                                         |
| --------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| crossref\_json\_dir   | ✓         | Crossref JSON files directory (input files)                                                                                         |
| output                | ✓         | Directory where output CSVs will be stored                                                                                          |
| orcid\_doi\_filepath  | ☓         | ORCID-DOI index directory. It can be generated via oc_meta.run.orcid\_process                                                          |
| wanted\_doi\_filepath | ☓         | Path of a CSV file containing what DOI to process. This file can be generated via oc_meta.run.coci\_process, if COCI's DOIs are needed |
| verbose               | ☓         | Show a loading bar, elapsed time and estimated time. This setting can be safely left as is.                                         |

### Get IDs from citations

You can get a CSV file containing all the IDs from citation data organized in the CSV format accepted by OpenCitations. This CSV file can be passed as an input to the `-wanted` argument of [`crossref_process.py`](https://github.com/opencitations/meta/blob/master/oc_meta/run/crossref_process.py). You can obtain this file by using the [`get_ids_from_citations.py`](https://github.com/opencitations/oc_meta/blob/master/oc_meta/run/get_ids_from_citations.py) script, in the following way:
```console
    python -m oc_meta.run.get_ids_from_citations -c <PATH> -out <PATH> -t <INTEGER> -v
```
Where:
- -c --citations: the directory containing the citations files, either in CSV or ZIP format
- -out --output: directory of the output CSV files
- -t --threshold: number of files to save after
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

### Generate CSVs from triplestore

This plugin generates CSVs from the Meta triplestore. You can run the [`csv_generator.py`](https://github.com/opencitations/meta/blob/master/oc_meta/plugins/csv_generator/csv_generator.py) script in the following way:
```console
    python -m oc_meta.run.csv_generator -c <PATH>
```
Where:
- -c --config : path to the configuration file.
The configuration file is a YAML file with the following keys (an example can be found in [`config/csv_generator_config.yaml`](https://github.com/opencitations/meta/blob/master/oc_meta/config/csv_generator_config.yaml)).

| Setting            | Mandatory | Description                                                                                                                                |
| ------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| triplestore\_url   | ✓         | URL of the endpoint where the data are located                                                                                             |
| output\_csv\_dir   | ✓         | Directory where the output CSV files will be stored                                                                                        |
| info\_dir          | ✓         | The folder where the counters of the various types of entities are stored.                                                                 |
| base\_iri          | ☓         | The base IRI of entities on the triplestore. This setting can be safely left as is                                                         |
| supplier\_prefix   | ☓         | A prefix for the sequential number in entities’ URIs. This setting can be safely left as is                                                |
| dir\_split\_number | ☓         | Number of files per folder. dir\_split\_number's value must be multiple of items\_per\_file's value. This setting can be safely left as is |
| items\_per\_file   | ☓         | Number of items per file. This setting can be safely left as is                                                                            |
| verbose            | ☓         | Show a loading bar, elapsed time and estimated time. This setting can be safely left as is                                                |

### Prepare the multiprocess

Before running Meta in multiprocess, it is necessary to prepare the input files. In particular, the CSV files must be divided by publisher, while venues and authors having an identifier must be loaded on the triplestore, in order not to generate duplicates during the multiprocess. These operations can be done by simply running the following script:

```console
    python -m oc_meta.run.prepare_multiprocess -c <PATH>
```

Where:
- -c --config : Path to the same configuration file you want to use for Meta.

Afterwards, launch Meta in multi-process by specifying the same configuration file. All the required modifications are done automatically.

