# OpenCitations Meta Software

OpenCitations Meta contains bibliographic metadata associated with the documents involved in the citations stored in the [OpenCitations](https://opencitations.net/) infrastructure. The OpenCitations Meta Software performs two main actions: a data curation of the provided CSV files and the generation of new RDF files compliant with the [OpenCitations Data Model](http://opencitations.net/model).
An example of a raw CSV input file can be found in [`example.csv`](https://github.com/opencitations/meta/blob/master/example.csv).

## Table of Contents

- [Meta](#meta)
- [Plugins](#plugins)
    * [Get a DOI-ORCID index](#get-a-doi-orcid-index)
    * [Get a Crossref member-name-prefix index](#get-a-crossref-member-name-prefix-index)
    * [Get raw CSV files from Crossref](#get-raw-csv-files-from-crossref)
    * [Get DOIs from COCI](#get-dois-from-coci)
    * [Generate CSVs from triplestore](#generate-csvs-from-triplestore)

## Meta

The Meta process is launched through the [`meta_process.py`](https://github.com/opencitations/meta/blob/master/run/meta_process.py) file via the prompt command:

```console
    python -m meta.run.meta_process -c <PATH>
```
Where:
- -c --config : path to the configuration file.

The configuration file is a YAML file with the following keys (an example can be found in [`config/meta_config.yaml`](https://github.com/opencitations/meta/blob/master/config/meta_config.yaml)).

| Setting                 | Mandatory | Description                                                                                                                                                                   |
| ----------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| triplestore\_url        | ✓         | Endpoint URL to load the output RDF                                                                                                                                          |
| info\_dir               | ✓         | A support folder to save counters of the various types of entities. It must not be deleted or moved until the end of the Meta process                                        |
| resp\_agent             | ✓         | A URI string representing the provenance agent which is considered responsible for the RDF graph manipulation                                                                |
| input\_csv\_dir         | ✓         | Directory where raw CSV files are stored                                                                                                                                      |
| output\_csv\_dir        | ✓         | Directory where cleaned CSV files will be stored                                                                                                                              |
| output\_rdf\_dir        | ✓         | Directory where both output RDF data and provenance files will be stored                                                                                                      |
| indexes\_dir            | ✓         | Indices are products of the CSV cleaning step, and they are necessary for RDF files' creation                                                                                 |
| cache\_path             | ✓         | Path of a text file containing a list of processed CSVs. It will be created if it doesn't exist. This file is helpful to avoid repeating the process twice for the same file. |
| base\_iri               | ☓         | The base URI of entities on Meta. This setting can be safely left as is                                                                                                      |
| context\_path           | ☓         | URL where the namespaces and prefixes used in the OpenCitations Data Model are defined. This setting can be safely left as is                                                |
| dir\_split\_number      | ☓         | Number of files per folder. dir_split_number's value must be multiple of items_per_file's value. This parameter is useful only if you choose to return the output in json-ld format                                                                                                                                                                              |
| items\_per\_file        | ☓         | Number of items per file. This parameter is useful only if you choose to return the output in json-ld format                                                                                                                                                                              |
| default\_dir            | ☓         |                                                                                                                                                                               |
| supplier\_prefix        | ☓         | A prefix for the sequential number in entities’ URIs. This setting can be safely left as is                                                                                  |
| rdf\_output\_in\_chunks | ☓         |                                                                                                                                                                               |
| source                  | ☓         | Data source URL. This setting can be safely left as is                                                                                                                       |
| verbose                 | ☓         | Show a loading bar, elapsed time and estimated time. This setting can be safely left as is                                                                                   |

## Plugins

### Get a DOI-ORCID index

[`orcid_process.py`](https://github.com/opencitations/meta/blob/master/run/orcid_process.py) generates an index between DOIs and the author's ORCIDs using the ORCID Summaries Dump (e.g. [ORCID_2019_summaries](https://orcid.figshare.com/articles/ORCID_Public_Data_File_2019/9988322)). The output is a folder containing CSV files with two columns, 'id' and 'value', where 'id' is a DOI or None, and 'value' is an ORCID. This process can be run via the following commad:

```console
    python -m meta.run.orcid_process -s <PATH> -out <PATH> -t <INTEGER> -lm -v
```
Where:
- -s --summaries: ORCID summaries dump path, subfolder will be considered too.
- -out --output: a directory where the output CSV files will be store, that is, the ORCID-DOI index.
- -t --threshold: threshold after which to update the output, not mandatory. A new file will be generated each time.
- -lm --low-memory: specify this argument if the available RAM is insufficient to accomplish the task. Warning: the processing time will increase.
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

### Get a Crossref member-name-prefix index

[`crossref_publishers_extractor.py`](https://github.com/opencitations/meta/blob/master/run/crossref_publishers_extractor.py) generates an index between Crossref members' ids, names and DOI prefixes. The output is a CSV file with three columns, 'id', 'name', and 'prefix'.
This process can be run via the following command:

```console
    python -m meta.run.crossref_publishers_extractor -o <PATH>
```

Where:
- -o --output: The output CSV file where to store relevant information.

### Get raw CSV files from Crossref

This process generates raw CSV files using JSON files from the Crossref data dump (e.g. [Crossref Works Dump - August 2019](https://figshare.com/articles/Crossref_Works_Dump_-_August_2019/9751865)), enriching them with ORCID IDs from the ORCID-DOI Index generated by [`orcid_process.py`](https://github.com/opencitations/meta/blob/master/run/orcid_process.py).
This function is launched through the [`crossref_process.py`](https://github.com/opencitations/meta/blob/master/run/crossref_process.py) file via the prompt command:

```console
    python -m meta.run.crossref_process -cf <PATH> -o <PATH> -out <PATH> -w <PATH> -v
```
Where:
- -cf --crossref: Crossref JSON files directory (input files).
- -p --publishers: CSV file path containing information about publishers (id, name, prefix). This file can be generated via [`crossref_publishers_extractor.py`](https://github.com/opencitations/meta/blob/master/run/crossref_publishers_extractor.py).
- -o --orcid: ORCID-DOI index filepath, generated by [`orcid_process.py`](https://github.com/opencitations/meta/blob/master/run/orcid_process.py).
- -out --output: directory where CSVs will be stored.
- -w --wanted: path of a CSV file containing what DOI to process, not mandatory.     
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

As the parameters are many, you can also specify them via YAML configuration file. In this case, the process is launched via the command:
```console
    python -m meta.run.crossref_process -c <PATH>
```
Where:
- -c --config : path to the configuration file.

The configuration file is a YAML file with the following keys (an example can be found in [`config/crossref_config.yaml`](https://github.com/opencitations/meta/blob/master/config/crossref_config.yaml)).

| Setting               | Mandatory | Description                                                                                                                         |
| --------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| crossref\_json\_dir   | ✓         | Crossref JSON files directory (input files)                                                                                         |
| output                | ✓         | Directory where output CSVs will be stored                                                                                          |
| orcid\_doi\_filepath  | ☓         | ORCID-DOI index directory. It can be generated via meta.run.orcid\_process                                                          |
| wanted\_doi\_filepath | ☓         | Path of a CSV file containing what DOI to process. This file can be generated via meta.run.coci\_process, if COCI's DOIs are needed |
| verbose               | ☓         | Show a loading bar, elapsed time and estimated time. This setting can be safely left as is.                                         |

### Get DOIs from COCI

You can get a CSV file containing all the DOIs from the [COCI's dump](https://opencitations.net/download). This CSV file can be passed as an input to the `-wanted` argument of [`crossref_process.py`](https://github.com/opencitations/meta/blob/master/run/crossref_process.py). You can obtain this file by using the [`coci_process.py`](https://github.com/opencitations/meta/blob/master/run/coci_process.py) script, in the following way:
```console
    python -m meta.run.coci_process -c <PATH> -out <PATH> -v
```
Where:
- -c --coci: COCI's dump path. It may contain either .zip or .csv files previously extracted.
- -out --output: path of the output CSV file.
- -v --verbose: show a loading bar, elapsed time and estimated time, not mandatory.

### Generate CSVs from triplestore

This plugin generates CSVs from the Meta triplestore. You can run the [`csv_generator.py`](https://github.com/opencitations/meta/blob/master/plugins/csv_generator/csv_generator.py) script in the following way:
```console
    python -m meta.run.csv_generator -c <PATH>
```
Where:
- -c --config : path to the configuration file.
The configuration file is a YAML file with the following keys (an example can be found in [`config/csv_generator_config.yaml`](https://github.com/opencitations/meta/blob/master/config/csv_generator_config.yaml)).

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

