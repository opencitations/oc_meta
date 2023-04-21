[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
[![run_tests](https://github.com/opencitations/identifier_manager/actions/workflows/run_tests.yaml/badge.svg)](https://github.com/opencitations/identifier_manager/actions/workflows/run_tests.yaml)
![Coverage](https://raw.githubusercontent.com/opencitations/identifier_manager/main/test/coverage/coverage.svg)
![PyPI](https://img.shields.io/pypi/pyversions/oc_idmanager)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/opencitations/identifier_manager)

# oc_idmanager
### Identifier Manager Classes

Repository collecting OpenCitations Identifier Managers, including:
<ol>
    <li>doi</li>
    <li>pmid</li>
    <li>isbn</li>
    <li>issn</li>
    <li>orcid</li>
    <li>wikidata</li>
    <li>wikipedia</li>
    <li>url</li>
</ol>

### Methods

All the identifier managers are instances of the class constructor <b>IdentifierManager</b>, which includes the methods listed below:
<ol>
<li><b>is_valid</b>: It takes in input the id string and returns True if the id is valid, false otherwise. The additional parameter get_extra_info (False by default) can be set as True to retrieve a dictionary containing additional information about the id, if possible. 
This method calls other class methods, such as <i>normalise</i>, <i>syntax_ok</i>, <i>check_digit</i> (if required), <i>exists</i> (if possible), and <i>extra_info</i> (if get_extra_info=True)</li>
<li><b>normalise</b>: It takes in input the id string and returns the normalised id. It is possible to specify if the optional parameter include_prefix=True to get the normalised id with its prefix.</li>
<li><b>check_digit</b>: This method takes in input the id string and returns True if the check digit on the id_string passes (this does not mean that the id is also registered). Note that not all id types have a check digit (it is implemented for ORCID, ISSN and ISBN only). </li>
<li><b>syntax_ok</b>: This method returns True if the id_string in input is correct, according to the id-specific syntax.</li>
<li><b>exists</b>: This method takes in input the id string and returns True if the id is valid, false otherwise. The additional parameter get_extra_info (False by default) can be set to True to retrieve a dictionary containing additional information about the id, if possible. The existence is verified by using id-specific APIs. Since not all the API services are freely accessible, the factual existence (i.e. id registration) of some id types (ISSN and ISBN) can't be verified. By default, the method "exists" returns True when the usage of API services is not enabled. The extra parameter allow_extra_api is None by default, but a list of extra API services can be specified, in order to perform additional API calls and retrieve the required data. "</li>
<li><b>extra_info</b>: takes in input the API response of the id-specific API service, if any, and returns a dictionary with additional information about the id. </li>
</ol>

### exists method with additional APIs enabled

```console
dm = DOIManager()
dm.exists(self.valid_doi_1, get_extra_info=True, allow_extra_api=["crossref"])
```
The output of this execution is:
```console
(True, {'valid': True, 'title': 'Setting our bibliographic references free: towards open citation data', 'author': ['Peroni, Silvio', 'Dutton, Alexander', 'Gray, Tanya', 'Shotton, David'], 'editor': [], 'pub_date': '2015-3-9', 'venue': 'Journal of Documentation', 'volume': '71', 'issue': '2', 'page': '253-277', 'type': ['journal article'], 'publisher': ['Emerald [crossref:140]']})
```

### Class Instantiation
Each class can be instantiated either with or without a dictionary storing previously retrieved information about ids. In case the class is instantiated with a validation dictionary, the information is searched in the dictionary before repeating the checks to assess the id validity (i.e.: syntax_ok, check_digit, exists). In addition to that, the access to API services can be disabled by setting the optional parameter use_api_service=False.

#### Class instantiation without validation dictionary with API access enabled
The validity of the id will be verified by performing all the required checks since no previously retrieved information is provided.
```console
om = ORCIDManager()
```
#### Class instantiation with validation dictionary and API access enabled
The validity of the id will be verified by accessing the validity dictionary and, in case the id is not found in the dictionary (or found with null value) all the required checks will be performed.

```console
om_file = ORCIDManager(self.data)
```

#### Class instantiation with validation dictionary and API access disabled
The validity of the id will be verified by accessing the validity dictionary only.
```console
om_file_noapi = ORCIDManager(self.data, use_api_service=False)
```

#### Class instantiation with API access disabled and no validation dictionary specified
The validity of the id can't be verified: with these settings no information will be retrieved. However, we implemented 
the check for existence so to have a positive behavior in case neither API can be used nor data dictionary is provided. 
Indeed, an id will result existent if the "exists" method is called with the API-use parameter set to False.  
```console
om_nofile_noapi = ORCIDManager(use_api_service=False)
```


### Code Testing 
Update the [`/test/test_identifier.py`](https://github.com/opencitations/identifier_manager/blob/main/test/test_identifier.py) file and run the following command to test the code
```console
$ python -m unittest discover -s test -p "test_identifier.py"
```

### Notes 
The doi syntax is checked by using the regular expression below.
```console
"^doi:10\.(\d{4,9}|[^\s/]+(\.[^\s/]+)*)/[^\s]+$"
```
The redundant alternative for the prefix was specified in order to mention the current convention of using 4-9 digits prefixes for the registrant code, following the Directory indicator ("10."). However, according to the <a href="https://www.doi.org/doi_handbook/2_Numbering.html#2.2">DOI Handbook</a>, there is no fixed rule for the registrant code syntax. Further, each registrant code may come with a subdivision code (which is separated by the registrant code itself by full stop). No specific syntax is defined for the subdivision code. 

