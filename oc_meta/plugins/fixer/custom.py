from rdflib import URIRef
from SPARQLWrapper import JSON, SPARQLWrapper

from oc_meta.plugins.editor import MetaEditor

server = SPARQLWrapper('http://127.0.0.1:19999/blazegraph/sparql')
query = '''
PREFIX pro: <http://purl.org/spar/pro/>
SELECT DISTINCT ?br ?ar
WHERE {
    <https://w3id.org/oc/meta/ra/0630115031> ^pro:isHeldBy ?ar.
    ?br pro:isDocumentContextFor ?ar.
}
'''
server.setQuery(query)
server.setReturnFormat(JSON)
result = server.queryAndConvert()
br_ar = dict()
meta_editor = MetaEditor('E:\meta_output_24_04_2023_datacite_fixes\meta_output\meta_config.yaml', 'https://orcid.org/0000-0002-8420-0696')
for r in result['results']['bindings']:
    br = r['br']['value']
    br_ar.setdefault(br, list())
    ar = r['ar']['value']
    br_ar[br].append(ar)
for br, ars in br_ar.items():
    if len(ars) == 2:
        meta_editor.merge(URIRef(ars[0]), URIRef(ars[1]))