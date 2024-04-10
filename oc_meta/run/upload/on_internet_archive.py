from internetarchive import upload

identifier = 'meta-triplestore-2024-04-06'
file_paths = ['/vltd/triplestore/meta/openalex_out/meta_output_current/blazegraph.zip']
metadata = {
    'identifier': 'meta-triplestore-2024-04-06', 
    'mediatype': 'data', 
    'collection': ['ia_biblio_metadata', 'theinternetarchive'], 
    'creator': 'Arcangelo Massari', 
    'date': '2024-04-06', 
    'description': 'The OpenCitations Meta database stores and delivers bibliographic metadata for all publications involved in the OpenCitations Index.', 
    'language': 'eng', 
    'licenseurl': 'http://creativecommons.org/publicdomain/zero/1.0/', 
    'subject': ['open citations', 'OpenCitations', 'COCI', 'RDF', 'triplestore', 'I4OC', 'open data', 'CC0'], 
    'title': 'Meta triplestore data, archived on 2024-04-06', 
    'website': 'https://opencitations.net/meta', 
    'year': '2024'
}

result = upload(identifier=identifier, files=file_paths, metadata=metadata)

if result[0].status_code == 200:
    print("Upload completato con successo!")
else:
    print("Upload fallito.")