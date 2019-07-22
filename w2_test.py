from migrator import *
import os

os.makedirs("rdf/id/")
os.makedirs("rdf/br/")
os.makedirs("rdf/ar/")
os.makedirs("rdf/ra/")

setgraph= GraphSet ("https://w3id.org/OC/meta/", "", "counter/")

data = {'id': 'doi:10.3233/DS-170012', 'title': 'Automating semantic publishing',
                   'author': 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'pub_date': '2017',
                   'venue': 'Data Science [issn:2451-8484; issn:2451-8492]', 'volume': '1', 'issue': '1-2',
                   'page': '155-173', 'type': 'journal article', 'publisher': 'IOS Press [crossref:7437]'}


process = Migrator(data, setgraph)

process.id_job()
process.title_job()
process.author_job()
process.pub_date_job()


#rdf serialization
BRgraph = process.br_graph.g
with open('counter/br.txt', 'r') as file:
    name = str(int(file.read()))
BRgraph.serialize(destination='rdf/br/' + name + '.json', format='json-ld')