from rdflib import Graph
from w1_test import *

setgraph= GraphSet ("https://w3id.org/OC/meta/", "", "container/")

data = {'id': 'doi:10.3233/DS-170012', 'title': 'Automating semantic publishing',
                   'author': 'Peroni, Silvio [orcid:0000-0003-0530-4305]', 'pub_date': '2017',
                   'venue': 'Data Science [issn:2451-8484; issn:2451-8492]', 'volume': '1', 'issue': '1-2',
                   'page': '155-173', 'type': 'journal article', 'publisher': 'IOS Press [crossref:7437]'}

#Migrator
data_init = Migrator(data)

#ID
pub_doi_val = data_init.id_job()['doi'][0]

pub_doi= setgraph.add_id ("agent", source_agent=None, source=None, res=None)

pub_doi.create_doi(pub_doi_val)


#Bibliographic Reference
newgraph = setgraph.add_br("agent", source_agent=None, source=None, res=None)

newgraph.create_title(data_init.title_job())

datelist= list()
datelist.append(int(data_init.pub_date_job()))
newgraph.create_pub_date(datelist)

newgraph.has_id(pub_doi)

#AuthorID
pub_aut_orcid = data_init.author_job()['orcid'][0]
pub_aut_id = setgraph.add_id ("agent", source_agent=None, source=None, res=None)
pub_aut_id.create_orcid(pub_aut_orcid)

#Author
pub_aut_name = data_init.author_job()['name']
pub_aut = setgraph.add_ra ("agent", source_agent=None, source=None, res=None)
pub_aut.has_id(pub_aut_id)


#AuthorRole
pub_autRole = setgraph.add_ar ("agent", source_agent=None, source=None, res=None)
pub_autRole.create_author(newgraph)


pub_aut.has_role(pub_autRole)


#rdf serialization
BRgraph = newgraph.g
BRgraph.serialize(destination='BR1.json', format='json-ld')

IDgraph = pub_doi.g
IDgraph.serialize(destination='ID1_doi.json', format='json-ld')


AUTgraph = pub_aut.g
AUTgraph.serialize(destination='AUT1.json', format='json-ld')

AUTIDgraph = pub_aut_id.g
AUTIDgraph.serialize(destination='AUT1ID.json', format='json-ld')


AUTRolgraph = pub_autRole.g
AUTRolgraph.serialize(destination='AUTRol.json', format='json-ld')