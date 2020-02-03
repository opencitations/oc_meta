from rdflib import *
from datetime import datetime
g = ConjunctiveGraph()

uri1 = URIRef("http://example.org/mygraph1")
uri2 = URIRef("http://example.org/mygraph2")

bob = URIRef(u'urn:bob')
likes = URIRef(u'urn:likes')
pizza = Literal("\"Agile\" 'Knowledge' Graph Testing Ù Ò À With TESTaLOD (!Incredible!) Έτος 汉字")

g.get_context(uri1).add((bob, likes, pizza))
g.get_context(uri2).add((bob, likes, pizza))

s = g.serialize("quand.nq", format='nquads', encoding='UTF-8')

u = ConjunctiveGraph()
u.parse("quand.nq", format='nquads', encoding='UTF-8')
u.serialize("quad.nq", format='nquads', encoding='UTF-8')
for x,y,z in u:
    print(z,y,x)