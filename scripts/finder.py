from scripts.graphlib import GraphEntity
from pymantic import sparql


class ResourceFinder:

    def __init__(self, ts_url):
        self.ts = sparql.SPARQLServer(ts_url)


    def __query(self, query):
        result = self.ts.query(query)
        return result

    def retrieve_br_from_id(self, value, schema):
        schema = GraphEntity.DATACITE + schema
        query = """
                SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
					 (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
					 (group_concat(?schema;separator=' ;and; ') as ?schema)
					 (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

				WHERE {
                    ?res a <%s>.
                    ?res <%s> ?title.
                    ?res <%s> ?id.
                    ?id <%s> ?schema.
                    ?id  <%s> ?value.
                    ?res <%s> ?knownId.
                    ?knownId <%s> <%s>.
                    ?knownId <%s> ?knownValue.
                    filter(?knownValue = "%s")
                } group by ?res

                """ % (GraphEntity.expression, GraphEntity.title, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, schema, GraphEntity.has_literal_value, value)
        result = self.__query(query)

        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            res = str(result["res"]["value"]).replace("https://w3id.org/OC/meta/br/", "")
            title = str(result["title"]["value"])
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list,id_value_list))
            id_list = list()
            for x in couple_list:
                id = str(x[0]) + ':' + str(x[1])
                id_list.append(id)
            final_list = list(zip(meta_id_list, id_list))

            return res, title, final_list
        else:
            return None


    def retrieve_autor_editor_from_id (self, value, schema):
        schema = GraphEntity.DATACITE + schema

        query = """
                SELECT DISTINCT ?res 
                     (group_concat(DISTINCT  ?name;separator=' ;and; ') as ?name)
                     (group_concat(DISTINCT  ?surname;separator=' ;and; ') as ?surname)
                     (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                     (group_concat(?schema;separator=' ;and; ') as ?schema)
                     (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                WHERE {
                    ?res a <%s>.
                    ?res <%s> ?name.
                    ?res <%s> ?surname.
                    ?res <%s> ?id.
                    ?id <%s> ?schema.
                    ?id  <%s> ?value.
                    ?res <%s> ?knownId.
                    ?knownId <%s> <%s>.
                    ?knownId <%s> ?knownValue.
                    filter(?knownValue = "%s")
                } group by ?res

                """ % (GraphEntity.agent, GraphEntity.given_name, GraphEntity.family_name, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, schema, GraphEntity.has_literal_value, value)

        result = self.__query(query)

        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            res = str(result["res"]["value"]).replace("https://w3id.org/OC/meta/ra/", "")
            title = str(result["surname"]["value"]) + ", " + str(result["name"]["value"])
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                id = str(x[0]) + ':' + str(x[1])
                id_list.append(id)
            final_list = list(zip(meta_id_list, id_list))

            return res, title, final_list
        else:
            return None

    def retrieve_publisher_from_id(self, value, schema):
        schema = GraphEntity.DATACITE + schema
        query = """
                SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                     (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                     (group_concat(?schema;separator=' ;and; ') as ?schema)
                     (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                WHERE {
                    ?res a <%s>.
                    ?res <%s> ?title.
                    ?res <%s> ?id.
                    ?id <%s> ?schema.
                    ?id  <%s> ?value.
                    ?res <%s> ?knownId.
                    ?knownId <%s> <%s>.
                    ?knownId <%s> ?knownValue.
                    filter(?knownValue = "%s")
                } group by ?res

                """ % (GraphEntity.agent, GraphEntity.name, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value,
                       GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, schema, GraphEntity.has_literal_value, value)
        result = self.__query(query)

        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            res = str(result["res"]["value"]).replace("https://w3id.org/OC/meta/ra/", "")
            title = str(result["title"]["value"])
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                id = str(x[0]) + ':' + str(x[1])
                id_list.append(id)
            final_list = list(zip(meta_id_list, id_list))

            return res, title, final_list
        else:
            return None

    def retrieve_venue_from_id(self, value, schema):
        schema = GraphEntity.DATACITE + schema
        query = """
                SELECT DISTINCT ?res 
                     (group_concat(DISTINCT  ?type;separator=' ;and; ') as ?type)
                     (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                     (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                     (group_concat(?schema;separator=' ;and; ') as ?schema)
                     (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                WHERE {
                    ?res a <%s>.
                    ?res a ?type.
                    ?res <%s> ?title.
                    ?res <%s> ?id.
                    ?id <%s> ?schema.
                    ?id  <%s> ?value.
                    ?res <%s> ?knownId.
                    ?knownId <%s> <%s>.
                    ?knownId <%s> ?knownValue.
                    filter(?knownValue = "%s")
                } group by ?res

                """ % (GraphEntity.expression, GraphEntity.title, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value,
                       GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, schema, GraphEntity.has_literal_value, value)
        result = self.__query(query)

        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            res = str(result["res"]["value"]).replace("https://w3id.org/OC/meta/br/", "")
            title = str(result["title"]["value"])
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                id = str(x[0]) + ':' + str(x[1])
                id_list.append(id)
            final_list = list(zip(meta_id_list, id_list))

            content = dict()
            types = str(result["type"]["value"]).split(" ;and; ")
            if any(str(typ) == str(GraphEntity.FABIO.Journal) for typ in types):
                content["issue"] = dict()
                content["volume"] = dict()
                content = self.retrieve_vvi_part_from_id(res, content)

            return res, title, final_list, content
        else:
            return None


    def retrieve_vvi_part_from_id(self, venue, content):
        query = """
                SELECT DISTINCT ?res 
                     (group_concat(DISTINCT  ?type;separator=' ;and; ') as ?type)
					 (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)

				WHERE {
                    ?res <%s> <%s>.
                    ?res a ?type.
                    ?res <%s> ?title.
                } group by ?res

                """ % (GraphEntity.part_of, "https://w3id.org/OC/meta/br/" + str(venue), GraphEntity.has_sequence_identifier)
        result = self.__query(query)
        if result["results"]["bindings"]:
            results = result["results"]["bindings"]
            for x in results:
                res = str(x["res"]["value"]).replace("https://w3id.org/OC/meta/br/", "")
                title = str(x["title"]["value"])
                types = str(x["type"]["value"]).split(" ;and; ")

                for t in types:
                    if str(t) == str(GraphEntity.FABIO.JournalIssue):
                        content["issue"][title] = res

                    elif str(t) == str(GraphEntity.FABIO.JournalVolume):
                        content["volume"][title] = dict()
                        content["volume"][title]["id"] = res
                        content["volume"][title]["issue"] = dict()
                        content["volume"][title]["issue"] = self.retrieve_vvi_part_from_id(res, content["volume"][title]["issue"])
        return content