from scripts.graphlib import GraphEntity
from pymantic import sparql


class ResourceFinder:

    def __init__(self, ts_url):
        self.ts = sparql.SPARQLServer(ts_url)


    def __query(self, query):
        result = self.ts.query(query)
        return result



    #_______________________________BR_________________________________#

    def retrieve_br_from_id(self, value, schema):
        schema = GraphEntity.DATACITE + schema
        query = """
                SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
					 (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
					 (group_concat(?schema;separator=' ;and; ') as ?schema)
					 (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

				WHERE {
                    ?res a <%s>.
                    OPTIONAL {?res <%s> ?title.}
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
        results = self.__query(query)

        if len(results["results"]["bindings"]):
            result_list = list()
            for result in results["results"]["bindings"]:
                res = str(result["res"]["value"]).replace("https://w3id.org/OC/meta/br/", "")
                title = str(result["title"]["value"])
                meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
                id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
                id_value_list = str(result["value"]["value"]).split(" ;and; ")

                couple_list = list(zip(id_schema_list,id_value_list))
                id_list = list()
                for x in couple_list:
                    id = str(x[0]).lower() + ':' + str(x[1])
                    id_list.append(id)
                final_list = list(zip(meta_id_list, id_list))
                result_list.append(tuple((res, title, final_list)))
            return result_list
        else:
            return None

    def retrieve_br_from_meta (self, meta_id):
        uri = "https://w3id.org/OC/meta/br/" + str(meta_id)
        query = """
                SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                     (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                     (group_concat(?schema;separator=' ;and; ') as ?schema)
                     (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                WHERE {
                    ?res a <%s>.
                    OPTIONAL {?res <%s> ?title.}
                    ?res <%s> ?id.
                    ?id <%s> ?schema.
                    ?id  <%s> ?value.
                    filter(?res = <%s>)
                } group by ?res

                """ % (GraphEntity.expression, GraphEntity.title, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, uri)
        result = self.__query(query)

        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            title = str(result["title"]["value"])
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                id = str(x[0]).lower() + ':' + str(x[1])
                id_list.append(id)
            final_list = list(zip(meta_id_list, id_list))

            return title, final_list
        else:
            return None


    #_______________________________ID_________________________________#

    def retrieve_id (self, value, schema):
        schema = GraphEntity.DATACITE + schema

        query = """
                        SELECT DISTINCT ?res 


                        WHERE {
                            ?res a <%s>.
                            ?res <%s> <%s>.
                            ?res <%s> ?knownValue.
                            filter(?knownValue = "%s")
                        } group by ?res

                        """ % (
        GraphEntity.identifier, GraphEntity.uses_identifier_scheme, schema, GraphEntity.has_literal_value,
        value)

        result = self.__query(query)
        if result["results"]["bindings"]:
            return str(result["res"]["value"]).replace("https://w3id.org/OC/meta/ra/", "")
        else:
            return None




    #_______________________________RA_________________________________#
    def retrieve_ra_from_meta (self, meta_id, publisher = False):
        uri = "https://w3id.org/OC/meta/ra/" + str(meta_id)
        query = """
                        SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                             (group_concat(DISTINCT  ?name;separator=' ;and; ') as ?name)
                             (group_concat(DISTINCT  ?surname;separator=' ;and; ') as ?surname)
                             (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                             (group_concat(?schema;separator=' ;and; ') as ?schema)
                             (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                        WHERE {
                            ?res a <%s>.
                            OPTIONAL {?res <%s> ?name.
                                ?res <%s> ?surname.}
                            OPTIONAL {?res <%s> ?title.}
                            ?res <%s> ?id.
                            ?id <%s> ?schema.
                            ?id  <%s> ?value.
                            filter(?res = <%s>)
                        } group by ?res

                        """ % (GraphEntity.agent, GraphEntity.given_name, GraphEntity.family_name, GraphEntity.name, GraphEntity.has_identifier,
                               GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            if str(result["title"]["value"]) and publisher:
                title = str(result["title"]["value"])
            elif str(result["surname"]["value"]) and str(result["name"]["value"]) and not publisher:
                title = str(result["surname"]["value"]) + ", " + str(result["name"]["value"])
            else:
                title = ""
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                id = str(x[0]).lower() + ':' + str(x[1])
                id_list.append(id)
            final_list = list(zip(meta_id_list, id_list))

            return title, final_list
        else:
            return None


    def retrieve_ra_from_id(self, value, schema, publisher):
        schema = GraphEntity.DATACITE + schema

        query = """
                SELECT DISTINCT ?res
                    (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                     (group_concat(DISTINCT  ?name;separator=' ;and; ') as ?name)
                     (group_concat(DISTINCT  ?surname;separator=' ;and; ') as ?surname)
                     (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                     (group_concat(?schema;separator=' ;and; ') as ?schema)
                     (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                WHERE {
                    ?res a <%s>.
                    OPTIONAL {?res <%s> ?name.
                            ?res <%s> ?surname.}
                    OPTIONAL {?res <%s> ?title.}
                    ?res <%s> ?id.
                    ?id <%s> ?schema.
                    ?id  <%s> ?value.
                    ?res <%s> ?knownId.
                    ?knownId <%s> <%s>.
                    ?knownId <%s> ?knownValue.
                    filter(?knownValue = "%s")
                } group by ?res

                """ % (GraphEntity.agent, GraphEntity.given_name, GraphEntity.family_name, GraphEntity.name, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, schema, GraphEntity.has_literal_value, value)

        results = self.__query(query)

        if len(results["results"]["bindings"]):
            result_list = list()
            for result in results["results"]["bindings"]:
                res = str(result["res"]["value"]).replace("https://w3id.org/OC/meta/ra/", "")
                if str(result["title"]["value"]) and publisher:
                    title = str(result["title"]["value"])
                elif str(result["surname"]["value"]) and str(result["name"]["value"]) and not publisher:
                    title = str(result["surname"]["value"]) + ", " + str(result["name"]["value"])
                else:
                    title = ""
                meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/OC/meta/id/", "").split(" ;and; ")
                id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
                id_value_list = str(result["value"]["value"]).split(" ;and; ")

                couple_list = list(zip(id_schema_list, id_value_list))
                id_list = list()
                for x in couple_list:
                    id = str(x[0]).lower() + ':' + str(x[1])
                    id_list.append(id)
                final_list = list(zip(meta_id_list, id_list))

                result_list.append(tuple((res, title, final_list)))
            return result_list
        else:
            return None



    #_______________________________VVI_________________________________#

    def retrieve_venue_from_meta(self, meta_id):
        content = dict()
        content["issue"] = dict()
        content["volume"] = dict()
        content = self.retrieve_vvi(meta_id, content)
        return content


    def retrieve_vvi(self, meta, content):
        query = """
                SELECT DISTINCT ?res 
                     (group_concat(DISTINCT  ?type;separator=' ;and; ') as ?type)
					 (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)

				WHERE {
                    ?res <%s> <%s>.
                    ?res a ?type.
                    ?res <%s> ?title.
                } group by ?res

                """ % (GraphEntity.part_of, "https://w3id.org/OC/meta/br/" + str(meta), GraphEntity.has_sequence_identifier)
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
                        content["volume"][title]["issue"] = self.retrieve_vvi(res, content["volume"][title]["issue"])
        return content



    def retrieve_ra_sequence_from_meta (self, meta_id, col_name):
        if col_name == "author":
            role = GraphEntity.author
        elif col_name == "editor":
            role = GraphEntity.editor
        else:
            role = GraphEntity.publisher
        uri = "https://w3id.org/OC/meta/br/" + str(meta_id)
        query = """
                SELECT DISTINCT ?role ?next ?agent

                WHERE {
                    ?res a <%s>.
                    ?res <%s> ?role.
                    ?role a <%s>.
                    ?role <%s> <%s>.
                    OPTIONAL {?role <%s> ?next.}
                    ?role <%s> ?agent.
                    filter(?res = <%s>)
                } 

                """ % (GraphEntity.expression, GraphEntity.is_document_context_for, GraphEntity.role_in_time, GraphEntity.with_role, role, GraphEntity.has_next, GraphEntity.is_held_by, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            results = result["results"]["bindings"]
            dict_ar = dict()
            for x in results:
                role = str(x["role"]["value"]).replace("https://w3id.org/OC/meta/ar/", "")
                if "next" in x:
                    next = str(x["next"]["value"]).replace("https://w3id.org/OC/meta/ar/", "")
                else:
                    next = ""
                agent = str(x["agent"]["value"]).replace("https://w3id.org/OC/meta/ra/", "")

                dict_ar[role] = dict()

                dict_ar[role]["next"] = next
                dict_ar[role]["agent"] = agent

            ar_list = list()

            last = ""
            while dict_ar:
                for x in dict_ar:
                    if dict_ar[x]["next"] == last:
                        if col_name == "publisher":
                            agent_info = self.retrieve_ra_from_meta (dict_ar[x]["agent"], publisher = True) + tuple(dict_ar[x]["agent"])
                        else:
                            agent_info = self.retrieve_ra_from_meta(dict_ar[x]["agent"], publisher = False) + tuple(dict_ar[x]["agent"])
                        ar_dic = dict()
                        ar_dic[x] = agent_info
                        ar_list.append(ar_dic)
                        last = x
                        del dict_ar[x]
                        break
            ar_list.reverse()
            return ar_list
        else:
            return None

    def re_from_meta (self, meta):
        uri = "https://w3id.org/OC/meta/br/" + str(meta)
        query = """
                        SELECT DISTINCT ?re
                        WHERE {
                            <%s> a <%s>.
                            <%s> <%s> ?re.
                        }

                        """ % (uri, GraphEntity.expression, uri, GraphEntity.embodiment)
        result = self.__query(query)
        if result["results"]["bindings"]:
            return result["results"]["bindings"]["re"]["value"].replace("https://w3id.org/OC/meta/re/", "")

