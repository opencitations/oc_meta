from meta.lib.graphlib import GraphEntity
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
                res = str(result["res"]["value"]).replace("https://w3id.org/oc/meta/br/", "")
                title = str(result["title"]["value"])
                meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
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
        uri = "https://w3id.org/oc/meta/br/" + str(meta_id)
        query = """
                SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                     (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                     (group_concat(?schema;separator=' ;and; ') as ?schema)
                     (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                WHERE {
                    ?res a <%s>.
                    OPTIONAL {?res <%s> ?title.}
                    OPTIONAL {?res <%s> ?id.
                        ?id <%s> ?schema.
                        ?id  <%s> ?value.}
                    filter(?res = <%s>)
                } group by ?res

                """ % (GraphEntity.expression, GraphEntity.title, GraphEntity.has_identifier,
                       GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            title = str(result["title"]["value"])
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
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
            return str(result["res"]["value"]).replace("https://w3id.org/oc/meta/ra/", "")
        else:
            return None




    #_______________________________RA_________________________________#
    def retrieve_ra_from_meta (self, meta_id, publisher = False):
        uri = "https://w3id.org/oc/meta/ra/" + str(meta_id)
        query = """
                        SELECT DISTINCT ?res (group_concat(DISTINCT  ?title;separator=' ;and; ') as ?title)
                             (group_concat(DISTINCT  ?name;separator=' ;and; ') as ?name)
                             (group_concat(DISTINCT  ?surname;separator=' ;and; ') as ?surname)
                             (group_concat(DISTINCT  ?id;separator=' ;and; ') as ?id)
                             (group_concat(?schema;separator=' ;and; ') as ?schema)
                             (group_concat(DISTINCT  ?value;separator=' ;and; ') as ?value)

                        WHERE {
                            ?res a <%s>.
                            OPTIONAL {?res <%s> ?name.}
                            OPTIONAL {?res <%s> ?surname.}
                            OPTIONAL {?res <%s> ?title.}
                            OPTIONAL {?res <%s> ?id.
                                ?id <%s> ?schema.
                                ?id  <%s> ?value.}
                            filter(?res = <%s>)
                        } group by ?res

                        """ % (GraphEntity.agent, GraphEntity.given_name, GraphEntity.family_name, GraphEntity.name, GraphEntity.has_identifier,
                               GraphEntity.uses_identifier_scheme, GraphEntity.has_literal_value, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            if str(result["title"]["value"]) and publisher:
                title = str(result["title"]["value"])
            elif str(result["surname"]["value"]) and not publisher:
                title = str(result["surname"]["value"]) + ", " + str(result["name"]["value"])
            else:
                title = ""
            meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                if x[0] and x[1]:
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
                    OPTIONAL {?res <%s> ?name. }
                    OPTIONAL {?res <%s> ?surname.}
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
                res = str(result["res"]["value"]).replace("https://w3id.org/oc/meta/ra/", "")
                if str(result["title"]["value"]) and publisher:
                    title = str(result["title"]["value"])
                elif str(result["surname"]["value"]) and not publisher:
                    title = str(result["surname"]["value"]) + ", " + str(result["name"]["value"])
                else:
                    title = ""
                meta_id_list = str(result["id"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
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

                """ % (GraphEntity.part_of, "https://w3id.org/oc/meta/br/" + str(meta), GraphEntity.has_sequence_identifier)
        result = self.__query(query)
        if result["results"]["bindings"]:
            results = result["results"]["bindings"]
            for x in results:
                res = str(x["res"]["value"]).replace("https://w3id.org/oc/meta/br/", "")
                title = str(x["title"]["value"])
                types = str(x["type"]["value"]).split(" ;and; ")

                for t in types:
                    if content:
                        if str(t) == str(GraphEntity.FABIO.JournalIssue):
                            content["issue"][title] = res

                        elif str(t) == str(GraphEntity.FABIO.JournalVolume):
                            content["volume"][title] = dict()
                            content["volume"][title]["id"] = res
                            content["volume"][title]["issue"] = dict()
                            content["volume"][title]["issue"] = self.retrieve_vvi(res, None)
                    else:
                        if str(t) == str(GraphEntity.FABIO.JournalIssue):
                            content = dict()
                            content[title] = dict()
                            content[title]['id'] = res
        return content



    def retrieve_ra_sequence_from_meta (self, meta_id, col_name):
        if col_name == "author":
            role = GraphEntity.author
        elif col_name == "editor":
            role = GraphEntity.editor
        else:
            role = GraphEntity.publisher
        uri = "https://w3id.org/oc/meta/br/" + str(meta_id)
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
                role = str(x["role"]["value"]).replace("https://w3id.org/oc/meta/ar/", "")
                if "next" in x:
                    next = str(x["next"]["value"]).replace("https://w3id.org/oc/meta/ar/", "")
                else:
                    next = ""
                agent = str(x["agent"]["value"]).replace("https://w3id.org/oc/meta/ra/", "")

                dict_ar[role] = dict()

                dict_ar[role]["next"] = next
                dict_ar[role]["agent"] = agent

            ar_list = list()

            last = ""
            while dict_ar:
                for x in dict_ar:
                    if dict_ar[x]["next"] == last:
                        if col_name == "publisher":
                            agent_info = self.retrieve_ra_from_meta (dict_ar[x]["agent"], publisher = True) + (dict_ar[x]["agent"],)
                        else:
                            agent_info = self.retrieve_ra_from_meta(dict_ar[x]["agent"], publisher = False) + (dict_ar[x]["agent"],)
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
        uri = "https://w3id.org/oc/meta/br/" + str(meta)
        query = """
                        SELECT DISTINCT ?re ?sp ?ep
                        WHERE {
                            <%s> a <%s>.
                            <%s> <%s> ?re.
                            ?re <%s> ?sp.
                            ?re <%s> ?ep.
                        }

                        """ % (uri, GraphEntity.expression, uri, GraphEntity.embodiment, GraphEntity.starting_page, GraphEntity.ending_page)
        result = self.__query(query)
        if result["results"]["bindings"]:
            meta = result["results"]["bindings"][0]["re"]["value"].replace("https://w3id.org/oc/meta/re/", "")
            pages = result["results"]["bindings"][0]["sp"]["value"] + "-" + result["results"]["bindings"][0]["ep"]["value"]
            return (meta, pages)
        else:
            return None


    def retrieve_br_info_from_meta(self, meta_id):
        uri = "https://w3id.org/oc/meta/br/" + str(meta_id)
        query = """
                        SELECT ?res 
                        (group_concat(DISTINCT  ?type;separator=' ;and; ') as ?type)
                        (group_concat(DISTINCT  ?date;separator=' ;and; ') as ?date)
                        (group_concat(DISTINCT  ?num;separator=' ;and; ') as ?num)
                        (group_concat(DISTINCT  ?part1;separator=' ;and; ') as ?part1)
                        (group_concat(DISTINCT  ?title1;separator=' ;and; ') as ?title1)
                        (group_concat(DISTINCT  ?num1;separator=' ;and; ') as ?num1)
                        (group_concat(DISTINCT  ?type1;separator=' ;and; ') as ?type1)
                        (group_concat(DISTINCT  ?part2;separator=' ;and; ') as ?part2)
                        (group_concat(DISTINCT  ?title2;separator=' ;and; ') as ?title2)
                        (group_concat(DISTINCT  ?num2;separator=' ;and; ') as ?num2)
                        (group_concat(DISTINCT  ?type2;separator=' ;and; ') as ?type2)
                        (group_concat(DISTINCT  ?part3;separator=' ;and; ') as ?part3)
                        (group_concat(DISTINCT  ?title3;separator=' ;and; ') as ?title3)
                        (group_concat(DISTINCT  ?num3;separator=' ;and; ') as ?num3)
                        (group_concat(DISTINCT  ?type3;separator=' ;and; ') as ?type3) 

                        WHERE {
                                ?res a ?type.
                                OPTIONAL {?res <%s> ?date.}
                                OPTIONAL {?res <%s> ?num.}
                                OPTIONAL {?res <%s> ?part1.
                                            OPTIONAL {?part1 <%s> ?title1.}
                                            OPTIONAL {?part1 <%s> ?num1.}
                                            ?part1 a ?type1.
                                            OPTIONAL{?part1 <%s> ?part2.
                                                     OPTIONAL {?part2 <%s> ?title2.}
                                                        OPTIONAL {?part2 <%s> ?num2.}
                                                        ?part2 a ?type2.
                                                     OPTIONAL{?part2 <%s> ?part3.
                                                              OPTIONAL {?part3 <%s> ?title3.}
                                                                OPTIONAL {?part3 <%s> ?num3.}
                                                        ?part3 a ?type3.
                                                    }
                                        }
                                }
                            filter(?res = <%s>)
                        } group by ?res

                        """ % (GraphEntity.has_publication_date, GraphEntity.has_sequence_identifier, GraphEntity.part_of, GraphEntity.title, GraphEntity.has_sequence_identifier, GraphEntity.part_of, GraphEntity.title, GraphEntity.has_sequence_identifier,GraphEntity.part_of, GraphEntity.title, GraphEntity.has_sequence_identifier,uri)
        result = self.__query(query)
        if result["results"]["bindings"]:

            result = result["results"]["bindings"][0]
            res_dict = dict()
            res_dict["pub_date"] = ""
            res_dict["type"] = ""
            res_dict["page"] = ""
            res_dict["issue"] = ""
            res_dict["volume"] = ""
            res_dict["venue"] = ""

            if "date" in result:
                res_dict["pub_date"] = result["date"]["value"]
            else:
                res_dict["pub_date"] = ""

            res_dict["type"] = self.typalo(result, "type")

            res_dict["page"] = self.re_from_meta(meta_id)

            if "num" in result:
                if "issue" in self.typalo(result, "type"):
                    res_dict["issue"] = str(result["num"]["value"])
                elif "volume" in self.typalo(result, "type"):
                    res_dict["volume"] = str(result["num"]["value"])

            res_dict = self.vvi_find(result, "part1", "type1", "title1", "num1", res_dict)
            res_dict = self.vvi_find(result, "part2", "type2", "title2", "num2", res_dict)
            res_dict = self.vvi_find(result, "part3", "type3", "title3", "num3", res_dict)

            return res_dict
        else:
            return None




    @staticmethod
    def typalo(result, type):
        t_type = ""
        if "type" in result:
            ty = result[type]["value"].split(" ;and; ")
            for t in ty:
                if "Expression" not in t:
                    t_type = str(t)
                    if str(t_type) == str(GraphEntity.archival_document):
                        t_type = "archival document"
                    if str(t_type) == str(GraphEntity.book):
                        t_type = "book"
                    if str(t_type) == str(GraphEntity.book_chapter):
                        t_type = "book chapter"
                    if str(t_type) == str(GraphEntity.part):
                        t_type = "book part"
                    if str(t_type) == str(GraphEntity.expression_collection):
                        t_type = "book section"
                    if str(t_type) == str(GraphEntity.book_series):
                        t_type = "book series"
                    if str(t_type) == str(GraphEntity.book_set):
                        t_type = "book set"
                    if str(t_type) == str(GraphEntity.data_file):
                        t_type = "data file"
                    if str(t_type) == str(GraphEntity.thesis):
                        t_type = "dissertation"
                    if str(t_type) == str(GraphEntity.journal):
                        t_type = "journal"
                    if str(t_type) == str(GraphEntity.journal_article):
                        t_type = "journal article"
                    if str(t_type) == str(GraphEntity.journal_issue):
                        t_type = "journal issue"
                    if str(t_type) == str(GraphEntity.journal_volume):
                        t_type = "journal volume"
                    if str(t_type) == str(GraphEntity.proceedings_paper):
                        t_type = "proceedings article"
                    if str(t_type) == str(GraphEntity.academic_proceedings):
                        t_type = "proceedings"
                    if str(t_type) == str(GraphEntity.reference_book):
                        t_type = "reference book"
                    if str(t_type) == str(GraphEntity.reference_entry):
                        t_type = "reference entry"
                    if str(t_type) == str(GraphEntity.series):
                        t_type = "series"
                    if str(t_type) == str(GraphEntity.report_document):
                        t_type = "report"
                    if str(t_type) == str(GraphEntity.specification_document):
                        t_type = "standard"
        return t_type

    def vvi_find(self, result, part, type, title, num, dic):
        typ = self.typalo(result, type)
        if "issue" in typ:
            dic["issue"] = str(result[num]["value"])
        elif "volume" in typ:
            dic["volume"] = str(result[num]["value"])
        elif typ:
            dic["venue"] = result[title]["value"] + " [meta:" + result[part]["value"].replace("https://w3id.org/oc/meta/", "") +"]"
        return dic



