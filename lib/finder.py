from typing import List, Dict, Tuple
from oc_ocdm.graph import GraphEntity
from pymantic import sparql


class ResourceFinder:

    def __init__(self, ts_url):
        self.ts = sparql.SPARQLServer(ts_url)

    def __query(self, query):
        result = self.ts.query(query)
        return result

    # _______________________________BR_________________________________ #

    def retrieve_br_from_id(self, schema:str, value:str) -> List[Tuple[str, str, list]]:
        '''
        Given an identifier, it retrieves bibliographic resources associated with that identifier, related titles and other identifiers MetaIDs and literal values.

        :params schema: an identifier schema
        :type schema: str
        :params value: an identifier literal value
        :type value: str
        :returns List[Tuple[str, str, list]]: -- it returns a list of three elements tuples. The first element is the MetaID of a resource associated with the input ID. The second element is a title of that resourse, if present. The third element is a list of MetaID-ID tuples related to identifiers associated with that resource. 
        '''
        schema = GraphEntity.DATACITE + schema
        query = f'''
            SELECT DISTINCT ?res (GROUP_CONCAT(DISTINCT ?title; separator=' ;and; ') AS ?title_)
                (GROUP_CONCAT(DISTINCT ?otherId; separator=' ;and; ') AS ?otherId_)
                (GROUP_CONCAT(?schema; separator=' ;and; ') AS ?schema_)
                (GROUP_CONCAT(DISTINCT ?value; separator=' ;and; ') AS ?value_)
            WHERE {{
                ?res a <{GraphEntity.iri_expression}>;
                    <{GraphEntity.iri_has_identifier}> ?knownId;
                    <{GraphEntity.iri_has_identifier}> ?otherId.
                OPTIONAL {{?res <{GraphEntity.iri_title}> ?title.}}
                ?otherId <{GraphEntity.iri_uses_identifier_scheme}> ?schema;
                    <{GraphEntity.iri_has_literal_value}> ?value.
                ?knownId <{GraphEntity.iri_uses_identifier_scheme}> <{schema}>;
                    <{GraphEntity.iri_has_literal_value}> '{value}'.
            }} GROUP BY ?res
        '''
        results = self.__query(query)
        if results['results']['bindings']:
            bindings = results['results']['bindings']
            result_list = list()
            for result in bindings:
                res = str(result['res']['value']).replace('https://w3id.org/oc/meta/br/', '')
                title = str(result['title_']['value'])
                metaid_list = str(result['otherId_']['value']).replace('https://w3id.org/oc/meta/id/', '').split(' ;and; ')
                id_schema_list = str(result['schema_']['value']).replace(GraphEntity.DATACITE, '').split(' ;and; ')
                id_value_list = str(result['value_']['value']).split(' ;and; ')
                schema_value_list = list(zip(id_schema_list, id_value_list))
                id_list = list()
                for schema, value in schema_value_list:
                    identifier = f'{schema}:{value}'
                    id_list.append(identifier)
                metaid_id_list = list(zip(metaid_list, id_list))
                result_list.append(tuple((res, title, metaid_id_list)))
            return result_list
        else:
            return None

    def retrieve_br_from_meta(self, meta_id):
        uri = "https://w3id.org/oc/meta/br/" + str(meta_id)
        query = """
                SELECT DISTINCT ?res (GROUP_CONCAT(DISTINCT  ?title;separator=' ;and; ') AS ?title_)
                     (GROUP_CONCAT(DISTINCT  ?id;separator=' ;and; ') AS ?id_)
                     (GROUP_CONCAT(?schema;separator=' ;and; ') AS ?schema_)
                     (GROUP_CONCAT(DISTINCT  ?value;separator=' ;and; ') AS ?value_)

                WHERE {
                    ?res a <%s>.
                    OPTIONAL {?res <%s> ?title.}
                    OPTIONAL {?res <%s> ?id.
                        ?id <%s> ?schema.
                        ?id  <%s> ?value.}
                    BIND (<%s> AS ?res)
                } group by ?res
                """ % (GraphEntity.iri_expression, GraphEntity.iri_title, GraphEntity.iri_has_identifier,
                       GraphEntity.iri_uses_identifier_scheme, GraphEntity.iri_has_literal_value, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            title = str(result["title_"]["value"])
            meta_id_list = str(result["id_"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema_"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value_"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                identifier = str(x[0]).lower() + ':' + str(x[1])
                id_list.append(identifier)
            final_list = list(zip(meta_id_list, id_list))

            return title, final_list
        else:
            return None

    # _______________________________ID_________________________________ #

    def retrieve_id(self, schema, value):
        schema = GraphEntity.DATACITE + schema

        query = """
        SELECT DISTINCT ?res 
        WHERE {
            ?res a <%s>.
            ?res <%s> <%s>.
            ?res <%s> ?knownValue.
            filter(?knownValue = "%s")
        } group by ?res

        """ % (GraphEntity.iri_identifier, GraphEntity.iri_uses_identifier_scheme, schema,
                GraphEntity.iri_has_literal_value, value)

        result = self.__query(query)
        if result["results"]["bindings"]:
            return str(result["results"]["bindings"][0]["res"]["value"]).replace("https://w3id.org/oc/meta/id/", "")
        else:
            return None

    # _______________________________RA_________________________________ #
    def retrieve_ra_from_meta(self, meta_id, publisher=False):
        uri = "https://w3id.org/oc/meta/ra/" + str(meta_id)
        query = """
                        SELECT DISTINCT ?res (GROUP_CONCAT(DISTINCT  ?title;separator=' ;and; ') AS ?title_)
                             (GROUP_CONCAT(DISTINCT  ?name;separator=' ;and; ') AS ?name_)
                             (GROUP_CONCAT(DISTINCT  ?surname;separator=' ;and; ') AS ?surname_)
                             (GROUP_CONCAT(DISTINCT  ?id;separator=' ;and; ') AS ?id_)
                             (GROUP_CONCAT(?schema;separator=' ;and; ') AS ?schema_)
                             (GROUP_CONCAT(DISTINCT  ?value;separator=' ;and; ') AS ?value_)

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

                        """ % (GraphEntity.iri_agent, GraphEntity.iri_given_name, GraphEntity.iri_family_name,
                               GraphEntity.iri_name, GraphEntity.iri_has_identifier,
                               GraphEntity.iri_uses_identifier_scheme, GraphEntity.iri_has_literal_value, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            result = result["results"]["bindings"][0]
            if str(result["title_"]["value"]) and publisher:
                title = str(result["title_"]["value"])
            elif str(result["surname_"]["value"]) and not publisher:
                title = str(result["surname_"]["value"]) + ", " + str(result["name_"]["value"])
            else:
                title = ""
            meta_id_list = str(result["id_"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
            id_schema_list = str(result["schema_"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
            id_value_list = str(result["value_"]["value"]).split(" ;and; ")

            couple_list = list(zip(id_schema_list, id_value_list))
            id_list = list()
            for x in couple_list:
                if x[0] and x[1]:
                    identifier = str(x[0]).lower() + ':' + str(x[1])
                    id_list.append(identifier)
            final_list = list(zip(meta_id_list, id_list))

            return title, final_list
        else:
            return None

    def retrieve_ra_from_id(self, value, schema, publisher):
        schema = GraphEntity.DATACITE + schema

        query = """
                SELECT DISTINCT ?res
                    (GROUP_CONCAT(DISTINCT  ?title;separator=' ;and; ') AS ?title_)
                     (GROUP_CONCAT(DISTINCT  ?name;separator=' ;and; ') AS ?name_)
                     (GROUP_CONCAT(DISTINCT  ?surname;separator=' ;and; ') AS ?surname_)
                     (GROUP_CONCAT(DISTINCT  ?id;separator=' ;and; ') AS ?id_)
                     (GROUP_CONCAT(?schema;separator=' ;and; ') AS ?schema_)
                     (GROUP_CONCAT(DISTINCT  ?value;separator=' ;and; ') AS ?value_)

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

                """ % (GraphEntity.iri_agent, GraphEntity.iri_given_name, GraphEntity.iri_family_name,
                       GraphEntity.iri_name, GraphEntity.iri_has_identifier, GraphEntity.iri_uses_identifier_scheme,
                       GraphEntity.iri_has_literal_value, GraphEntity.iri_has_identifier,
                       GraphEntity.iri_uses_identifier_scheme, schema, GraphEntity.iri_has_literal_value, value)

        results = self.__query(query)

        if len(results["results"]["bindings"]):
            result_list = list()
            for result in results["results"]["bindings"]:
                res = str(result["res"]["value"]).replace("https://w3id.org/oc/meta/ra/", "")
                if str(result["title_"]["value"]) and publisher:
                    title = str(result["title_"]["value"])
                elif str(result["surname_"]["value"]) and not publisher:
                    title = str(result["surname_"]["value"]) + ", " + str(result["name_"]["value"])
                else:
                    title = ""
                meta_id_list = str(result["id_"]["value"]).replace("https://w3id.org/oc/meta/id/", "").split(" ;and; ")
                id_schema_list = str(result["schema_"]["value"]).replace(GraphEntity.DATACITE, "").split(" ;and; ")
                id_value_list = str(result["value_"]["value"]).split(" ;and; ")

                couple_list = list(zip(id_schema_list, id_value_list))
                id_list = list()
                for x in couple_list:
                    identifier = str(x[0]).lower() + ':' + str(x[1])
                    id_list.append(identifier)
                final_list = list(zip(meta_id_list, id_list))

                result_list.append(tuple((res, title, final_list)))
            return result_list
        else:
            return None

    # _______________________________VVI_________________________________ #

    def retrieve_venue_from_meta(self, meta_id:str) -> Dict[str, Dict[str, str]]:
        '''
        Given a MetaID, it returns the structure of volumes and issues contained in the related venue.
        The output has the following format: ::

            {
                'issue': {SEQUENCE_IDENTIFIER: {'id': META_ID}},
                'volume': {
                    SEQUENCE_IDENTIFIER: {
                        'id': META_ID,
                        'issue' {SEQUENCE_IDENTIFIER: {'id': META_ID}}
                    }
                }
            }

            {
                'issue': {}, 
                'volume': {
                    '166': {'id': '4388', 'issue': {'4': {'id': '4389'}}}, 
                    '172': {'id': '4434', 
                        'issue': {
                            '22': {'id': '4435'}, 
                            '20': {'id': '4436'}, 
                            '21': {'id': '4437'}, 
                            '19': {'id': '4438'}
                        }
                    }
                }
            }    

        The first level 'issue' field includes the issues contained directly in the venue, 
        while the 'volume' field includes the volumes in the venue and the related issues.

        :params meta_id: a MetaID
        :type meta_id: str
        :returns: Dict[str, Dict[str, str]] -- the string with normalized hyphens
        '''
        content = dict()
        content['issue'] = dict()
        content['volume'] = dict()
        content = self.__retrieve_vvi(meta_id, content)
        return content

    def __retrieve_vvi(self, meta:str, content:Dict[str, dict]) -> dict:
        query = f'''
            SELECT DISTINCT ?res
                (GROUP_CONCAT(DISTINCT ?container; separator=' ;and; ') AS ?container_)
                (GROUP_CONCAT(DISTINCT ?type; separator=' ;and; ') AS ?type_)
                (GROUP_CONCAT(DISTINCT ?title) AS ?title_)
            WHERE {{
                ?res <{GraphEntity.iri_part_of}>+ <https://w3id.org/oc/meta/br/{meta}>;
                    <{GraphEntity.iri_part_of}> ?container;
                    a ?type;
                    <{GraphEntity.iri_has_sequence_identifier}> ?title.
            }} group by ?res
        '''
        result = self.__query(query)
        if result['results']['bindings']:
            results = result['results']['bindings']
            for x in results:
                res = str(x['res']['value']).replace('https://w3id.org/oc/meta/br/', '')
                container = str(x['container_']['value'])
                title = str(x['title_']['value'])
                types = str(x['type_']['value']).split(' ;and; ')
                if str(GraphEntity.iri_journal_issue) in types and self.__is_contained_in_venue(results, container):
                    content['issue'].setdefault(title, dict())
                    content['issue'][title]['id'] = res
                elif str(GraphEntity.iri_journal_volume) in types:
                    content['volume'].setdefault(title, dict())
                    content['volume'][title]['id'] = res
                    content['volume'][title]['issue'] = self.__retrieve_issues_by_volume(results, res)
        return content
    
    def __is_contained_in_venue(self, data:List[Dict[str, Dict[str, str]]], container:str) -> bool:
        is_contained_in_venue = False
        container_dictionary = next(item for item in data if item['res']['value'] == container)
        if str(GraphEntity.iri_journal) in container_dictionary['type_']['value'].split(' ;and; '):
            is_contained_in_venue= True
        return is_contained_in_venue

    def __retrieve_issues_by_volume(self, data:List[Dict[str, Dict[str, str]]], res:str) -> dict:
        content = dict()
        for item in data:
            if res in item['container_']['value'] and str(GraphEntity.iri_journal_issue in item['type_']['value']):
                title = item['title_']['value']
                content[title] = dict()
                content[title]['id'] = item['res']['value'].replace('https://w3id.org/oc/meta/br/', '')
        return content
    
    def retrieve_ra_sequence_from_meta(self, meta_id, col_name):
        if col_name == "author":
            role = GraphEntity.iri_author
        elif col_name == "editor":
            role = GraphEntity.iri_editor
        else:
            role = GraphEntity.iri_publisher
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

                """ % (GraphEntity.iri_expression, GraphEntity.iri_is_document_context_for,
                       GraphEntity.iri_role_in_time, GraphEntity.iri_with_role, role, GraphEntity.iri_has_next,
                       GraphEntity.iri_is_held_by, uri)
        result = self.__query(query)
        if result["results"]["bindings"]:
            results = result["results"]["bindings"]
            dict_ar = dict()
            for x in results:
                role = str(x["role"]["value"]).replace("https://w3id.org/oc/meta/ar/", "")
                if "next" in x:
                    next_role = str(x["next"]["value"]).replace("https://w3id.org/oc/meta/ar/", "")
                else:
                    next_role = ""
                agent = str(x["agent"]["value"]).replace("https://w3id.org/oc/meta/ra/", "")

                dict_ar[role] = dict()

                dict_ar[role]["next"] = next_role
                dict_ar[role]["agent"] = agent

            ar_list = list()

            last = ""
            while dict_ar:
                for x in dict_ar:
                    if dict_ar[x]["next"] == last:
                        if col_name == "publisher":
                            agent_info = self.retrieve_ra_from_meta(dict_ar[x]["agent"], publisher=True) +\
                                         (dict_ar[x]["agent"],)
                        else:
                            agent_info = self.retrieve_ra_from_meta(dict_ar[x]["agent"], publisher=False) +\
                                         (dict_ar[x]["agent"],)
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

    def re_from_meta(self, meta):
        uri = "https://w3id.org/oc/meta/br/" + str(meta)
        query = """
                        SELECT DISTINCT ?re ?sp ?ep
                        WHERE {
                            <%s> a <%s>.
                            <%s> <%s> ?re.
                            ?re <%s> ?sp.
                            ?re <%s> ?ep.
                        }

                        """ % (uri, GraphEntity.iri_expression, uri, GraphEntity.iri_embodiment,
                               GraphEntity.iri_starting_page, GraphEntity.iri_ending_page)
        result = self.__query(query)
        if result["results"]["bindings"]:
            meta = result["results"]["bindings"][0]["re"]["value"].replace("https://w3id.org/oc/meta/re/", "")
            pages = result["results"]["bindings"][0]["sp"]["value"] + "-" +\
                result["results"]["bindings"][0]["ep"]["value"]
            return meta, pages
        else:
            return None

    def retrieve_br_info_from_meta(self, meta_id):
        uri = "https://w3id.org/oc/meta/br/" + str(meta_id)
        query = """
                        SELECT ?res 
                        (GROUP_CONCAT(DISTINCT ?type;separator=' ;and; ') AS ?type_)
                        (GROUP_CONCAT(DISTINCT ?date;separator=' ;and; ') AS ?date_)
                        (GROUP_CONCAT(DISTINCT ?num;separator=' ;and; ') AS ?num_)
                        (GROUP_CONCAT(DISTINCT ?part1;separator=' ;and; ') AS ?part1_)
                        (GROUP_CONCAT(DISTINCT ?title1;separator=' ;and; ') AS ?title1_)
                        (GROUP_CONCAT(DISTINCT ?num1;separator=' ;and; ') AS ?num1_)
                        (GROUP_CONCAT(DISTINCT ?type1;separator=' ;and; ') AS ?type1_)
                        (GROUP_CONCAT(DISTINCT ?part2;separator=' ;and; ') AS ?part2_)
                        (GROUP_CONCAT(DISTINCT ?title2;separator=' ;and; ') AS ?title2_)
                        (GROUP_CONCAT(DISTINCT ?num2;separator=' ;and; ') AS ?num2_)
                        (GROUP_CONCAT(DISTINCT ?type2;separator=' ;and; ') AS ?type2_)
                        (GROUP_CONCAT(DISTINCT ?part3;separator=' ;and; ') AS ?part3_)
                        (GROUP_CONCAT(DISTINCT ?title3;separator=' ;and; ') AS ?title3_)
                        (GROUP_CONCAT(DISTINCT ?num3;separator=' ;and; ') AS ?num3_)
                        (GROUP_CONCAT(DISTINCT ?type3;separator=' ;and; ') AS ?type3_) 

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

                        """ % (GraphEntity.iri_has_publication_date, GraphEntity.iri_has_sequence_identifier,
                               GraphEntity.iri_part_of, GraphEntity.iri_title, GraphEntity.iri_has_sequence_identifier,
                               GraphEntity.iri_part_of, GraphEntity.iri_title, GraphEntity.iri_has_sequence_identifier,
                               GraphEntity.iri_part_of, GraphEntity.iri_title, GraphEntity.iri_has_sequence_identifier,
                               uri)
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

            if "date_" in result:
                res_dict["pub_date"] = result["date_"]["value"]
            else:
                res_dict["pub_date"] = ""

            res_dict["type"] = self.typalo(result, "type_")

            res_dict["page"] = self.re_from_meta(meta_id)

            if "num_" in result:
                if "issue" in self.typalo(result, "type_"):
                    res_dict["issue"] = str(result["num_"]["value"])
                elif "volume" in self.typalo(result, "type_"):
                    res_dict["volume"] = str(result["num_"]["value"])

            res_dict = self.vvi_find(result, "part1_", "type1_", "title1_", "num1_", res_dict)
            res_dict = self.vvi_find(result, "part2_", "type2_", "title2_", "num2_", res_dict)
            res_dict = self.vvi_find(result, "part3_", "type3_", "title3_", "num3_", res_dict)

            return res_dict
        else:
            return None

    @staticmethod
    def typalo(result, type_):
        t_type = ""
        if type_ in result:
            ty = result[type_]["value"].split(" ;and; ")
            for t in ty:
                if "Expression" not in t:
                    t_type = str(t)
                    if str(t_type) == str(GraphEntity.iri_archival_document):
                        t_type = "archival document"
                    if str(t_type) == str(GraphEntity.iri_book):
                        t_type = "book"
                    if str(t_type) == str(GraphEntity.iri_book_chapter):
                        t_type = "book chapter"
                    if str(t_type) == str(GraphEntity.iri_part):
                        t_type = "book part"
                    if str(t_type) == str(GraphEntity.iri_expression_collection):
                        t_type = "book section"
                    if str(t_type) == str(GraphEntity.iri_book_series):
                        t_type = "book series"
                    if str(t_type) == str(GraphEntity.iri_book_set):
                        t_type = "book set"
                    if str(t_type) == str(GraphEntity.iri_data_file):
                        t_type = "data file"
                    if str(t_type) == str(GraphEntity.iri_thesis):
                        t_type = "dissertation"
                    if str(t_type) == str(GraphEntity.iri_journal):
                        t_type = "journal"
                    if str(t_type) == str(GraphEntity.iri_journal_article):
                        t_type = "journal article"
                    if str(t_type) == str(GraphEntity.iri_journal_issue):
                        t_type = "journal issue"
                    if str(t_type) == str(GraphEntity.iri_journal_volume):
                        t_type = "journal volume"
                    if str(t_type) == str(GraphEntity.iri_proceedings_paper):
                        t_type = "proceedings article"
                    if str(t_type) == str(GraphEntity.iri_academic_proceedings):
                        t_type = "proceedings"
                    if str(t_type) == str(GraphEntity.iri_reference_book):
                        t_type = "reference book"
                    if str(t_type) == str(GraphEntity.iri_reference_entry):
                        t_type = "reference entry"
                    if str(t_type) == str(GraphEntity.iri_series):
                        t_type = "series"
                    if str(t_type) == str(GraphEntity.iri_report_document):
                        t_type = "report"
                    if str(t_type) == str(GraphEntity.iri_specification_document):
                        t_type = "standard"
        return t_type

    def vvi_find(self, result, part_, type_, title_, num_, dic):
        type_value = self.typalo(result, type_)
        if "issue" in type_value:
            dic["issue"] = str(result[num_]["value"])
        elif "volume" in type_value:
            dic["volume"] = str(result[num_]["value"])
        elif type_value:
            dic["venue"] = result[title_]["value"] + " [meta:" + result[part_]["value"] \
                .replace("https://w3id.org/oc/meta/", "") + "]"
        return dic
