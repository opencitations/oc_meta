import html
import re
import warnings

from bs4 import BeautifulSoup
from oc_idmanager.doi import DOIManager

from oc_meta.lib.master_of_regex import *
from oc_meta.plugins.ra_processor import RaProcessor
from oc_meta.preprocessing.datacite import DatacitePreProcessing

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


class DataciteProcessing(RaProcessor):
    def __init__(self, orcid_index: str = None, doi_csv: str = None, publishers_filepath: str = None, inp_dir: str = None, out_dir: str = None, interval: int = 1000, filter:list = []):
        super(DataciteProcessing, self).__init__(orcid_index, doi_csv, publishers_filepath)
        # self.preprocessor = DatacitePreProcessing(inp_dir, out_dir, interval, filter)
        self.RIS_types_map = {'abst': 'abstract',
  'news': 'newspaper article',
  'slide': 'presentation',
  'book': 'book',
  'data': 'dataset',
  'thes': 'dissertation',
  'jour': 'journal article',
  'mgzn': 'journal article',
  'gen': 'other',
  'advs': 'other',
  'video': 'other',
  'unpb': 'other',
  'ctlg': 'other',
  'art': 'other',
  'case': 'other',
  'icomm': 'other',
  'inpr': 'other',
  'map': 'other',
  'mpct': 'other',
  'music': 'other',
  'pamp': 'other',
  'pat': 'other',
  'pcomm': 'other',
  'catalog': 'other',
  'elec': 'other',
  'hear': 'other',
  'stat': 'other',
  'bill': 'other',
  'unbill': 'other',
  'cpaper': 'proceedings article',
  'rprt': 'report',
  'chap': 'book chapter',
  'ser': 'book series',
  'jfull': 'journal',
  'conf': 'proceedings',
  'comp': 'computer program',
  'sound': 'audio document'}
        self.BIBTEX_types_map = {'book': 'book',
  'mastersthesis': 'dissertation',
  'phdthesis': 'dissertation',
  'article': 'journal article',
  'misc': 'other',
  'unpublished': 'other',
  'manual': 'other',
  'booklet': 'other',
  'inproceedings': 'proceedings article',
  'techreport': 'report',
  'inbook': 'book chapter',
  'incollection': 'book part',
  'proceedings': 'proceedings'}
        self.CITEPROC_types_map = {'book': 'book',
  'dataset': 'dataset',
  'thesis': 'dissertation',
  'article-journal': 'journal article',
  'article': 'other',
  'graphic': 'other',
  'post-weblog': 'web content',
  'paper-conference': 'proceedings article',
  'report': 'report',
  'chapter': 'book chapter',
  'song': 'audio document'}
        self.SCHEMAORG_types_map = {'book': 'book',
  'dataset': 'dataset',
  'thesis': 'dissertation',
  'scholarlyarticle': 'journal article',
  'article': 'journal article',
  'creativework': 'other',
  'event': 'other',
  'service': 'other',
  'mediaobject': 'other',
  'review': 'other',
  'collection': 'other',
  'imageobject': 'other',
  'blogposting': 'web content',
  'report': 'report',
  'chapter': 'book chapter',
  'periodical': 'journal',
  'publicationissue': 'journal issue',
  'publicationvolume': 'journal volume',
  'softwaresourcecode': 'computer program',
  'audioobject': 'audio document'}
        self.RESOURCETYPEGENERAL_types_map = {'book': 'book',
  'dataset': 'dataset',
  'dissertation': 'dissertation',
  'journalarticle': 'journal article',
  'text': 'other',
  'other': 'other',
  'datapaper': 'other',
  'audiovisual': 'other',
  'interactiveresource': 'other',
  'physicalobject': 'other',
  'event': 'other',
  'service': 'other',
  'collection': 'other',
  'image': 'other',
  'model': 'other',
  'peerreview': 'peer review',
  'conferencepaper': 'proceedings article',
  'report': 'report',
  'bookchapter': 'book chapter',
  'journal': 'journal',
  'conferenceproceeding': 'proceedings',
  'standard': 'standard',
  'outputmanagementplan': 'data management plan',
  'preprint': 'preprint',
  'software': 'computer program',
  'sound': 'audio document',
  'workflow': 'workflow'}

    # def input_preprocessing(self):
    #     self.preprocessor.split_input()

    def csv_creator(self, item: dict) -> dict: #input: un dizionario della lista "data" nel json in input
        row = dict()
        doi = DOIManager().normalise(str(item['id']))
        if (doi and self.doi_set and doi in self.doi_set) or (doi and not self.doi_set):
            # create empty row
            keys = ['id', 'title', 'author', 'pub_date', 'venue', 'volume', 'issue', 'page', 'type',
                    'publisher', 'editor']
            for k in keys:
                row[k] = ''

            attributes = item['attributes']

            # row['type']
            if attributes.get('types') is not None:
                types_dict = attributes['types']
                for k,v in types_dict.items():
                    if k.lower() == 'ris':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.RIS_types_map.keys():
                                row['type'] = self.RIS_types_map[norm_v]
                                break
                    if k.lower() == 'bibtex':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.BIBTEX_types_map.keys():
                                row['type'] = self.BIBTEX_types_map[norm_v]
                                break
                    if k.lower() == 'schemaorg':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.SCHEMAORG_types_map.keys():
                                row['type'] = self.SCHEMAORG_types_map[norm_v]
                                break
                    if k.lower() == 'citeproc':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.CITEPROC_types_map.keys():
                                row['type'] = self.CITEPROC_types_map[norm_v]
                                break
                    if k.lower() == 'resourcetypegeneral':
                        if type(v) is str:
                            norm_v = v.strip().lower()
                            if norm_v in self.RESOURCETYPEGENERAL_types_map.keys():
                                row['type'] = self.RESOURCETYPEGENERAL_types_map[norm_v]
                                break


            # row['id']
            ids_list = list()
            ids_list.append(str('doi:' + doi))

            if attributes.get('identifiers'):
                for other_id in attributes.get('identifiers'):
                    if other_id.get('identifier') and other_id.get('identifierType'):
                        o_id_type = other_id.get('identifierType')
                        o_id = other_id.get('identifier')


                        if o_id_type == 'ISBN':
                            if row['type'] in {'book', 'dissertation', 'edited book', 'monograph', 'reference book', 'report',
                                               'standard'}:
                                self.id_worker(o_id, ids_list, self.isbn_worker)

                        elif o_id_type == 'ISSN':
                            if row['type'] in {'book series', 'book set', 'journal', 'proceedings series', 'series',
                                               'standard series', 'report series'}:
                                self.id_worker(o_id, ids_list, self.issn_worker)




            row['id'] = ' '.join(ids_list)

            # row['title']
            pub_title = ""
            if attributes.get("titles"):
                for title in attributes.get("titles"):
                    if title.get("title"):
                        p_title = title.get("title")
                        soup = BeautifulSoup(p_title, 'html.parser')
                        title_soup = soup.get_text().replace('\n', '')
                        title_soup_space_replaced = ' '.join(title_soup.split())
                        title_soup_strip = title_soup_space_replaced.strip()
                        clean_tit = html.unescape(title_soup_strip)
                        pub_title = clean_tit if clean_tit else p_title

            row['title'] = pub_title

            agent_list_authors_only = self.add_authors_to_agent_list(attributes, [])
            agents_list = self.add_editors_to_agent_list(attributes, agent_list_authors_only)

            authors_strings_list, editors_string_list = self.get_agents_strings_list(doi, agents_list)

            # row['author']
            if 'creators' in attributes:
                row['author'] = '; '.join(authors_strings_list)


            # row['pub_date']
            cur_date = ""
            dates = attributes.get("dates")
            if dates:
                for date in dates:
                    if date.get("dateType") == "Issued":
                        cur_date = date.get("date")
                        break

            if cur_date == "":
                if attributes.get("publicationYear"):
                    cur_date = str(attributes.get("publicationYear"))

            row['pub_date'] = cur_date

            # row['venue']
            row['venue'] = self.get_venue_name(attributes, row)

            issue = ""
            volume = ""

            container = attributes.get("container")
            if container and container.get("identifierType") == "ISSN" or container.get(
                    "identifierType") == "ISBN":
                if container.get("issue"):
                    issue = container.get("issue")
                if container.get("volume"):
                    volume = container.get("volume")

            if not issue or not volume:
                relatedIdentifiers = attributes.get("relatedIdentifiers")
                if relatedIdentifiers:
                    for related in relatedIdentifiers:
                        if related.get("relationType"):
                            if related.get("relationType").lower() == "ispartof":
                                if related.get("relatedIdentifierType") == "ISSN" or related.get("relatedIdentifierType") == "ISBN":
                                    if not issue and related.get("issue"):
                                        issue = related.get("issue")
                                    if not volume and related.get("volume"):
                                        volume = related.get("volume")
            # row['volume']
            row['volume'] = volume

            # row['issue']
            row['issue'] = issue

            # row['page']
            row['page'] = self.get_datacite_pages(attributes)

            # row['publisher']
            row['publisher'] = self.get_publisher_name(doi, attributes)

            # row['editor']
            if attributes.get("contributors"):
                editors = [contributor for contributor in attributes.get("contributors") if
                           contributor.get("contributorType") == "Editor"]
                if editors:
                    row['editor'] = '; '.join(editors_string_list)
            try:
                return self.normalise_unicode(row)
            except TypeError:
                print(row)
                raise(TypeError)

    def get_datacite_pages(self, item: dict) -> str:
        '''
        This function returns the pages interval.

        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'START-END', for example, '583-584'. If there are no pages, the output is an empty string.
        '''
        container_pages_list = list()
        related_pages_list = list()
        container = item.get("container")
        if container:
            if container.get("identifierType") == "ISSN" or container.get(
                "identifierType") == "ISBN":
                if container.get("firstPage"):
                    container_pages_list.append(container.get("firstPage"))
                if container.get("lastPage"):
                    container_pages_list.append(container.get("lastPage"))

        relatedIdentifiers = item.get("relatedIdentifiers")
        if relatedIdentifiers:
            for related in relatedIdentifiers:
                if related.get("relationType"):
                    if related.get("relationType").lower() == "ispartof":
                        if related.get("relatedIdentifierType") == "ISSN" or related.get("relatedIdentifierType") == "ISBN":
                            if related.get("firstPage"):
                                related_pages_list.append(related.get("firstPage"))
                            if related.get("lastPage"):
                                related_pages_list.append(related.get("lastPage"))

        page_list = related_pages_list if len(related_pages_list)> len(container_pages_list) else container_pages_list
        return self.get_pages(page_list)

    def get_publisher_name(self, doi: str, item: dict) -> str:
        '''
        This function aims to return a publisher's name and id. If a mapping was provided,
        it is used to find the publisher's standardized name from its id or DOI prefix.

        :params doi: the item's DOI
        :type doi: str
        :params item: the item's dictionary
        :type item: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'American Medical Association (AMA) [crossref:10]'. If the id does not exist, the output is only the name. Finally, if there is no publisher, the output is an empty string.
        '''
        publisher = item.get("publisher")
        if type(publisher) is str:
            if publisher.lower().strip() == "(:unav)":
                publisher = ""

        data = {
            'publisher': publisher,
            'prefix': doi.split('/')[0]
        }

        publisher = data['publisher']
        prefix = data['prefix']

        if self.publishers_mapping:
            member_dict = next(
                    ({member: data} for member, data in self.publishers_mapping.items() if prefix in data['prefixes']),
                    None)
            if member_dict:
                member = list(member_dict.keys())[0]
                name_and_id = f"{member_dict[member]['name']} [datacite:{member}]"
            else:
                name_and_id = publisher
        else:
            name_and_id = publisher

        return name_and_id

    def get_venue_name(self, item: dict, row: dict) -> str:
        '''
        This method deals with generating the venue's name, followed by id in square brackets, separated by spaces.
        HTML tags are deleted and HTML entities escaped. In addition, any ISBN and ISSN are validated.
        Finally, the square brackets in the venue name are replaced by round brackets to avoid conflicts with the ids enclosures.

        :params item: the item's dictionary
        :type item: dict
        :params row: a CSV row
        :type row: dict
        :returns: str -- The output is a string in the format 'NAME [SCHEMA:ID]', for example, 'Nutrition & Food Science
         [issn:0034-6659]'. If the id does not exist, the output is only the name. Finally, if there is no venue,
         the output is an empty string.
         '''

        cont_title = ""
        venids_list = list()

        # container
        container = item.get("container")
        if container:
            # TITLE
            if container.get("title"):
                cont_title = (container["title"].lower()).replace('\n', '')
                ven_soup = BeautifulSoup(cont_title, 'html.parser')
                ventit = html.unescape(ven_soup.get_text())
                ambiguous_brackets = re.search('\[\s*((?:[^\s]+:[^\s]+)?(?:\s+[^\s]+:[^\s]+)*)\s*\]', ventit)
                if ambiguous_brackets:
                    match = ambiguous_brackets.group(1)
                    open_bracket = ventit.find(match) - 1
                    close_bracket = ventit.find(match) + len(match)
                    ventit = ventit[:open_bracket] + '(' + ventit[open_bracket + 1:]
                    ventit = ventit[:close_bracket] + ')' + ventit[close_bracket + 1:]
                    cont_title = ventit

            # IDS
            if container.get("identifierType") == "ISBN":
                if row['type'] in {'book chapter', 'book part', 'book section', 'book track', 'reference entry'}:
                    self.id_worker(container.get("identifier"), venids_list, self.isbn_worker)

            if container.get("identifierType") == "ISSN":
                if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article', 'journal volume',
                                   'journal issue', 'monograph', 'proceedings', 'peer review', 'reference book',
                                   'reference entry', 'report'}:
                    self.id_worker(container.get("identifier"), venids_list, self.issn_worker)
                elif row['type'] == 'report series':
                    if container.get("title"):
                        if container.get("title"):
                            self.id_worker(container.get("identifier"), venids_list, self.issn_worker)


        if not venids_list:
            relatedIdentifiers = item.get("relatedIdentifiers")
            if relatedIdentifiers:
                for related in relatedIdentifiers:
                    if related.get("relationType"):
                        if related.get("relationType").lower() == "ispartof":
                            if related.get("relatedIdentifierType") == "ISBN":
                                if row['type'] in {'book chapter', 'book part', 'book section', 'book track',
                                                   'reference entry'}:
                                    self.id_worker(related.get("relatedIdentifier"), venids_list, self.isbn_worker)
                            if related.get("relatedIdentifierType") == "ISSN":
                                if row['type'] in {'book', 'data file', 'dataset', 'edited book', 'journal article',
                                                   'journal volume',
                                                   'journal issue', 'monograph', 'proceedings', 'peer review',
                                                   'reference book',
                                                   'reference entry', 'report'}:
                                    self.id_worker(related.get("relatedIdentifier"), venids_list, self.issn_worker)
                                elif row['type'] == 'report series':
                                    if related.get("title"):
                                        if related.get("title"):
                                            self.id_worker(related.get("relatedIdentifier"), venids_list, self.issn_worker)

        if venids_list:
            name_and_id = cont_title + ' [' + ' '.join(venids_list) + ']' if cont_title else '[' + ' '.join(venids_list) + ']'
        else:
            name_and_id = cont_title

        return name_and_id

    def add_authors_to_agent_list(self, item: dict, ag_list: list) -> list:
        '''
        This function returns the the agents list updated with the authors dictionaries, in the correct format.

        :params item: the item's dictionary (attributes), ag_list: the
        :type item: dict, ag_list: list

        :returns: listthe agents list updated with the authors dictionaries, in the correct format.
        '''
        agent_list = ag_list
        if item.get("contributors"):
            editors = [contributor for contributor in item.get("contributors") if
                       contributor.get("contributorType") == "Editor"]
            for ed in editors:
                agent = {}
                agent["role"] = "editor"
                if ed.get('name'):
                    agent["name"] = ed.get("name")
                if ed.get("nameType") == "Personal" or ("familyName" in ed or "givenName" in ed):
                    agent["family"] = ed.get("familyName")
                    agent["given"] = ed.get("givenName")
                    if ed.get("nameIdentifiers"):
                        orcid_ids = [x.get("nameIdentifier") for x in ed.get("nameIdentifiers") if
                                     x.get("nameIdentifierScheme") == "ORCID"]
                        if orcid_ids:
                            agent["orcid"] = orcid_ids

                missing_names = [x for x in ["family", "given", "name"] if x not in agent]
                for mn in missing_names:
                    agent[mn] = ""
                agent_list.append(agent)
        return agent_list

    def add_editors_to_agent_list(self, item: dict, ag_list: list) -> list:
        '''
        This function returns the the agents list updated with the editors dictionaries, in the correct format.

        :params item: the item's dictionary (attributes), ag_list: the
        :type item: dict, ag_list: list

        :returns: listthe agents list updated with the editors dictionaries, in the correct format.
        '''
        agent_list = ag_list
        if item.get("creators"):
            creators = item.get("creators")
            for c in creators:
                agent = {}
                agent["role"] = "author"
                if c.get("name"):
                    agent["name"] = c.get("name")
                if c.get("nameType") == "Personal" or ("familyName" in c or "givenName" in c):
                    agent["family"] = c.get("familyName")
                    agent["given"] = c.get("givenName")
                    if c.get("nameIdentifiers"):
                        orcid_ids = [x.get("nameIdentifier") for x in c.get("nameIdentifiers") if
                                     x.get("nameIdentifierScheme") == "ORCID"]
                        if orcid_ids:
                            agent["orcid"] = orcid_ids
                missing_names = [x for x in ["family", "given", "name"] if x not in agent]
                for mn in missing_names:
                    agent[mn] = ""
                agent_list.append(agent)
        return agent_list