import csv, re, os
from scripts.finder import*


class Converter:

    def __init__(self, data, ts, separator = None, info_dir ="converter_counter/"):

        self.finder = ResourceFinder(ts)
        self.separator = separator
        self.data = data

        # Counter local paths
        self.info_dir = info_dir
        self.br_info_path = info_dir + "br.txt"
        self.id_info_path = info_dir + "id.txt"
        self.ra_info_path = info_dir + "ra.txt"

        self.brdict = {}  # key br metaid; value id
        self.radict = {}  # key ra metaid; value id
        self.vvi = {}  #Venue, Volume, Issue
        self.idra = {}  # key id; value metaid of id related to ra
        self.idbr = {}  # key id; value metaid of id related to br

        self.rameta = dict()
        self.brmeta = dict()


        #wannabe counter
        self.wnb_cnt = 0

        self.rowcnt = 0

        self.conflict_list = list()
        self.data = data

        for row in self.data:
            self.clean_id(row)
            #self.clean_vvi(row)
            self.rowcnt = + 1

        #reset row counter
        self.rowcnt = 0

        for row in self.data:
            self.clean_ra(row, "author")
            self.clean_ra(row, "publisher")
            self.clean_ra(row, "editor")
            self.rowcnt =+ 1

        self.meta_maker()
        self.dry()

        #self.indexer()


    #ID
    def clean_id(self, row):
        #TODO check resource embodiment
        if row['id']:
            if self.separator:
                idslist = re.sub(r'\s*:\s*', ':', row['id']).split(self.separator)
            else:
                idslist = re.split(r'\s+', re.sub(r'\s*:\s*', ':', row['id']))
            name = self.clean_title(row['title'])
            metaval = self.id_worker(row, name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        else:
            name = self.clean_title(row['title'])
            metaval = self.new_entity(self.brdict, name)
        row['id'] = metaval



    # VVI
    def clean_vvi(self, row):
        #todo
        vol_meta = None
        if row["venue"]:
            venue_id = re.search(r'\[\s*(.*?)\s*\]', row["venue"])
            if venue_id:
                venue_id = venue_id.group(1)
                if self.separator:
                    idslist = re.sub(r'\s*\:\s*', ':', venue_id).split(self.separator)
                else:
                    idslist = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', venue_id))
                name = self.clean_name(row['title'])
                metaval = self.id_worker(row, name, idslist, ra_ent=False, br_ent=True, vvi_ent=True, publ_entity=False)


    # RA
    def clean_ra(self, row, col_name):
        #todo authors list + AgentRole
            if row[col_name]:
                ra_list = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row[col_name]) #split authors by ";" outside "[]" (any spaces before and after ";")
                final_ra_list = list()

                #if br has meta and not wannabe ---> a sequence of ra already exists
                if "wannabe" not in row["id"]:
                    #query ra sequence
                    #se non sono presenti nel dizionario aggiungi i ra al radict
                    #crea la nuova sequenza di ra
                    #aggiungi la sequenza nel dizionario
                    #todo abbina ra-ruolo-br nel roledict
                    pass

                for ra in ra_list:
                    #todo se abbiamo una sequenza aggiungi i metaid ai ra (per fare prima)
                    ra_id = re.search(r'\[\s*(.*?)\s*\]', ra) #takes string inside "[]" ignoring any space between (ex: [ TARGET  ] --> TARGET
                    #clean ra id
                    if ra_id:
                        ra_id = ra_id.group(1)
                        if self.separator:
                            ra_id_list = re.sub(r'\s*\:\s*', ':', ra_id).split(self.separator)
                        else:
                            ra_id_list = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', ra_id))
                        name = self.clean_name(re.search(r'\s*(.*?)\s*\[.*?\]', ra).group(1))
                        if col_name == "publisher":
                            metaval = self.id_worker(row, name, ra_id_list, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
                        else:
                            metaval = self.id_worker(row, name, ra_id_list, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
                    else:
                        name = self.clean_name(row[col_name])
                        metaval = self.new_entity(self.radict, name)
                    #todo abbina ra-ruolo-br nel roledict (roledict sarà da aggiornare una volta assegnati i meta finali), se abbiamo la sequenza abbiamo già i ruoli
                    final_ra_list.append(metaval)
                newrow = "; ".join(final_ra_list)
                row[col_name] = newrow


    def find_update_other_ID (self, list2match, metaval, dict2match, temporary_name):
        found_others = self.local_match(list2match, dict2match)
        if found_others["wannabe"]:
            for obj in found_others["wannabe"]:
                for x in dict2match[obj]["ids"]:
                    if x not in dict2match[metaval]["ids"]:
                        dict2match[metaval]["ids"].append(x)

                for x in dict2match[obj]["others"]:
                    if x not in dict2match[metaval]["others"]:
                        dict2match[metaval]["others"].append(x)

                dict2match[metaval]["others"].append(obj)

                if not dict2match[metaval]["title"]:
                    if dict2match[obj]["title"]:
                        dict2match[metaval]["title"] = dict2match[obj]["title"]
                    else:
                        dict2match[metaval]["title"] = temporary_name
                del dict2match[obj]


    @staticmethod
    # All in One recursion, clean ids and meanwhile check if there's metaId
    def clean_id_list(id_list, br = True):
        if br:
            pattern = "br/"
        else:
            pattern = "ra/"
        metaid = ""
        id_list = list(filter(None, id_list))
        for pos, elem in enumerate(id_list):
            try:
                id = elem.split(":")
                value = id[1]
                schema = id[0].lower()
                if schema == "meta":
                    if "meta:" + pattern in elem:
                        metaid = value.replace(pattern, "")
                    else:
                        id_list[pos] = ""
                else:
                    newid = schema + ":" + value
                    id_list[pos] = newid
            except:
                id_list[pos] = ""
        if metaid:
            id_list.remove("meta:" + pattern + metaid)
        id_list = list(filter(None, id_list))
        return id_list, metaid



    def conflict (self, idslist, entity_dict, name, id_dict):
        self.conflict_list.append(self.rowcnt)
        metaval = self.new_entity(entity_dict, name)
        for id in idslist:
            entity_dict[metaval]["ids"].append(id)
            if id not in id_dict:
                ids = id.split(":")
                found_m = self.finder.retrieve_id(ids[0],ids[1])
                if found_m:
                    id_dict[id] = found_m
                else:
                    count = self._add_number(self.id_info_path)
                    id_dict[id] = count
        return metaval



    def finder_sparql(self, list2find, br=True, ra=False, vvi=False, publ=False):
        match_elem = list()
        id_set = set()
        res = None
        for elem in list2find:
            if len(match_elem) < 2:
                id = elem.split(":")
                value = id[1]
                schema = id[0]
                if br:
                    res = self.finder.retrieve_br_from_id(value, schema)
                elif ra:
                    res = self.finder.retrieve_ra_from_id(value, schema, publ)
                elif vvi:
                    res = self.finder.retrieve_venue_from_id(value, schema)
                if res:
                    for f in res:
                        if f[0] not in id_set:
                            match_elem.append(f)
                            id_set.add(f[0])
        return match_elem


    def ra_update(self, row, rowname):
        if row[rowname]:
            ras_list = row[rowname].split("; ")
            for pos, ra in enumerate(ras_list):
                if "wannabe" in ra:
                    for k in self.rameta:
                        if ra in self.rameta[k]["others"]:
                            ras_list[pos] = self.rameta[k]["title"] + " [" + " ".join(self.rameta[k]["ids"]) + "]"
                else:
                    k = ra
                    ras_list[pos] = self.rameta[k]["title"] + " [" + " ".join(self.rameta[k]["ids"]) + "]"
            row[rowname] = "; ".join(ras_list)


    @staticmethod
    def local_match (list2match, dict2match):
        match_elem = dict()
        match_elem["existing"] = list()
        match_elem["wannabe"] = list()
        for elem in list2match:
            for k,va in dict2match.items():
                if elem in va["ids"]:
                    if "wannabe" in k:
                        if k not in match_elem["wannabe"]:
                            match_elem["wannabe"].append(k)
                    else:
                        if k not in match_elem["existing"]:
                            match_elem["existing"].append(k)
        return match_elem

    def meta_maker(self):
        for x in self.brdict:
            if "wannabe" in x:
                other = x
                count = self._add_number(self.br_info_path)
                meta = str(count)
                self.brmeta[meta] = self.brdict[x]
                self.brmeta[meta]["others"].append(other)
                self.brmeta[meta]["ids"].append("meta:br/" + meta)
            else:
                self.brmeta[x] = self.brdict[x]
                self.brmeta[x]["ids"].append("meta:br/" + x)

        for x in self.radict:
            if "wannabe" in x:
                other = x
                count = self._add_number(self.ra_info_path)
                meta = str(count)
                self.rameta[meta] = self.radict[x]
                self.rameta[meta]["others"].append(other)
                self.rameta[meta]["ids"].append("meta:ra/" + meta)

            else:
                self.rameta[x] = self.radict[x]
                self.rameta[x]["ids"].append("meta:ra/" + x)



    def dry(self):
        for row in self.data:
            if "wannabe" in row["id"]:
                for k in self.brmeta:
                    if row["id"] in self.brmeta[k]["others"]:
                        row["id"] = " ".join(self.brmeta[k]["ids"])
                        row["title"] = self.brmeta[k]["title"]
            else:
                k = row["id"]
                row["id"] = " ".join(self.brmeta[k]["ids"])
                row["title"] = self.brmeta[k]["title"]

            self.ra_update(row, "author")
            self.ra_update(row, "publisher")
            self.ra_update(row, "editor")

    '''
    #VVI
    def clean_vvi(self, row):
        vol_meta = None
        if row["venue"]:
            venue_id = re.search(r'\[\s*(.*?)\s*\]', row["venue"])

            if venue_id:
                venue_id = venue_id.group(1)
                if self.separator:
                    venue_id_list = re.sub(r'\s*\:\s*', ':', venue_id).split(self.separator)
                else:
                    venue_id_list = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', venue_id))

                venue_id_list = self.clean_id_list(venue_id_list)

                # check if br exists in temporary dict

                local_match, elem = self.local_any(venue_id_list, self.brdict)
                if local_match:
                    ven_meta = str(elem)
                    newid = "meta:br/" + ven_meta

                    ven = self.brdict[ven_meta]["title"]

                    for x in self.brdict[ven_meta]["ids"]:
                        if x not in venue_id_list:
                            venue_id_list.append(x)

                else:
                    # check if br exists in graph
                    finder_match, found_elem = self.finder_any_vii(venue_id_list)
                    if finder_match:
                        ven_meta = found_elem[0]
                        newid = "meta:br/" + ven_meta

                        title = found_elem[1]
                        ven = title

                        self.brdict[ven_meta] = dict()
                        self.brdict[ven_meta]["ids"] = list()
                        self.brdict[ven_meta]["rows"] = list()
                        self.brdict[ven_meta]["title"] = title

                        existing_ids = found_elem[2]

                        for id in existing_ids:
                            if id[1] not in self.idbr:
                                self.idbr[id[1]] = id[0]
                            if id[1] not in venue_id_list:
                                venue_id_list.append(id[1])

                        if found_elem[3]:
                            #self.vendict[ven_meta] = dict()
                            self.vvi[ven_meta] = found_elem[3]


                    else:
                        # create new br metaid
                        count = self._add_number(self.br_info_path)
                        ven_meta = str(count)
                        newid = "meta:br/" + ven_meta

                        ven = self.clean_title(re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1))

                        self.brdict[ven_meta] = dict()
                        self.brdict[ven_meta]["ids"] = list()
                        self.brdict[ven_meta]["rows"] = list()
                        self.brdict[ven_meta]["title"] = ven

                        self.vvi[ven_meta] = dict()
                        self.vvi[ven_meta]["volume"] = dict()
                        self.vvi[ven_meta]["issue"] = dict()

                id2update = list()

                # add new ids
                for id in venue_id_list:
                    if id not in self.idbr:
                        count = self._add_number(self.id_info_path)
                        self.idbr[id] = count

                    if id not in self.brdict[ven_meta]["ids"]:
                        self.brdict[ven_meta]["ids"].append(id)
                        id2update.append(id)
                    if self.rowcnt not in self.brdict[ven_meta]["rows"]:
                        self.brdict[ven_meta]["rows"].append(self.rowcnt)

                if id2update and len(self.brdict[ven_meta]["rows"]) > 1:
                    self.update_venue(id2update, ven_meta)


                venue_id_list.append(newid)
                newids = " ".join(venue_id_list)
                # br without IDs, new br
            else:
                # if enity has no ID
                count = self._add_number(self.br_info_path)
                ven_meta = str(count)
                newids = "meta:br/" + ven_meta
                ven = self.clean_title(row["venue"])


            row['venue'] = ven + " [" + newids + "]"


        #VOLUME
            if row["volume"] and (row["type"] == "journal issue" or row["type"] == "journal article"):
                vol = row["volume"].strip()
                if ven_meta in self.vvi:
                    if vol in self.vvi[ven_meta]["volume"]:
                            vol_meta = self.vvi[ven_meta]["volume"][vol]["id"]
                    else:
                        count = self._add_number(self.br_info_path)
                        vol_meta = str(count)

                        self.vvi[ven_meta]["volume"][vol] = dict()
                        self.vvi[ven_meta]["volume"][vol]["id"] = vol_meta
                        self.vvi[ven_meta]["volume"][vol]["issue"] = dict()
                else:
                    count = self._add_number(self.br_info_path)
                    vol_meta = str(count)
                newids = "meta:br/" + vol_meta
                row['volume'] = vol + " [" + newids + "]"


        #ISSUE
            if row["issue"] and row["type"] == "journal article":
                issue = row["issue"].strip()
                if ven_meta in self.vvi:
                    if vol_meta:
                        #issue inside vol
                        if issue in self.vvi[ven_meta]["volume"][vol]["issue"]:
                            issue_meta = self.vvi[ven_meta]["volume"][vol]["issue"][issue]
                        else:
                            count = self._add_number(self.br_info_path)
                            issue_meta = str(count)
                            self.vvi[ven_meta]["volume"][vol]["issue"][issue] = issue_meta
                    else:
                        #issue inside venue (without volume)
                        if issue in self.vvi[ven_meta]["issue"]:
                            issue_meta = self.vvi[ven_meta]["issue"][issue]
                        else:
                            count = self._add_number(self.br_info_path)
                            issue_meta = str(count)
                            self.vvi[ven_meta]["issue"][issue] = issue_meta
                else:
                    if vol_meta:
                        row["type"] = "journal article"
                    count = self._add_number(self.br_info_path)
                    issue_meta = str(count)

                newids = "meta:br/" + issue_meta
                row['issue'] = issue + " [" + newids + "]"
    '''

    @staticmethod
    def clean_name(name):
        if "," in name:
            split_name = re.split(r'\s*,\s*', name)
            first_name = split_name[1].split()
            for pos,w in enumerate(first_name):
                first_name[pos] = w.title()
            new_first_name = " ".join(first_name)
            surname = split_name[0].split()
            for pos, w in enumerate(surname):
                surname[pos] = w.title()
            new_surname = " ".join(surname)
            new_name = new_surname + ", " + new_first_name
        else:
            split_name = name.split()
            for pos,w in enumerate(split_name):
                split_name[pos] = w.capitalize()
            new_name = " ".join(split_name)

        return new_name


    @staticmethod
    def clean_title(title):
        words = title.split()
        for pos, w in enumerate(words):
            if any(x.isupper() for x in w):
                pass
            else:
                words[pos] = w.title()
        newtitle = " ".join(words)
        return newtitle

    @staticmethod
    def _read_number(file_path, line_number=1):
        cur_number = 0
        try:
            with open(file_path) as f:
                cur_number = int(f.readlines()[line_number - 1])
        except:
            pass  # Do nothing
        return cur_number

    @staticmethod
    def _add_number(file_path, line_number=1):
        cur_number = Converter._read_number(file_path, line_number) + 1
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        if os.path.exists(file_path):
            with open(file_path) as f:
                all_lines = f.readlines()
        else:
            all_lines = []
        line_len = len(all_lines)
        zero_line_number = line_number - 1
        for i in range(line_number):
            if i >= line_len:
                all_lines += ["\n"]
            if i == zero_line_number:
                all_lines[i] = str(cur_number) + "\n"
        with open(file_path, "w") as f:
            f.writelines(all_lines)
        return cur_number

    '''
    def indexer (self):
        rowsra= list()
        if self.idra:
            for x in self.idra:
                row= dict()
                row["id"] = str(x)
                row["meta"] = str(self.idra[x])
                rowsra.append(row)
        else:
            row = dict()
            row["id"] = ""
            row["meta"] = ""
            rowsra.append(row)

        with open(PATH-RA, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, rowsra[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(rowsra)

        rowsbr = list()
        if self.idbr:
            for x in self.idbr:
                row = dict()
                row["id"] = str(x)
                row["meta"] = str(self.idbr[x])
                rowsra.append(row)
        else:
            row = dict()
            row["id"] = ""
            row["meta"] = ""
            rowsbr.append(row)

        with open(PATH-BR, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, rowsbr[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(rowsbr)
    '''

    def id_worker(self, row, name, idslist, ra_ent=False, br_ent=False, vvi_ent=False, publ_entity=False):

        if not ra_ent:
            id_dict = self.idbr
            entity_dict = self.brdict
            idslist, metaval = self.clean_id_list(idslist)
        else:
            id_dict = self.idra
            entity_dict = self.radict
            idslist, metaval = self.clean_id_list(idslist, br=False)


        # there's meta
        if metaval:

            # meta already in entity_dict (no care about conflicts, we have a meta specified)
            if metaval in entity_dict:
                self.find_update_other_ID(idslist, metaval, entity_dict, name)

                for id in idslist:
                    if id not in entity_dict[metaval]["ids"]:
                        entity_dict[metaval]["ids"].append(id)
                    if id not in id_dict:
                        count = self._add_number(self.id_info_path)
                        id_dict[id] = count

                if not entity_dict[metaval]["title"] and name:
                    entity_dict[metaval]["title"] = name
                row['id'] = metaval

            else:
                found_meta_ts = None
                if vvi_ent:
                    #todo
                    #found_meta_ts = self.finder.retrieve_br_from_meta(metaval)
                    pass
                elif ra_ent:
                    found_meta_ts = self.finder.retrieve_ra_from_meta(metaval, publisher = publ_entity)
                elif br_ent:
                    found_meta_ts = self.finder.retrieve_br_from_meta(metaval)

                # meta in triplestore
                if found_meta_ts:
                    title = found_meta_ts[0]
                    entity_dict[metaval] = dict()
                    entity_dict[metaval]["ids"] = list()
                    entity_dict[metaval]["title"] = title
                    entity_dict[metaval]["others"] = list()

                    self.find_update_other_ID(idslist, metaval, entity_dict, name)

                    for id in idslist:
                        if id not in entity_dict[metaval]["ids"]:
                            entity_dict[metaval]["ids"].append(id)
                        if id not in id_dict:
                            count = self._add_number(self.id_info_path)
                            id_dict[id] = count

                    existing_ids = found_meta_ts[1]

                    for id in existing_ids:
                        if id[1] not in id_dict:
                            id_dict[id[1]] = id[0]
                        if id[1] not in entity_dict[metaval]["ids"]:
                            entity_dict[metaval]["ids"].append(id[1])

                    if not entity_dict[metaval]["title"] and name:
                        entity_dict[metaval]["title"] = name
                    row['id'] = metaval

                # wrong meta
                else:
                    metaval = None

        # there's no meta or there was one but it didn't exist
        if idslist and not metaval:
            local_match = self.local_match(idslist, entity_dict)
            # check in entity_dict
            if local_match["existing"]:
                # ids refer to multiple existing enitities
                if len(local_match["existing"]) > 1:
                    return self.conflict(idslist, entity_dict, name, id_dict)

                # ids refer to ONE existing enitity
                elif len(local_match["existing"]) == 1:
                    metaval = str(local_match["existing"][0])
                    supsected_ids = list()
                    for id in entity_dict[metaval]["ids"]:
                        supsected_ids.append(id)
                    if supsected_ids:
                        sparql_match = self.finder_sparql(supsected_ids, br=br_ent, ra=ra_ent, vvi = vvi_ent, publ = publ_entity)
                        if len(sparql_match) > 1:
                            return self.conflict(idslist, entity_dict, name, id_dict)


            # ids refers to 1 or more wannabe enitities
            elif local_match["wannabe"]:
                metaval = str(local_match["wannabe"][0])
                local_match["wannabe"].pop(0)

                for obj in local_match["wannabe"]:
                    for x in entity_dict[obj]["ids"]:
                        if x not in entity_dict[metaval]["ids"]:
                            entity_dict[metaval]["ids"].append(x)

                    for x in entity_dict[obj]["others"]:
                        if x not in entity_dict[metaval]["others"]:
                            entity_dict[metaval]["others"].append(x)

                    entity_dict[metaval]["others"].append(obj)
                    if entity_dict[obj]["title"]:
                        entity_dict[metaval]["title"] = entity_dict[obj]["title"]
                    del entity_dict[obj]

                if not entity_dict[metaval]["title"]:
                    entity_dict[metaval]["title"] = name

                supsected_ids = list()
                for id in idslist:
                    if id not in entity_dict[metaval]["ids"]:
                        supsected_ids.append(id)
                if supsected_ids:
                    sparql_match = self.finder_sparql(supsected_ids, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                    if sparql_match:
                        if "wannabe" not in metaval or len(sparql_match) > 1:
                            return self.conflict(idslist, entity_dict, name, id_dict)
                        else:
                            existing_ids = sparql_match[0][2]
                            new_idslist = [x[1] for x in existing_ids]
                            new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                            if len(new_sparql_match) > 1:
                                return self.conflict(idslist, entity_dict, name, id_dict)
                            else:
                                old_metaval = metaval
                                metaval = sparql_match[0][0]
                                entity_dict[metaval] = dict()
                                entity_dict[metaval]["ids"] = list()
                                entity_dict[metaval]["others"] = list()
                                entity_dict[metaval]["title"] = ""

                                for x in entity_dict[old_metaval]["ids"]:
                                    if x not in entity_dict[metaval]["ids"]:
                                        entity_dict[metaval]["ids"].append(x)

                                for x in entity_dict[old_metaval]["others"]:
                                    if x not in entity_dict[metaval]["others"]:
                                        entity_dict[metaval]["others"].append(x)

                                entity_dict[metaval]["others"].append(old_metaval)

                                if entity_dict[old_metaval]["title"]:
                                    entity_dict[metaval]["title"] = entity_dict[old_metaval]["title"]
                                del entity_dict[old_metaval]

                                if not entity_dict[metaval]["title"]:
                                    entity_dict[metaval]["title"] = name

                                for id in existing_ids:
                                    if id[1] not in id_dict:
                                        id_dict[id[1]] = id[0]
                                    if id[1] not in entity_dict[metaval]["ids"]:
                                        entity_dict[metaval]["ids"].append(id[1])

            else:
                sparql_match = self.finder_sparql(idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)

                if len(sparql_match) > 1:
                    return self.conflict(idslist, entity_dict, name, id_dict)
                elif len(sparql_match) == 1:
                    existing_ids = sparql_match[0][2]
                    new_idslist = [x[1] for x in existing_ids]
                    new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                    if len(new_sparql_match) > 1:
                        return self.conflict(idslist, entity_dict, name, id_dict)
                    elif len(new_sparql_match) == 1:
                        metaval = sparql_match[0][0]
                        entity_dict[metaval] = dict()
                        entity_dict[metaval]["ids"] = list()
                        entity_dict[metaval]["others"] = list()
                        entity_dict[metaval]["title"] = sparql_match[0][1]
                        if not entity_dict[metaval]["title"] and name:
                            entity_dict[metaval]["title"] = name

                        for id in existing_ids:
                            if id[1] not in id_dict:
                                id_dict[id[1]] = id[0]
                            if id[1] not in entity_dict[metaval]["ids"]:
                                entity_dict[metaval]["ids"].append(id[1])

                else:
                    metaval = "wannabe_" + str(self.wnb_cnt)
                    self.wnb_cnt += 1

                    entity_dict[metaval] = dict()
                    entity_dict[metaval]["ids"] = list()
                    entity_dict[metaval]["others"] = list()
                    entity_dict[metaval]["title"] = name

            for id in idslist:
                if id not in id_dict:
                    count = self._add_number(self.id_info_path)
                    id_dict[id] = count

                if id not in entity_dict[metaval]["ids"]:
                    entity_dict[metaval]["ids"].append(id)

            if not entity_dict[metaval]["title"] and name:
                entity_dict[metaval]["title"] = name

        if not idslist and not metaval:
            metaval = self.new_entity(entity_dict, name)

        return metaval


    def new_entity(self, entity_dict, name):
        metaval = "wannabe_" + str(self.wnb_cnt)
        self.wnb_cnt += 1
        entity_dict[metaval] = dict()
        entity_dict[metaval]["ids"] = list()
        entity_dict[metaval]["others"] = list()
        entity_dict[metaval]["title"] = name
        return metaval