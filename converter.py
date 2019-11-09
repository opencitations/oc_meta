import csv, re, os, json
from scripts.finder import*
from dateutil.parser import parse
from datetime import datetime

class Converter:

    def __init__(self, data, ts, separator = None, info_dir ="converter_counter/", filename = None, path = None):


        self.finder = ResourceFinder(ts)
        self.separator = separator
        self.data = data

        # Counter local paths
        self.info_dir = info_dir
        self.br_info_path = info_dir + "br.txt"
        self.id_info_path = info_dir + "id.txt"
        self.ra_info_path = info_dir + "ra.txt"
        self.ar_info_path = info_dir + "ar.txt"
        self.re_info_path = info_dir + "re.txt"

        self.brdict = {}
        self.radict = {}
        self.ardict = {}
        self.vvi = {}  #Venue, Volume, Issue
        self.idra = {}  # key id; value metaid of id related to ra
        self.idbr = {}  # key id; value metaid of id related to br
        self.conflict_br = {}
        self.conflict_ra = {}

        self.rameta = dict()
        self.brmeta = dict()
        self.armeta = dict()
        self.remeta = dict()

        #wannabe counter
        self.wnb_cnt = 0

        self.rowcnt = 0

        self.log = dict()
        self.new_sequence_list = list()
        self.data = data

        for row in self.data:
            self.log[self.rowcnt] = dict()
            key_list = ["id", "author", "venue", "editor", "publisher", "page", "volume", "issue", "pub_date", "type"]
            for key in key_list:
                self.log[self.rowcnt][key] = dict()
            self.clean_id(row)
            self.rowcnt = + 1

        self.check_equality()

        # reset row counter
        self.rowcnt = 0
        for row in self.data:
            self.clean_vvi(row)
            self.rowcnt = + 1

        #reset row counter
        self.rowcnt = 0

        for row in self.data:
            self.clean_ra(row, "author")
            self.clean_ra(row, "publisher")
            self.clean_ra(row, "editor")
            self.rowcnt =+ 1

        self.brdict.update(self.conflict_br)
        self.radict.update(self.conflict_ra)
        self.meta_maker()
        self.log = self.log_update()
        self.dry()

        self.path = path
        if self.path:
            if not os.path.exists(os.path.dirname(self.path)):
                os.makedirs(os.path.dirname(self.path))

        self.filename = filename
        self.indexer()


    #ID
    def clean_id(self, row):
        if row['title']:
            name = self.clean_title(row['title'])
        else:
            name = ""

        if row['id']:
            if self.separator:
                idslist = re.sub(r'\s*:\s*', ':', row['id']).split(self.separator)
            else:
                idslist = re.split(r'\s+', re.sub(r'\s*:\s*', ':', row['id']))
            metaval = self.id_worker("id", name, idslist, ra_ent=False, br_ent=True, vvi_ent=False, publ_entity=False)
        else:
            metaval = self.new_entity(self.brdict, name)

        row['id'] = metaval

        if "wannabe" not in metaval:
            self.equalizer(row, metaval)

        #page
        if row['page']:
            row['page'] = Converter.string_fix(row['page'].strip())
        #date
        if row['pub_date']:
            date = Converter.string_fix(row['pub_date'].strip())
            try:
                date = self.parse_hack(date)
            except:
                try:
                    if len(date) == 10:
                        try:
                            newdate = date[:-3]
                            date = self.parse_hack(newdate)
                        except:
                            try:
                                newdate = date[:-6]
                                date = self.parse_hack(newdate)
                            except:
                                date = ""
                    elif len(date) == 7:
                        try:
                            newdate = date[:-3]
                            date = self.parse_hack(newdate)
                        except:
                            date = ""
                except:
                    date = ""

            row['pub_date'] = date

        #type
        if row['type']:
            type = " ".join((row['type'].strip().lower()).split())
            if type == "edited book" or type == "monograph":
                type = "book"
            elif type == "report series" or type == "standard series":
                type = "series"
            if type in {"archival document" ,"book" ,"book chapter" ,"book part" , "book section" ,"book series" ,"book set" ,"data file" ,"dissertation" ,"journal" ,"journal article" ,"journal issue" ,"journal volume" ,"proceedings article" ,"proceedings" ,"reference book" ,"reference entry" ,"series" ,"report" ,"standard"}:
                row['type'] = type
            else:
                row['type'] = ""


    # VVI
    def clean_vvi(self, row):
        vol_meta = None
        if row["venue"]:
            venue_id = re.search(r'\[\s*(.*?)\s*\]', row["venue"])
            if venue_id:
                name = self.clean_title(re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1))
                venue_id = venue_id.group(1)
                if self.separator:
                    idslist = re.sub(r'\s*\:\s*', ':', venue_id).split(self.separator)
                else:
                    idslist = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', venue_id))
                metaval = self.id_worker("venue", name, idslist, ra_ent=False, br_ent=True, vvi_ent=True, publ_entity=False)

                if metaval not in self.vvi:
                    ts_vvi = None
                    if "wannabe" not in metaval:
                        ts_vvi = self.finder.retrieve_venue_from_meta(metaval)

                    if "wannabe" in metaval or not ts_vvi:
                        self.vvi[metaval] = dict()
                        self.vvi[metaval]["volume"] = dict()
                        self.vvi[metaval]["issue"] = dict()
                    elif ts_vvi:
                            self.vvi[metaval] = ts_vvi

            else:
                name = self.clean_title(row['venue'])
                metaval = self.new_entity(self.brdict, name)
                self.vvi[metaval] = dict()
                self.vvi[metaval]["volume"] = dict()
                self.vvi[metaval]["issue"] = dict()

            row["venue"] = metaval

        # VOLUME
            if row["volume"] and (row["type"] == "journal issue" or row["type"] == "journal article"):
                vol = row["volume"].strip()
                row["volume"] = vol
                if vol in self.vvi[metaval]["volume"]:
                    vol_meta = self.vvi[metaval]["volume"][vol]["id"]
                else:
                    vol_meta = self.new_entity(self.brdict, "")
                    self.vvi[metaval]["volume"][vol] = dict()
                    self.vvi[metaval]["volume"][vol]["id"] = vol_meta
                    self.vvi[metaval]["volume"][vol]["issue"] = dict()


            elif row['volume'] and row["type"] == "journal volume":
                vol = row["volume"].strip()
                row["volume"] = ""
                row["issue"] = ""
                vol_meta = row["id"]
                self.volume_issue(vol_meta, self.vvi[metaval]["volume"], vol, row)

            # ISSUE
            if row["issue"] and row["type"] == "journal article":
                issue = row["issue"].strip()
                row["issue"] = issue
                if vol_meta:
                    # issue inside vol
                    if issue not in self.vvi[metaval]["volume"][vol]["issue"]:
                        issue_meta = self.new_entity(self.brdict, "")
                        self.vvi[metaval]["volume"][vol]["issue"][issue] = dict()
                        self.vvi[metaval]["volume"][vol]["issue"][issue]['id'] = issue_meta

                else:
                    # issue inside venue (without volume)
                    if issue not in self.vvi[metaval]["issue"]:
                        issue_meta = self.new_entity(self.brdict, "")
                        self.vvi[metaval]["issue"][issue] = dict()
                        self.vvi[metaval]["issue"][issue]['id'] = issue_meta

            elif row["issue"] and row["type"] == "journal issue":
                issue = row["issue"].strip()
                row["issue"] = ""
                issue_meta = row["id"]
                if vol_meta:
                    self.volume_issue(issue_meta, self.vvi[metaval]["volume"][vol]["issue"], issue, row)
                else:
                    self.volume_issue(issue_meta, self.vvi[metaval]["issue"], issue, row)
        else:
            row["venue"] = ""
            row["volume"] = ""
            row["issue"] = ""

    # RA
    def clean_ra(self, row, col_name):
            if row[col_name]:
                ra_list = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row[col_name]) #split authors by ";" outside "[]" (any spaces before and after ";")
                if row["id"] in self.brdict:
                    br_metaval = row["id"]
                else:
                    for x in self.brdict:
                        if row["id"] in self.brdict[x]["others"]:
                            br_metaval = x
                            break

                if br_metaval not in self.ardict or not self.ardict[br_metaval][col_name]:
                    #new sequence
                    if "wannabe" in br_metaval:
                        if br_metaval not in self.ardict:
                            self.ardict[br_metaval] = dict()
                            self.ardict[br_metaval]["author"] = list()
                            self.ardict[br_metaval]["editor"] = list()
                            self.ardict[br_metaval]["publisher"] = list()
                        sequence = []
                    else:
                        #sequence can be in TS
                        sequence_found = self.finder.retrieve_ra_sequence_from_meta(br_metaval, col_name)
                        if sequence_found:
                            sequence = []
                            for x in sequence_found:
                                for k in x:
                                    sequence.append(tuple((k, x[k][2])))
                                    if x[k][2] not in self.radict:
                                        self.radict[x[k][2]] = dict()
                                        self.radict[x[k][2]]["ids"] = list()
                                        self.radict[x[k][2]]["others"] = list()
                                        self.radict[x[k][2]]["title"] = x[k][0]
                                    for i in x[k][1]:
                                        if i[0] not in self.idra:
                                            self.idra[i[1]] = i[0]
                                        if i[1] not in self.radict[x[k][2]]["ids"]:
                                            self.radict[x[k][2]]["ids"].append(i[1])

                            if br_metaval not in self.ardict:
                                self.ardict[br_metaval] = dict()
                                self.ardict[br_metaval]["author"] = list()
                                self.ardict[br_metaval]["editor"] = list()
                                self.ardict[br_metaval]["publisher"] = list()
                                self.ardict[br_metaval][col_name].extend(sequence)
                            else:
                                self.ardict[br_metaval][col_name].extend(sequence)
                        else:
                            # totally new sequence
                            if br_metaval not in self.ardict:
                                self.ardict[br_metaval] = dict()
                                self.ardict[br_metaval]["author"] = list()
                                self.ardict[br_metaval]["editor"] = list()
                                self.ardict[br_metaval]["publisher"] = list()
                            sequence = []
                else:
                    sequence = self.ardict[br_metaval][col_name]

                new_sequence = list()
                change_order = False
                for pos,ra in enumerate(ra_list):
                    new_elem_seq = True
                    ra_id = re.search(r'\[\s*(.*?)\s*\]', ra) #takes string inside "[]" ignoring any space between (ex: [ TARGET  ] --> TARGET
                    if ra_id:
                        ra_id = ra_id.group(1)
                        name = self.clean_name(re.search(r'\s*(.*?)\s*\[.*?\]', ra).group(1))
                    else:
                        name = self.clean_name(ra)

                    if not ra_id and sequence:
                        for x, k in sequence:
                            if self.radict[k]["title"] == name:
                                ra_id = "meta:ra/" + str(k)
                                new_elem_seq = False
                                break
                    if ra_id:
                        #ra_id = ra_id.group(1)
                        if self.separator:
                            ra_id_list = re.sub(r'\s*\:\s*', ':', ra_id).split(self.separator)
                        else:
                            ra_id_list = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', ra_id))

                        if sequence:
                            kv = None
                            for ps, el in enumerate(sequence):
                                k = el[1]
                                for i in ra_id_list:
                                    if i in self.radict[k]['ids']:
                                        if ps != pos:
                                            change_order = True
                                        new_elem_seq = False
                                        if "wannabe" not in k:
                                            kv = k
                                            for pos,i in enumerate(ra_id_list):
                                                if "meta" in i:
                                                    ra_id_list[pos] = ""
                                                break
                                            ra_id_list = list(filter(None, ra_id_list))
                                            ra_id_list.append("meta:ra/" + kv)
                            if not kv:
                                #new element
                                for x, k in sequence:
                                    if self.radict[k]['title'] == name:
                                        new_elem_seq = False
                                        if "wannabe" not in k:
                                            kv = k
                                            for pos, i in enumerate(ra_id_list):
                                                if "meta" in i:
                                                    ra_id_list[pos] = ""
                                                break
                                            ra_id_list = list(filter(None, ra_id_list))
                                            ra_id_list.append("meta:ra/" + kv)

                        if col_name == "publisher":
                            metaval = self.id_worker("publisher", name, ra_id_list, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=True)
                        else:
                            metaval = self.id_worker(col_name, name, ra_id_list, ra_ent=True, br_ent=False, vvi_ent=False, publ_entity=False)
                    else:
                        metaval = self.new_entity(self.radict, name)
                    if new_elem_seq:
                        added_element = True
                        role = self._add_number(self.ar_info_path)
                        new_sequence.append(tuple((role, metaval)))
                        self.new_sequence_list.append(tuple((self.rowcnt, role,  metaval)))
                if change_order:
                    self.log[self.rowcnt][col_name]["Info"] = "Proposed new RA sequence: REFUSED"

                sequence.extend(new_sequence)
                self.ardict[br_metaval][col_name] = sequence


    def find_update_other_ID (self, list2match, metaval, dict2match, temporary_name):
        found_others = self.local_match(list2match, dict2match)
        if found_others["wannabe"]:
            for obj in found_others["wannabe"]:
                self.update(dict2match, metaval, obj, temporary_name)

    @staticmethod
    def update(dict2match, metaval, old_meta, temporary_name):
        for x in dict2match[old_meta]["ids"]:
            if x not in dict2match[metaval]["ids"]:
                dict2match[metaval]["ids"].append(x)

        for x in dict2match[old_meta]["others"]:
            if x not in dict2match[metaval]["others"]:
                dict2match[metaval]["others"].append(x)

        dict2match[metaval]["others"].append(old_meta)

        if not dict2match[metaval]["title"]:
            if dict2match[old_meta]["title"]:
                dict2match[metaval]["title"] = dict2match[old_meta]["title"]
            else:
                dict2match[metaval]["title"] = temporary_name
        del dict2match[old_meta]

    @staticmethod
    # All in One recursion, clean ids and meanwhile check if there's metaId
    def clean_id_list(id_list, br = True):
        if br:
            pattern = "br/"
        else:
            pattern = "ra/"
        metaid = ""
        id_list = list(filter(None, id_list))
        how_many_meta = [i for i in id_list if i.lower().startswith('meta')]
        if len(how_many_meta) > 1:
            for pos, elem in enumerate(id_list):
                if "meta" in elem:
                    id_list[pos] = ""
        else:
            for pos, elem in enumerate(id_list):
                try:
                    elem = Converter.string_fix(elem)
                    id = elem.split(":", 1)
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



    def conflict (self, idslist, name, id_dict, col_name):
        if col_name == "id" or col_name == "venue":
            entity_dict = self.conflict_br
            metaval = self.new_entity(entity_dict, name)
        elif col_name == "author" or col_name == "editor" or col_name == "publisher":
            entity_dict = self.conflict_ra
            metaval = self.new_entity(entity_dict, name)
        self.log[self.rowcnt][col_name]['Conflict Entity'] = metaval
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
                if res:
                    for f in res:
                        if f[0] not in id_set:
                            match_elem.append(f)
                            id_set.add(f[0])
        return match_elem


    def ra_update(self, row, br_key, col_name):
        if row[col_name]:
            sequence = self.armeta[br_key][col_name]
            ras_list = list()
            for x,y in sequence:
                ra_name = self.rameta[y]["title"]
                ra_ids = " ".join(self.rameta[y]["ids"])
                ra = ra_name+ " [" + ra_ids + "]"
                ras_list.append(ra)
            row[col_name] = "; ".join(ras_list)


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


    def meta_ar(self, newkey, oldkey, role):
        for x,k in self.ardict[oldkey][role]:
            if "wannabe" in k:
                for m in self.rameta:
                    if k in self.rameta[m]['others']:
                        new_v = m
                        break
            else:
                new_v = k
            self.armeta[newkey][role].append(tuple((x, new_v)))


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

        for x in self.ardict:
            if "wannabe" in x:
                for w in self.brmeta:
                    if x in self.brmeta[w]["others"]:
                        br_key = w
                        break
            else:
                br_key = x

            self.armeta[br_key] = dict()
            self.armeta[br_key]["author"] = list()
            self.armeta[br_key]["editor"] = list()
            self.armeta[br_key]["publisher"] = list()

            self.meta_ar(br_key, x, "author")
            self.meta_ar(br_key, x, "editor")
            self.meta_ar(br_key, x, "publisher")




    def dry(self):
        for row in self.data:
            if "wannabe" in row["id"]:
                for i in self.brmeta:
                    if row["id"] in self.brmeta[i]["others"]:
                        k = i
            else:
                k = row["id"]

            if row["page"] and (k not in self.remeta):
                re = self.finder.re_from_meta(k)
                if re:
                    self.remeta[k] = re
                    row["page"] = re[1]
                else:
                    count = self._add_number(self.re_info_path)
                    page = row["page"].strip()
                    self.remeta[k] = (count, page)
                    row["page"] = page
            elif k in self.remeta:
                row["page"] = self.remeta[k][1]

            self.ra_update(row, k, "author")
            self.ra_update(row, k, "publisher")
            self.ra_update(row, k, "editor")
            row["id"] = " ".join(self.brmeta[k]["ids"])
            row["title"] = self.brmeta[k]["title"]

            if row["venue"]:
                venue= row["venue"]
                if "wannabe" in venue:
                    for i in self.brmeta:
                        if venue in self.brmeta[i]["others"]:
                            ve = i
                else:
                    ve = venue
                row["venue"] = self.brmeta[ve]["title"] + " [" + " ".join(self.brmeta[ve]["ids"]) + "]"

    @staticmethod
    def name_check(ts_name, name):
        if "," in ts_name:
            names = ts_name.split(",")
            if names[0] and not names[1].strip():
                #there isn't a given name in ts
                if "," in name:
                    gname = name.split(", ")[1]
                    if gname.strip():
                        ts_name = names[0] + ", " + gname
        return ts_name


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
            if new_surname:
                new_name = new_surname + ", " + new_first_name
            else:
                new_name = ""
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


    @staticmethod
    def write_csv(path, list):
        with open(path, 'w', newline='',encoding="utf-8") as output_file:
            dict_writer = csv.DictWriter(output_file, list[0].keys(), delimiter='\t')
            dict_writer.writeheader()
            dict_writer.writerows(list)

    @staticmethod
    def write_txt():
        pass

    def indexer (self):
        if self.path:
            path = self.path
        else:
            path = ""
        #ID
        index_id_ra= list()
        if self.idra:
            for x in self.idra:
                row= dict()
                row["id"] = str(x)
                row["meta"] = str(self.idra[x])
                index_id_ra.append(row)
        else:
            row = dict()
            row["id"] = ""
            row["meta"] = ""
            index_id_ra.append(row)

        if self.filename:
            ra_path = path + "index_id_ra_" + self.filename + ".csv"
            self.write_csv(ra_path, index_id_ra)
        self.index_id_ra = index_id_ra

        index_id_br = list()
        if self.idbr:
            for x in self.idbr:
                row = dict()
                row["id"] = str(x)
                row["meta"] = str(self.idbr[x])
                index_id_br.append(row)
        else:
            row = dict()
            row["id"] = ""
            row["meta"] = ""
            index_id_br.append(row)
        if self.filename:
            br_path = path + "index_id_br_" + self.filename + ".csv"
            self.write_csv(br_path, index_id_br)
        self.index_id_br = index_id_br


        #AR
        ar_index = list()
        if self.armeta:
            for x in self.armeta:
                index = dict()
                index["meta"] = x
                for y in self.armeta[x]:
                    list_ar = list()
                    for ar, id in self.armeta[x][y]:
                        list_ar.append(str(ar) + ", " +  str(id))
                    index[y] = "; ".join(list_ar)
                ar_index.append(index)
        else:
            row = dict()
            row["meta"] = ""
            row["author"] = ""
            row["editor"] = ""
            row["publisher"] = ""
            ar_index.append(row)
        if self.filename:
            ar_path = path + "index_ar_" + self.filename + ".csv"
            self.write_csv(ar_path, ar_index)
        self.ar_index = ar_index

        #RE
        re_index = list()
        if self.remeta:
            for x in self.remeta:
                r = dict()
                r["br"] = x
                r["re"] = str(self.remeta[x][0])
                re_index.append(r)
        else:
            row = dict()
            row["br"] = ""
            row["re"] = ""
            re_index.append(row)
        if self.filename:
            re_path = path + "index_re_" + self.filename + ".csv"
            self.write_csv(re_path, re_index)
        self.re_index = re_index


        #VI
        vi_index = list()
        self.VolIss = dict()
        if self.vvi:
            for x in self.vvi:
                if self.vvi[x]["issue"]:
                    for iss in self.vvi[x]["issue"]:
                        if "wannabe" in self.vvi[x]["issue"][iss]["id"]:
                            for i in self.brmeta:
                                if self.vvi[x]["issue"][iss]["id"] in self.brmeta[i]["others"]:
                                    self.vvi[x]["issue"][iss]["id"] = str(i)
                if self.vvi[x]["volume"]:
                    for vol in self.vvi[x]["volume"]:
                        if "wannabe" in self.vvi[x]["volume"][vol]["id"]:
                            for i in self.brmeta:
                                if self.vvi[x]["volume"][vol]["id"] in self.brmeta[i]["others"]:
                                    self.vvi[x]["volume"][vol]["id"] = str(i)
                        if self.vvi[x]["volume"][vol]["issue"]:
                            for iss in self.vvi[x]["volume"][vol]["issue"]:
                                if "wannabe" in self.vvi[x]["volume"][vol]["issue"][iss]["id"]:
                                    for i in self.brmeta:
                                        if self.vvi[x]["volume"][vol]["issue"][iss]["id"] in self.brmeta[i]["others"]:
                                            self.vvi[x]["volume"][vol]["issue"][iss]["id"] = str(i)
                if "wannabe" in x:
                    for i in self.brmeta:
                        if x in self.brmeta[i]["others"]:
                            self.VolIss[i] = self.vvi[x]
                else:
                    self.VolIss[x] = self.vvi[x]
        if self.filename:
            vvi_file = path + "index_vi_" + self.filename + ".json"
            with open(vvi_file, 'w') as fp:
                json.dump(self.VolIss, fp)

        if self.log:
            log_file = path + "log_" + self.filename + ".json"
            with open(log_file, 'w') as lf:
                json.dump(self.log, lf)

        if self.data:
            data_file = path + "data_" + self.filename + ".csv"
            self.write_csv(data_file, self.data)


    def id_worker(self, col_name, name, idslist, ra_ent=False, br_ent=False, vvi_ent=False, publ_entity=False):

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

            else:
                found_meta_ts = None
                if ra_ent:
                    found_meta_ts = self.finder.retrieve_ra_from_meta(metaval, publisher = publ_entity)
                elif br_ent:
                    found_meta_ts = self.finder.retrieve_br_from_meta(metaval)

                # meta in triplestore
                if found_meta_ts:
                    entity_dict[metaval] = dict()
                    entity_dict[metaval]["ids"] = list()
                    if col_name == "author" or col_name == "editor":
                        entity_dict[metaval]["title"] = self.name_check(found_meta_ts[0], name)
                    else:
                        entity_dict[metaval]["title"] = found_meta_ts[0]
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
                    return self.conflict(idslist, name, id_dict, col_name)

                # ids refer to ONE existing enitity
                elif len(local_match["existing"]) == 1:
                    metaval = str(local_match["existing"][0])
                    supsected_ids = list()
                    for id in idslist:
                        if id not in entity_dict[metaval]["ids"]:
                            supsected_ids.append(id)
                    if supsected_ids:
                        sparql_match = self.finder_sparql(supsected_ids, br=br_ent, ra=ra_ent, vvi = vvi_ent, publ = publ_entity)
                        if len(sparql_match) > 1:
                            return self.conflict(idslist, name, id_dict, col_name)


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
                            return self.conflict(idslist, name, id_dict, col_name)
                        else:
                            existing_ids = sparql_match[0][2]
                            new_idslist = [x[1] for x in existing_ids]
                            new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                            if len(new_sparql_match) > 1:
                                return self.conflict(idslist, name, id_dict, col_name)
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
                    return self.conflict(idslist, name, id_dict, col_name)
                elif len(sparql_match) == 1:
                    existing_ids = sparql_match[0][2]
                    new_idslist = [x[1] for x in existing_ids]
                    new_sparql_match = self.finder_sparql(new_idslist, br=br_ent, ra=ra_ent, vvi=vvi_ent, publ=publ_entity)
                    if len(new_sparql_match) > 1:
                        return self.conflict(idslist, name, id_dict, col_name)
                    elif len(new_sparql_match) == 1:
                        metaval = sparql_match[0][0]
                        entity_dict[metaval] = dict()
                        entity_dict[metaval]["ids"] = list()
                        entity_dict[metaval]["others"] = list()
                        if col_name == "author" or col_name == "editor":
                            entity_dict[metaval]["title"] = self.name_check(sparql_match[0][1], name)
                        else:
                            entity_dict[metaval]["title"] = sparql_match[0][1]

                        if not entity_dict[metaval]["title"] and name:
                            entity_dict[metaval]["title"] = name

                        for id in existing_ids:
                            if id[1] not in id_dict:
                                id_dict[id[1]] = id[0]
                            if id[1] not in entity_dict[metaval]["ids"]:
                                entity_dict[metaval]["ids"].append(id[1])

                else:
                    metaval = self.new_entity(entity_dict, name)


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


    def volume_issue (self, meta, path, value, row):
        if "wannabe" not in meta:
            if value in path:
                if "wannabe" in path[value]["id"]:
                    old_meta = path[value]["id"]
                    self.update(self.brdict, meta, old_meta, row["title"])
                    path[value]["id"] = meta
                else:
                    if meta == path[value]["id"]:
                        raise ValueError('A very specific bad thing happened! A Volume or Issue in conflict at row ' + str(self.rowcnt))
                    pass
            else:
                path[value] = dict()
                path[value]["id"] = meta
                if "issue" not in path:
                    path[value]["issue"] = dict()
        else:
            if value in path:
                if "wannabe" in path[value]["id"]:
                    old_meta = path[value]["id"]
                    self.update(self.brdict, meta, old_meta, row["title"])
                    path[value]["id"] = meta
                else:
                    old_meta = path[value]["id"]
                    if "wannabe" not in old_meta and old_meta not in self.brdict:
                        br4dict = self.finder.retrieve_br_from_meta(old_meta)
                        self.brdict[old_meta] = dict()
                        self.brdict[old_meta]['ids'] = list()
                        self.brdict[old_meta]['others'] = list()
                        self.brdict[old_meta]['title'] = br4dict[0]
                        for x in br4dict[1]:
                            id = x[1]
                            self.brdict[old_meta]['ids'].append(id)
                            if id not in self.idbr:
                                self.idbr[id] = x[0]

                    self.update(self.brdict, old_meta, meta, row["title"])
            else:
                path[value] = dict()
                path[value]["id"] = meta
                if "issue" not in path: #it's a Volume
                    path[value]["issue"] = dict()

    def log_update(self):
        new_log = dict()
        for x in self.log:
            if any(self.log[x][y].values() for y in self.log[x]):
                for y in self.log[x]:
                    if "Conflict Entity" in self.log[x][y]:
                        v = self.log[x][y]["Conflict Entity"]
                        if "wannabe" in v:
                            if y == "id" or y == "venue":
                                for brm in self.brmeta:
                                    if v in self.brmeta[brm]["others"]:
                                        m = "br/" + str(brm)
                            elif y == "author" or y == "editor" or y == "publisher":
                                for ram in self.rameta:
                                    if v in self.rameta[ram]["others"]:
                                        m = "ra/" + str(ram)
                        else:
                            m = v
                        self.log[x][y]["Conflict Entity"] = m
                new_log[x] = self.log[x]

                if "wannabe" in self.data[x]["id"]:
                    for brm in self.brmeta:
                        if self.data[x]["id"] in self.brmeta[brm]["others"]:
                            met = "br/" + str(brm)
                else:
                    met = "br/" + str(self.data[x]["id"])
                new_log[x]["id"]["meta"] = met
        return new_log


    @staticmethod
    def string_fix(st):
        dash_list = ["‐", "–", "—", "−", "‑", "⁃", "­"]     #Hyphen, En-Dash, Em-Dash, Minus Sign, Non-breaking Hyphen, Hyphen Bullet, Soft Hyphen
        for d in dash_list:
            if d in st:
                    st.replace(d, "-")
        if "isbn:" in st:
            st.replace("-", "")
        return st


    @staticmethod
    #hack dateutil automatic-today-date
    def parse_hack(date):
        dt = parse(date, default=datetime(2001, 1, 1))
        dt2 = parse(date, default=datetime(2002, 2, 2))

        if dt.year == dt2.year and dt.month == dt2.month and dt.day == dt2.day:
            clean_date = parse(date).strftime("%Y-%m-%d")
        elif dt.year == dt2.year and dt.month == dt2.month:
            clean_date = parse(date).strftime("%Y-%m")
        elif dt.year == dt2.year:
            clean_date = parse(date).strftime("%Y")
        else:
            clean_date = ""

        return clean_date


    def check_equality(self):
        for row in self.data:
            if "wannabe" in row["id"]:
                for i in self.brdict:
                    if row["id"] in self.brdict[i]["others"] and "wannabe" not in i:
                        row["id"] = i
                        self.equalizer(row, i)




    def equalizer(self, row, metaval):
        self.log[self.rowcnt]["id"]["status"] = "ENTITY ALREADY EXISTS"
        known_data = self.finder.retrieve_br_info_from_meta(metaval)
        row["venue"] = known_data["venue"]
        row["volume"] = known_data["volume"]
        row["issue"] = known_data["issue"]
        if known_data["pub_date"]:
            row["pub_date"] = known_data["pub_date"]
        elif row["pub_date"]:
            self.log[self.rowcnt]["pub_date"]["status"] = "NEW VALUE PROPOSED"
        if known_data["page"]:
            row["page"] = known_data["page"][1]
            self.remeta[metaval] = known_data["page"]
        elif row["page"]:
            self.log[self.rowcnt]["page"]["status"] = "NEW VALUE PROPOSED"
        if known_data["type"]:
            row["type"] = known_data["type"]
        elif row["type"]:
            self.log[self.rowcnt]["type"]["status"] = "NEW VALUE PROPOSED"
