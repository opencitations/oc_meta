import csv, re, os
from scripts.finder import*


class Converter:

    def __init__(self, data, ts, separator = None, info_dir ="converter_counter/"):

        self.ts = ts
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


        self.rowcnt = 0

        self.data = data

        for row in self.data:
            self.clean_id(row)
            self.clean_ra(row, "author")
            self.clean_ra(row, "publisher")
            self.clean_ra(row, "editor")
            self.clean_vvi(row)

            self.rowcnt += 1

        #self.indexer()

    #ID
    def clean_id(self, row):
        if row['id']:
            if self.separator:
                idslist = re.sub(r'\s*\:\s*', ':', row['id']).split(self.separator)
            else:
                idslist = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', row['id']))

            # check if br exists in temporary dict
            local_match, elem = self.local_any(idslist, self.brdict)
            if local_match:
                meta = str(elem)
                newid = "meta:br/" + meta

                row["title"] = self.brdict[meta]["title"]

                for x in self.brdict[meta]["ids"]:
                    if x not in idslist:
                        idslist.append(x)

            else:
                # check if br exists in graph
                finder_match, found_elem = self.finder_any_br(idslist)
                if finder_match:

                    meta = found_elem[0]
                    newid = "meta:br/" + meta

                    title = found_elem[1]
                    row["title"] = title

                    self.brdict[meta]= dict()
                    self.brdict[meta]["ids"] = list()
                    self.brdict[meta]["rows"] = list()
                    self.brdict[meta]["title"] = title


                    existing_ids = found_elem[2]

                    for id in existing_ids:
                        if id[1] not in self.idbr:
                            self.idbr[id[1]] = id[0]
                        if id[1] not in idslist:
                            idslist.append(id[1])

                #new br
                else:
                    #create new br metaid
                    count = self._add_number(self.br_info_path)
                    meta = str(count)
                    newid = "meta:br/" + meta

                    self.brdict[meta] = dict()
                    self.brdict[meta]["ids"] = list()
                    self.brdict[meta]["rows"] = list()

                    title = self.clean_title(row["title"])
                    self.brdict[meta]["title"] = title
                    row["title"] = title


            id2update = list()
            for id in idslist:
                if id not in self.idbr:
                    count = self._add_number(self.id_info_path)
                    self.idbr[id] = count

                if id not in self.brdict[meta]["ids"]:
                    self.brdict[meta]["ids"].append(id)
                    id2update.append(id)
                if self.rowcnt not in self.brdict[meta]["rows"]:
                    self.brdict[meta]["rows"].append(self.rowcnt)


            if id2update and len(self.brdict[meta]["rows"])>1:
                self.update_br(id2update, meta)

            idslist.append(newid)
            newrow = " ".join(idslist)
            row['id'] = newrow

        #br without IDs, new br
        else:
            #if enity has no ID
            count = self._add_number(self.br_info_path)
            meta = str(count)
            newid = "meta:br/" + meta
            row['id'] = newid
            title = self.clean_title(row["title"])
            row["title"] = title

    # RA
    def clean_ra(self, row, rowname):
            if row[rowname]:
                ra_list = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row[rowname]) #split authors by ";" outside "[]" (any spaces before and after ";")
                final_ra_list = list()
                for ra in ra_list:
                    ra_id = re.search(r'\[\s*(.*?)\s*\]', ra) #takes string inside "[]" ignoring any space between (ex: [ TARGET  ] --> TARGET

                    # clean ra name
                    if ra_id:
                        ra_name = re.search(r'\s*(.*?)\s*\[.*?\]', ra).group(1)  # takes autor name and surname ignoring spaces around


                    #clean ra id
                    if ra_id:
                        ra_id = ra_id.group(1)
                        if self.separator:
                            ra_id_list = re.sub(r'\s*\:\s*', ':', ra_id).split(self.separator)
                        else:
                            ra_id_list = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', ra_id))

                        # check if ra exists in temporary dict
                        local_match, elem = self.local_any(ra_id_list, self.radict)
                        if local_match:
                            meta = str(elem)
                            newid = "meta:ra/" + meta

                            ra_name = self.radict[meta]["name"]

                            for x in self.radict[meta]["ids"]:
                                if x not in ra_id_list:
                                    ra_id_list.append(x)

                        else:
                            finder_match, found_elem = self.finder_any_ra(ra_id_list, rowname)
                            if finder_match:

                                meta = found_elem[0]
                                newid = "meta:ra/" + meta

                                ra_name = found_elem[1]

                                self.radict[meta] = dict()
                                self.radict[meta]["ids"] = list()
                                self.radict[meta]["rows"] = list()
                                self.radict[meta]["name"] = ra_name

                                existing_ids = found_elem[2]

                                for id in existing_ids:
                                    if id[1] not in self.idra:
                                        self.idra[id[1]] = id[0]
                                    if id[1] not in ra_id_list:
                                        ra_id_list.append(id[1])

                            # new ra
                            else:
                                count = self._add_number(self.ra_info_path)
                                meta = str(count)
                                newid = "meta:ra/" + meta

                                ra_name = self.clean_name(re.search(r'\s*(.*?)\s*\[.*?\]', ra).group(1))  # takes autor name and surname ignoring spaces around

                                self.radict[meta] = dict()
                                self.radict[meta]["ids"] = list()
                                self.radict[meta]["rows"] = list()
                                self.radict[meta]["name"] = ra_name




                        id2update = list()
                        for id in ra_id_list:
                            if id not in self.idra:
                                count = self._add_number(self.id_info_path)
                                self.idra[id] = count

                            if id not in self.radict[meta]["ids"]:
                                self.radict[meta]["ids"].append(id)
                                id2update.append(id)
                            if self.rowcnt not in self.radict[meta]["rows"]:
                                self.radict[meta]["rows"].append(self.rowcnt)

                        if id2update and len(self.radict[meta]["rows"]) > 1:
                            self.update_ra(id2update, meta, rowname, newid)

                        ra_id_list.append(newid)
                        newids = " ".join(ra_id_list)

                    else:
                        # if enity has no ID
                        count = self._add_number(self.ra_info_path)
                        meta = str(count)
                        newids = "meta:ra/" + meta
                        ra_name = self.clean_name(row[rowname].strip())



                    newra = ra_name + " [" + newids + "]"
                    final_ra_list.append(newra)

                newrow = "; ".join(final_ra_list)
                row[rowname] = newrow

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
                    count = self._add_number(self.id_info_path)
                    issue_meta = str(count)

                newids = "meta:br/" + issue_meta
                row['issue'] = issue + " [" + newids + "]"



    #Ancillary
    #an "any" function that returns matched element
    def local_any(self, list, dict):
        matched = False
        match_elem = ''
        for elem in list:
            for k,va in dict.items():
                if elem in va["ids"]:
                    matched = True
                    match_elem = k
                    break
        return matched, match_elem

    def finder_any_br(self, list):
        matched = False
        match_elem = None
        for elem in list:
            id = elem.split(":")
            value = id[1]
            schema = id[0]
            res = ResourceFinder(self.ts).retrieve_br_from_id(value, schema)
            if res:
                matched = True
                match_elem = res
                break
        return matched, match_elem


    def finder_any_ra(self, list, rowname):
        matched = False
        match_elem = None
        for elem in list:
            id = elem.split(":")
            value = id[1]
            schema = id[0]
            if rowname == "publisher":
                res = ResourceFinder(self.ts).retrieve_publisher_from_id(value, schema)
            else:
                res = ResourceFinder(self.ts).retrieve_autor_editor_from_id(value, schema)
            if res:
                matched = True
                match_elem = res
                break
        return matched, match_elem

    def finder_any_vii(self, list):
        matched = False
        match_elem = None
        for elem in list:
            id = elem.split(":")
            value = id[1]
            schema = id[0]
            res = ResourceFinder(self.ts).retrieve_venue_from_id(value, schema)
            if res:
                matched = True
                match_elem = res
                break
        return matched, match_elem

    def update_br(self, id_list, meta):
        rows = list()
        meta_id = "meta:br/" + str(meta)
        for r in self.brdict[meta]["rows"]:
            if r != self.rowcnt:
                rows.append(r)
        for n in rows:
            if re.search(r'\b' + meta_id + r'\b', self.data[n]["id"]):
                old_id = self.data[n]["id"].split(" ")
                old_meta = old_id[-1]
                old_id.remove(old_meta)
                for x in id_list:
                    if x not in old_id:
                        old_id.append(x)
                old_id.append(old_meta)
                newrow = " ".join(old_id)
                self.data[n]["id"] = newrow

            elif re.search(r'\b' + meta_id + r'\b', self.data[n]["venue"]):
                br = self.data[n]["venue"]
                name = re.search(r'\s*(.*?)\s*\[.*?\]', br).group(1)
                old_id = re.search(r'\[\s*(.*?)\s*\]', br).group(1).split(" ")
                meta = old_id[-1]
                old_id.remove(meta)
                for x in id_list:
                    if x not in old_id:
                        old_id.append(x)
                old_id.append(meta)
                newidrow = " ".join(old_id)
                newrow = name + " [" + newidrow + "]"
                self.data[n]["venue"] = newrow



    def update_ra (self, id_list, meta, rowname, newid):
        rows = list()
        for r in self.radict[meta]["rows"]:
            if r != self.rowcnt:
                rows.append(r)
        for n in rows:
            line = self.data[n][rowname]
            list_ra = line.split("; ")
            for pos,ra in enumerate(list_ra):
                if re.search(r'\b' + newid + r'\b', ra):
                    name = re.search(r'\s*(.*?)\s*\[.*?\]', ra).group(1)
                    old_id = re.search(r'\[\s*(.*?)\s*\]', ra).group(1).split(" ")
                    meta = old_id[-1]
                    old_id.remove(meta)
                    for x in id_list:
                        if x not in old_id:
                            old_id.append(x)
                    old_id.append(meta)
                    newidrow = " ".join(old_id)
                    newra = name + " [" + newidrow + "]"
                    list_ra[pos] = newra
                    newrow = "; ".join(list_ra)
                    self.data[n][rowname] = newrow

    def update_venue (self, id_list, meta):
        rows = list()
        meta_id = "meta:br/" + str(meta)

        for r in self.brdict[meta]["rows"]:
            if r != self.rowcnt:
                rows.append(r)
        for n in rows:
            if re.search(r'\b' + meta_id + r'\b', self.data[n]["id"]):
                old_id = self.data[n]["id"].split(" ")
                old_meta = old_id[-1]
                old_id.remove(old_meta)
                for x in id_list:
                    if x not in old_id:
                        old_id.append(x)
                old_id.append(old_meta)
                newrow = " ".join(old_id)
                self.data[n]["id"] = newrow

            elif re.search(r'\b' + meta_id + r'\b', self.data[n]["venue"]):
                br = self.data[n]["venue"]
                name = re.search(r'\s*(.*?)\s*\[.*?\]', br).group(1)
                old_id = re.search(r'\[\s*(.*?)\s*\]', br).group(1).split(" ")
                meta = old_id[-1]
                old_id.remove(meta)
                for x in id_list:
                    if x not in old_id:
                        old_id.append(x)
                old_id.append(meta)
                newidrow = " ".join(old_id)
                newrow = name + " [" + newidrow + "]"
                self.data[n]["venue"] = newrow

    @staticmethod
    def clean_title(title):
        words = title.split()
        for pos,w in enumerate(words):
            if any(x.isupper() for x in w):
                pass
            else:
                words[pos] = w.capitalize()

        newtitle = " ".join(words)
        return newtitle

    @staticmethod
    def clean_name(name):
        if "," in name:
            split_name = re.split(r'\s*,\s*', name)
            first_name = split_name[1].title()
            surname = split_name[0].title()
            new_name = surname + ", " + first_name
        else:
            new_name = name.title()

        return new_name

    @staticmethod
    def _read_number(file_path, line_number=1):
        cur_number = 0

        try:
            with open(file_path) as f:
                cur_number = int(f.readlines()[line_number - 1])
        except Exception as e:
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