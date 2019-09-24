import csv, re


class converter():

    def __init__(self, data, pathra, pathbr):
        self.data = data
        dataIDRA, dataIDBR = self.conversion()

        try:
            rowsra= list()
            for x in dataIDRA:
                row= dict()
                row["id"] = str(x)
                row["meta"] = str(dataIDRA[x])
                rowsra.append(row)

            with open(pathra, 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, rowsra[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(rowsra)

            rowsbr = list()
            for x in dataIDBR:
                row = dict()
                row["id"] = str(x)
                row["meta"] = str(dataIDBR[x])
                rowsbr.append(row)

            with open(pathbr, 'w', newline='') as output_file:
                dict_writer = csv.DictWriter(output_file, rowsbr[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(rowsbr)
        except:
            try:
                rowsbr = list()
                for x in dataIDBR:
                    row = dict()
                    row["id"] = str(x)
                    row["meta"] = str(dataIDBR[x])
                    rowsbr.append(row)

                with open(pathbr, 'w', newline='') as output_file:
                    dict_writer = csv.DictWriter(output_file, rowsbr[0].keys())
                    dict_writer.writeheader()
                    dict_writer.writerows(rowsbr)
            except:
                return






    def conversion(self):
        brct = 0
        idct = 0
        ract = 0

        brdict = {}
        idra = {}
        idbr = {}
        radict = {}


        for row in self.data:


            if row['id']:
                idslist = re.split(r'\s*;\s*', row['id'])
                for id in idslist:
                    if id not in idbr:
                        idct = idct + 1
                        idbr[id] = idct

                brct = brct + 1
                newid = "meta:br/" + str(brct)
                idslist.append(newid)

                newrow = "; ".join(idslist)
                row['id'] = newrow
            else:
                brct = brct + 1
                newid = "meta:br/" + str(brct)
                newrow = list()
                newrow.append(newid)

                row['id'] = newrow[0]




            if row['author']:

                authorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row['author'])
                finalautlist = list()
                for aut in authorslist:
                    aut_id = re.search(r'\[\s*(.*?)\s*\]', aut)
                    if aut_id:
                        aut_id = aut_id.group(1)
                        aut_id_list = re.split(r'\s*;\s*', aut_id)

                        # lists of authors' IDs
                        for id in aut_id_list:
                            if id not in idra:
                                idct = idct + 1
                                idra[id] = idct
                    else:
                        aut_id_list = list()
                    if aut_id:
                        author_name = re.search(r'(.*?)\s*\[.*?\]', aut).group(1)
                    else:
                        author_name= row["author"]

                    if author_name in radict:
                        autnewid = "meta:ra/" + str(radict[author_name])
                        aut_id_list.append(autnewid)
                    else:
                        ract = ract + 1
                        radict[author_name] = ract
                        newid = "meta:ra/" + str(ract)
                        aut_id_list.append(newid)


                    newfinalidlist = "; ".join(aut_id_list)
                    newaut = author_name + " [" + newfinalidlist + "]"
                    finalautlist.append(newaut)

                newrow = "; ".join(finalautlist)
                row['author'] = newrow

            if row["venue"]:
                venue_id = re.search(r'\[\s*(.*?)\s*\]', row["venue"])
                if venue_id:
                    venue_id= venue_id.group(1)
                    venue_id_list = re.split(r'\s*;\s*', venue_id)
                    for id in venue_id_list:
                        if id not in idbr:
                            idct = idct + 1
                            idbr[id] = idct
                else:
                   venue_id_list = list()
                if venue_id:
                    ven = re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1)
                else:
                    ven = row["venue"]
                if ven in brdict:
                    vennewid = "meta:br/" + str(brdict[ven]["id"])
                    venue_id_list.append(vennewid)
                else:
                    brct = brct + 1
                    brdict[ven] = dict()
                    brdict[ven]["vol"] = dict()
                    brdict[ven]["iss"] = dict()
                    brdict[ven]["id"] = brct
                    newid = "meta:br/" + str(brct)
                    venue_id_list.append(newid)

                finaliddlist = "; ".join(venue_id_list)
                newrow = ven + " [" + finaliddlist + "]"
                row['venue'] = newrow



            if row["volume"]:
                vol = row["volume"]
                ven = re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1)

                if vol in brdict[ven]["vol"]:
                    volid = "meta:br/" + str(brdict[ven]["vol"][vol])
                else:
                    brct = brct + 1
                    brdict[ven]["vol"][vol] = brct
                    volid = "meta:br/" + str(brct)

                newrow = vol + " ["+volid+"]"
                row["volume"] = newrow



            if row["issue"]:
                iss = row["issue"]
                ven = re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1)

                if iss in brdict[ven]["iss"]:
                    issid = "meta:br/" + str(brdict[ven]["iss"][iss])
                else:
                    brct = brct + 1
                    brdict[ven]["iss"][iss] = brct
                    issid = "meta:br/" + str(brct)

                newrow = iss + " [" + issid + "]"
                row["issue"] = newrow



            if row["publisher"]:
                pub_id = re.search(r'\[\s*(.*?)\s*\]', row["publisher"])
                if pub_id:
                    pub_id = pub_id.group(1)
                    pub_id_list = re.split(r'\s*;\s*', pub_id)
                    for id in pub_id_list:
                        if id not in idra:
                            idct = idct + 1
                            idra[id] = idct
                else:
                    pub_id_list = list()
                if pub_id:
                    pub = re.search(r'(.*?)\s*\[.*?\]', row["publisher"]).group(1)
                else:
                    pub = row["publisher"]
                if pub in radict:
                    pubnewid = "meta:ra/" + str(radict[pub])
                    pub_id_list.append(pubnewid)
                else:
                    ract = ract + 1
                    radict[pub] = ract
                    newid = "meta:ra/" + str(ract)
                    pub_id_list.append(newid)

                finaliddlist = "; ".join(pub_id_list)
                newrow = pub + " [" + finaliddlist + "]"
                row['publisher'] = newrow

            if row['editor']:

                editlist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row['editor'])
                finaledlist = list()
                for ed in editlist:
                    ed_id = re.search(r'\[\s*(.*?)\s*\]', ed)
                    if ed_id:
                        ed_id = ed_id.group(1)
                        ed_id_list = re.split(r'\s*;\s*', ed_id)

                        # lists of editors' IDs
                        for id in ed_id_list:
                            if id not in idra:
                                idct = idct + 1
                                idra[id] = idct
                    else:
                        ed_id_list = list()
                    if ed_id:
                        ed_name = re.search(r'(.*?)\s*\[.*?\]', ed).group(1)
                    else:
                        ed_name = row["editor"]

                    if ed_name in radict:
                        ednewid = "meta:ra/" + str(radict[ed_name])
                        ed_id_list.append(ednewid)
                    else:
                        ract = ract + 1
                        radict[ed_name] = ract
                        newid = "meta:ra/" + str(ract)
                        ed_id_list.append(newid)

                    newfinalidlist = "; ".join(ed_id_list)
                    newed = ed_name + " [" + newfinalidlist + "]"
                    finaledlist.append(newed)

                newrow = "; ".join(finaledlist)
                row['editor'] = newrow

        self.newdata = self.data
        return idra, idbr