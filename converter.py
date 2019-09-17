import csv, re


class converter():

    def __init__(self, data, path):
        self.data = data
        dataID = self.conversion()

        for x in dataID:
            val = str(x) + " , " + str(dataID[x])
            with open(path, 'a') as file:
                file.write( val + '\n')




    def conversion(self):
        brct = 0
        idct = 0
        ract = 0

        brdict = {}
        iddict = {}
        radict = {}


        for row in self.data:


            if row['id']:
                idslist = re.split(r'\s*;\s*', row['id'])
                for id in idslist:
                    if id not in iddict:
                        idct = idct + 1
                        iddict[id] = idct

                brct = brct + 1
                newid = "meta:br/" + str(brct)
                idslist.append(newid)

                newrow = "; ".join(idslist)
                row['id'] = newrow



            if row['author']:

                authorslist = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row['author'])
                finalautlist = list()
                for aut in authorslist:
                    aut_id = re.search(r'\[\s*(.*?)\s*\]', aut).group(1)
                    aut_id_list = re.split(r'\s*;\s*', aut_id)

                    # lists of authors' IDs
                    for id in aut_id_list:
                        if id not in iddict:
                            idct = idct + 1
                            iddict[id] = idct

                    author_name = re.search(r'(.*?)\s*\[.*?\]', aut).group(1)

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
                venue_id = re.search(r'\[\s*(.*?)\s*\]', row["venue"]).group(1)
                venue_id_list = re.split(r'\s*;\s*', venue_id)
                for id in venue_id_list:
                    if id not in iddict:
                        idct = idct + 1
                        iddict[id] = idct

                ven = re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1)
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
                pub_id = re.search(r'\[\s*(.*?)\s*\]', row["publisher"]).group(1)
                pub_id_list = re.split(r'\s*;\s*', pub_id)
                for id in pub_id_list:
                    if id not in iddict:
                        idct = idct + 1
                        iddict[id] = idct


                pub = re.search(r'(.*?)\s*\[.*?\]', row["publisher"]).group(1)
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

        self.newdata = self.data
        return iddict