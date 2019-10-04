import csv, re
from scripts.resfinder import*

class Converter():

    def __init__(self, data):

        # counters TODO make them TXT files
        self.brcnt = 0
        self.idcnt = 0
        self.racnt = 0

        self.brdict = {}  # key id; value br metaid
        self.radict = {}  # key id; value ra metaid
        self.vvi = {}  #Venue, Volume, Issue

        self.idra = {}  # key id; value id metaid related to ra
        self.idbr = {}  # key id; value id metaid related to br

        self.venuedict ={}


        for row in data:
            self.clean_id(row)
            self.clean_ra(row, "author")
            self.clean_ra(row, "publisher")
            self.clean_ra(row, "editor")
            self.clean_vvi(row)


        self.data = data


    #ID
    def clean_id(self, row):
        if row['id']:
            idslist = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', row['id']).lower())

            #check if br exists in graph
            finder_match, found_elem = self.finder_any(idslist, GraphEntity.identifier)
            if finder_match:
                #TODO
                #trovami il br e dammi il meta ID
                #trovami il metaID degli ID già esistenti
                print("ciaociao") #placeholder...useless

            # check if br exists in temporary dict
            else:
                local_match, elem = self.local_any(idslist, self.brdict)
                if local_match:
                    meta = str(self.brdict[elem])
                    newid = "meta:br/" + meta
                    #add new ids
                    for id in idslist:
                        if id not in self.idbr:
                            self.idcnt = self.idcnt + 1
                            self.idbr[id] = self.idcnt
                            self.brdict[id] = meta
                #new br
                else:
                    #create new br metaid
                    self.brcnt = self.brcnt + 1
                    meta = str(self.brcnt)
                    newid = "meta:br/" + meta

                    # add new ids
                    for id in idslist:
                        if id not in self.idbr:
                            self.idcnt = self.idcnt + 1
                            self.idbr[id] = self.idcnt
                            self.brdict[id] = meta

            idslist.append(newid)
            newrow = " ".join(idslist)
            row['id'] = newrow
        #br without IDs, new br
        else:
            #if enity has no ID
            self.brcnt = self.brcnt + 1
            meta = str(self.brcnt)
            newid = "meta:br/" + meta
            row['id'] = newid

    # RA
    def clean_ra(self, row, rowname):
            if row[rowname]:
                ra_list = re.split(r'\s*;\s*(?=[^]]*(?:\[|$))', row[rowname]) #split authors by ";" outside "[]" (any spaces before and after ";")
                final_ra_list = list()
                for ra in ra_list:
                    ra_id = re.search(r'\[\s*(.*?)\s*\]', ra) #takes string inside "[]" ignoring any space between (ex: [ TARGET  ] --> TARGET
                    #clean ra name
                    if ra_id:
                        ra_name = re.search(r'\s*(.*?)\s*\[.*?\]', ra).group(1) #takes autor name and surname ignoring spaces between
                    else:
                        ra_name_raw= row[rowname]
                        ra_name = re.search(r'\[\s*(.*?)\s*\]', ra_name_raw)

                    if rowname == "publisher":
                        ra_name = ra_name
                    else:
                        split_name = re.split(r'\s*,\s*', ra_name)
                        first_name = split_name[0]
                        surname = split_name[1]
                        ra_name = first_name + ", " + surname

                    #clean ra id
                    if ra_id:
                        ra_id = ra_id.group(1)
                        ra_id_list = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', ra_id).lower())
                        finder_match, found_elem = self.finder_any(ra_id_list, GraphEntity.identifier)
                        if finder_match:
                            # TODO
                            # trovami il ra e dammi il meta ID
                            # trovami il metaID degli ID già esistenti
                            print("ciaociao") # placeholder...useless return

                        # check if ra exists in temporary dict
                        else:
                            local_match, elem = self.local_any(ra_id_list, self.radict)
                            if local_match:
                                meta = str(self.radict[elem])
                                newid = "meta:ra/" + meta
                                # add new ids
                                for id in ra_id_list:
                                    if id not in self.idra:
                                        self.idcnt = self.idcnt + 1
                                        self.idra[id] = self.idcnt
                                        self.radict[id] = meta

                            # new ra
                            else:
                                # create new ra metaid
                                self.racnt = self.racnt + 1
                                meta = str(self.racnt)
                                newid = "meta:ra/" + meta

                                # add new ids
                                for id in ra_id_list:
                                    if id not in self.idra:
                                        self.idcnt = self.idcnt + 1
                                        self.idra[id] = self.idcnt
                                        self.radict[id] = meta

                        ra_id_list.append(newid)
                        newids = " ".join(ra_id_list)

                    else:
                        # if enity has no ID
                        self.racnt = self.racnt + 1
                        meta = str(self.racnt)
                        newids = "meta:br/" + meta


                    newra = ra_name + " [" + newids + "]"
                    final_ra_list.append(newra)

                newrow = "; ".join(final_ra_list)
                row[rowname] = newrow



    #VVI
    def clean_vvi(self, row):
        if row["venue"]:
            vol = None
            issue = None
            vol_meta = None
            issue_meta = None

            venue_id = re.search(r'\[\s*(.*?)\s*\]', row["venue"])
            if venue_id:
                ven = re.search(r'(.*?)\s*\[.*?\]', row["venue"]).group(1)
            else:
                ven = row["venue"]
            if venue_id:
                venue_id = venue_id.group(1)
                venue_id_list = re.split(r'\s+', re.sub(r'\s*\:\s*', ':', venue_id).lower())

                # check if br exists in graph
                finder_match, found_elem = self.finder_any(venue_id_list, GraphEntity.identifier)
                if finder_match:
                    # TODO
                    # trovami il br e dammi il meta ID
                    # trovami il metaID degli ID già esistenti
                    print("ciaociao")  # placeholder...useless
                    #venue_meta
                    #vol_meta
                    #issue_meta

                # check if br exists in temporary dict
                else:
                    local_match, elem = self.local_any(venue_id_list, self.brdict)
                    if local_match:
                        ven_meta = str(self.brdict[elem])
                        newid = "meta:br/" + ven_meta
                        # add new ids
                        for id in venue_id_list:
                            if id not in self.idbr:
                                self.idcnt = self.idcnt + 1
                                self.idbr[id] = self.idcnt
                                self.brdict[id] = ven_meta
                    # new br
                    else:
                        # create new br metaid
                        self.brcnt = self.brcnt + 1
                        ven_meta = str(self.brcnt)
                        newid = "meta:br/" + ven_meta
                        self.vvi[ven_meta] = dict()
                        self.vvi[ven_meta]["vol"] = dict()
                        self.vvi[ven_meta]["issue"] = dict()

                        # add new ids
                        for id in venue_id_list:
                            if id not in self.idbr:
                                self.idcnt = self.idcnt + 1
                                self.idbr[id] = self.idcnt
                                self.brdict[id] = ven_meta

                venue_id_list.append(newid)
                newids = " ".join(venue_id_list)
                # br without IDs, new br
            else:
                # if enity has no ID
                self.brcnt = self.brcnt + 1
                ven_meta = str(self.brcnt)
                newids = "meta:br/" + ven_meta
                self.vvi[ven_meta] = dict()
                self.vvi[ven_meta]["vol"] = dict()
                self.vvi[ven_meta]["issue"] = dict()

            row['venue'] = ven + " [" + newids + "]"


        #VOLUME
            if row["volume"]:
                vol = row["volume"].strip()
                if not vol_meta:
                    #if we don't find volume meta id in the graph we have to find it in our dictionary or create a new one
                    if vol in self.vvi[ven_meta]["vol"]:
                            vol_meta = self.vvi[ven_meta]["vol"][vol]["id"]
                    else:
                        self.brcnt = self.brcnt + 1
                        vol_meta = str(self.brcnt)
                        self.vvi[ven_meta]["vol"][vol] = dict()
                        self.vvi[ven_meta]["vol"][vol]["id"] = vol_meta
                        self.vvi[ven_meta]["vol"][vol]["issue"] = dict()
                newids = "meta:br/" + vol_meta
                row['volume'] = vol + " [" + newids + "]"


        #ISSUE
            if row["issue"]:
                issue = row["issue"].strip()
                if not issue_meta:
                    # if we don't find issue meta id in the graph we have to find it in our dictionary or create a new one
                    if vol_meta:
                        #issue inside vol
                        if issue in self.vvi[ven_meta]["vol"][vol]["issue"]:
                            issue_meta = self.vvi[ven_meta]["vol"][vol]["issue"][issue]
                        else:
                            self.brcnt = self.brcnt + 1
                            issue_meta = str(self.brcnt)
                            self.vvi[ven_meta]["vol"][vol]["issue"][issue] = issue_meta
                    else:
                        #issue inside venue (without volume)
                        if issue in self.vvi[ven_meta]["issue"]:
                            issue_meta = self.vvi[ven_meta]["issue"]
                        else:
                            self.brcnt = self.brcnt + 1
                            issue_meta = str(self.brcnt)
                            self.vvi[ven_meta]["vol"][vol]["issue"] = issue_meta

                newids = "meta:br/" + issue_meta
                row['issue'] = vol + " [" + newids + "]"




    #Ancillary
    #an "any" function that returns matched element
    def local_any(self, list, dict):
        matched = False
        match_elem = ''
        for elem in list:
            if elem in dict:
                matched = True
                match_elem = elem
                break
        return matched, match_elem

    def finder_any(self, list, type):
        matched = False
        match_elem = ''
        for elem in list:
            found = ResourceFinder().retrieve_entity(elem, type)
            if found:
                matched = True
                match_elem = found
                break
        return matched, match_elem
