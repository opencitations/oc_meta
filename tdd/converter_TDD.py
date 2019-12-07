import unittest
from converter import*
import csv
from SPARQLWrapper import SPARQLWrapper
from pathlib import Path

def reset():
    with open("converter_counter/br.txt", 'w') as br:
        br.write('0')
    with open("converter_counter/id.txt", 'w') as br:
        br.write('0')
    with open("converter_counter/ra.txt", 'w') as br:
        br.write('0')
    with open("converter_counter/ar.txt", 'w') as br:
        br.write('0')
    with open("converter_counter/re.txt", 'w') as br:
        br.write('0')

def reset_server(server):
    ts = sparql.SPARQLServer(server)
    ts.update('delete{?x ?y ?z} where{?x ?y ?z}')

def add_data_ts (server):
    ts = SPARQLWrapper(server)
    ts.method = "POST"
    ts.setQuery("LOAD <file:C:/Users/Fabio/Documents/GitHub/meta/tdd/testcases/ts/testcase_ts-13.ttl>")
    ts.query()

def datacollect():
    with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    return data

def prepare2test(data, name):
    reset()
    server = "http://127.0.0.1:9999/blazegraph/sparql"
    reset_server(server)
    if float(name)>12:
        add_data_ts(server)

    testcase_csv = "testcases/testcase_data/testcase_" + name + "_data.csv"
    testcase_id_br = "testcases/testcase_data/indices/" + name + "/index_id_br_" + name + ".csv"
    testcase_id_ra = "testcases/testcase_data/indices/" + name + "/index_id_ra_" + name + ".csv"
    testcase_ar = "testcases/testcase_data/indices/" + name + "/index_ar_" + name + ".csv"
    testcase_re = "testcases/testcase_data/indices/" + name + "/index_re_" + name + ".csv"
    testcase_vi = "testcases/testcase_data/indices/" + name + "/index_vi_" + name + ".json"

    #convert = Converter(data, server, filename=name, path="testcases/testcase_data/indices/" + name + "/")
    convert = Converter(data, server)
    with open(testcase_csv, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        testcase_csv = [dict(x) for x in reader]

    with open(testcase_id_br, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        testcase_id_br = [dict(x) for x in reader]

    with open(testcase_id_ra, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        testcase_id_ra = [dict(x) for x in reader]

    with open(testcase_ar, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        testcase_ar = [dict(x) for x in reader]

    with open(testcase_re, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        testcase_re = [dict(x) for x in reader]

    with open(testcase_vi) as json_file:
        testcase_vi = json.load(json_file)

    testcase = [testcase_csv, testcase_id_br, testcase_id_ra, testcase_ar, testcase_re, testcase_vi]
    conversion = [convert.data, convert.index_id_br, convert.index_id_ra, convert.ar_index, convert.re_index, convert.VolIss]
    return conversion, testcase


class testcase_01 (unittest.TestCase):
    def test (self):
        # testcase1: 2 different issues of the same venue (no volume)
        name = "01"
        data = datacollect()
        partial_data = list()
        partial_data.append(data[0])
        partial_data.append(data[5])
        conversion, testcase = prepare2test(partial_data, name)
        for pos,x in enumerate(conversion):
            self.assertEqual(x, testcase[pos])



class testcase_02(unittest.TestCase):
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        name = "02"
        data = datacollect()
        partial_data = list()
        partial_data.append(data[1])
        partial_data.append(data[3])
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_03(unittest.TestCase):
    def test(self):
        # testcase3: 2 different issues of the same volume
        name = "03"
        data = datacollect()
        partial_data = list()
        partial_data.append(data[2])
        partial_data.append(data[4])
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_04 (unittest.TestCase):
    def test (self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        name = "04"
        data = datacollect()
        partial_data = list()
        partial_data.append(data[6])
        partial_data.append(data[7])
        conversion, testcase = prepare2test(partial_data, name)
        for pos,x in enumerate(conversion):
            self.assertEqual(x, testcase[pos])


class testcase_05 (unittest.TestCase):
    def test (self):
        # testcase5: NO ID scenario
        name = "05"
        data = datacollect()
        partial_data = list()
        partial_data.append(data[8])
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_06 (unittest.TestCase):
    def test (self):
        # testcase6: ALL types test
        name = "06"
        data = datacollect()
        partial_data = data[9:33]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_07 (unittest.TestCase):
    def test (self):
        # testcase7: all journal related types with an editor
        name = "07"
        data = datacollect()
        partial_data = data[34:40]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_08 (unittest.TestCase):
    def test (self):
        # testcase8: all book related types with an editor
        name = "08"
        data = datacollect()
        partial_data = data[40:43]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_09 (unittest.TestCase):
    def test (self):
        # testcase09: all proceeding related types with an editor
        name = "09"
        data = datacollect()
        partial_data = data[43:45]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_10 (unittest.TestCase):
    def test (self):
        # testcase10: a book inside a book series and a book inside a book set
        name = "10"
        data = datacollect()
        partial_data = data[45:49]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_11 (unittest.TestCase):
    def test (self):
        # testcase11: real time entity update
        name = "11"
        data = datacollect()
        partial_data = data[49:52]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_12 (unittest.TestCase):
    def test (self):
        # testcase12: clean name, title, ids
        name = "12"
        data = datacollect()
        partial_data = data[52:53]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_13 (unittest.TestCase):
    # testcase13: ID_clean massive test

    def test1(self):
        #1--- meta specified br in a row, wannabe with a new id in a row, meta specified with an id related to wannabe in a row
        server = 'http://127.0.0.1:9999/blazegraph/sparql'
        name = "13.1"
        data = datacollect()
        partial_data = data[53:56]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test2(self):
        #2---Conflict with META precedence: a br has a meta_id and an id related to another meta_id, the first specified meta has precedence
        data = datacollect()
        name = "13.2"
        partial_data = data[56:57]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)
    def test3(self):
        #3--- conflict: br with id shared with 2 meta
        data = datacollect()
        name = "13.3"
        partial_data = data[57:58]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)




class testcase_14 (unittest.TestCase):

    def test1(self):
        # update existing sequence, in particular, a new author and an existing author without an existing id (matched tanks to surname,name(BAD WRITTEN!)
        name = "14.1"
        data = datacollect()
        partial_data = data[58:59]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test2(self):
        # same sequence different order, with new ids
        name = "14.2"
        data = datacollect()
        partial_data = data[59:60]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test3(self):
        # RA
        name = "14.3"
        data = datacollect()
        partial_data = data[60:61]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)
    
    def test4(self):
        #meta specified ra in a row, wannabe ra with a new id in a row, meta specified with an id related to wannabe in a ra
        name = "14.4"
        data = datacollect()
        partial_data = data[61:64]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_15 (unittest.TestCase):

    def test1(self):
        # venue volume issue  already exists in ts
        name = "15.1"
        data = datacollect()
        partial_data = data[64:65]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test2(self):
        # venue conflict
        name = "15.2"
        data = datacollect()
        partial_data = data[65:66]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test3(self):
        # venue in ts is now the br
        name = "15.3"
        data = datacollect()
        partial_data = data[66:67]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test4(self):
        # br in ts is now the venue
        name = "15.4"
        data = datacollect()
        partial_data = data[67:68]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test5(self):
        # volume in ts is now the br
        name = "15.5"
        data = datacollect()
        partial_data = data[71:72]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test6(self):
        # br is a volume
        name = "15.6"
        data = datacollect()
        partial_data = data[72:73]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test7(self):
        # issue in ts is now the br
        name = "15.7"
        data = datacollect()
        partial_data = data[73:74]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test8(self):
        # br is a issue
        name = "15.8"
        data = datacollect()
        partial_data = data[74:75]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


class testcase_16(unittest.TestCase):

    def test1(self):
        name = "16.1"
        add_data_ts("http://127.0.0.1:9999/blazegraph/sparql")
        #wrong date (2019/02/29)
        data = datacollect()
        partial_data = data[75:76]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


    def test2(self):
        #existing re
        name = "16.2"
        data = datacollect()
        partial_data = data[76:77]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)

    def test3(self):
        #given name for an RA with only a family name in TS
        name = "16.3"
        data = datacollect()
        partial_data = data[77:78]
        conversion, testcase = prepare2test(partial_data, name)
        self.assertEqual(conversion, testcase)


def suite(testobj):
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(testobj))
    return test_suite


TestSuit = suite(testcase_15)
runner = unittest.TextTestRunner()
runner.run(TestSuit)
'''
x = 1
while x < 17:
    if x < 10:
        y = "0" + str(x)
    else:
        y = str(x)
    t = "testcase_" + y
    TestSuit=suite(eval(t))
    x += 1
    runner=unittest.TextTestRunner()
    runner.run(TestSuit)
'''