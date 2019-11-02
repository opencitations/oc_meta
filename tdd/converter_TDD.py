import unittest
from converter import *
import csv

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


def datacollect():
    with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    return data

def prepare2test(data, testcase_csv):
    reset()

    conversion = Converter(data, 'http://127.0.0.1:9999/blazegraph/sparql').data
    with open(testcase_csv, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        testcase = [dict(x) for x in reader]


    return conversion, testcase


class testcase_01 (unittest.TestCase):
    def test (self):
        # testcase1: 2 different issues of the same venue (no volume)
        data = datacollect()
        partial_data = list()
        partial_data.append(data[0])
        partial_data.append(data[5])
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_01_data.csv")
        self.assertEqual(conversion, testcase)


class testcase_02(unittest.TestCase):
    def test(self):
        # testcase2: 2 different volumes of the same venue (no issue)
        data = datacollect()
        partial_data = list()
        partial_data.append(data[1])
        partial_data.append(data[3])
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_02_data.csv")
        self.assertEqual(conversion, testcase)




class testcase_03(unittest.TestCase):
    def test(self):
        # testcase3: 2 different issues of the same volume
        data = datacollect()
        partial_data = list()
        partial_data.append(data[2])
        partial_data.append(data[4])
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_03_data.csv")
        self.assertEqual(conversion, testcase)





class testcase_04 (unittest.TestCase):
    def test (self):
        # testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
        data = datacollect()
        partial_data = list()
        partial_data.append(data[6])
        partial_data.append(data[7])
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_04_data.csv")
        self.assertEqual(conversion, testcase)




class testcase_05 (unittest.TestCase):
    def test (self):
        # testcase5: NO ID scenario
        data = datacollect()
        partial_data = list()
        partial_data.append(data[8])
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_05_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_06 (unittest.TestCase):
    def test (self):
        # testcase6: ALL types test
        data = datacollect()
        partial_data = data[9:33]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_06_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_07 (unittest.TestCase):
    def test (self):
        # testcase7: all journal related types with an editor
        data = datacollect()
        partial_data = data[34:40]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_07_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_08 (unittest.TestCase):
    def test (self):
        # testcase8: all book related types with an editor
        data = datacollect()
        partial_data = data[40:43]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_08_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_09 (unittest.TestCase):
    def test (self):
        # testcase09: all proceeding related types with an editor
        data = datacollect()
        partial_data = data[43:45]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_09_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_10 (unittest.TestCase):
    def test (self):
        # testcase10: a book inside a book series and a book inside a book set
        data = datacollect()
        partial_data = data[45:49]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_10_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_11 (unittest.TestCase):
    def test (self):
        # testcase11: real time entity update
        data = datacollect()
        partial_data = data[49:52]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_11_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_12 (unittest.TestCase):
    def test (self):
        # testcase12: clean name, title, ids
        data = datacollect()
        partial_data = data[52:53]
        #partial_data = list()
        #partial_data.append(data[52])
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_12_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_13 (unittest.TestCase):
    # testcase13: ID_clean massive test

    def test1(self):
        #1--- meta specified br in a row, wannabe with a new id in a row, meta specified with an id related to wannabe in a row
        server = 'http://127.0.0.1:9999/blazegraph/sparql'
        #reset_server(server)
        data = datacollect()
        partial_data = data[53:56]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_13.1_data.csv")
        self.assertEqual(conversion, testcase)

    def test2(self):
        #2---Conflict with META precedence: a br has a meta_id and an id related to another meta_id, the first specified meta has precedence
        data = datacollect()
        partial_data = data[56:57]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_13.2_data.csv")
        self.assertEqual(conversion, testcase)

    def test3(self):
        #3--- conflict: br with id shared with 2 meta
        data = datacollect()
        partial_data = data[57:58]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_13.3_data.csv")
        self.assertEqual(conversion, testcase)

#todo check!
class conflict(unittest.TestCase):
    #todo possibile soluzione (mode-1), quando ho un conflitto inserisco tutti i meta coinvolti nel dizionario

    def mode_1(self):
        # con il meta nel dizionario la nuova si associa al meta originario
        data = datacollect()
        partial_data = data[68:71]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_13.3_data.csv")
        print(conversion)

    def mode_2(self):
        # senza meta nel dizionario la nuova si associa al meta del conflitto
        data = datacollect()
        partial_data = data[69:71]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_13.3_data.csv")
        print(conversion)



class testcase_14 (unittest.TestCase):
    def test1(self):
        # update existing sequence, in particular, a new author and an existing author without an existing id (matched tanks to surname,name(BAD WRITTEN!)
        data = datacollect()
        partial_data = data[58:59]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_14.1_data.csv")
        self.assertEqual(conversion, testcase)

    def test2(self):
        # same sequence different order, with new ids
        data = datacollect()
        partial_data = data[59:60]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_14.2_data.csv")
        self.assertEqual(conversion, testcase)

    def test3(self):
        # RA CONFLICT
        data = datacollect()
        partial_data = data[60:61]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_14.3_data.csv")
        self.assertEqual(conversion, testcase)

    def test4(self):
        #meta specified ra in a row, wannabe ra with a new id in a row, meta specified with an id related to wannabe in a ra
        data = datacollect()
        partial_data = data[61:64]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_14.4_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_15 (unittest.TestCase):
    def test1(self):
        # venue volume issue  already exists in ts
        data = datacollect()
        partial_data = data[64:65]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.1_data.csv")
        self.assertEqual(conversion, testcase)

    def test2(self):
        # venue conflict
        data = datacollect()
        partial_data = data[65:66]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.2_data.csv")
        self.assertEqual(conversion, testcase)

    def test3(self):
        # venue in ts is now the br
        data = datacollect()
        partial_data = data[66:67]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.3_data.csv")
        self.assertEqual(conversion, testcase)

    def test4(self):
        # br in ts is now the venue
        data = datacollect()
        partial_data = data[67:68]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.4_data.csv")
        self.assertEqual(conversion, testcase)

    def test5(self):
        # volume in ts is now the br
        data = datacollect()
        partial_data = data[71:72]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.5_data.csv")
        self.assertEqual(conversion, testcase)

    def test6(self):
        # br is a volume
        data = datacollect()
        partial_data = data[72:73]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.6_data.csv")
        self.assertEqual(conversion, testcase)

    def test7(self):
        # issue in ts is now the br
        data = datacollect()
        partial_data = data[73:74]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.7_data.csv")
        self.assertEqual(conversion, testcase)

    def test8(self):
        # br is a issue
        data = datacollect()
        partial_data = data[74:75]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_15.8_data.csv")
        self.assertEqual(conversion, testcase)



class testcase_16(unittest.TestCase):
    def test1(self):
        #wrong date (2019/02/29)
        data = datacollect()
        partial_data = data[75:76]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_16.1_data.csv")
        self.assertEqual(conversion, testcase)
    def test2(self):
        #existing re
        data = datacollect()
        partial_data = data[76:77]
        conversion, testcase = prepare2test(partial_data, "testcases/testcase_data/testcase_16.2_data.csv")
        self.assertEqual(conversion, testcase)


#todo testcase_15 only local

def suite(testobj):
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(testobj))
    return test_suite


TestSuit=suite(testcase_15)

runner=unittest.TextTestRunner()
runner.run(TestSuit)