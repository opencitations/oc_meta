from migrator import *
import csv

def reset():
    with open("counter/re.txt", 'w') as br:
        br.write('0')
    with open("counter/ar.txt", 'w') as br:
        br.write('0')

def creator(path, ra_index, br_index, filename):
    with open(path, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        data = [dict(x) for x in reader]

    reset()
    migrator_processed = Migrator(data, ra_index, br_index)
    new_graph = migrator_processed.final_graph
    new_graph.serialize(destination=filename, format='ttl')

def test1():
    #testcase1: 2 different issues of the same venue (no volume)
    creator("testcase_01_data.csv", "index1_ra.csv", "index1_br.csv", 'testcase_01.ttl')


def test2():
    #testcase2: 2 different volumes of the same venue (no issue)
    creator("testcase_02_data.csv", "index2_ra.csv", "index2_br.csv", 'testcase_02.ttl')


def test3():
    #testcase3: 2 different issues of the same volume
    creator("testcase_03_data.csv", "index3_ra.csv", "index3_br.csv", 'testcase_03.ttl')


def test4():
    #testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
    creator("testcase_04_data.csv", "index4_ra.csv", "index4_br.csv", 'testcase_04.ttl')


def test5():
    # testcase5: NO ID scenario
    creator("testcase_05_data.csv", "index5_ra.csv", "index5_br.csv", 'testcase_05.ttl')


def test6():
    # testcase6: ALL types test
    creator("testcase_06_data.csv", "index6_ra.csv", "index6_br.csv", 'testcase_06.ttl')


def test7():
    # testcase7: archival document in an archival document set
    creator("testcase_07_data.csv", "index7_ra.csv", "index7_br.csv", 'testcase_07.ttl')


def test8():
    # testcase8: all journal related types with an editor
    creator("testcase_08_data.csv", "index8_ra.csv", "index8_br.csv", 'testcase_08.ttl')


def test9():
    # testcase9: all book related types with an editor
    creator("testcase_09_data.csv", "index9_ra.csv", "index9_br.csv", 'testcase_09.ttl')


def test10():
    # testcase10: all proceeding related types with an editor
    creator("testcase_10_data.csv", "index10_ra.csv", "index10_br.csv", 'testcase_10.ttl')

def test11():
    # testcase11: a book inside a book series and a book inside a book set
    creator("testcase_11_data.csv", "index11_ra.csv", "index11_br.csv", 'testcase_11.ttl')

test11()