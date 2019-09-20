from migrator import *
import csv

def test1():
    #testcase1: 2 different issues of the same venue (no volume)
    with open("testcase_01_data.csv", 'r', encoding='utf-8') as csvfile1:
        reader = csv.DictReader(csvfile1)
        data1 = [dict(x) for x in reader]

    migrator_processed1 = Migrator(data1, "index1.txt")
    new_graph1 = migrator_processed1.final_graph
    new_graph1.serialize(destination='testcase_01.ttl', format='ttl')



def test2():
    #testcase2: 2 different volumes of the same venue (no issue)
    with open("testcase_02_data.csv", 'r', encoding='utf-8') as csvfile2:
        reader = csv.DictReader(csvfile2)
        data2 = [dict(x) for x in reader]

    migrator_processed2 = Migrator(data2, "index2.txt")
    new_graph2 = migrator_processed2.final_graph
    new_graph2.serialize(destination='testcase_02.ttl', format='ttl')


def test3():
    #testcase3: 2 different issues of the same volume
    with open("testcase_03_data.csv", 'r', encoding='utf-8') as csvfile3:
        reader = csv.DictReader(csvfile3)
        data3 = [dict(x) for x in reader]

    migrator_processed3 = Migrator(data3, "index3.txt")
    new_graph3 = migrator_processed3.final_graph
    new_graph3.serialize(destination='testcase_03.ttl', format='ttl')

def test4():
    #testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
    with open("testcase_04_data.csv", 'r', encoding='utf-8') as csvfile4:
        reader = csv.DictReader(csvfile4)
        data4 = [dict(x) for x in reader]

    migrator_processed4 = Migrator(data4, "index4.txt")
    new_graph4 = migrator_processed4.final_graph
    new_graph4.serialize(destination='testcase_04.ttl', format='ttl')

def test5():
    # testcase5: NO ID scenario
    with open("testcase_05_data.csv", 'r', encoding='utf-8') as csvfile4:
        reader = csv.DictReader(csvfile4)
        data5 = [dict(x) for x in reader]

    migrator_processed4 = Migrator(data5, "index5.txt")
    new_graph5 = migrator_processed4.final_graph
    new_graph5.serialize(destination='testcase_05.ttl', format='ttl')

def test6():
    # testcase6: ALL types test
    with open("testcase_06_data.csv", 'r', encoding='utf-8') as csvfile6:
        reader = csv.DictReader(csvfile6)
        data6 = [dict(x) for x in reader]

    migrator_processed6 = Migrator(data6, "index6.txt")
    new_graph6 = migrator_processed6.final_graph
    new_graph6.serialize(destination='testcase_06.ttl', format='ttl')


test6()