from migrator import *
import csv

def uno():
    #testcase1: 2 different issues of the same venue (no volume)
    with open("testcase_01_data.csv", 'r', encoding='utf-8') as csvfile1:
        reader = csv.DictReader(csvfile1)
        data1 = [dict(x) for x in reader]

    migrator_processed1 = Migrator(data1)
    new_graph1 = migrator_processed1.final_graph
    new_graph1.serialize(destination='testcase_01.ttl', format='ttl')



def due():
    #testcase2: 2 different volumes of the same venue (no issue)
    with open("testcase_02_data.csv", 'r', encoding='utf-8') as csvfile2:
        reader = csv.DictReader(csvfile2)
        data2 = [dict(x) for x in reader]

    migrator_processed2 = Migrator(data2)
    new_graph2 = migrator_processed2.final_graph
    new_graph2.serialize(destination='testcase_02.ttl', format='ttl')


def tre():
    #testcase3: 2 different issues of the same volume
    with open("testcase_03_data.csv", 'r', encoding='utf-8') as csvfile3:
        reader = csv.DictReader(csvfile3)
        data3 = [dict(x) for x in reader]

    migrator_processed3 = Migrator(data3)
    new_graph3 = migrator_processed3.final_graph
    new_graph3.serialize(destination='testcase_03.ttl', format='ttl')

tre()