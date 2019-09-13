from migrator import *
import csv

with open("new_test_clean_data.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [dict(x) for x in reader]


#testcase1: 2 different issues of the same venue (no volume)
partial_data1 = list()
partial_data1.append(data[0])
partial_data1.append(data[5])
migrator_processed1 = Migrator(partial_data1)
new_graph1 = migrator_processed1.final_graph
new_graph1.serialize(destination='testcase-01.ttl', format='ttl')


#testcase2: 2 different volumes of the same venue (no issue)
partial_data2 = list()
partial_data2.append(data[1])
partial_data2.append(data[3])
migrator_processed2 = Migrator(partial_data2)
new_graph2 = migrator_processed2.final_graph
new_graph2.serialize(destination='testcase-02.ttl', format='ttl')


#testcase3: 2 different issues of the same volume
partial_data3 = list()
partial_data3.append(data[2])
partial_data3.append(data[4])
migrator_processed3 = Migrator(partial_data3)
new_graph3 = migrator_processed3.final_graph
new_graph3.serialize(destination='testcase-03.ttl', format='ttl')