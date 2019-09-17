from converter import*

with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter="\t")
    newdata = [dict(x) for x in reader]

def uno (newdata):
    #testcase1: 2 different issues of the same venue (no volume)
    partial_data1 = list()
    partial_data1.append(newdata[0])
    partial_data1.append(newdata[5])
    newcleandata1 = converter(partial_data1).newdata
    keys = newcleandata1[0].keys()

    with open('testcase_01_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata1)


def due(newdata):
    #testcase2: 2 different volumes of the same venue (no issue)
    partial_data2 = list()
    partial_data2.append(newdata[1])
    partial_data2.append(newdata[3])
    newcleandata2 = converter(partial_data2).newdata
    keys = newcleandata2[0].keys()

    with open('testcase_02_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata2)


def tre(newdata):
    #testcase3: 2 different issues of the same volume
    partial_data3 = list()
    partial_data3.append(newdata[2])
    partial_data3.append(newdata[4])
    newcleandata3 = converter(partial_data3, "index3.txt").newdata
    keys = newcleandata3[0].keys()

    with open('testcase_03_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata3)


tre(newdata)