from converter import*

with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter="\t")
    newdata = [dict(x) for x in reader]


def testX():
    with open("test_data.csv", 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        newdata = [dict(x) for x in reader]

    #testcaseX: The example.csv test
    newcleandata = converter(newdata, "indexX.txt").newdata
    keys = newcleandata[0].keys()

    with open('testcase_X_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata)


def test1(newdata):
    #testcase1: 2 different issues of the same venue (no volume)
    partial_data1 = list()
    partial_data1.append(newdata[0])
    partial_data1.append(newdata[5])
    newcleandata1 = converter(partial_data1, "index1.txt").newdata
    keys = newcleandata1[0].keys()

    with open('testcase_01_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata1)


def test2(newdata):
    #testcase2: 2 different volumes of the same venue (no issue)
    partial_data2 = list()
    partial_data2.append(newdata[1])
    partial_data2.append(newdata[3])
    newcleandata2 = converter(partial_data2, "index2.txt").newdata
    keys = newcleandata2[0].keys()

    with open('testcase_02_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata2)


def test3(newdata):
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

def test4(newdata):
    #testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
    partial_data4 = list()
    partial_data4.append(newdata[6])
    partial_data4.append(newdata[7])
    newcleandata4 = converter(partial_data4, "index4.txt").newdata
    keys = newcleandata4[0].keys()

    with open('testcase_04_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata4)


def test5(newdata):
    # testcase5: NO ID scenario
    partial_data5 = list()
    partial_data5.append(newdata[8])
    newcleandata5 = converter(partial_data5, "index5.txt").newdata
    keys = newcleandata5[0].keys()

    with open('testcase_05_data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata5)


test5(newdata)