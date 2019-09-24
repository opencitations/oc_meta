from converter import*

with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter="\t")
    newdata = [dict(x) for x in reader]


def testmaker(partial_data, path, index_ra, index_br):
    newcleandata = converter(partial_data, index_ra, index_br).newdata
    keys = newcleandata[0].keys()

    with open(path, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, delimiter="\t")
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata)

def testX():
    with open("test_data.csv", 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        newdata = [dict(x) for x in reader]

    #testcaseX: The example.csv test
    testmaker(newdata, "testcase_X_data.csv", "indexX_ra.csv", "indexX_br.csv")



def test1(newdata):
    #testcase1: 2 different issues of the same venue (no volume)
    partial_data = list()
    partial_data.append(newdata[0])
    partial_data.append(newdata[5])
    testmaker(partial_data, 'testcase_01_data.csv',"index1_ra.csv", "index1_br.csv")



def test2(newdata):
    #testcase2: 2 different volumes of the same venue (no issue)
    partial_data = list()
    partial_data.append(newdata[1])
    partial_data.append(newdata[3])
    testmaker(partial_data, 'testcase_02_data.csv',"index2_ra.csv", "index2_br.csv")


def test3(newdata):
    #testcase3: 2 different issues of the same volume
    partial_data = list()
    partial_data.append(newdata[2])
    partial_data.append(newdata[4])
    testmaker(partial_data, 'testcase_03_data.csv',"index3_ra.csv", "index3_br.csv")


def test4(newdata):
    #testcase4: 2 new IDS and different date format (yyyy-mm and yyyy-mm-dd)
    partial_data = list()
    partial_data.append(newdata[6])
    partial_data.append(newdata[7])
    testmaker(partial_data, 'testcase_04_data.csv',"index4_ra.csv", "index4_br.csv")



def test5(newdata):
    # testcase5: NO ID scenario
    partial_data = list()
    partial_data.append(newdata[8])
    testmaker(partial_data, 'testcase_05_data.csv',"index5_ra.csv", "index5_br.csv")



def test6(newdata):
    # testcase6: ALL types test
    partial_data= newdata[9:33]
    testmaker(partial_data, 'testcase_06_data.csv',"index6_ra.csv", "index6_br.csv")


def test7(newdata):
    # testcase7: archival document in an archival document set
    partial_data = list()
    partial_data.append(newdata[33])
    testmaker(partial_data, 'testcase_07_data.csv',"index7_ra.csv", "index7_br.csv")



def test8(newdata):
    # testcase8: all journal related types with an editor
    partial_data= newdata[34:40]
    testmaker(partial_data, 'testcase_08_data.csv',"index8_ra.csv", "index8_br.csv")


def test9(newdata):
    # testcase9: all book related types with an editor
    partial_data= newdata[40:43]
    testmaker(partial_data, 'testcase_09_data.csv',"index9_ra.csv", "index9_br.csv")


def test10(newdata):
    # testcase10: all proceeding related types with an editor
    partial_data= newdata[43:45]
    testmaker(partial_data, 'testcase_10_data.csv',"index10_ra.csv", "index10_br.csv")

test10(newdata)