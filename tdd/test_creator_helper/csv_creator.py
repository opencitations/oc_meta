from converter import*

with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter="\t")
    newdata = [dict(x) for x in reader]


def testmaker(partial_data, path):
    newcleandata = Converter(partial_data, 'http://127.0.0.1:9999/blazegraph/sparql').data
    keys = newcleandata[0].keys()

    with open(path, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, delimiter="\t")
        dict_writer.writeheader()
        dict_writer.writerows(newcleandata)


def test11(newdata):
    partial_data =  list()
    partial_data.append(newdata[52])
    testmaker(partial_data, 'testcase_12_data.csv')

test11(newdata)