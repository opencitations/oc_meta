from converter import*

with open("new_test_data.csv", 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter="\t")
    newdata = [dict(x) for x in reader]

newcleandata = converter(newdata).data
keys = newcleandata[0].keys()

with open('new_test_clean_data.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(newcleandata)