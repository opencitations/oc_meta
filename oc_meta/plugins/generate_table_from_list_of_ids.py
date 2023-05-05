from oc_meta.lib.file_manager import get_csv_data, write_csv


def generate_table_from_list_of_ids(filepath:str):
    table = []
    ids = set()
    data = get_csv_data(filepath)
    for row in data:
        ids.add(row['id'])
    for identifier in ids:
        fieldnames = ['id', 'title', 'author', 'issue', 'volume', 'venue', 'pub_date', 'page', 'type', 'editor', 'publisher']
        fields_dict = {field: '' for field in fieldnames}
        fields_dict['id'] = f"doi:{identifier}"
        table.append(fields_dict)
    write_csv('E:/doci_entities_check/citing_entities_with_no_omid_input.csv', table)

generate_table_from_list_of_ids('E:/doci_entities_check/citing_entities_with_no_omid.csv')