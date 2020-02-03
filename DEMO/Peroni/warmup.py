from preprocess import preprocess
from process import process
from orcid import index_orcid_doi




crossref_json_dir =  "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\json\\"
orcid_doi_filepath = "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\preprocess\\orcid.csv"
wanted_doi_filepath = "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\preprocess\\doi.csv"
csv_dir = "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\raw_csv\\"

preprocess(crossref_json_dir, orcid_doi_filepath, wanted_doi_filepath, csv_dir)

print("preprocess")



crossref_csv_dir=  csv_dir
clean_csv_dir= "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\csv_e\\"
index_dir= "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\csv_e\\index\\"
auxiliary_path= "C:\\Users\\Fabio\\Documents\\GitHub\\meta\\DEMO\\Peroni\\raw_csv\\auxilia\\log.txt"
triplestore=  "http://127.0.0.1:9999/blazegraph/sparql"

process(crossref_csv_dir, clean_csv_dir, index_dir, auxiliary_path, triplestore)
