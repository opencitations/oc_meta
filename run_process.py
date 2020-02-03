from meta.scripts.creator import *
from meta.scripts.curator import *
from meta.lib.storer import *
from meta.lib.resfinder import ResourceFinder
from meta.lib.conf import base_iri, context_path, info_dir, triplestore_url, \
    base_dir, temp_dir_for_rdf_loading, dir_split_number, items_per_file, default_dir
from datetime import datetime
from argparse import ArgumentParser



def process(crossref_csv_dir, csv_dir, index_dir, auxiliary_path, triplestore, source=None):
    for filename in os.listdir(crossref_csv_dir):
        pathoo(auxiliary_path)
        aux_file = open(auxiliary_path, "r+")
        completed = [line.rstrip('\n') for line in aux_file]
        if filename.endswith(".csv") and filename not in completed:
            filepath = os.path.join(crossref_csv_dir, filename)
            data = unpack(filepath)
            curator_obj = Curator(data, triplestore, info_dir=info_dir)
            name = datetime.now().strftime("%Y-%m-%dT%H_%M_%S")
            pathoo(csv_dir)
            pathoo(index_dir)
            curator_obj.curator(filename=name, path_csv=csv_dir, path_index=index_dir)

            creator_obj = Creator(curator_obj.data, base_iri, curator_obj.index_id_ra, curator_obj.index_id_br,
                              curator_obj.re_index, curator_obj.ar_index, curator_obj.VolIss)
            creator = creator_obj.creator(source=source)
            prov = ProvSet(creator, base_iri, context_path, default_dir, "counter_prov/counter_",
                           ResourceFinder(base_dir=base_dir, base_iri=base_iri,
                                          tmp_dir=temp_dir_for_rdf_loading,
                                          context_map= {},
                                          dir_split=dir_split_number,
                                          n_file_item=items_per_file,
                                          default_dir=default_dir), dir_split_number,
                                          items_per_file, "", wanted_label=False)
            prov.generate_provenance("https://w3id.org/oc/meta/prov/pa/1")

            res_storer = Storer(creator,
                                context_map={},
                                dir_split=dir_split_number,
                                n_file_item=items_per_file,
                                default_dir=default_dir,
                                nt=True)

            prov_storer = Storer(prov,
                                 context_map={},
                                 dir_split=dir_split_number,
                                 n_file_item=items_per_file,
                                 nq=True)

            res_storer.upload_and_store(
                base_dir, triplestore_url, base_iri, context_path,
                temp_dir_for_rdf_loading)

            prov_storer.store_all(
                base_dir, base_iri, context_path,
                temp_dir_for_rdf_loading)

            aux_file.write(filename +"\n")
            aux_file.close()


def pathoo(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

def unpack(path):
    with open(path, 'r', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        data = [dict(x) for x in reader]
    return data



if __name__ == "__main__":
    arg_parser = ArgumentParser("run_process.py", description="This script runs OCMeta data processing workflow")

    arg_parser.add_argument("-c", "--crossref", dest="crossref_csv_dir", required=True,
                            help="Csv files directory")

    arg_parser.add_argument("-v", "--csv", dest="csv_dir", required=True,
                            help="Directory where cleaned CSV will be stored")

    arg_parser.add_argument("-i", "--ind", dest="index_dir", required=True,
                            help="Directory where cleaned indices will be stored")

    arg_parser.add_argument("-a", "--aux", dest="auxiliary_path", required=True,
                            help="Txt file containing processed CSV list filepath")

    arg_parser.add_argument("-t", "--tri", dest="triplestore", required=True,
                            help="Triplestore URL")

    arg_parser.add_argument("-s", "--src", dest="source", required=False,
                            help="Data source, not mandatory")

    args = arg_parser.parse_args()

    process(args.crossref_csv_dir, args.csv_dir, args.index_dir, args.auxiliary_path, args.triplestore, source=args.source)
