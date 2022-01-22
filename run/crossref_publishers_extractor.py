from meta.plugins.crossref.extract_crossref_publishers import process
from argparse import ArgumentParser



if __name__ == "__main__":
    arg_parser = ArgumentParser("Extract publisher information from Crossref")
    arg_parser.add_argument("-o", "--output", required=True,
                            help="The output CSV file where to store relevant information.")
    args = arg_parser.parse_args()

    print("Start process")
    process(args.output)
    print("Process finished")
