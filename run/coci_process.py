from argparse import ArgumentParser
from meta.plugins.coci.get_dois_from_coci import get_dois_from_coci


if __name__ == '__main__':
    arg_parser = ArgumentParser('run_preprocess.py', description='This script create csv files from Crossref json,'
                                                                 ' enriching them thanks to an doi-orcid index')
    arg_parser.add_argument('-c', '--coci', dest='coci_dir', required=True,
                            help='COCI files directory')
    arg_parser.add_argument('-out', '--output', dest='output_file_path', required=True,
                            help='Path of the output CSV file')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    get_dois_from_coci(coci_dir=args.coci_dir, output_file_path=args.output_file_path, verbose=args.verbose)