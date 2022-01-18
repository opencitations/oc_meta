from meta.plugins.multiprocess.split_by_publisher import *
from argparse import ArgumentParser


if __name__ == '__main__':
    arg_parser = ArgumentParser('index_orcid_doi.py', description='')
    arg_parser.add_argument('-c', '--csv', dest='meta_input', required=True,
                        help='The Meta input CSV files directory.')
    arg_parser.add_argument('-out', '--output', dest='output_dir', required=True,
                        help='The Meta input CSV files directory.')
    args = arg_parser.parse_args()
    output = split_by_publisher(args.meta_input)
    # dump_csvs_by_publisher(output, args.output_dir)