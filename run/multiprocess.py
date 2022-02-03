from meta.plugins.multiprocess.split_by_publisher import *
from argparse import ArgumentParser


if __name__ == '__main__':
    arg_parser = ArgumentParser('prepare_multiprocess.py', description='')
    arg_parser.add_argument('-c', '--csv', dest='meta_input', required=True,
                        help='The Meta input CSV files directory.')
    arg_parser.add_argument('-out', '--output', dest='output_dir', required=True,
                        help='The Meta input CSV files directory.')
    arg_parser.add_argument('-w', '--wanted', dest='wanted_dois', required=False,
                            help='A CSV filepath containing what DOI to process, not mandatory')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    output = run(csv_dir=args.meta_input, output_dir=args.output_dir, wanted_dois=args.wanted_dois, verbose=args.verbose)