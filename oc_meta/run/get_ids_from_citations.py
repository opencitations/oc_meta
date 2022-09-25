from argparse import ArgumentParser
from oc_meta.plugins.get_ids_from_citations import get_ids_from_citations


if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('run_preprocess.py', description='This script extracts the identifiers of the citing and cited documents from citation data organized in the CSV format accepted by OpenCitations. ')
    arg_parser.add_argument('-c', '--citations', dest='citations_dir', required=True,
                            help='The directory containing the citations files, either in CSV or ZIP format')
    arg_parser.add_argument('-out', '--output', dest='output_dir', required=True,
                            help='Directory of the output CSV files')
    arg_parser.add_argument('-t', '--threshold', dest='threshold', required=False,
                            help='Number of files to save after')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time')
    args = arg_parser.parse_args()
    get_ids_from_citations(citations_dir=args.citations_dir, output_dir=args.output_dir, threshold=args.threshold, verbose=args.verbose)