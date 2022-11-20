from argparse import ArgumentParser

from oc_meta.plugins.orcid.index_orcid_doi import Index_orcid_doi

if __name__ == '__main__': # pragma: no cover
    arg_parser = ArgumentParser('index_orcid_doi.py', description='This script builds a csv index of DOIs associated'
                                                                  ' with ORCIDs, starting from XML files containing'
                                                                  ' ORCID data.')
    arg_parser.add_argument('-out', '--output', dest='output_path', required=True,
                            help='The output CSV file path.')
    arg_parser.add_argument('-s', '--summaries', dest='summaries_path', required=True,
                            help='The folder path containing orcid summaries, subfolder will be considered too.')
    arg_parser.add_argument('-t', '--threshold', dest='threshold', required=False,
                            help='Number of files to save after.')
    arg_parser.add_argument('-lm', '--low-memory', dest='low_memory', required=False, action='store_true',
                            help='Specify this argument if the available RAM is insufficient to accomplish the task. Warning: the processing time will increase.')
    arg_parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', required=False,
                            help='Show a loading bar, elapsed time and estimated time.')
    args = arg_parser.parse_args()
    iOd = Index_orcid_doi(output_path = args.output_path, threshold=args.threshold, verbose=args.verbose)
    iOd.explorer(summaries_path=args.summaries_path)