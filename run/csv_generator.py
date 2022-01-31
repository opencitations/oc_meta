from meta.plugins.csv_generator.csv_generator import *
from argparse import ArgumentParser

ENDPOINT = 'http://localhost:29999/blazegraph/sparql'
BASE_IRI = 'https://w3id.org/oc/meta'
PREFIX = '060'
COUNTER_DIR = 'C:\meta\output\info_dir'

if __name__ == '__main__':
    csv_generator = CSVGenerator(ENDPOINT, COUNTER_DIR)
    csv_generator.generate_csv(BASE_IRI, PREFIX)