from argparse import ArgumentParser
from meta.lib.file_manager import normalize_path
from meta.run.meta_process import MetaProcess, run_meta_process
from meta.plugins.multiprocess.prepare_multiprocess import prepare_relevant_items, split_by_publisher
import os
import shutil
import yaml


TMP_DIR = './tmp_dir'

if __name__ == '__main__':
    arg_parser = ArgumentParser('prepare_multiprocess.py', description='Venues are preprocessed not to create duplicates when running Meta in multi-process')
    arg_parser.add_argument('-c', '--config', dest='config', required=True,
                        help='Configuration file path')
    args = arg_parser.parse_args()
    config = args.config
    with open(config, encoding='utf-8') as file:
        settings = yaml.full_load(file)
    csv_dir = normalize_path(settings['input_csv_dir'])
    items_per_file = settings['items_per_file']
    verbose = settings['verbose']
    
    prepare_relevant_items(csv_dir=csv_dir, output_dir=TMP_DIR, items_per_file=items_per_file, verbose=verbose)
    meta_process = MetaProcess(config)
    meta_process.input_csv_dir = TMP_DIR
    run_meta_process(meta_process=meta_process)
    shutil.rmtree(TMP_DIR)
    split_by_publisher(csv_dir=csv_dir, output_dir=TMP_DIR, verbose=verbose)
    os.rename(csv_dir, csv_dir + '_old')
    os.mkdir(csv_dir)
    for file in os.listdir(TMP_DIR):
        shutil.move(os.path.join(TMP_DIR, file), csv_dir)
    shutil.rmtree(TMP_DIR)
    os.remove(meta_process.cache_path)
