import os
import time
from subprocess import Popen

import wget


def launch_blazegraph(port:int):
    '''
    Launch Blazegraph triplestore at a given port.
    '''
    Popen(
        ['java', '-server', '-Xmx4g', '-Dcom.bigdata.journal.AbstractJournal.file=./blazegraph.jnl', f'-Djetty.port={port}', '-jar', f'./blazegraph.jar']
    )

def main():
    if not os.path.isfile('blazegraph.jar'):
        url = 'https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/blazegraph.jar'
        wget.download(url=url, out='.')
    launch_blazegraph(9999)
    time.sleep(5)
    Popen(
        ['python', '-m', 'unittest', 'discover', '-s', 'test', '-p', '*test.py', '-b']
    )