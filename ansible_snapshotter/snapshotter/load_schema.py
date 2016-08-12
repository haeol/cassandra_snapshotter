import os
import sys
import argparse
import subprocess
import shutil
import datetime
import time
import zipfile

from cass_functions import (get_data_dir, get_keyspaces, get_dir_structure,
                            get_rpc_address, cassandra_query)

def parse_cmd():

    parser = argparse.ArgumentParser(description='Cassandra Schema Loader')
    parser.add_argument('-k', '-ks', '--keyspace',
                        required=False,
                        nargs='+',
                        help='Specify a keyspace'
    )
    return parser.parse_args()


def _load(host, load_path):
    
    with open(load_path, 'r') as f:
        cassandra_query(host, f.read())


def load_schema(keyspace = None):

    temp_path = sys.path[0] + '/.temp'
    host = get_rpc_address()

    # unzip schemas.zip
    print('Unzipping schemas.zip')
    z = zipfile.ZipFile(temp_path + '/schemas.zip', 'r')
    z.extractall(temp_path)

    if keyspace:
        for ks in keyspace:
            print('Loading keyspace: %s' % ks)
            _load(host, temp_path + '/' + ks + '/' + ks + '_schema.cql')
    else:
        _load(host, temp_path + '/schema.cql') 
        

if __name__ == '__main__':
    cmds = parse_cmd()
    load_schema(cmds.keyspace)
