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


def load_schema(save_path, keyspace = None):
    snapshot_root = sys.path[0] + '/.snapshots/'

    load_path = snapshot_root + cqlsh_host
    print('Unzipping schema files')
    os.mkdir(load_path)
    zip_path = load_path + '.zip'
    zipf = zipfile.ZipFile(zip_path, 'r')
    zipf.extractall(load_path)
    zipf.close()

    if keyspace:
        save_path = save_path + '/' + keyspace + '/'
        filename = keyspace + '_schema.cql'
        query = ("DESCRIBE KEYSPACE %s;" % keyspace)
    else:
        save_path = save_path + '/'
        filename = 'schema.cql'
        query = ("DESCRIBE SCHEMA;")

    with open(save_path + '/' + filename, 'w') as f:
        query_process = subprocess.Popen(['echo', query], stdout=subprocess.PIPE)
        cqlsh = subprocess.Popen(('/bin/cqlsh', host),
                                  stdin=query_process.stdout, stdout=f)
        cqlsh.wait()
        query_process.stdout.close()

    return (save_path + filename)

if __name__ == '__main__':
    load_schema(
