import os
import sys
import argparse
import subprocess
import shutil
import time
import zipfile

from cass_functions import get_rpc_address

def parse_cmd():

    parser = argparse.ArgumentParser(description='Snapshot Restorer')
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=True,
                        nargs='+',
                        help='Enter the host IPs'
    )
    parser.add_argument('-k', '-ks', '--keyspace',
                        required=False,
                        nargs='+',
                        help="Specify keyspace(s)"
    )
    parser.add_argument('-tb', '-t', '--table', '-cf', '--column_family',
                        required=False,
                        nargs='+',
                        help="Specify table(s)"
    )
    return parser.parse_args()


def clean_dir(path):

    for f in os.listdir(path):
        if os.path.isdir(path + '/' + f):
            shutil.rmtree(path + '/' + f)
        else:
            os.remove(path + '/' + f)


def make_dir(path):

    exists = False
    if not os.path.isdir(path):
        os.makedirs(path)
    else:
        exists = True
    return exists


def restore(hosts, keyspace_arg = None, table_arg = None):

    cqlsh_host = get_rpc_address()
    snapshot_path = sys.path[0] + '/.snapshots'
    temp_path = sys.path[0] + '/.temp'

    print('Unzipping snapshot file')
    if make_dir(temp_path):
        clean_dir(temp_path)

    zip_path = snapshot_path + '/' + cqlsh_host + '.zip'
    zipf = zipfile.ZipFile(zip_path, 'r')
    zipf.extractall(temp_path)
    zipf.close()

    for ks in os.listdir(temp_path):
        print('Loading keyspace: %s' % ks)
        for tb in os.listdir(temp_path + '/' + ks):
            print('\tLoading table: %s' % tb)
            tb_dir = temp_path + '/' + ks + '/' + tb
            subprocess.call(['/bin/sstableloader', '-d', ','.join(hosts), tb_dir])

    print('Restoration complete')


if __name__ == '__main__':

    cmds = parse_cmd()

    start = time.time()
    restore(cmds.nodes, cmds.keyspace, cmds.table)
    end = time.time()

    print('Elapsed time: %s' % (end - start))

