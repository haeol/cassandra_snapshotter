import argparse
import os
import sys

import zipfile

from utils import *

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Snapshotter')
    parser.add_argument('-d', '--path',
                        type=check_file,
                        required=False,
                        help='Specify a path to the snapshot zip file'
    )
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=False,
                        nargs='+',
                        help='Enter the host IPs'
    )
    parser.add_argument('-k', '-ks', '--keyspace',
                        required=False,
                        nargs='+',
                        help='Specify a keyspace'
    )
    parser.add_argument('-tb', '--table', '-cf', '--column-family',
                        required=False,
                        nargs='+',
                        help='Enter table(s) corresponding to a single keyspace'
    )
    parser.add_argument('-t', '--title', '--tag', '--name',
                        required=False,
                        help='Enter title/name for snapshot'
    )
    return parser.parse_args()


def check_file(f):

    if not os.path.isfile(f):
        raise argparse.ArgumentTypeError('File does not exist')
    if os.access(f, os.R_OK):
        return f
    else:
        raise argparse.ArgumentTypeError('File is not readable')


def schema_parser(path):

    archive = zipfile.ZipFile(path + '/schemas.zip', 'r')
    schema = archive.read('schema.cql')


def ansible_restore(cmds):
    
    if cmds['path']:
        zip_path = cmds['path']
    else:
        # option to select from snapshots based on date last modified
        raise Exception('No file specified.')

    # prepare working directories
    if make_dir(sys.path[0] + '/output_logs'):
        clean_dir(sys.path[0] + '/output_logs')

    temp_path = sys.path[0] + '/.temp'
    if make_dir(temp_path):
        clean_dir(temp_path)

    # unzip 
    print('Unzipping snapshot file')
    z = zipfile.ZipFile(zip_path, 'r')
    z.extractall(temp_path)
    exit(0)

    # TODO check snapshot files by regex on schema in zip


    # remove None values and path
    playbook_args = dict((key, value) for key, value in cmds.iteritems()
                        if value != None and key != 'path')

    
    
    






if __name__ == '__main__':
    cmds = parse_cmd()
    ansible_restore(vars(cmds))
