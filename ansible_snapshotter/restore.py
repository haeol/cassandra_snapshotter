import argparse
import os
import sys

import zipfile

from ansible_wrapper import run_playbook

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Snapshotter')
    parser.add_argument('-d', '--path',
                        type=check_dir,
                        required=True,
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


def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')
    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def schema_parser(path):

    archive = zipfile.ZipFile(path + '/schemas.zip', 'r')
    schema = archive.read('schema.cql')


def ansible_restore(cmds):
    
    # unzip 
    print('Unzipping snapshot file')
    zip_path = cmds.path
    if zip_path.endswith('/') or zip_path.endswith('\\'):
        zip_path = zip_path[:-1]

    zipf = zipfile.ZipFile(path + '.zip', 'r')
    zipf.extractall(path)
    zipf.close()

    # remove None values and path
    playbook_args = dict((key, value) for key, value in cmds.iteritems()
                        if value != None and key != 'path')

    
    
    
    #run_playbook('snapshot.yml', 






if __name__ == '__main__':
    cmds = parse_cmd()
    ansible_restore(vars(cmds))
