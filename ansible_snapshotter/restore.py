import argparse
import os
import sys
import re
import zipfile

from utils import *

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Restorer')
    parser.add_argument('-d', '--path',
                        type=check_file,
                        required=True,
                        help='Specify a path to the snapshot zip file'
    )
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=True,
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
    parser.add_argument('--reload',
                        required=False,
                        action='store_true',
                        help='Reset the snapshotter files in the nodes'
    )
    return parser.parse_args()


def check_file(f):

    if not os.path.isfile(f):
        raise argparse.ArgumentTypeError('File does not exist')
    if os.access(f, os.R_OK):
        if zipfile.is_zipfile(f):
            return f
        else:
            raise argparse.ArgumentTypeError('File is not a zip file')
    else:
        raise argparse.ArgumentTypeError('File is not readable')


def schema_parser(path):

    archive = zipfile.ZipFile(path + '/schemas.zip', 'r')
    schema = archive.read('schema.cql')


def ansible_restore(cmds):
    
    if cmds.path:
        zip_path = cmds.path
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

    # check args
    archive = zipfile.ZipFile(temp_path + '/schemas.zip', 'r')
    schema_cql = archive.read('schema.cql')

    matcher = 'CREATE TABLE (\w{1,})\.(\w{1,})'
    r = re.compile(matcher)

    schema = {}
    for ks, tb in re.findall(r, schema_cql):

        if ks in schema:
            schema[ks].add(tb)
        else:
            schema[ks] = set([tb])

    print('Checking arguments . . .')
    restore_command = 'restore.py '
    load_schema_command = 'load_schema.py '
    if cmds.keyspace:

        for keyspace in cmds.keyspace:
            if keyspace not in schema.keys():
                raise Exception('Keyspace "%s" not in snapshot schema' % keyspace)

        keyspace_arg = '-ks ' + ' '.join(cmds.keyspace)
        restore_command += keyspace_arg
        load_schema_command += keyspace_arg
                
        if cmds.table:

            if len(cmds.keyspace) != 1:
                raise Exception('ERROR: One keyspace must be specified with table argument')

            ks = cmds.keyspace[0]
            for tb in cmds.table:
                if tb not in schema[ks]:
                    raise Exception('Table "%s" not found in keyspace "%s"' % (tb, ks))

            restore_command += ' -tb ' + ' '.join(cmds.table)

    elif cmds.table:
        raise Exception('ERROR: Keyspace must be specified with tables')

    playbook_args = {
        'nodes': cmds.nodes,
        'restore_command' : restore_command,
        'load_schema_command' : load_schema_command,
        'reload' : cmds.reload
    }
    return_code = run_playbook('restore.yml', playbook_args)
    
    if return_code != 0:
        print('ERROR: Ansible script failed to run properly. ' +
              'If this persists, try --hard-reset.')
    else:
        print('Process complete.')
        print('Output logs saved in %s' % (sys.path[0] + '/output_logs'))
    

if __name__ == '__main__':
    cmds = parse_cmd()
    ansible_restore(cmds)
