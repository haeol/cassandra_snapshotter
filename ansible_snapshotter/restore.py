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
                        required=False,
                        help='Specify a path to the snapshot zip file'
    )
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=False,
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
    parser.add_argument('--hard-reset',
                        required=False,
                        action='store_true',
                        help='Hard reset Cassandra on all nodes, then restore'
    )
    parser.add_argument('--s3',
                        required=False,
                        nargs='?',
                        const=True,
                        help='Specify an s3 object key, or search S3 bucket for a snapshot'
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


def get_zipped_schema(path):

    archive = zipfile.ZipFile(path, 'r')
    schema_cql = archive.read('schema.cql')

    matcher = 'CREATE TABLE (\w{1,})\.(\w{1,})'
    r = re.compile(matcher)

    schema = {}
    for ks, tb in re.findall(r, schema_cql):

        if ks in schema:
            schema[ks].add(tb)
        else:
            schema[ks] = set([tb])
    return schema


def ansible_restore(cmds):

    if not (bool(cmds.path) ^ bool(cmds.s3)):
        raise Exception('Only one of --path or --s3 must be specified') 

    if not cmds.nodes:
        config = ConfigParser()
        if len(config.read('config.ini')) == 0:
            raise Exception('ERROR: Cannot find config.ini in script directory')
        nodes = config.get('cassandra-info', 'hosts')
        if not nodes:
            raise Exception('Hosts argument in config.ini not specified')
    else:
        nodes = cmds.nodes

    # prepare working directories
    print('Preparing ./output_logs')
    if make_dir(sys.path[0] + '/output_logs'):
        clean_dir(sys.path[0] + '/output_logs')
    print('Preparing ./.temp')
    temp_path = sys.path[0] + '/.temp'
    if make_dir(temp_path):
        clean_dir(temp_path)
    
    if cmds.path:
        zip_path = cmds.path
    elif cmds.s3:

        s3 = s3_bucket()
        s3_snapshots = set(s3_list_snapshots(s3))

        if cmds.s3 == True: # not a string parameter
            if len(s3_snapshots) == 0:
                print('No snapshots found in s3')
                exit(0)
            # search 
            # TODO refine this process to select by index, list date ss made
            print('\nSnapshots found:')
            for snap in s3_snapshots:
                print(snap)
            choice = ''
            while choice not in s3_snapshots:
                choice = raw_input('Enter snapshot: ')
            s3_key = choice
        else:
            if cmds.s3 not in s3_snapshots:
                raise Exception('S3 Snapshot not found')
            s3_key = cmds.s3

        s3.download_file(s3_key, temp_path + '/temp.zip') 
        zip_path = temp_path + '/temp.zip'
    else:
        raise Exception('No file specified.')

    # unzip 
    print('Unzipping snapshot file')
    z = zipfile.ZipFile(zip_path, 'r')
    z.extractall(temp_path)

    # check schema specification args
    print('Checking arguments . . .')
    restore_command = 'restore.py '
    load_schema_command = 'load_schema.py '
    if cmds.keyspace:

        schema = get_zipped_schema(temp_path + '/schemas.zip')
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
        'nodes': nodes,
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
