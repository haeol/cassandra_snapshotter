import argparse
import os
import sys
import time
import shutil
try:
    from ConfigParser import ConfigParser
except:
    from configparser import ConfigParser # python3

from utils import (run_playbook, s3_bucket, clean_dir, make_dir, check_dir, zip_dir)

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Snapshotter')
    parser.add_argument('-d', '--path',
                        type=check_dir,
                        required=True,
                        help='Specify a path to the snapshot zip file'
    )
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=False,
                        help='Enter the host group from the inventory'
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
    parser.add_argument('--s3',
                        required=False,
                        action='store_true',
                        help='Store in s3 using config.ini settings'
    )
    parser.add_argument('--reload',
                        required=False,
                        action='store_true',
                        help='Reset the snapshotter files in the nodes'
    )
    return parser.parse_args()


def ansible_snapshot(cmds):

    # set title of snapshot file
    timestamp = str(time.time()).split('.')[0]
    if cmds.title:
        title = cmds.title
    else:
        title = timestamp

    if not cmds.nodes:
        config = ConfigParser()
        if len(config.read('config.ini')) == 0:
            raise Exception('ERROR: Cannot find config.ini in script directory')
        nodes = config.get('cassandra-info', 'hosts')
        if not nodes:
            raise Exception('Hosts argument in config.ini not specified')
    else:
        nodes = cmds.nodes

    if cmds.s3:
        s3 = s3_bucket() # checks config.ini args

    # path to save snapshot in
    if cmds.path:
        save_path = cmds.path
    else:
        save_path = sys.path[0] + '/snapshots'
        make_dir(save_path)
    
    if os.path.isfile(save_path + '/' + title + '.zip'):
        raise Exception('%s has already been created' %
                        save_path + '/' + title + '.zip')

    # create working directories
    if make_dir(sys.path[0] + '/output_logs'):
        clean_dir(sys.path[0] + '/output_logs')

    temp_path = sys.path[0] + '/.temp'
    if make_dir(temp_path):
        clean_dir(temp_path)
    os.makedirs(temp_path + '/' + title)

    # check keyspace and table args
    snapshotter_command = 'snapshotter.py '
    save_schema_command = 'save_schema.py '
    if cmds.keyspace:

        keyspace_arg = '-ks ' + ' '.join(cmds.keyspace)
        snapshotter_command += keyspace_arg
        save_schema_command += keyspace_arg

        if cmds.table:
            if len(cmds.keyspace) != 1:
                raise Exception('ERROR: One keyspace must be specified with table argument')
            snapshotter_command += ' -tb ' + ' '.join(cmds.table)

    elif cmds.table:
        raise Exception('ERROR: Keyspace must be specified with tables')

    playbook_args = {
        'nodes' : nodes,
        'snapshotter_command' : snapshotter_command,
        'save_schema_command' : save_schema_command,
        'path' : temp_path + '/' + title,
        'reload' : cmds.reload
    }

    # call playbook
    return_code = run_playbook('snapshot.yml', playbook_args)

    if return_code != 0:
        shutil.rmtree(temp_path + '/' + title)
        print('Error running ansible script')
    else:
        zip_dir(temp_path + '/' + title, save_path, title)
        if cmds.s3:
            file_size = os.path.getsize(save_path + '/' + title + '.zip')
            options = set(['y', 'Y', 'n', 'N'])
            confirm = ''
            while confirm not in options:
                confirm = raw_input('Snapshot size is %i bytes. Upload? [y/n] '
                                   % file_size)
            if confirm.lower() == 'y':
                print('Uploading to s3 . . .')
                key = 'cassandra-snapshot-' + title
                upload = True
                if key in [str(obj.key) for obj in s3.objects.all()]:
                    confirm = ''
                    while confirm not in options:
                        confirm = raw_input('"%s" already exists in the S3 bucket. Overwrite? [y/n] '
                                            % key)
                    if confirm.lower() == 'n':
                        upload = False
                        print('Skipping upload to s3 . . .')

                if upload:
                    s3.upload_file(save_path + '/' + title + '.zip', key)
                    print('Uploaded with key "%s"' % key)

        print('Process complete.')
        print('Output logs saved in %s' % (sys.path[0] + '/output_logs'))
        print('Snapshot zip saved in %s' % save_path)


if __name__ == '__main__':

    cmds = parse_cmd()
    ansible_snapshot(cmds)
