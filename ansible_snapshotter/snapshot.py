import argparse
import os
import sys
import time
import zipfile
import shutil

from ansible_wrapper import run_playbook

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Snapshotter')
    parser.add_argument('-d', '--path',
                        type=check_dir,
                        required=False,
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
    return parser.parse_args()


def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')
    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def ansible_snapshot(cmds):

    if cmds['title']:
        title = cmds['title']
    else:
        title = str(time.time()).split('.')[0]

    # remove None values and path
    if cmds['path']:
        save_path = cmds['path']
    else:
        save_path = sys.path[0] + '/snapshots'
        if not os.path.isdir(save_path):
            os.makedirs(save_path)

    save_path = save_path + '/' + title
    if os.path.isdir(save_path):
        raise Exception('%s has already been created' % save_path)
    else:
        os.makedirs(save_path)
    
    if not os.path.isdir(sys.path[0] + '/output_logs'):
        os.makedirs(sys.path[0] + '/output_logs')
    
    playbook_args = dict((key, value) for key, value in cmds.iteritems()
                        if value != None and key != 'path' and key != 'title')
    playbook_args['path'] = save_path

    return_code = run_playbook('snapshot.yml', playbook_args)
    if return_code != 0:
        shutil.rmtree(save_path)
    else:
        shutil.make_archive(save_path, 'zip', save_path)


if __name__ == '__main__':

    cmds = parse_cmd()
    ansible_snapshot(vars(cmds))
