import argparse
import os
import sys
import time
import shutil

from utils import (run_playbook, clean_dir, make_dir, check_dir, zip_dir)

def parse_cmd():

    parser = argparse.ArgumentParser(description='Ansible Cassandra Snapshotter')
    parser.add_argument('-d', '--path',
                        type=check_dir,
                        required=False,
                        help='Specify a path to the snapshot zip file'
    )
    parser.add_argument('-n', '--nodes', '--hosts',
                        required=True,
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
    parser.add_argument('--reset',
                        required=False,
                        action='store_true',
                        help='Reset the snapshotter files in the nodes'
    )
    return parser.parse_args()


def ansible_snapshot(cmds):

    if cmds['title']:
        title = cmds['title']
    else:
        title = str(time.time()).split('.')[0]

    if cmds['path']:
        save_path = cmds['path']
    else:
        save_path = sys.path[0] + '/snapshots'
        make_dir(save_path)
    
    if os.path.isfile(save_path + '/' + title + '.zip'):
        raise Exception('%s has already been created' %
                        save_path + '/' + title + '.zip')

    if make_dir(sys.path[0] + '/output_logs'):
        clean_dir(sys.path[0] + '/output_logs')

    temp_path = sys.path[0] + '/.temp'
    if make_dir(temp_path):
        clean_dir(temp_path)
    os.makedirs(temp_path + '/' + title)

    # remove None values and path argument
    playbook_args = dict((key, value) for key, value in cmds.iteritems()
                        if value != None and key != 'path' and key != 'title')
    playbook_args['path'] = temp_path + '/' + title

    return_code = run_playbook('snapshot.yml', playbook_args)
    if return_code != 0:
        shutil.rmtree(temp_path + '/' + title)
    else:
        zip_dir(temp_path + '/' + title, save_path, title)

    print('Process complete.')
    print('Output logs saved in %s' % (sys.path[0] + '/output_logs'))
    print('Snapshot zip saved in %s' % save_path)


if __name__ == '__main__':

    cmds = parse_cmd()
    ansible_snapshot(vars(cmds))
