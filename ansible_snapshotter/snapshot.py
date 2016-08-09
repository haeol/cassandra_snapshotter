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
    return parser.parse_args()


def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')
    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def zipdir(root_path, save_path, title):

    zipf = zipfile.ZipFile(save_path + '/' + title + '.zip',
                           'w', zipfile.ZIP_DEFLATED)
    rootlength = len(root_path)
    for root, dirs, files in os.walk(root_path):
        for f in files:
            filename = os.path.join(root_path, f)
            zipf.write(filename, filename[rootlength:])


def make_directory(path):

    exists = False
    if not os.path.isdir(path):
        os.makedirs(path)
    else:
        exists = True
    return exists


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

    temp_path = sys.path[0] + '/.temp'
    zipfile = save_path + '/' + title + '.zip'
    if os.path.isfile(zipfile):
        raise Exception('%s has already been created' % zipfile)

    make_directory(save_path)
    make_directory(sys.path[0] + '/output_logs')
    if make_directory(temp_path):
        for f in os.listdir(temp_path):
            if os.path.isdir(temp_path + '/' + f):
                shutil.rmtree(temp_path + '/' + f)
            else:
                os.remove(temp_path + '/' + f)
    os.makedirs(temp_path + '/' + title)

    playbook_args = dict((key, value) for key, value in cmds.iteritems()
                        if value != None and key != 'path' and key != 'title')
    playbook_args['path'] = temp_path + '/' + title

    return_code = run_playbook('snapshot.yml', playbook_args)
    if return_code != 0:
        shutil.rmtree(temp_path + '/' + title)
    else:
        zipdir(temp_path + '/' + title, save_path, title)

    print('Process complete, output logs saved in %s' %
         (sys.path[0] + '/output_logs'))


if __name__ == '__main__':

    cmds = parse_cmd()
    ansible_snapshot(vars(cmds))
