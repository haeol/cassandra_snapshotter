import argparse
import os
import subprocess
import json
import shutil
import zipfile

def run_playbook(play, args):
    # pass args as a dict
    
    if not play.endswith('.yml'):
        play += '.yml'
    if not os.path.isfile(play):
        raise Exception('File does not exist: %s' % play)

    cmd = ['ansible-playbook', play]
    
    if args:
        cmd.append('--extra-vars')
        cmd.append(json.dumps(args))

    return subprocess.call(cmd) # 0 success 1 fail


def clean_dir(path):

    for f in os.listdir(path):
        if os.path.isdir(path + '/' + f):
            shutil.rmtree(path + '/' + f)
        else:
            os.remove(path + '/' + f)


def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')
    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def zip_dir(root_path, save_path, title): # use shutil.make_archive in python2.7

    rootlength = len(root_path)
    z = zipfile.ZipFile(save_path + '/' + title + '.zip',
                        'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(root_path):
        for f in files:
            filename = os.path.join(root_path, f)
            z.write(filename, filename[rootlength:])
    z.close()


def make_dir(path):

    exists = False
    if not os.path.isdir(path):
        os.makedirs(path)
    else:
        exists = True
    return exists


