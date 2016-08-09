import argparse
import os
import sys
import subprocess
import json

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
