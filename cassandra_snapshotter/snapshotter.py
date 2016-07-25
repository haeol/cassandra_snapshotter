import os
import argparse
import subprocess
import shutil
import datetime
import time

from cass_functions import (get_data_dir, get_keyspaces, get_dir_structure)

def parse_cmd():

    parser = argparse.ArgumentParser(description='Snapshotter')

    parser.add_argument('-d', '--path',
                        type=check_dir,
                        required=True,
                        help='Specify path to save snapshots'
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
    parser.add_argument('-t', '--title', '--tag', '-n', '--name',
                        required=False,
                        help='Enter title/name for snapshot'
    )
    parser.add_argument('-n', '--node', '-h', '--host',
                        required=False,
                        help='Enter the host ip'
    )
    return parser.parse_args()


def check_dir(folder):

    if not os.path.isdir(folder):
        raise argparse.ArgumentTypeError('Directory does not exist')

    if os.access(folder, os.R_OK):
        return folder
    else:
        raise argparse.ArgumentTypeError('Directory is not readable')


def write_schema(save_path, keyspace = None):

    if keyspace:
        save_path = save_path + '/' + keyspace + '/'
        filename = keyspace + '_schema.cql'
        query = ("DESCRIBE KEYSPACE %s;" % keyspace)
    else:
        save_path = save_path + '/'
        filename = 'schema.cql'
        query = ("DESCRIBE SCHEMA;")

    with open(save_path + '/' + filename, 'w') as f:
        query_process = subprocess.Popen(['echo', query], stdout=subprocess.PIPE)
        cqlsh = subprocess.Popen(('/bin/cqlsh'), stdin=query_process.stdout,
                                                 stdout=f)
        query_process.stdout.close()

    return (save_path + filename)


def run_snapshot(title, host='localhost', keyspace=None, table=None):

    cmd = 'nodetool -h %(host)s snapshot -t %(title)s ' % \
          dict(host=host, title=title)
    if keyspace:

        if table:
            cmd = '%(cmd)s -cf %(table)s ' % dict(cmd=cmd, table=table)
        cmd = cmd + keyspace

    subprocess.call(cmd.split())
    '''
    if keyspace:
        if table:
            subprocess.call(['nodetool', 'snapshot', '-t', title, \
                             '-cf', table, keyspace])
        else:
            subprocess.call(['nodetool', 'snapshot', '-t', title, keyspace])
    else:
        subprocess.call(['nodetool', 'snapshot', '-t', title])
    '''



def snapshot(save_path, host='localhost', title_arg=None, keyspace_arg=None,
             table_arg=None):

    # clear snapshot in default snapshot directory TODO: host and port option
    print('Clearing previous cassandra data snapshots . . .')
    try:
        subprocess.check_output(['nodetool', 'status'])
        subprocess.call(['nodetool', 'clearsnapshot'])
    except:
        raise Exception('Cassandra has not yet started')

    if not title_arg:
        title = '{:%Y-%m-%d_%H-%M-%S}'.format(datetime.datetime.now())
    else:
        title = title_arg

    save_path = save_path + title
    if os.path.exists(save_path):
        raise Exception('Error: Snapshot directory already created')

    keyspaces = get_keyspaces() # set of keyspaces

    if len(keyspaces) == 0: # edge case
        print('No keyspaces to snapshot')
        return

    structure = get_dir_structure(keyspaces)

    print('Checking keyspace arguments . . .')
    if keyspace_arg:
        for ks in keyspace_arg:
            if ks not in keyspaces:
                raise Exception('Keyspace "%s" not found.' % ks)
        else:
            keyspaces = set(keyspace_arg)
    else:
        print('No keyspace arguments.')

    print('Checking table arguments . . .')
    if table_arg:
        if not keyspace_arg or len(keyspace_arg) != 1:
            raise Exception('Only one keyspace can be specified with table arg')
        ks = next(iter(keyspaces)) # retrieve only element in set
        for tb in table_arg:
            if tb not in structure[ks]:
                raise Exception('Table %s not found in keyspace %s' % (tb, ks))
        else:
            tables = set(table_arg)
    else:
        print('No table arguments.')
    print('Valid arguments.\n')

    print('Saving snapshot into %s . . .' % title)
    print('Producing snapshots . . .')
    if keyspace_arg:
        if table_arg:
            ks = next(iter(keyspaces))
            for table in tables:
                run_snapshot(title, host, ks, table)
        else:
            run_snapshot(title, host, ' '.join(keyspaces))
    else:
        run_snapshot(title, host)

    cassandra_data_dir = get_data_dir()
    for ks in keyspaces:
        if not table_arg:
            tables = structure[ks]
        for tb in tables:
            save_table_path = '%(save_path)s/%(keyspace)s/%(table)s/' \
                          % dict(save_path = save_path,
                                 keyspace  = ks,
                                 table     = tb)
            load_dir = '%(data_dir)s/%(keyspace)s/%(table_dir)s/snapshots/%(ss_title)s' \
                   % dict(data_dir  = cassandra_data_dir,
                          keyspace  = ks,
                          table_dir = structure[ks][tb],
                          ss_title  = title)
            print('Storing %s in %s' % (tb, save_table_path))
            shutil.copytree(load_dir, save_table_path)

    print('Saving schema . . .')
    print_save_path = write_schema(save_path)
    print('Saved schema as %s' % print_save_path)
    for ks in keyspaces:
        print_save_path = write_schema(save_path, ks)
        print('Saved keyspace schema as %s' % print_save_path)

    print('\nProcess complete. Snapshot stored in %s\n\n' % save_path)


if __name__ == '__main__':
    cmds = parse_cmd()

    if cmds.path.endswith('\\') or cmds.path.endswith('/'):
        save_path = cmds.path
    else:
        save_path = cmds.path + '/'

    start = time.time()    
    snapshot(save_path, cmds.title, cmds.keyspace, cmds.table)
    end = time.time()

    print('Elapsed time: %s' % (end - start))
