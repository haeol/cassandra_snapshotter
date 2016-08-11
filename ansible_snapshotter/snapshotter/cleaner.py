import os
import sys
import shutil
import argparse

from cass_functions import (_SYSTEM_KEYSPACES, cassandra_query, check_host,
                            get_data_dir, get_keyspaces, get_rpc_address,
                            get_table_directories, get_dir_structure)

def data_cleaner(host, backups=False):
    # This fuction finds inactive data directories and removes them
    # This includes unused keyspace directories and table directories

    if check_host(host) != 0:
        raise Exception('Invalid host parameter')
    keyspaces = get_keyspaces(host)
    structure = get_dir_structure(host, keyspaces)
    cass_data_dir = get_data_dir()

    print('Deleting old keyspaces . . .')
    for ks in os.listdir(cass_data_dir):
        if ks not in keyspaces:
            print('\tDeleting: ' + cass_data_dir + '/' + ks)
            shutil.rmtree(cass_data_dir + '/' + ks)

    print('\nDeleting old tables . . .')
    for keyspace in keyspaces:
        if keyspace not in _SYSTEM_KEYSPACES:
            print('\nProcessing keyspace: %s' % keyspace)
            # should only be directories in this folder
            data_dirs = set(os.listdir(cass_data_dir + '/' + keyspace))
            table_dirs = set()

            for table in structure[keyspace].keys():
                table_dirs.add(structure[keyspace][table])

            inactive_dirs = data_dirs - table_dirs

            print('Removing inactive directories . . .')
            for d in inactive_dirs:
                print('\tDeleting: ' + cass_data_dir + '/' + keyspace + '/' + d)
                shutil.rmtree(cass_data_dir + '/' + keyspace + '/' + d)

            if backups:
                print('Removing old backup db files')
                for d in table_dirs:
                    clean_directory(cass_data_dir + '/' + keyspace + '/' + d + '/backups')


def clean_directory(table_directory):
    # TODO does incremental backups work with this?
    for f in os.listdir(table_directory):
        if os.isfile(f):
            os.remove(table_directory + '/' + f)


if __name__ == '__main__':

    data_cleaner(get_rpc_address(), backups=True)


