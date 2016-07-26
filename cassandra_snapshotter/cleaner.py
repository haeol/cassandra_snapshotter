import os
import sys
import shutil
from cass_functions import (cassandra_query, get_data_dir, get_keyspaces,
                            get_table_directories, get_dir_structure)

def data_cleaner():
    # This fuction finds inactive data directories and removes them
    # This includes unused keyspace directories and table directories

    structure = get_dir_structure(get_keyspaces(system=True))
    cass_data_dir = get_data_dir()

    print('Deleting old keyspaces . . .')
    for ks in os.listdir(cass_data_dir):
        if ks not in structure.keys():
            confirm = str(raw_input('Delete ks %s? [y/n] ' % ks))

            if confirm == 'y':
                shutil.rmtree(cass_data_dir+ '/' + ks)
            else:
                print('Exiting script')
                sys.exit(1)

    print('\nDeleting old tables')
    for keyspace in structure.keys():

        # should only be directories in this folder
        data_dirs = set(os.listdir(cass_data_dir + '/' + keyspace))
        table_dirs = set()

        for table in structure[keyspace].keys():
            table_dirs.add(structure[keyspace][table])

        inactive_dirs = data_dirs - table_dirs

        print('Removing inactive directories . . .')
        for d in inactive_dirs:
            print('\t' + cass_data_dir + '/' + keyspace + '/' + d)
            shutil.rmtree(cass_data_dir + '/' + keyspace + '/' + d)

        print('Removing excess db files in data directory')
        for d in table_dirs:
            clean_directory(cass_data_dir + '/' + keyspace + '/' + d)
            #clean_directory(cass_data_dir + '/' + keyspace + '/' + d + '/backups')


def clean_directory(table_directory):                                            
    # TODO does incremental backups work with this?                              
    for f in os.listdir(table_directory):                                        
        if f.endswith('.db') or f.endswith('.crc32') or f.endswith('.txt'):                              
            os.remove(table_directory + '/' + f)                                 
                                                

if __name__ == '__main__':
    # TODO add option for -y flag?
    data_cleaner()

