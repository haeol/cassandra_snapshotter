import os                                                                        
import sys                                                                       
import subprocess                                                                
import shutil                                                                    
import zipfile                                                                   
                                                                                 
from cass_functions import (get_data_dir, get_keyspaces, get_dir_structure,      
                            get_rpc_address, cassandra_query)  


def write_ring_info(save_path):
    
    with open(save_path + '/ring_info.txt', 'w') as f:
        nodetool = subprocess.Popen(['nodetool', 'ring'], stdout=f)
        nodetool.wait()


def write_schema(host, save_path, keyspace=None):

    if keyspace:
        save_path = save_path + '/' + keyspace
        filename = keyspace + '_schema.cql'
        query = ("DESCRIBE KEYSPACE %s;" % keyspace)
    else:
        filename = 'schema.cql'
        query = ("DESCRIBE SCHEMA;")

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    with open(save_path + '/' + filename, 'w') as f:
        query_process = subprocess.Popen(['echo', query], stdout=subprocess.PIPE)
        cqlsh = subprocess.Popen(('/bin/cqlsh', host),
                                  stdin=query_process.stdout, stdout=f)
        cqlsh.wait()
        query_process.stdout.close()

    return (save_path + '/' + filename)


def save_schema():

    host = get_rpc_address()
    save_path = sys.path[0] + '/.snapshots/schemas'
    keyspaces = get_keyspaces(host)

    print('Saving schema . . .')
    print_save_path = write_schema(host, save_path)
    print('Saved schema as %s' % print_save_path)
    for ks in keyspaces:
        print_save_path = write_schema(host, save_path, ks)
        print('Saved keyspace schema as %s' % print_save_path)

    print('Saving ring information . . .')
    write_ring_info(sys.path[0] + '/.snapshots')


if __name__ == '__main__':
    save_schema()
