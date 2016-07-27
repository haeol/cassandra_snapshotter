import yaml
import os
import subprocess

# TODO host argument
def cassandra_query(host, query, output=True):

    if type(query) is str:
        query = ['echo', query]
    elif type(query) is not list: # TODO needed?
        raise Exception('Query not recognized')

    query_process = subprocess.Popen(query, stdout=subprocess.PIPE)
    cqlsh = subprocess.Popen(('/bin/cqlsh', host),
                             stdin=query_process.stdout,
                             stdout=subprocess.PIPE)
    query_process.stdout.close()

    if output:

        output = cqlsh.communicate()[0]
        query_process.wait()
        return output


def get_data_dir():

    install_locations = ['/etc/cassandra/conf/', # package install on centos
                         '/etc/cassandra/',      # ubuntu package install
                         '/etc/dse/cassandra/'   # datastax enterprise package
                         ] #TODO tarball install needs install location

    for loc in install_locations:
        if os.path.exists(loc + 'cassandra.yaml'):
            yaml_dir = loc + 'cassandra.yaml'
            break
    else:
        # TODO user manual yaml location input through txt(?) file
        raise Exception('Could not find cassandra YAML file.')

    with open(yaml_dir, 'r') as f:
        cass_yaml = yaml.load(f)
    return cass_yaml['data_file_directories'][0]


def get_keyspaces(host, system=False): # include system keyspaces?
    _SYSTEM_KEYSPACES = set(['system_schema',
                             'system_auth',
                             'system',
                             'system_distributed',
                             'system_traces'])

    keyspaces_string = cassandra_query(host, 'DESCRIBE keyspaces;')
    keyspaces = set(keyspaces_string.strip().split())
    if not system:
        keyspaces = keyspaces - _SYSTEM_KEYSPACES
    return keyspaces

def get_table_directories(host, keyspace):

    cmd = ("SELECT table_name, id FROM system_schema.tables \
            WHERE keyspace_name='%s';" % keyspace)
    query = cassandra_query(host, cmd).split('\n')

    # format of query is as follows, may need to be updated
    '''

    table_name  | id
    ------------+---------------------------
    first_table | first_uuid
    . . .       | . . .
    last_table  | last_uuid

    (num rows)
    '''

    table_directory = {}

    query = query[3:-3] #TODO check if before and after match regex?

    for row in query:

        table, uuid = row.strip().split(' | ')
        uuid = uuid.replace('-', '')
        table_directory[table] = table + '-' + uuid

    return table_directory


def get_dir_structure(host, keyspaces):

    '''
    structure: {
        keyspace1 : {
            'table1' : 'directory1',
            'table2' : 'directory2',
            . . .
        },
        keyspace2 : {
            . . .
        }
    }
    '''

    structure = {}
    for keyspace in keyspaces:
        structure[keyspace] = get_table_directories(host, keyspace)

    return structure
