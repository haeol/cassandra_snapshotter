## Synopsis
Cassandra does not provide an easy way to snapshot files and store these snapshots in local directories. This script does that for the user, and restores them using the same snapshot files created by the snapshotter.

## Installation
(TODO)
Has only been tested on CentOs 7.2, is unlikely to be compatible with any other operating systems
Only real dependecy right now is PyYaml

```bash
sudo python setup.py install
```
or
```bash
pip install PyYaml
```


## Usage
snapshotter
``` bash
python snapshotter.py -d save-location
                      -t snapshot-title
                      -ks keyspace
                      -tb table
```
restore
``` bash
python restore.py -d snapshot-location
                  -n node_ip_address
                  -ks keyspace
                  -tb table
```

## How it works
Snapshotter does the following:
1. Clear all old snapshots using nodetool
2. Find the structure of the current Cassandra schema
3. Find the currently active table uuids in use by Cassandra to find the correct table folder in Cassandra's data directory
4. Take the snapshot
5. Copy the files in the snapshot directories inside Cassandra's data directory to the specified save location
6. Querys Cassandra to retrieve the schema and saves them in each keyspace in the snapshot save location

Restore does the following:
1. Destroy the existing database by dropping each keyspace and removing each keyspace data directory
2. Load the schema inside the snapshot directory
3. Find the structure of the schema and find active table uuids to find the correct table folder in Cassandra's data directory
4. Copy corresponding files from the snapshot directory to the table's data directory
5. Run sstableloader to load the data
6. Clean the files inside the data directory since they wil not be in use anymore

## Contributors
Kevin Tom

