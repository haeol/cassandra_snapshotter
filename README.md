## Synopsis
Cassandra does not provide an easy way to snapshot files and store these snapshots in local directories. This script does that for the user, and restores them using the same snapshot files created by the snapshotter.

## Installation
Has only been tested on CentOs 7.2, unknown if compatible with other operating systems

Only real dependecy right now is PyYaml

```bash
sudo python setup.py install
```
or
```bash
pip install PyYaml
```


## Usage
snapshotter: creates a folder with the snapshot files
``` bash
python snapshotter.py -d save-directory
                      -t snapshot-title
                      -ks keyspace
                      -tb table
```
restore: loads the folder with the snapshot files into Cassandra
``` bash
python restore.py -d snapshot-directory
                  -n node_ip_address    # recommended to use the local node ip
                  -ks keyspace          # optional
                  -tb table             # optional
```
cleaner: work in progress; this removes unused directories and data from Cassandra's data directory
``` bash
python cleaner.py -n node_ip_address
```

## How it works
Snapshotter does the following:

1. Find the structure of the current Cassandra schema

2. Find the currently active table uuids in use by Cassandra to find the correct table folder in Cassandra's data directory

3. Clear all old snapshots using nodetool

4. Take the snapshot

5. Copy the files in the snapshot directories inside Cassandra's data directory to the specified save location

6. Querys Cassandra to retrieve the schema and saves them in each keyspace in the snapshot save location


Restore does the following:

1. Destroy the existing database by dropping each keyspace and removing each keyspace data directory

2. Load the schema that is inside the snapshot directory

3. Run sstableloader to load the data in each of the table directories in the snapshot file

