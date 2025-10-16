dirdb
=====

dirdb is a companion tool that helps to reduce unnecessary file transfers
when a tool like rsync is used to synchronize file hierarchies.

In each source and destination hierarchy, dirdb stores at least one sqlite
database that contains information about each file like size and hash of its
contents. With this information about each source and destination directory,
dirdb generates a shell script that tries to synchronize the destination to
the source directory by simple file operations like "move" or "copy".

A file hierarchy can contain multiple sqlite databases in order to quickly
synchronize only a part of the tree.

To unambiguously identfy a file, the content of files in the source and
destination hierarchy are hashed. As this can be a time expensive operation,
dirdb can be instructed to only hash parts of the file.

Usage
=====

```
usage: dirdb.py [-h] [-v] [--dbfilename DBFILENAME] [--scriptname SCRIPTNAME]
                [--list-dups] [-P] [--partial-hash-size PARTIAL_HASH_SIZE]
                [-g] [-s SOURCE] [-d DESTINATION] [-u UPDATE]

options:
  -h, --help            show this help message and exit
  -v, --verbose
  --dbfilename DBFILENAME
                        the filename of the db (default: .dir.db)
  --scriptname SCRIPTNAME
                        the script filename (default: update.sh)
  --list-dups           list duplicate files
  -P, --partial-hash    only hash a number of bytes from the beginning and end
                        of a file
  --partial-hash-size PARTIAL_HASH_SIZE
                        size of bytes that are used to create a hash of a file
                        (default: 4096)
  -g, --gen-sync-script
                        create shell script that executes commands necessary
                        to sync source and destination directories
  -s, --source SOURCE   one or more source directories
  -d, --destination DESTINATION
                        one or more destination directories
  -u, --update UPDATE   update all databases under this directory (can be
                        passed multiple times)
```

To create a database in the current directory:
```
dirdb.py -u .
```

To create the script `update.sh` that will modify the destination directory
`dest` to look like the source directory:
```
dirdb.py -s . -d dest/
```
