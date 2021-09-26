
# backup
Pybackup is cyclic backup controlled by a python script using tar as a subprocess.

## How it works

Steps:
* pybackup reads a configuration file which specifies a database location (*db*), 
which item to back up and what to skip.
* in case the database (sqlite3) does not exist, it is created.
* starts the incremental backup in the following step
* for each specified location (*backup*):
    * it looks for files that are newer than registered in the database
    * checks if they are too recent - *max_age* parameter controls that
    * checks if they match some exclusion patterns (*exclude*) - 
        note that those are regular expressions, which must match parts of the filename
    * checks if there is a flag file, telling us to exclude this directory
    * checks if we can access this file for reading
    * tells tar to backup this file
* starts the cyclic backup in the following step
    * determines the number of backed up files
    * determines the share of files to back up this time (configurable by *split*)
    * looks at the filesystem if it exists
        * in case it does not exist, it will be removed from the database
        * if it exists, it will be backed up
