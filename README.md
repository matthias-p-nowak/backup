# Disclaimer

The first pipe between the python program and tar failed. 
When one closes the pipe even after flushing, the tar program exits immediately. 

Replaced by "cyclic backup".

# Backup
There are tape and disk based backup systems. 
The former combines a bunch of files into an archive format, which traditionally has been written on a tape medium.
The latter retrieves files from each client machine, discovers duplicates and stores the content on a server. 
Moreover, it needs to ensure protection against corruption and dangers.

The simplest form is tape based using *tar/rmt*. 
Usually, a full backup is taken once in a while. 
Then, files newer than the full backup (aka differential) or 
    newer than the last backup (aka incremental) are saved into smaller archives.

Pybackup is cyclic backup controlled by a *python* script using *tar* as a subprocess. 
The first part is a incremental backup, the second phase is a partial full backup. 
This means, it archives files which were least recently backed up.
In order to achieve this, *Pybackup* uses a Sqlite database.

## How it works

Steps:
* pybackup reads a configuration file which specifies a database location (*db*), 
which item to back up and what to skip
* prints the used configuration
* in case the database (sqlite3) does not exist, it is created
* starts the incremental backup in the following step
* for each specified location (*backup*):
    * it looks for files that are newer than registered in the database
    * checks if they are too recent - *max_age* parameter controls that
    * checks if they match some exclusion patterns (*exclude*) - 
        **note** that those are regular expressions, which must match parts of the filename
    * checks if there is a flag file, telling us to exclude this directory
    * checks if we can access this file for reading
    * tells tar to backup this file
* starts the cyclic backup in the following step
    * determines the number of backed up files
    * determines the share of files to back up this time (configurable by *split*)
    * looks at the filesystem if it exists
        * in case it does not exist, it will be removed from the database
        * if it exists, it will be backed up
* it finishes
    * showing the statistics on *stdout*
    * sending email, if configured under *email*
        * includes login details for the *smtp* server, if *user* is configured
    * finishes
    
## Configuration
~~~
log: pybackup.log
db: /tmp/pybackup.db
split: 5
max_age: 300
exclude_flag: ".bkexclude"
email:
  server: smtp.online.no:578
  user: username
  password: top-secret
  from: backup@mysystem.com
  to: 
    - me
    - root
backup:
  - /tmp
exclude:
  - "fstab"
  - "\\.git"
  - "bak$"
~~~

Commands:
* tar -cv -C / --no-recursion -T -
* xz
* gpg --symmetric --batch --cipher-algo AES256 --passphrase hello <t.out >t.enc
