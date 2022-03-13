#!/bin/env python3
import atexit
import datetime
import getopt
import logging
import os
import platform
import re
import sqlite3
import subprocess
import sys
import time

import yaml
from concurrent.futures.thread import ThreadPoolExecutor

config = {}
defaultCfg = """
---
# default configuration
log: pybackup.log
db: /tmp/pybackup.db
min_age: 300
max_target_size: 500m
key: topsecret
exclude_flag: ".bkexclude"
email:
    server: localhost
    subject: Result from pybackup
    #  user: username
    #  password: top-secret
    from: backup@mysystem.com
    to: 
        - boss@mysystem.com
backup: []
exclude: []
resultT: |
  The counts are:

     backed up files:{{ "%7d" | format(backed_up) }}
         incremental:{{ "%7d" | format(incremental) }}
    skipped 2 recent:{{ "%7d" | format(too_recent) }}
     skipped as same:{{ "%7d" | format(same_old) }}
        skipped flag:{{ "%7d" | format(flagged_exc) }}
       skipped perm.:{{ "%7d" | format(permission) }}
              cyclic:{{ "%7d" | format(cyclic) }}
     removed from db:{{ "%7d" | format(removed) }}

  Errors:
  {% for error in errors %}
   {{ error }}
  {% endfor %}
  Messages:
  {% for msg in msgs %}
   {{ msg }}
  {% endfor %}
resultH: |
  <html>
  <head>
  <style>
    table {
        border-collapse: collapse; 
        padding: 3px;
        border: solid thin red;
    }
    table td {
        border: solid thin red;
        padding: 3px;
    }
    table .tl {
        color: green;
        text-align: right;
    }
    table .tr {
        color: blue;
        text-align: center;
    }
    ul.tar-errors li {
        color: red;
    }
    ul.msg li {
        color: green;
    }
  </style>
  </head>
  <body><h1>Pybackup results</h1>
  <h2>Counts:</h2>
  <table>
    <tr><td class="tl">backed up files:</td><td class="tr">{{ backed_up }}</td></tr>
    <tr><td class="tl">incremental:</td><td class="tr">{{ incremental }}</td></tr>
    <tr><td class="tl">skipped 2 recent:</td><td class="tr">{{ too_recent }}</td></tr>
    <tr><td class="tl">skipped as same:</td><td class="tr">{{ same_old }}</td></tr>
    <tr><td class="tl">skipped flag:</td><td class="tr">{{ flagged_exc }}</td></tr>
    <tr><td class="tl">skipped perm.:</td><td class="tr">{{ permission }}</td></tr>
    <tr><td class="tl">cyclic:</td><td class="tr">{{ cyclic }}</td></tr>
    <tr><td class="tl">removed from db:</td><td class="tr">{{ removed }}</td></tr>
  </table>
  <h2>Errors from tar:</h2>
  <ul class="tar-errors">
  {% for error in errors %}
   <li>{{ error | escape }}</li>
  {% endfor %}
  </ul>
  <h2>Messages:</h2>
  <ul class="msg">
  {% for msg in msgs %}
   <li>{{ msg | escape }} </li>
  {% endfor %}
  </ul>
  </body></html>
done: {}
"""
"""configuration as a nested dictionary"""
db_conn: sqlite3.Connection = None
"""Database connection """
vol_num = 0
"""current volume number"""
tar_proc: subprocess.Popen = None
"""tar subprocess"""
enc_proc: subprocess.Popen = None
"""gpg encryption process"""
xz_proc: subprocess.Popen = None
"""xz subprocess"""
error_list: list[str] = []
msg_list: list[str] = []


def prep_database():
    """
    prepares the database
    """
    global db_conn, vol_num
    version: int = 0
    try:
        row = db_conn.execute('select max(version) from dbv').fetchone()
        if row is not None:
            version = row[0]
    except:
        logging.info('db has no version')
    if version == 0:
        logging.info("creating db from scratch")
        schema_stmts = [
            'CREATE TABLE files (name TEXT NOT NULL, mtime REAL NOT NULL,volume INTEGER)',
            'CREATE UNIQUE INDEX "prime" on files (name ASC)',
            'CREATE INDEX vols on files (volume ASC)',
            'CREATE TABLE backup (num INTEGER NOT NULL, tarfile TEXT NOT NULL)',
            'CREATE INDEX bknum on backup (num ASC)',
            'CREATE TABLE dbv(version INTEGER NOT NULL)',
            'insert into dbv values(1)'
        ]
        for stmt in schema_stmts:
            db_conn.execute(stmt)
        db_conn.commit()
    row = db_conn.execute('select max(volume) from files').fetchone()
    if row is not None and row[0] is not None:
        vol_num = row[0] + 1


def handle_finished():
    pass


def handle_tar_errors():
    global error_list
    while True:
        line = tar_proc.stderr.readline()
        if not line:
            break
        line = line.strip()
        print(f"tar stderr {line}")


def handle_enc_errors():
    pass


def handle_xz_errors():
    pass


def do_backup():
    global tar_proc, config
    excluding = []
    blacklist = {}
    try:
        for pattern in config['exclude']:
            comp_pattern = re.compile(pattern)
            excluding.append(comp_pattern)
        max_age = time.time() - config['min_age']
        for entry in config['backup']:
            stat_buf = os.lstat(entry)
            start_device = stat_buf.st_dev
            for path, dirs, files in os.walk(entry):
                for item in files:
                    if item == config['exclude_flag']:
                        blacklist[path] = True
                        
    except Exception as e:
        logging.error("exception", e)
        exit(2)
    finally:
        logging.debug("closing tar input")
        tar_proc.stdin.close()


def main():
    """
    Use: pybackup { options }
      options:
        -c <config> -- merge with this config
        -d -- dump resulting config
        -h -- display help
        -k -- set encryption key
        -l <logfile> -- write to this logfile
        -t <target> -- write archive to this file
    """
    global config, defaultCfg, db_conn, tar_proc, enc_proc, xz_proc
    config = yaml.safe_load(defaultCfg)
    opts, arg = getopt.getopt(sys.argv[1:], 'c:t:l:dh')
    for opt, opt_arg in opts:
        if opt == '-c':
            with open(opt_arg) as cf:
                config.update(yaml.safe_load(cf))
        elif opt == '-d':
            yaml.safe_dump(config, sys.stderr)
        elif opt == '-h':
            print(main.__doc__)
            sys.exit(2)
        elif opt == '-k':
            config['key'] = opt_arg
        elif opt == '-l':
            config['log'] = opt_arg
        elif opt == '-t':
            config['target'] = opt_arg
    logging.basicConfig(filename=config['log'], level=logging.DEBUG, filemode='w',
                        format='%(asctime)s [%(levelname)s] %(pathname)s:%(lineno)d %(funcName)s:\t%(message)s')
    logging.debug("pybackup started")
    tar_args = ['tar', '-cv', '--no-recursion', '-T', '-']
    enc_args = ['gpg', '-c', '--symmetric', '--batch', '--cipher-algo', 'TWOFISH', '--passphrase', config['key']]
    xz_args = ['xz']
    target: str = config['target']
    target = target.replace('%h', platform.node())
    dt = datetime.datetime.now()
    target = target.replace('%t', dt.strftime('%y-%m-%d_%H-%M-%S'))
    with sqlite3.connect(config['db'], check_same_thread=False) as db_conn:
        prep_database()
        db_conn.execute('insert into backup(num,tarfile) values(?,?)', (vol_num, target))
        db_conn.commit()
        with open(target, 'wb') as target:
            tar_proc = subprocess.Popen(tar_args, cwd='/', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        encoding='UTF-8')
            enc_proc = subprocess.Popen(enc_args, stdin=tar_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            xz_proc = subprocess.Popen(xz_args, stdin=enc_proc.stdout, stdout=target, stderr=subprocess.PIPE)
            with ThreadPoolExecutor(max_workers=7) as tpe:
                tpe.submit(handle_finished)
                tpe.submit(handle_tar_errors)
                tpe.submit(handle_enc_errors)
                tpe.submit(handle_xz_errors)
                do_backup()


if __name__ == '__main__':
    print('pybackup started')
    atexit.register(print, 'pybackup exited')
    main()
