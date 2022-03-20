#!/bin/env python3.9
import atexit
import datetime
import getopt
import logging
import os
import platform
import re
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import BinaryIO

import yaml

KILO = 1000
HEADER_SZ = 512
config = {}
cnt_excluded = 0
cnt_same_old = 0
cnt2recent = 0
cnt_permission = 0
cnt_incremental = 0
cnt_backed_up = 0
cnt_removed = 0
db_conn: sqlite3.Connection
"""Database connection """
db_lock = threading.Lock()
defaultCfg = """
---
# default configuration
log: pybackup.log
db: /tmp/pybackup.db
min_age: 300
max_target_size: 500m
target: /tmp/backup-%h-%t.tar.enc.xz
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
error_list: list[str] = []
vol_num = 0
"""current volume number"""
tar_proc: subprocess.Popen
"""tar subprocess"""
enc_proc: subprocess.Popen
"""gpg encryption process"""
xz_proc: subprocess.Popen
"""xz subprocess"""
msg_list: list[str] = []
target_file: BinaryIO
blacklist = {}
excluding = []
start_device = 0
max_age = 0
tarring = set()
set_lock = threading.Lock()


class SizeCheck:
    def __init__(self, size: str, fd: int):
        self.fd = fd
        self.reserved = 0
        size_pat = re.compile('(\\d+)([kmgGM])')
        m = size_pat.search(size)
        if m is not None:
            s = int(m.group(1))
            u = m.group(2)
            if u == 'k':
                s *= KILO
            elif u == 'm' or u == 'M':
                s *= KILO * KILO
            elif u == 'g' or u == 'G':
                s *= KILO * KILO * KILO
            self.target = s
        else:
            self.target = 500 * KILO * KILO

    def reserve(self, size: int):
        nsz = size + HEADER_SZ + self.reserved
        if nsz >= self.target:
            return False
        self.reserved += size + HEADER_SZ
        return True

    def is_filled(self):
        if self.reserved >= self.target:
            return True
        return False


target_sc: SizeCheck


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
    except sqlite3.DatabaseError:
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


def handle_tar_stderr():
    global error_list, tarring, tar_proc, db_lock, cnt_backed_up, set_lock
    while True:
        line = tar_proc.stderr.readline()
        if not line:
            return
        line = line.strip()
        f2 = line
        while f2.endswith('/'):
            f2 = f2[:-1]
        with set_lock:
            found = f2 in tarring
        if found:
            with set_lock:
                tarring.remove(f2)
            line = os.path.sep + line
            statbuf = os.lstat(line)
            mtime = int(statbuf.st_mtime)
            with db_lock:
                db_conn.execute('replace into files(name,mtime,volume) values(?,?,?)', (line, mtime, vol_num))
                db_conn.commit()
                cnt_backed_up += 1
        else:
            print(f"tar stderr {line}")
            error_list.append(line)


def handle_enc_errors():
    global error_list, enc_proc
    while True:
        line = enc_proc.stderr.readline()
        if not line:
            return
        line = line.strip()
        if len(line) == 0:
            return
        print(f"enc stderr {line}")
        error_list.append(line)


def handle_xz_errors():
    global error_list, xz_proc
    while True:
        line = xz_proc.stderr.readline()
        if not line:
            return
        line = line.strip()
        if len(line) == 0:
            return

        print(f"enc stderr {line}")
        error_list.append(line)


def remove_file(fn: str):
    global db_conn, db_lock, cnt_removed
    with db_lock:
        try:
            db_conn.execute('delete from files where name=?', (fn,))
            db_conn.commit()
            cnt_removed += 1
        except Exception as ex:
            logging.error(f'exception {ex}')


def do_incremental(fullname):
    global blacklist, cnt_excluded, excluding, config, start_device, \
        cnt2recent, cnt_same_old, cnt_permission, cnt_incremental, tarring, target_sc, set_lock
    for bl_item in blacklist:
        if fullname.startswith(bl_item):
            cnt_excluded += 1
            return
    for pattern in excluding:
        m = pattern.search(fullname)
        if m is not None:
            cnt_excluded += 1
            return
    if fullname == config['db']:
        return
    if fullname == config['target']:
        return
    stat_buf = os.lstat(fullname)
    if stat_buf.st_dev != start_device:
        return
    if stat.S_ISSOCK(stat_buf.st_mode):
        return
    mtime = int(stat_buf.st_mtime)
    if mtime > max_age:
        cnt2recent += 1
        return
    if stat.S_ISDIR(stat_buf.st_mode):
        fullname += '/'
    # checking age against database
    row = db_conn.execute('select mtime from files where name=?', (fullname,)).fetchone()
    if row is not None:
        if row[0] == mtime:
            # logging.debug('same old file: ' + fullname)
            cnt_same_old += 1
            return
    if not os.access(fullname, os.R_OK):
        logging.warning('missing permissions: ' + fullname)
        cnt_permission += 1
        return
    if target_sc.reserve(stat_buf.st_size):
        logging.debug(f"backing up: {fullname}")
        cnt_incremental += 1
        # no starting '/'
        fullname = fullname[1:]
        f2 = fullname
        # no ending '/'
        while f2.endswith('/'):
            f2 = f2[:-1]
        with set_lock:
            tarring.add(f2)
        print(fullname, file=tar_proc.stdin)
        tar_proc.stdin.flush()
    else:
        logging.debug(f"size too big for {fullname}")


def do_cyclic(fullname: str):
    global blacklist, excluding, tarring, tar_proc, target_sc, set_lock
    try:
        for bl_item in blacklist:
            if fullname.startswith(bl_item):
                remove_file(fullname)
                return
        for pattern in excluding:
            m = pattern.search(fullname)
            if m is not None:
                remove_file(fullname)
                return
        stat_buf = os.lstat(fullname)
        if stat.S_ISSOCK(stat_buf.st_mode):
            return
        mtime = int(stat_buf.st_mtime)
        if mtime > max_age:
            remove_file(fullname)
            return
        if target_sc.reserve(stat_buf.st_size):
            logging.debug(f"backing up {fullname} {len(tarring)}")
            fullname = fullname[1:]
            f2 = fullname
            while f2.endswith(os.path.sep):
                f2 = f2[:-1]
            with set_lock:
                tarring.add(f2)
            print(fullname, file=tar_proc.stdin)
            tar_proc.stdin.flush()
    except FileNotFoundError:
        remove_file(fullname)


def do_backup():
    global tar_proc, config, blacklist, excluding, start_device, max_age, target_sc, tarring, vol_num
    try:
        for pattern in config['exclude']:
            comp_pattern = re.compile(pattern)
            excluding.append(comp_pattern)
        max_age = time.time() - config['min_age']
        # start incremental backup
        logging.debug('backing up new/changed files')
        for entry in config['backup']:
            stat_buf = os.lstat(entry)
            start_device = stat_buf.st_dev
            for path, dirs, files in os.walk(entry):
                for item in files:
                    if item == config['exclude_flag']:
                        blacklist[path] = True
                        continue
                    fullname = os.path.join(path, item)
                    do_incremental(fullname)
                    if target_sc.is_filled():
                        return
                for item in dirs:
                    fullname = os.path.join(path, item)
                    do_incremental(fullname)
                    if target_sc.is_filled():
                        return
        # end incremental backup
        # start cyclic backup
        logging.debug('starting cycling backup')
        rs = db_conn.execute('select name, volume  from files where volume < ? order by volume ASC', (vol_num,))
        while True:
            row = rs.fetchone()
            if row is None:
                return
            do_cyclic(row[0])
            if target_sc.is_filled():
                return
        # end cyclic backup
    except Exception as e:
        logging.error("exception", e)
        exit(2)
    finally:
        logging.debug(f"closing tar input - {len(tarring)} unfinished")
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
        -s <size> -- size of the archive file at max (<number>{k,m,M,g,G})
        -t <target> -- write archive to this file
    """
    global config, defaultCfg, db_conn, tar_proc, enc_proc, xz_proc, target_file, target_sc
    config = yaml.safe_load(defaultCfg)
    opts, arg = getopt.getopt(sys.argv[1:], 'c:t:l:dhs:')
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
        elif opt == '-s':
            config['max_target_size'] = opt_arg
        elif opt == '-t':
            config['target'] = opt_arg
    logging.basicConfig(filename=config['log'], level=logging.DEBUG, filemode='w',
                        format='%(asctime)s [%(levelname)s] %(pathname)s:%(lineno)d %(funcName)s:\t%(message)s')
    logging.debug("pybackup started")
    tar_args = ['tar', '-cv', '--no-recursion', '-T', '-']
    enc_args = ['gpg', '-c', '--symmetric', '--batch', '--cipher-algo', 'TWOFISH', '--passphrase', config['key']]
    xz_args = ['xz', '-9']
    target_fn = config['target']
    target_fn = target_fn.replace('%h', platform.node())
    dt = datetime.datetime.now()
    target_fn = target_fn.replace('%t', dt.strftime('%y-%m-%d_%H-%M-%S'))
    with sqlite3.connect(config['db'], check_same_thread=False) as db_conn:
        prep_database()
        db_conn.execute('insert into backup(num,tarfile) values(?,?)', (vol_num, target_fn))
        db_conn.commit()
        with open(target_fn, 'wb') as target_file:
            target_sc = SizeCheck(config['max_target_size'], target_file.fileno())
            tar_proc = subprocess.Popen(tar_args, cwd='/', stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        encoding='UTF-8')
            enc_proc = subprocess.Popen(enc_args, stdin=tar_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            xz_proc = subprocess.Popen(xz_args, stdin=enc_proc.stdout, stdout=target_file, stderr=subprocess.PIPE)
            with ThreadPoolExecutor(max_workers=7) as tpe:
                tpe.submit(handle_tar_stderr)
                tpe.submit(handle_enc_errors)
                tpe.submit(handle_xz_errors)
                do_backup()
        logging.debug('tar file closed')


if __name__ == '__main__':
    print('pybackup started')
    atexit.register(print, 'pybackup exited')
    main()
