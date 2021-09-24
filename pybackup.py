#!/usr/bin/python
import atexit
import concurrent.futures.thread
import logging
import os
import pprint
import re
import sqlite3
import subprocess
import sys
import time

import yaml

cfg = {'log': 'pybackup.log', 'db': 'pybackup.db', 'split': 5,
       'backup': [], 'exclude': []}
db_conn: sqlite3.Connection = None
tar_proc: subprocess.Popen = None
excludes: list[re.Pattern] = []


def prep_database():
    """
    prepares the database
    """
    global db_conn
    version: int = 0
    try:
        for r in db_conn.execute('select version from dbv'):
            if version < r[0]:
                version = r[0]
    except:
        logging.info('db has no version')
    if version == 0:
        logging.info("creating db from scratch")
        schemaStmts = [
            'CREATE TABLE dbv(version INTEGER NOT NULL)',
            'insert into dbv values(1)'
        ]
        for stmt in schemaStmts:
            db_conn.execute(stmt)
        db_conn.commit()

def incremental(fullname: str):
    global excludes
    for pt in excludes:
        m=pt.search(fullname)
        if m is not None:
            return
    print(fullname,file=tar_proc.stdin)


def do_backup():
    logging.debug('starting incremental backup')
    for bl in cfg['backup']:
        for path, dirs, files in os.walk(bl):
            for item in files:
                fullname = os.path.join(path, item)
                incremental(fullname)
            for item in dirs:
                fullname = os.path.join(path, item)
                incremental(fullname)
    logging.debug('starting cyclic backup')
    logging.debug("ending backup")
    tar_proc.stdin.close()


def handle_finished():
    while True:
        line = tar_proc.stdout.readline()
        if not line:
            break
        line=line.strip()
        print('stdout ' + line)
    logging.debug('reading tar output stopped')


def handle_errors():
    while True:
        line = tar_proc.stderr.readline()
        if not line:
            break
        line=line.strip()
        print('stderr ' + line)
    logging.debug('reading tar errors stopped')


def main():
    """
    Use: pybackup <cfg-file> <target tar file>
    """
    global db_conn, tar_proc,excludes
    if len(sys.argv) < 3:
        print(main.__doc__)
        sys.exit(2)
    cfgFile = sys.argv[1]
    tarFile = sys.argv[2]
    with open(cfgFile) as cf:
        cfg.update(yaml.safe_load(cf))
    pprint.pprint(cfg)
    logging.basicConfig(filename=cfg['log'], level=logging.DEBUG, filemode='w',
                        format='%(asctime)s [%(levelname)s] %(pathname)s:%(lineno)d %(funcName)s: %(message)s')
    logging.debug("pybackup started")
    for pt in cfg['exclude']:
        cpt=re.compile(pt)
        excludes.append(cpt)
    with concurrent.futures.thread.ThreadPoolExecutor() as exec, \
          sqlite3.connect(cfg['db']) as _dbcon:
        db_conn = _dbcon
        prep_database()
        pcs = ['tar', '-cavf', tarFile, '--no-recursion', '-T', '-']
        tar_proc = subprocess.Popen(pcs, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
                                    encoding='UTF-8')
        exec.submit(do_backup)
        exec.submit(handle_finished)
        exec.submit(handle_errors)


if __name__ == '__main__':
    print('pybackup started')
    atexit.register(print, 'pybackup exited')
    main()
