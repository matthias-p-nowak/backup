#!/usr/bin/python
import atexit
import concurrent.futures.thread
import logging
import pprint
import sqlite3
import sys
import yaml

cfg = {'log':'pybackup.log','db':'pybackup.db'}
dbCon=None
tarFile: str=None

def prepDb():
    global dbCon
    version: int = 0
    try:
        for r in dbCon.execute('select version from dbv'):
            if version < r[0]:
                version = r[0]
    except:
        logging.info('db has no version')
    if version==0:
        logging.info("creating db from scratch")
        schemaStmts=[
            'CREATE TABLE dbv(version INTEGER NOT NULL)',
            'insert into dbv values(1)'
        ]
        for stmt in schemaStmts:
            dbCon.execute(stmt)
        dbCon.commit()

def main():
    """
    Use: pybackup <cfg-file> <target tar file>
    """
    global dbCon,tarFile
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
    with concurrent.futures.thread.ThreadPoolExecutor() as exec, \
          sqlite3.connect(cfg['db']) as _dbCon:
        dbCon=_dbCon
        prepDb()


if __name__ == '__main__':
    print('pybackup started')
    atexit.register(print, 'pybackup exited')
    main()
