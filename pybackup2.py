#!/usr/bin/python
import atexit
import concurrent.futures.thread
import logging
import math
import os
import pprint
import re
import smtplib
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import jinja2
import yaml

cfg = {}
defaultCfg = """
---
# default configuration
log: pybackup.log
db: /tmp/pybackup.db
split: 5
max_age: 300
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
db_conn: sqlite3.Connection = None
"""Database connection """
tar_proc: subprocess.Popen = None
"""the tar process"""
excludes: list[re.Pattern] = []
"""compiled exclude patterns"""
max_age: float = 0
"""cut of for recent files"""
tar_file: str = 'pybackup.tar.xz'
"""filename of the tar file, is second argument"""
vol_num = 1
"""current volume number"""
db_lock = threading.Lock()
cnt_excluded = 0
cnt2recent = 0
cnt_same_old = 0
cnt_permission = 0
cnt_incremental = 0
cnt_cyclic = 0
cnt_flagged_exc = 0
cnt_backed_up = 0
cnt_removed = 0
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
        schemaStmts = [
            'CREATE TABLE files (name TEXT NOT NULL, mtime REAL NOT NULL,volume INTEGER)',
            'CREATE UNIQUE INDEX "prime" on files (name ASC)',
            'CREATE INDEX vols on files (volume ASC)',
            'CREATE TABLE backup (num INTEGER NOT NULL, tarfile TEXT NOT NULL)',
            'CREATE INDEX bknum on backup (num ASC)',
            'CREATE TABLE dbv(version INTEGER NOT NULL)',
            'insert into dbv values(1)'
        ]
        for stmt in schemaStmts:
            db_conn.execute(stmt)
        db_conn.commit()
    row = db_conn.execute('select max(volume) from files').fetchone()
    if row is not None and row[0] is not None:
        vol_num = row[0] + 1


def do_incremental(fullname: str, dev: int):
    global excludes, tar_file, tar_proc, cnt_excluded, cnt2recent, cnt_same_old, cnt_permission, cnt_incremental
    # logging.debug('incremental: '+fullname)
    for pt in excludes:
        m = pt.search(fullname)
        if m is not None:
            # logging.debug('excluded: ' + fullname)
            cnt_excluded += 1
            return
    if fullname == cfg['db']:
        # logging.debug('got db name: ' + fullname)
        return
    if fullname == tar_file:
        # logging.debug('got tarfile: ' + fullname)
        return
    statbuf = os.lstat(fullname)
    if statbuf.st_dev != dev:
        # logging.debug('on different fs: ' + fullname)
        return
    if stat.S_ISSOCK(statbuf.st_mode):
        # logging.debug('not saving socket: '+fullname)
        return
    mtime = int(statbuf.st_mtime)
    if mtime > max_age:
        # logging.debug('too recent: ' + fullname)
        cnt2recent += 1
        return
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
    # logging.debug(f'backing up: {fullname}')
    cnt_incremental += 1
    print(fullname[1:], file=tar_proc.stdin)


def remove_file(fullname: str):
    global cnt_removed
    with db_lock:
        try:
            db_conn.execute('delete from files where name=?', (fullname,))
            db_conn.commit()
            cnt_removed += 1
        except Exception as ex:
            logging.error(f'exception {ex}')


def do_cyclic(fullname: str, vol: int):
    global cnt_cyclic
    try:
        statbuf = os.lstat(fullname)
        # logging.debug(f'cyclic backup {fullname}({vol})')
        cnt_cyclic += 1
        print(fullname[1:], file=tar_proc.stdin)
    except FileNotFoundError:
        remove_file(fullname)
    except Exception as ex:
        logging.error(f'exception: {ex}')


def do_backup():
    global max_age, cnt_flagged_exc, cfg
    blacklist = {}
    try:
        logging.debug('starting incremental backup')
        max_age = time.time() - cfg['max_age']
        for bl in cfg['backup']:
            statbuf = os.lstat(bl)
            dev = statbuf.st_dev
            for path, dirs, files in os.walk(bl):
                for item in files:
                    if item == cfg['exclude_flag']:
                        blacklist[path] = True
                    fullname = os.path.join(path, item)
                    backup = True
                    for bs in blacklist:
                        if fullname.startswith(bs):
                            backup = False
                    if backup:
                        do_incremental(fullname, dev)
                    else:
                        cnt_flagged_exc += 1
                for item in dirs:
                    if path in blacklist:
                        continue
                    fullname = os.path.join(path, item) + os.path.sep
                    backup = True
                    for bs in blacklist:
                        if fullname.startswith(bs):
                            backup = False
                    if backup:
                        do_incremental(fullname, dev)
                    else:
                        cnt_flagged_exc += 1
        logging.debug('starting cyclic backup')
        row = db_conn.execute('select count(*) from files').fetchone()
        if row is not None:
            cnt = int(row[0])
            cnt = math.ceil(cnt / cfg['split'])
        rs = db_conn.execute('select name, volume  from files order by volume ASC')
        for i in range(cnt):
            row = rs.fetchone()
            if row is None:
                break
            # logging.debug(f'cyclic: {row[0]} from volume {row[1]}')
            backup = True
            for bs in blacklist:
                if row[0].startswith(bs):
                    backup = False
            if backup:
                do_cyclic(row[0], row[1])
            else:
                remove_file(row[0])
                cnt_flagged_exc += 1
    except Exception as e:
        logging.error('exception in do_backup %s', e)
        exit(2)
    finally:
        tar_proc.stdin.close()
    logging.debug("ending backup")


def handle_finished():
    global db_lock, vol_num, cnt_backed_up
    logging.debug('reading tar output')
    try:
        while True:
            line = tar_proc.stdout.readline()
            if not line:
                break
            line = '/' + line.strip()
            statbuf = os.lstat(line)
            mtime = int(statbuf.st_mtime)
            with db_lock:
                db_conn.execute('replace into files(name,mtime,volume) values(?,?,?)', (line, mtime, vol_num))
                db_conn.commit()
                cnt_backed_up += 1
    except Exception as ex:
        print('exception in handle_finish: %s', ex)
    logging.debug('reading tar output stopped')


def handle_errors():
    global error_list
    logging.debug('reading tar errors')
    while True:
        line = tar_proc.stderr.readline()
        if not line:
            break
        line = line.strip()
        print('stderr ' + line)
        error_list.append(line)
    logging.debug('reading tar errors stopped')


def main():
    """
    Use: pybackup <cfg-file> <target tar file>
    """
    global db_conn, tar_proc, excludes, tar_file, cfg, error_list, msg_list
    if len(sys.argv) < 3:
        print(main.__doc__)
        sys.exit(2)
    cfg = yaml.safe_load(defaultCfg)
    cfg_file = sys.argv[1]
    tar_file = sys.argv[2]
    with open(cfg_file) as cf:
        cfg.update(yaml.safe_load(cf))
    pprint.pprint(cfg)
    logging.basicConfig(filename=cfg['log'], level=logging.DEBUG, filemode='w',
                        format='%(asctime)s [%(levelname)s] %(pathname)s:%(lineno)d %(funcName)s:\t%(message)s')
    print(f'-----')
    logging.debug("pybackup started")
    for pt in cfg['exclude']:
        cpt = re.compile(pt)
        excludes.append(cpt)
    with sqlite3.connect(cfg['db'], check_same_thread=False) as _dbcon:
        db_conn = _dbcon
        pcs = ['tar', '-cavf', tar_file, '-C', '/', '--no-recursion', '-T', '-']
        prep_database()
        db_conn.execute('insert into backup(num,tarfile) values(?,?)', (vol_num, tar_file))
        db_conn.commit()
        tar_proc = subprocess.Popen(pcs, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
                                    encoding='UTF-8')
        with concurrent.futures.thread.ThreadPoolExecutor() as exec:
            exec.submit(do_backup)
            exec.submit(handle_finished)
            exec.submit(handle_errors)
        for row in db_conn.execute('select b.num,b.tarfile, count(f.name) from backup as b left join'
                                   + ' files as f on b.num=f.volume group by b.num'):
            if int(row[2]) == 0:
                msg_list.append(f'tarfile {row[1]} from backup {row[0]} can be deleted')
                db_conn.execute('delete from backup where num=?', (row[0],))
                db_conn.commit()
    results = {'backed_up': cnt_backed_up,
               'incremental': cnt_incremental,
               'too_recent': cnt2recent,
               'same_old': cnt_same_old,
               'flagged_exc': cnt_flagged_exc,
               'permission': cnt_permission,
               'cyclic': cnt_cyclic,
               'removed': cnt_removed,
               'errors': error_list,
               'msgs': msg_list
               }
    result_txt = cfg['resultT']
    templ = jinja2.Template(result_txt)
    result_txt = templ.render(results)
    print(result_txt)
    result_html = cfg['resultH']
    with open('test.html','w') as outp:
        print(result_html,file=outp)
    templ = jinja2.Template(result_html)
    result_html = templ.render(results)
    with open('test.html','w') as outp:
        print(result_html,file=outp)
    # emailing
    s=None
    try:
        logging.debug("starting emailing")
        msg = MIMEMultipart('alternative')
        email = cfg['email']
        msg['Subject'] = email['subject']
        msg['From'] = email['from']
        recipients=email['to']
        if isinstance(recipients, list):
            msg['To'] = ",".join(recipients)
        else:
            msg['To'] = recipients
        msg.attach(MIMEText(result_txt, 'plain'))
        msg.attach(MIMEText(result_html, 'html'))
        s = smtplib.SMTP(email['server'])
        s.set_debuglevel(1)
        if 'user' in email:
            s.login(email['user'], email['password'])
        s.sendmail(email['from'], recipients, msg.as_string())
    except Exception as e:
        logging.error("got an exception",e)
    finally:
        if s is not None:
            s.quit()
    logging.debug("ended")


if __name__ == '__main__':
    print('pybackup started')
    atexit.register(print, 'pybackup exited')
    main()
