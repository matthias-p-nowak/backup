#!/bin/env python3
import atexit
import getopt
import logging
import pprint
import sys

import yaml

config = {}
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


def main():
    """
    Use: pybackup { options }
      options:
        -c <config> -- merge with this config
        -d -- dump resulting config
        -h -- display help
        -l <logfile> -- write to this logfile
        -t <target> -- write archive to this file
    """
    global config, defaultCfg
    config = yaml.safe_load(defaultCfg)
    opts, arg = getopt.getopt(sys.argv[1:], 'c:t:l:dh')
    pprint.pprint(opts)
    for opt, opt_arg in opts:
        if opt == '-h':
            print(main.__doc__)
            sys.exit(2)
        elif opt == '-c':
            with open(opt_arg) as cf:
                config.update(yaml.safe_load(cf))
        elif opt == '-t':
            config['target']=opt_arg
        elif opt == '-l':
            config['log']=opt_arg
        elif opt == '-d':
            yaml.safe_dump(config, sys.stderr)
    logging.basicConfig(filename=config['log'], level=logging.DEBUG, filemode='w',
                        format='%(asctime)s [%(levelname)s] %(pathname)s:%(lineno)d %(funcName)s:\t%(message)s')
    logging.debug("pybackup started")

if __name__ == '__main__':
    print('pybackup started')
    atexit.register(print, 'pybackup exited')
    main()
