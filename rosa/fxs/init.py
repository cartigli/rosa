#!/usr/bin/env python3
"""Interacting with and initiating the database.

Can create, truncate, or drop tables. 
It can also create, drop, or replace triggers.
Queries server for tables/triggers, asks user what to do.
Can create and initiate the index? / 
No, should wipe index if user wipes tables, 
delete it if user drops tables, 
and create it if they make tables.
"""

import os
import shutil
import logging
from pathlib import Path

from rosa.confs import LOCAL_DIR, BLACKLIST, TABLE_CHECK, _DROP
from rosa.lib import (
    phones, mini_ps, finale, _config,
    init_remote, init_index, _r, confirm,
    init_dindex
)

NOMIC = "[init]"

def scraper(dir_):
    logger = logging.getLogger('rosa.log')
    pfx = len(dir_) + 1
    dirx = Path(dir_)
    frps = []
    drps = []

    if dirx.exists():
        logger.debug('scoping local directory...')
        for item in dirx.rglob('*'):
            path_str = item.as_posix()
            if any(blocked in path_str for blocked in BLACKLIST):
                continue # skip item if blkd item in its path
            elif item.is_file():
                p = item.as_posix()
                rp = p[pfx:]

            elif item.is_dir():
                dp = item.as_posix()
                drp = dp[pfx:]
                drps.append(drp)

    else:
        logger.warning('local directory does not exist')
        sys.exit(1)
    
    return drps, frps

def main(args=None):
    """Initiating the local & remote databases. 
    """
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        with conn.cursor() as cursor:
            cursor.execute(TABLE_CHECK)
            rez = cursor.fetchall()
    
    res = [rex[0] for rex in rez]

    if any(res):
        logger.info(f"found these tables in the server {res}.")

        dec = input("initiation appears to be complete; [w] wipe everything? [or Return to pass] ").lower()
        if dec in ('w', 'wipe'):
            with phones() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(_DROP)

                    while cursor.nextset():
                        pass

                confirm(conn, force)

            home = _config()
            shutil.rmtree(home.parent)

    else:
        dec = input("nothing has been configured; [i] intiate local & remote? [or Return to pass] ").lower()

        if dec in('i', 'init', 'initiate'):
            drps, frps = scraper(LOCAL_DIR)

            with phones() as conn:
                init_remote(conn, drps, frps)
                init_dindex(drps)
                init_index()

                conn.commit()

    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()