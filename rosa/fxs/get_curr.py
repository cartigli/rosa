#!/usr/bin/env python3
"""Retrieves the current version from the server.

Whole thing gotta be rewrote.
"""


import os
import sys
import time
import shutil
import logging

from rosa.lib import (
    phones, calc_batch, 
    sfat_boy, mk_rrdir, 
    mini_ps, finale
)

NOMIC = "[get][current]"

def main(args=None):
    """Downloads the latest version.""" 

    logger, force, prints, start = mini_ps(args, NOMIC)

    target = os.path.expanduser('~')

    tdir = os.path.join(target, "rosa_current")

    with phones() as conn:
        with sfat_boy(tdir) as tmpd:
            dquery = "SELECT rp FROM directories;"
            with conn.cursor(buffered=False) as cursor:
                cursor.execute(dquery)
                c_dirs = cursor.fetchall()
            
            mk_rrdir(c_dirs, tmpd)

            batch_size, row_size = calc_batch(conn)
            cquery = "SELECT rp, content FROM files;"

            with conn.cursor(buffered=False) as cursor:
                cursor.execute(cquery)

                while True:
                    fdata = cursor.fetchmany(batch_size)

                    if not fdata:
                        break

                    for rp, content in fdata:
                        # fp = tmpd / rp
                        fp = os.path.join(tmpd, rp)

                        with open(fp, 'wb') as f:
                            f.write(content)

    finale(NOMIC, start, prints)

    logger.info('All set.')

if __name__=="__main__":
    main()