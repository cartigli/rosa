#!/usr/bin/env python3
"""Retrieves the current version from the server.

Used to sync before another version can be uploaded.
If the local and remote versions do not match, i.e.,
there has been an update from another machine since you pulled,
you cannot give. Sync before trying to give.

Why pull everything w.out checking for diffs??
Did I mean for this to be only if the directory doesn't exist? Good lord.
So clarity is needed on the scripts function, fo sho.

Good lord, it doesn't even look for the index. Fourth piece of evidence.
"""

import sys
import time
import shutil
import logging
from pathlib import Path

# LOCAL_DIR used once (besides import)
from rosa.confs import LOCAL_DIR
from rosa.lib import (
    phones, calc_batch, 
    sfat_boy, mk_rrdir, mini_ps, finale
)

NOMIC = "[get][current]"

def main(args=None):
    """Syncs directory to latest version.""" # has no actual logging; need to add that
    logger, force, prints, start = mini_ps(args, NOMIC)

    abs_path = Path(LOCAL_DIR).resolve()

    if not abs_path.exists():
        abs_path.mkdir()

    with phones() as conn:
        with sfat_boy(abs_path) as tmpd: # sfat_boy inside phones ensures sfat_boy catches errors before phones
            # wait, why the f*ck am I using sfat_boy instead of fat_boy? This function needs to restore the given directory on failure.
            # It feels dumb as hell to retrace my own steps like this, but this is more 
            # evidence that I meant this to be a single execution comprehensive pull.
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
                        fp = tmpd / rp

                        with open(fp, 'wb') as f:
                            f.write(content)

    finale(NOMIC, start, prints)

    logger.info('All set.')

if __name__=="__main__":
    main()