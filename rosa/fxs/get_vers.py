#!/usr/bin/env python3
"""Write the server's state to the disk.

Make remote-only files/directories, 
delete local-only files/directories, 
and updated content for altered files. 
Abandon if the server or local directory are empty.

This main() should also update the index because it makes changes to the local data.
Do the vice-versa of the give edits. Make deleted, delete new, and update altered.
Insert deleted, delete created, and revert edits (diffs).
"""

import shutil
import sqlite3
from datetime import datetime

from rosa.confs import LOCAL_DIR, VERSIONS, VDIRECTORIES
from rosa.lib import (
    phones, fat_boy, mk_rrdir, 
    save_people, mini_ps, finale, query_index, _config, refresh_index
)

NOMIC = "[get]"

def originals(replace, tmpd):
    home = _config()
    origin = home.parent
    originals = origin / "originals"

    for rp in replace:
        fp = originals / rp
        bp = tmpd / rp
        if not bp.exists():
            print(bp, "DOESN'T EXIST [BP]")
        if not fp.exists():
            print(fp, "DOESN'T EXIST [FP]")
        (bp.parent).mkdir(parents=True, exist_ok=True)

        shutil.copy(fp, bp)

def main(args=None):
    """Reverts the local state to the most recent commit"""
    xdiff = False
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        with conn.cursor() as cursor:
            cursor.execute(VERSIONS)
            versions = cursor.fetchall()
    
    fvers = []
    vers = []
    
    for version, moment, message in versions:
        date_obj = datetime.utcfromtimestamp(moment)
        date = date_obj.strftime("%Y-%m-%d - %M:%H:%S")
        if message:
            fvers.append((version, date, message))
        else:
            fvers.append((version, date))
    
        vers.append(version)
    
    print('all the currently recorded commitments:')
    for v in fvers:
        print(v)

    version = input(f"Enter the version you would like to receive ({vers}): ")


    # if xdiff is True:
    if version:
        logger.info(f"requested version recieved: v{version}")
        dpath = Path(LOCAL_DIR).resolve()

        tmpd = dpath.parent / f"rosa_v{version}"
        tmpd.mkdir(parents=True)
        with phones() as conn:
            with conn.cursor() as cursor:

                cursor.execute(VDIRECTORIES, (version,))
                drps = cursor.fetchall()

                cursor.execute(VD_DIRECTORIES, (version,))
                ddrps = cursor.fetchall()
                for d in ddrps:
                    drps.append(d)

                logger.info('copying directory tree...')
                mk_rrdir(d, tmpd)

                get

            # logger.info('hard linking unchanged files...')
            # save_people(remaining, backup, tmp_)
            # # ignore new files

            for d in deleted:
                diffs.append(d)

            logger.info('replacing files with deltas')
            originals(diffs, tmp_)

            for r in remaining:
                diffs.append(r)

        refresh_index(diffs)
        # refresh_index() # this is delicate and senstive, but not as much as 'give'. Since the new/deleted/altered files' content is not recorded, their new values are not 
        # in the index, and the local copy of the directory is left at the state of the original, the index only needs to be updated for the files that actually get 
        # touched during this procedure (local copy of directory remains untouched). The files who are touched (a.k.a. ctimes change) are the altered files, the deleted files,
        # and the unchanged files. Altered files get overwritten, deleted files get replaced, and unchanged files get hard-linked, which alone doesn't doesn't change their ctimes,
        # but upon deletion of the original path, the ctime does change (also meaning the index can't be updated until the original directory is deleted, a.k.a. when fat_boy closes.
        # This means that the refresh_index won't break the system if it fails, but it will force it out of sync and need to be resynced. The local copy of the directory is untouched, 
        # so we don't need a backup of the orginal like fat_boy uses, but it will require retry on failure (or perfect code, which is unlikely). This also means refresh_index should 
        # get a list of modified, deleted, and unchanged files to update in the index. 
    else:
        logger.info('no diff!')
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()