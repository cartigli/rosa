#!/usr/bin/env python3
"""Rolls local directory back to state of latest commit.

Does not query or connect to the server EXCEPT to verify the hashes.
Hashes are only verified if the file's timestamp shows a discrepancy.
"""

import shutil
import sqlite3

# LOCAL_DIR used once (besides import)
from rosa.confs import LOCAL_DIR
from rosa.lib import (
    phones, fat_boy, mk_rrdir, 
    save_people, mini_ps, finale, 
    query_index, _config, refresh_index, 
    scrape_dindex
)

NOMIC = "[get]"

def originals(replace, tmpd):
    """Copies the originals of deleted or altered files to replace edits."""
    home = _config()
    origin = home.parent
    originals = origin / "originals"

    for rp in replace:
        fp = originals / rp
        bp = tmpd / rp
        (bp.parent).mkdir(parents=True, exist_ok=True)

        shutil.copy(fp, bp)

def main(args=None):
    """Reverts the local state to the most recent commit."""
    xdiff = False
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        new, deleted, diffs, remaining, xdiff = query_index(conn)
    
    indexed_dirs = scrape_dindex()

    if xdiff is True:
        logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")
        with fat_boy(LOCAL_DIR) as (tmp_, backup):
            logger.info('copying directory tree...')
            mk_rrdir(indexed_dirs, tmp_)

            logger.info('hard linking unchanged files...')
            save_people(remaining, backup, tmp_)

            # ignore new files

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

    logger.info('All set.')

if __name__=="__main__":
    main()