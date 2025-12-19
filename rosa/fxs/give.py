#!/usr/bin/env python3
"""Upload local state to the database.

Upload local-only files/directories, 
delete remote-only files/directories, 
and update altered files. 
Abandon if the server or local directory are empty.

This main() should update the index to what was uploaded.
Twin the server edits; delete, insert, and update from the index as you did to the server.
Insert new files, delete deleted files, and update altered (diffs).
"""

from rosa.confs import LOCAL_DIR, RED, RESET
from rosa.lib import (
    phones, rm_remfile, confirm, 
    mini_ps, finale, collector,
    query_index, version_check, 
    diff_gen, remote_records, 
    upload_patches, local_audit_,
    historian, fat_boy, refresh_index,
    xxdeleted, rm_remdir, local_daudit,
    upload_dirs, query_dindex
)

NOMIC = "[give]"

def main(args=None):
    """Forces the local state onto the server. 

    Uploads new and altered files to the server. 
    Removes files/directories not found locally.
    Quits if server or local directory is empty.
    """
    xdiff = False
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        new, deleted, diffs, remaining, xdiff = query_index(conn)
    
    newd, deletedd, ledeux = query_dindex()

    if xdiff is True:
        logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")
        with phones() as conn:
            vok, v, home = version_check(conn)
            if vok is True:
                logger.info('versions: twinned')
                cv = v + 1

                # if force is True:
                    # message = f"upload v{version}"
                # else:
                message = input("attach a message to this version (or enter for None): ") or None
                remote_records(conn, cv, message)

                logger.info('uploading new files...')
                collector(conn, new, LOCAL_DIR, cv, key="new_files")

                logger.info('uploading altered files...')
                collector(conn, diffs, LOCAL_DIR, cv, key="altered_files") 

                logger.info('generating altered files\' patches')
                patches, originals = diff_gen(diffs, home.parent, LOCAL_DIR)

                logger.info('uploading altered files\' patches')
                upload_patches(conn, patches, cv)

                logger.info('removing deleted files from server')
                # needs to find the deleted file's original version first
                rm_remfile(conn, deleted)

                logger.info('updating remote directories')
                rm_remdir(conn, deletedd, cv)
                upload_dirs(conn, newd, cv)

                logger.info('updating local indexes')
                with fat_boy(originals) as secure:
                    local_audit_(new, diffs, remaining, cv, secure)
                    local_daudit(newd, deletedd, cv)

                    logger.info('backing up deleted files')
                    xxdeleted(conn, deleted, cv, secure)

                    logger.info('final confirmations')
                    historian(cv, message) # should this & confirm() be indented??
                    confirm(conn, force)

            else:
                logger.critical(f"{RED}versions did not align; pull most recent upload from server before committing{RESET}")
                return

        logger.info('phone hung up.')

        updates = list(remaining)

        for n in new:
            updates.append(n) # new & remaining need to get updated
        
        refresh_index(updates)

    else:
        logger.info('no diff!')
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()