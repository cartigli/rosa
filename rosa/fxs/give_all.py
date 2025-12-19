#!/usr/bin/env python3
"""Upload everything to the server. 

Abandon if the server already contains (any) data.
This main() should initiate the local index after uploading & committing.
If it exists, delete it. It will be useless as this only uploads *everything.
"""

import sys
import time
from pathlib import Path

# from rosa.confs import *
from rosa.lib import (
    phones, scope_rem, ping_cass, upload_dirs, 
    confirm, mini_ps, counter, finale, collector
)

NOMIC = "[give][all]"

def scraper():
    """Modified scope_rem() to go straight to data collection instead of comparing to the server first.

    Args:
        None
    
    Returns:
        serpents (list): Every file's full path.
        caves (list): Single-item tuples containing every directory's relative path.
        abs_path (Path): The LOCAL_DIR's full path.
    """
    local_dir = LOCAL_DIR

    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    abs_path = Path(local_dir).resolve()

    serpents = []
    caves = []

    for item in abs_path.rglob('*'):
        path_str = item.resolve().as_posix()
        if any(blocked in path_str for blocked in blk_list):
            continue # skip item if blkd item in path
        else:
            if item.is_file():
                frp = item.relative_to(abs_path).as_posix()

                serpents.append(frp)

            elif item.is_dir():
                drp = item.relative_to(abs_path).as_posix()

                caves.append((drp,))
    
    return serpents, caves, abs_path

def main(args=None): # this is where init_index should be called/activated
    """Brute force upload of all the local data. 
    
    Uploads everything.
    Quits if the server contains data, i.e., not empty.
    """
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        logger.info('conn is connected')
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        if not any((raw_heaven, heaven_dirs)): # double check it is empty
            logger.info('heaven\'s empty; processing local data...')

            if Path(LOCAL_DIR).exists():
                serpents, caves, abs_path = scraper()
                logger.info('collected local paths; uploading...')

                try:
                    if caves:
                        logger.info('uploading local-only directory[s]...')
                        upload_dirs(conn, caves) # upload local-only[s] to server

                    if serpents:
                        key = "new_file"
                        logger.info('uploading local-only file[s]...')

                        collector(conn, serpents, abs_path, key)

                    counter(start, NOMIC)

                except (ConnectionError, TimeoutError) as c:
                    logger.critical(f"{RED}exception encountered while uploading data:{RESET} {c}", exc_info=True)
                    sys.exit(1)
                except KeyboardInterrupt as k:
                    logger.warning('boss killed it; aborting')
                    sys.exit(1)
                else:
                    confirm(conn, force)
            else:
                logger.warning(f"{RED}local directory doesn\'t exist{RESET}")
                sys.exit(1)
        else:
            logger.info('server contains data; truncate tables before attempting again. aborting')
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()
