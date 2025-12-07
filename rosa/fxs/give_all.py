#!/usr/bin/env python3
import sys
import time
from pathlib import Path

from tqdm import tqdm # this does not belong here
from tqdm.contrib.logging import logging_redirect_tqdm

# if __name__=="__main__":
#     cd = Path(__file__).resolve().parent.parent
#     if str(cd) not in sys.path:
#         sys.path.insert(0, str(cd))

from rosa.confs.config import *

from rosa.lib.opps import mini_ps, counter
from rosa.lib.dispatch import phones, confirm
from rosa.lib.analyst import scope_rem, ping_cass
from rosa.lib.technician import collect_info, collect_data, upload_dirs, upload_created

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

NOMIC = "[give][all]"

def scraper():
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

                caves.append({
                    'drp':drp
                }) # keep the empty list of dirs
    
    return serpents, caves, abs_path

def main(args=None):
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        logger.info('conn is connected')
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        if not any((raw_heaven, heaven_dirs)):
            logger.info('heaven\'s empty; processing local data...')

            if Path(LOCAL_DIR).exists():
                serpents, caves, abs_path = scraper()
                logger.info('collected local paths; uploading...')

                try:
                    if caves:
                        logger.info('uploading local-only directory[s]...')
                        upload_dirs(conn, caves) # upload local-only[s] to server

                    if serpents:
                        logger.info('uploading local-only file[s]...')

                        with logging_redirect_tqdm(loggers=[logger]): # tqdm method
                            with tqdm(collect_info(serpents, abs_path), unit="batches", leave=False) as pbar:

                                for batch in pbar:
                                    serpent_data = collect_data(batch, abs_path)

                                    if serpent_data:
                                        upload_created(conn, serpent_data)

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
