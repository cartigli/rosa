#!/usr/bin/env python3
import sys
import time
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import(scope_rem, ping_cass, 
        contrast, compare, rm_remdir, init_logger, rm_remfile, 
        collect_info, collect_data, upload_dirs, upload_created, 
        confirm, phone_duty, mini_ps
    )

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

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
            else:
                continue
    
    return serpents, caves, abs_path


def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)

    logger.info('rosa [give] executed')

    start = time.perf_counter()
    if start:
        logger.info('[give] [all] timer started')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        logger.info('conn is connected')
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        if not raw_heaven or heaven_dirs:
            logger.info('heaven\'s empty; processing local data...')

            if LOCAL_DIR.exists():
                serpents, caves, abs_path = scraper()
                logger.info('collected local paths; uploading...')
            else:
                logger.warning('local directory doesn\'t exist; aborting')
                sys.exit(1)

            try:
                if caves: 
                    logger.info('uploading local-only directory[s]...')
                    upload_dirs(conn, caves) # upload local-only[s] to server
                    logger.info('directory[s] uploaded')

                if serpents:
                    logger.info('uploading local-only file[s]...')
                    # serpents_ = sorted(serpents, key=str.lower)
                    with logging_redirect_tqdm(loggers=[logger]): # tqdm method
                        with tqdm(collect_info(serpents, abs_path), unit="batches", leave=False) as pbar:
                            for batch in pbar:
                                serpent_data = collect_data(batch, abs_path, conn)

                                if serpent_data:
                                    upload_created(conn, serpent_data)
                    logger.info('file[s] uploaded')

                if start:
                    end = time.perf_counter()
                    proc_time = end - start
                    if proc_time > 60:
                        min_time = proc_time / 60
                        logger.info(f"upload time [in minutes] for rosa [give] [all]: {min_time:.3f}.")
                    else:
                        logger.info(f"upload time [in seconds] for rosa [give] [all]: {proc_time:.3f}.")

            except (ConnectionError, TimeoutError) as c:
                logger.critical(f"exception encountered while uploading data{c}", exc_info=True)
                sys.exit(1)
            except KeyboardInterrupt as k:
                logger.warning('boss killed it; aborting')
                sys.exit(1)
            else:
                if force:
                    try:
                        conn.commit()
                    except Exception as e:
                        logger.critical(f"error on --forced commit: {e}", exc_info=True)
                        sys.exit(3) # auto_commit: False, so error handling to rollback is not necessary
                    else:
                        logger.info('forced commit w.o exception')
                else:
                    try:
                        confirm(conn)
                    except:
                        raise
                    else:
                        logger.info('confirmed commitment w.o exception')

        else: # if server is empty, let us know
            logger.info('server contains data; truncate tables before attempting again. Aborting [give] [all]')

    logger.info('[give] [all] complete')

    if prints:
        print('All set.')


if __name__=="__main__":
    from config import *
    from lib import(scope_rem, ping_cass, mini_ps,
        contrast, compare, rm_remdir, init_logger, rm_remfile, 
        collect_info, collect_data, upload_dirs, upload_created, 
        confirm, phone_duty
    )
    main(args=None)
