#!/usr/bin/env python3
import logging
import datetime
from pathlib import Path

from rosa.abilities.config import *
from rosa.abilities.lib import (scope_loc, scope_rem,
    ping_cass, contrast, compare,
    calc_batch, download_batches5,
    fat_boy, save_people,
    mk_rdir, phone_duty
)

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

logger = logging.getLogger(__name__)


def main():
    # logger = logging.getLogger(__name__)
    logger.info('Rosa [get] executed.')

    start = datetime.datetime.now(datetime.UTC).timestamp()
    if start:
        logger.info('[get] timer started.')
    
    abs_path = Path( LOCAL_DIR ).resolve()

    # raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)
    # files, directories, folder full path

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn: # context manager for peace of mind
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        souls = [s[0] for s in raw_heaven]
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpath's
        soul_dirs = [h[0] for h in heaven_dirs]

        heaven_data = [raw_heaven, heaven_dirs]
        if heaven_data: # remote hashes & relative paths
            logger.info('Data returned from heaven.')
            with fat_boy(abs_path) as (tmp_, backup): # context manager [atomic]
                batch_size = calc_batch(conn)
                try:
                    if soul_dirs:
                        logger.debug(f"{len(soul_dirs)} remote-only directories found.")
                        mk_rdir(soul_dirs, tmp_) # write directory heirarchy to tmp_ directory
                        logger.info('New directories written to disk.')

                    if souls:
                        logger.debug(f"{len(souls)} files with hash discrepancies found.")
                        download_batches5(souls, conn, batch_size, tmp_)
                        # same here as w.cherubs but for altered file[s] (hash discrepancies)

                except (PermissionError, KeyboardInterrupt) as p:
                    logger.critical('Exception encountered while attempting atomic download & write.', exc_info=True)
                    raise
                except Exception as e:
                    logger.critical(f"Exception encountered while attempting atomic download & write: {e}.", exc_info=True)
                    raise
                else:
                    logger.info('Atomic downlaod & write completed without exception.')
            
            logger.info('Fat boy closed.')
        
        else:
            logger.info('Server is devoid of data.')

    logger.info('[get] completed.')

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"Processing time [in minutes] for rosa [get]: {min_time:.4f}.")
        else:
            logger.info(f"Processing time [in seconds] for rosa [get]: {proc_time:.4f}.")

    print('All set.')


def init_logger():
    f_handler = logging.FileHandler('rosa.log', mode='a')
    f_handler.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    # cons_handler.setLevel(logging.INFO)
    cons_handler.setLevel(LOGGING_LEVEL.upper())

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[f_handler, cons_handler]
    ) # DEBUG, INFO, WARNING, ERROR, CRITICAL


if __name__=="__main__":
    # init_logger()
    logger = logging.getLogger(__name__)
    logger.info('Rosa [get] executed.')

    start = datetime.datetime.now(datetime.UTC).timestamp()
    if start:
        logger.info('[get] timer started.')

    main()

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"Processing time [in minutes] for rosa [get]: {min_time:.4f}.")
        else:
            logger.info(f"Processing time [in seconds] for rosa [get]: {proc_time:.4f}.")