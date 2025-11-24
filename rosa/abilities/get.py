#!/usr/bin/env python3
import logging
import datetime

from rosa.abilities.config import *
from rosa.abilities.lib import (scope_loc, scope_rem,
    ping_cass, contrast, compare,
    calc_batch, download_batches,
    fat_boy, save_people,
    mk_dir, phone_duty
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

    raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)
    # files, directories, folder full path

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn: # context manager for peace of mind
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpath's

        heaven_data = [raw_heaven, heaven_dirs]
        if heaven_data: # remote hashes & relative paths
            logger.info('Data returned from heaven.')

            cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # new, old, unchanged, changed file[s]
            f_delta = [cherubs, serpents, souls] # f_delta = altered file data

            gates, caves = compare(heaven_dirs, hell_dirs) # new directory[s], old directory[s]
            d_delta = [gates, caves] # altered directory data

            if any(f_delta) or any(d_delta): # if file or folder data has been changed, continue to processing
                logger.info('Discrepancies found; processing.')
                with fat_boy(abs_path) as (tmp_, backup): # context manager [atomic]
                    try:
                        # deal with local file data first; then make directories, then make new and altered files
                        if stags:
                            logger.info(f"{len(stags)} unchanged files in both places.")
                            save_people(stags, backup, tmp_) # hard link unchanged files - fast
                            logger.info('Linked unchanged files to tmp_ directory.')

                        if serpents:
                            logger.debug(f"Ignoring {len(serpents)} local-only files.")
                            # files only found locally can be ignored; they will remain in the backup dir
                            # and be deleted when the atomic wr completes w.o exception

                        if any(d_delta):

                            if gates:
                                logger.debug(f"{len(gates)} remote-only directories found.")
                                mk_dir(gates, tmp_) # write directory heirarchy to tmp_ directory
                                logger.info('New directories written to disk.')

                            if caves:
                                logger.debug(f"Ignoring {len(caves)} local-only directories.")

                        if any(f_delta):
                            try:
                                try:
                                    # ensure connection is present before conitnuing with meat & potatoes
                                    conn.ping(reconnect=True, attempts=3, delay=0.5)
                                    batch_size = calc_batch(conn)

                                except (mysql.connector.Error, ConnectionError, TimeoutError) as nce:
                                    logger.critical(f"Connection to server is faulty: {nce}.", exc_info=True)
                                    raise
                                else:
                                    logger.info('Connection confirmed.')

                                    if cherubs:
                                        logger.info(f"{len(cherubs)} Remote-only files found; downloading.")
                                        download_batches(cherubs, conn, batch_size, tmp_)
                                        # handles pulling new file data, giving it batch by batch
                                        # to the wr batches function, and continuing until list is empty

                                    if souls:
                                        logger.debug(f"{len(souls)} files with hash discrepancies found.")
                                        download_batches(souls, conn, batch_size, tmp_)
                                        # same here as w.cherubs but for altered file[s] (hash discrepancies)

                            except (PermissionError, KeyboardInterrupt) as p:
                                logger.critical('Exception encountered while attempting atomic download & write.', exc_info=True)
                                raise
                            except Exception as e:
                                logger.critical(f"Exception encountered while attempting atomic download & write: {e}.", exc_info=True)
                                raise
                            else:
                                logger.info('Batched atomic download completed without exception.')

                    except (PermissionError, KeyboardInterrupt) as p:
                        logger.error('Exception encountered while attempting atomic write.', exc_info=True)
                        raise
                    except Exception as e:
                        logger.error('Exception encountered while attempting atomic write.', exc_info=True)
                        raise
                    else:
                        logger.info('Atomic downlaod & write completed without exception.')
                
                logger.info('Fat boy closed.')

            else:
                logger.info('No discrepancies found; All set.')
        
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