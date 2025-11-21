import os
import sys
import shutil
import hashlib
import logging
import datetime
from config import *
import mysql.connector
from pathlib import Path
from contextlib import closing
from rosa_lib import (scope_loc, scope_rem,
    ping_cass, contrast, compare,
    calc_batch, # configure,
    download_batches, # apply_atomicy, 
    fat_boy,
    mk_dir, phone_duty #, init_conn
)

f_handler = logging.FileHandler('rosa.log', mode='a')
f_handler.setLevel(logging.DEBUG)

cons_handler = logging.StreamHandler()
cons_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[f_handler, cons_handler]
)

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""


if __name__ == "__main__":
    logging.info('Rosa [get] executed.')
    start = datetime.datetime.now(datetime.UTC).timestamp()
    logging.info('Timer started.')

    raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        heaven_data = [raw_heaven, heaven_dirs]
        if heaven_data:
            logging.info('Data returned from heaven.')

            cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # new files, dead files, changed files
            f_delta = [cherubs, serpents, souls]

            gates, caves = compare(heaven_dirs, hell_dirs) # new folders, dead folders
            d_delta = [gates, caves]

            if any(f_delta) or any(d_delta):
                logging.info('Discrepancies found; processing.')
                with fat_boy(abs_path) as (tmp_, backup):
                    try:
                        if stags:
                            logging.info(f"{len(stags)} unchanged files in both places.")
                            save_people(stags, backup, tmp_)
                            logging.info('Linked unchanged files to tmp_ directory.')

                        if serpents:
                            logging.debug(f"Ignoring {len(serpents)} local-only files.")

                        if any(d_delta):

                            if gates:
                                logging.debug(f"{len(gates)} remote-only directories found.")
                                mk_dir(gates, tmp_) # write dir heirarchy to tmp_
                                logging.info('New directories written to disk.')

                            if caves:
                                logging.debug(f"Ignoring {len(caves)} local-only files.")

                        if any(f_delta):
                            try:
                                try:
                                    conn.ping(reconnect=True, attempts=3, delay=0.5)
                                    batch_size = calc_batch(conn)

                                except mysql.connector.Error as mce:
                                    logging.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                                    raise
                                else:
                                    logging.info('Connection confirmed.')

                                    if cherubs:
                                        logging.info(f"{len(cherubs)} Remote-only files found; downloading.")
                                        download_batches(cherubs, conn, batch_size, tmp_)

                                    if souls:
                                        logging.debug(f"{len(souls)} files with hash discrepancies found.")
                                        download_batches(souls, conn, batch_size, tmp_)

                                    # context manager deals with apply atomicy now

                            except PermissionError as p:
                                logging.critical('Permission Error encountered while attempting atomic download & write.', exc_info=True)
                                raise
                            except KeyboardInterrupt as kb:
                                logging.critical('Atomic wr interupted.', exc_info=True)
                                raise
                            except Exception as e:
                                logging.critical(f"Exception encountered while attempting atomic download & write: {e}.", exc_info=True)
                                raise
                            except:
                                logging.critical('Exception encountered while attempting atomic download & wr.', exc_info=True)
                                raise
                            else:
                                logging.info('Batched atomic download completed without exception.')

                    except PermissionError as p:
                        logging.error('Permission Error encountered while attempting atomic write.', exc_info=True)
                        raise
                    except KeyboardInterrupt as kb:
                        logging.error('Keyboard Interrupted attempt at atomic write.', exc_info=True)
                        raise
                    except Exception as e:
                        logging.error('Exception encountered while attempting atomic write.', exc_info=True)
                        raise
                    else:
                        logging.info('Atomic downlaod & write completed without exception.')
                
                logging.info('Fat boy closed.')

            else:
                logging.info('No discrepancies found; All set.')
        
        else:
            logging.info('Server is devoid of data.')
    
    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logging.info(f"Processing time [in minutes] for rosa [get]: {min_time}.")
        else:
            logging.info(f"Processing time [in seconds] for rosa [get]: {proc_time}.")

    logging.info('[get] completed.')
    print('All set.')