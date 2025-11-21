import os
import sys
import hashlib
import logging
import datetime
from config import *
import mysql.connector
from pathlib import Path
from contextlib import closing
from rosa_lib import(scope_loc, scope_rem, 
    ping_cass, contrast, compare, rm_remdir, 
    rm_remfile, collect_info, collect_data, 
    upload_dirs, upload_created, 
    upload_edited, confirm, 
    phone_duty #,init_conn
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
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""


if __name__ == "__main__":
    logging.info('Rosa [give] executed.')
    start = datetime.datetime.now(datetime.UTC).timestamp()
    logging.info('Timer started.')

    raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        heaven_data = [raw_heaven, heaven_dirs]
        if heaven_data:
            logging.info('Heaven returned data.')

            cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # new files, dead files, changed files
            f_delta = [cherubs, serpents, souls]

            gates, caves = compare(heaven_dirs, hell_dirs) # new folders, dead folders
            d_delta = [gates, caves]

            if any(f_delta) or any(d_delta): # need context manager for connection
                try:
                    logging.info('Found discrepancies; proceeding to processing differences.')

                    if any(d_delta):

                        try:
                            conn.ping(reconnect=True, attempts=3, delay=0.5)

                        except (mysql.connector.Error, ConnectionError, Exception) as mce:
                            logging.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                            raise
                        else:
                            logging.info('Connection confirmed.')

                            if gates:
                                logging.info(f"{len(gates)} remote-only directories found.")

                                try:
                                    rm_remdir(conn, gates)
                                    logging.info('Removed remote-only directories.')

                                except (mysql.connector.Error, ConnectionError, Exception) as c:
                                    logging.critical(f"Exception while altering database: {c}. Will attempt to roll back on exit.", exc_info=True)
                                    raise

                            if caves:
                                logging.info(f"{len(caves)} local-only directories found.")

                                try:
                                    upload_dirs(conn, caves)
                                    logging.info('Local-only directories uploaded.')
                        
                                except (mysql.connector.Error, ConnectionError, Exception) as c:
                                    logging.critical(f"Exception while altering database: {c}. Will attempt to roll back on exit.", exc_info=True)
                                    raise

                    if any(f_delta):
                        soul_size = 0
                        serpent_size = 0

                        csoul_size = 0
                        cserpent_size = 0

                        t_compression_size = 0

                        try:
                            conn.ping(reconnect=True, attempts=3, delay=0.5)

                        except (mysql.connector.Error, ConnectionError, Exception) as mce:
                            logging.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                            raise
                        else:
                            logging.info('Connection confirmed.')

                            if cherubs:
                                logging.info(f"{len(cherubs)} remote-only files found.")

                                try:
                                    rm_remfile(conn, cherubs)
                                    logging.info('Removed remote-only files.')

                                except (mysql.connector.Error, ConnectionError, Exception) as c:
                                    logging.critical(f"Exception while deleting remote-only files: {c}. Will attempt roll back on exit.", exc_info=True)
                                    raise

                            if souls:
                                soul_batches, soul_size = collect_info(souls, abs_path)
                                for batch in soul_batches:
                                    soul_data, csoul_size = collect_data(batch, abs_path)
                                    t_compression_size += csoul_size
                                    if soul_data:
                                        logging.info('Obtained batched data for files with hash discrepancies.')
                                        try:

                                            upload_edited(conn, soul_data)
                                            logging.info('Wrote batch to server.')

                                        except (mysql.connector.Error, ConnectionError, Exception) as c:
                                            logging.critical(f"Exception while uploading altered files: {c}. Will attempt roll back on exit.", exc_info=True)
                                            raise

                            if serpents:
                                serpent_batches, serpent_size = collect_info(serpents, abs_path)
                                for batch in serpent_batches:
                                    serpent_data, cserpent_size = collect_data(batch, abs_path)
                                    t_compression_size += cserpent_size
                                    if serpent_data:
                                        logging.info('Obtained batched data for local-only files.')
                                        try:

                                            upload_created(conn, serpent_data)
                                            logging.info('Wrote batch to server.')

                                        except (mysql.connector.Error, ConnectionError, Exception) as c:
                                            logging.critical(f"Exception while uploading local-only files: {c}. Will attempt roll back on exit.", exc_info=True)
                                            raise
                            
                            if soul_size or serpent_size:
                                t_size = soul_size + serpent_size
                                print(f"Total size before compression: {t_size}.")
                            
                            if csoul_size or cserpent_size:
                                ct_size = t_compression_size
                                print(f"Total size after compression: {ct_size}.")
                            
                            if ct_size and t_size:
                                compressed = t_size - ct_size
                                if compressed > 1024:
                                    comp_kb = compressed / 1024
                                    if comp_kb > 1000:
                                        comp_mb = comp_kb / 1000
                                        if comp_mb > 1000:
                                            comp_gb = comp_mb / 1000
                                            print(f"Disk space [in gb] saved by compression: {comp_gb:.4f}.")
                                        else:
                                            print(f"Disk space [in mb] saved by compression: {comp_mb:.4f}.")
                                    else:
                                        print(f"Disk space [in kb] saved by compression: {comp_kb:.4f}.")
                                else:
                                    print(f"Disk space [in bytes] saved by compression: {compressed:.4f}.")

                    if start:
                        end = datetime.datetime.now(datetime.UTC).timestamp()
                        proc_time = end - start
                        if proc_time > 60:
                            min_time = proc_time / 60
                            logging.info(f"Upload time [in minutes] for rosa [give]: {min_time:.3f}.")
                        else:
                            logging.info(f"Upload time [in seconds] for rosa [give]: {proc_time:.3f}.")

                except (mysql.connector.Error, ConnectionError, Exception) as c:
                    logging.critical('Exception encountered while attempting to upload data.', exc_info=True)
                    raise
                else:
                    confirm(conn)
                    logging.info('Uploaded data to server.')

            else:
                logging.info('No discrepancies found; All set.')

        else:
            logging.info('Server is devoid of data.')

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start
        if proc_time > 60:
            mins = proc_time / 60
            logging.info(f"Total processing time [in minutes] for rosa [give]: {mins:.3f}.")
        else:
            logging.info(f"Total processing time [in seconds] for rosa [give]: {proc_time:.3f}.")

    logging.info('[give] complete.')
    print('All set.')