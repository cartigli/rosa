import os
import sys
import hashlib
import logging
import datetime
# import mysql.connector
from pathlib import Path
from contextlib import closing

# import mysql.connector

from config import *
# from rosa_addfxs import batcher
from rosa_lib import(scope_loc, scope_rem, 
    ping_cass, contrast, compare, rm_remdir, 
    rm_remfile, collect_info, collect_data, 
    upload_dirs, upload_created, # calc_compression,
    upload_edited, confirm, 
    phone_duty #,init_conn
)

# f_handler = logging.FileHandler('rosa.log', mode='a')
# f_handler.setLevel(logging.DEBUG)

# cons_handler = logging.StreamHandler()
# cons_handler.setLevel(LOGGING_LEVEL.upper())

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[f_handler, cons_handler]
# ) # DEBUG, INFO, WARNING, ERROR, CRITICAL

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""


# if __name__ == "__main__":
def main():
    raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)
    # files, directories, folder full path

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        heaven_data = [raw_heaven, heaven_dirs]
        if heaven_data: # remote hashes & relative paths
            logging.info('Heaven returned data.')

            cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # old, new, unchanged, changed file[s]
            f_delta = [cherubs, serpents, souls] # f_felta = altered file data

            gates, caves = compare(heaven_dirs, hell_dirs) # old directory[s], new directory[s]
            d_delta = [gates, caves] # d_delta = directory data

            if any(f_delta) or any(d_delta): # need context manager for connection
                try:
                    logging.info('Found discrepancies; proceeding to processing differences.')
                    if any(d_delta):
                        # confirm connection before attempting to alter server
                        try:
                            conn.ping(reconnect=True, attempts=3, delay=0.5)

                        # except (mysql.connector.Error, ConnectionError, Exception) as mce:
                        except (ConnectionError, Exception) as mce:
                            logging.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                            raise
                        else:
                            logging.info('Connection confirmed.')

                            if gates:
                                logging.info(f"{len(gates)} remote-only directories found.")

                                try:
                                    rm_remdir(conn, gates) # delete remote-only[s] from server
                                    logging.info('Removed remote-only directories.')

                                # except (mysql.connector.Error, ConnectionError, Exception) as c:
                                except (ConnectionError, Exception) as c:
                                    logging.critical(f"Exception while altering database: {c}. Will attempt to roll back on exit.", exc_info=True)
                                    raise

                            if caves: 
                                # when uploading to server, order of when to upload new directories is not as sensitive 
                                # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                                logging.info(f"{len(caves)} local-only directories found.")

                                try:
                                    upload_dirs(conn, caves) # upload local-only[s] to server
                                    logging.info('Local-only directories uploaded.')
                        
                                # except (mysql.connector.Error, ConnectionError, Exception) as c:
                                except (ConnectionError, Exception) as c:
                                    logging.critical(f"Exception while altering database: {c}. Will attempt to roll back on exit.", exc_info=True)
                                    raise

                    if any(f_delta): # these next three lines initiate the variables to avoid an UnboundLocalError
                        # soul_size = serpent_size = 0 # before compression
                        # csoul_size = cserpent_size = 0 # after compression
                        # t_compression_size = ct_size = t_size = 0 # count diff

                        try:
                            # reconfirm connection again; meat & potatoes are here
                            conn.ping(reconnect=True, attempts=3, delay=0.5)

                        # except (mysql.connector.Error, ConnectionError, Exception) as mce:
                        except (ConnectionError, Exception) as mce:
                            logging.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                            raise
                        else:
                            logging.info('Connection confirmed.')

                            if cherubs:
                                logging.info(f"{len(cherubs)} remote-only files found.")

                                try:
                                    rm_remfile(conn, cherubs) # delete remote-only file[s]
                                    logging.info('Removed remote-only files.')

                                # except (mysql.connector.Error, ConnectionError, Exception) as c:
                                except (ConnectionError, Exception) as c:
                                    logging.critical(f"Exception while deleting remote-only files: {c}. Will attempt roll back on exit.", exc_info=True)
                                    raise

                            if souls:
                                # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                                # soul_batches, soul_size = collect_info(souls, abs_path) # returns batches
                                soul_batches = collect_info(souls, abs_path) # returns batches
                                for batch in soul_batches:
                                    # batch by batch, collect the content/hashes/relative paths into memory . . .
                                    # soul_data, csoul_size = collect_data(batch, abs_path)
                                    soul_data = collect_data(batch, abs_path)
                                    # t_compression_size += csoul_size # count size rq
                                    if soul_data:
                                # soul_data = batcher(souls, abs_path)
                                        logging.info('Obtained batched data for files with hash discrepancies.')
                                        try:
                                            # and upload the batch tp the server; repeat
                                            upload_edited(conn, soul_data)
                                            logging.info('Wrote batch to server.')

                                        # except (mysql.connector.Error, ConnectionError, Exception) as c:
                                        except (ConnectionError, Exception) as c:
                                            logging.critical(f"Exception while uploading altered files: {c}. Will attempt roll back on exit.", exc_info=True)
                                            raise

                            if serpents:
                                # identical to souls upload block
                                # serpent_batches, serpent_size = collect_info(serpents, abs_path)
                                serpent_batches = collect_info(serpents, abs_path)
                                for batch in serpent_batches:
                                    # serpent_data, cserpent_size = collect_data(batch, abs_path)
                                    serpent_data = collect_data(batch, abs_path)
                                    # t_compression_size += cserpent_size # count compressed size
                                    if serpent_data:
                                # serpent_data = batcher(serpents, abs_path)
                                        logging.info('Obtained batched data for local-only files.')
                                        try:
                                            # upload the current batch
                                            upload_created(conn, serpent_data)
                                            logging.info('Wrote batch to server.')

                                        # except (mysql.connector.Error, ConnectionError, Exception) as c:
                                        except (ConnectionError, Exception) as c:
                                            logging.critical(f"Exception while uploading local-only files: {c}. Will attempt roll back on exit.", exc_info=True)
                                            raise
                            
                            # if soul_size or serpent_size:
                                # t_size = soul_size + serpent_size # float 4 for both
                                # print(f"Total size before compression: {t_size:.4f}.")
                            
                            # if csoul_size or cserpent_size:
                            #     ct_size = t_compression_size
                                # print(f"Total size after compression: {ct_size:.4f}.")
                            
                            # if ct_size and t_size:
                                # calc_compression(t_size, ct_size)
                                # compressed = t_size - ct_size
                                # if compressed > 1024:
                                #     comp_kb = compressed / 1024
                                #     if comp_kb > 1000:
                                #         comp_mb = comp_kb / 1000
                                #         if comp_mb > 1000:
                                #             comp_gb = comp_mb / 1000
                                #             print(f"Disk space [in gb] saved by compression: {comp_gb:.4f}.")
                                #         else:
                                #             print(f"Disk space [in mb] saved by compression: {comp_mb:.4f}.")
                                #     else:
                                #         print(f"Disk space [in kb] saved by compression: {comp_kb:.4f}.")
                                # else:
                                #     print(f"Disk space [in bytes] saved by compression: {compressed:.4f}.")

                    if start:
                        end = datetime.datetime.now(datetime.UTC).timestamp()
                        proc_time = end - start
                        if proc_time > 60:
                            min_time = proc_time / 60
                            # logging.info(f"Upload time [in minutes] for rosa [give]: {min_time:.3f}.")
                            print(f"Upload time [in minutes] for rosa [give]: {min_time:.3f}.")
                        else:
                            # logging.info(f"Upload time [in seconds] for rosa [give]: {proc_time:.3f}.")
                            print(f"Upload time [in seconds] for rosa [give]: {proc_time:.3f}.")

                # except (mysql.connector.Error, ConnectionError, Exception) as c:
                except (ConnectionError, Exception) as c:
                    logging.critical('Exception encountered while attempting to upload data.', exc_info=True)
                    raise
                else:
                    confirm(conn)
                    logging.info('Uploaded data to server.')

            else: # if no diff, wrap it up
                logging.info('No discrepancies found; All set.')

        else: # if server is empty, let us know
            logging.info('Server is devoid of data.')

    logging.info('[give] complete.')
    print('All set.')


def init_logger():
    f_handler = logging.FileHandler('rosa.log', mode='a')
    f_handler.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    cons_handler.setLevel(LOGGING_LEVEL.upper())

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[f_handler, cons_handler]
    ) # DEBUG, INFO, WARNING, ERROR, CRITICAL


if __name__=="__main__":
    init_logger()
    logging.info('Rosa [give] executed.')

    start = datetime.datetime.now(datetime.UTC).timestamp()
    if start:
        logging.info('[give] timer started.')

    main()

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start
        if proc_time > 60:
            mins = proc_time / 60
            logging.info(f"Total processing time [in minutes] for rosa [give]: {mins:.3f}.")
        else:
            logging.info(f"Total processing time [in seconds] for rosa [give]: {proc_time:.3f}.")