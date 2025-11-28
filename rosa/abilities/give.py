#!/usr/bin/env python3
import sys
import time
import logging
# import datetime

from rosa.abilities.config import *
from rosa.abilities.lib import(scope_loc, scope_rem, 
    ping_cass, contrast, compare, rm_remdir, 
    rm_remfile, collect_info, collect_info2, collect_data, 
    upload_dirs, upload_created,
    upload_edited, confirm, phone_duty
)

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

logger = logging.getLogger(__name__)


def main():
    # logger = logging.getLogger(__name__)
    logger.info('Rosa [give] executed.')

    # start = datetime.datetime.now(datetime.UTC).timestamp()
    # if start:
    #     logger.info('[give] timer started.')

    # raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)  # files, directories, folder full path

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        logging.info('Conn is connected.')
        raw_heaven = scope_rem(conn) # raw remote files & hash_id's
        heaven_dirs = ping_cass(conn) # raw remote dirs' rpats

        heaven_data = [raw_heaven, heaven_dirs]
        # if heaven_data[0]: # remote hashes & relative paths
        if heaven_data: # functionally usless; need to implement logic for when heaven is deserted
            # print(heaven_data)
            logger.info('Heaven returned data.')

            raw_hell, hell_dirs, abs_path, avg_size = scope_loc(LOCAL_DIR)  # files, directories, folder full path

            # print(avg_size)

            cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # old, new, unchanged, changed file[s]
            f_delta = [cherubs, serpents, souls] # f_felta = altered file data

            gates, caves = compare(heaven_dirs, hell_dirs) # old directory[s], new directory[s]
            d_delta = [gates, caves] # d_delta = directory data

            if any(f_delta) or any(d_delta): # need context manager for connection
                # start = datetime.datetime.now(datetime.UTC).timestamp()
                batch_sz = int(MAX_ALLOWED_PACKET / avg_size)

                time.sleep(7)
                logging.info(f"Batch size: {batch_sz}")

                start = time.perf_counter()
                if start:
                    logger.info('[give] timer started.')
                try:
                    logger.info('Found discrepancies; proceeding to processing differences.')
                    if any(d_delta):
                        # confirm connection before attempting to alter server
                        try:
                            conn.ping(reconnect=True, attempts=3, delay=0.5)

                        except (ConnectionError, Exception) as mce:
                            logger.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                            raise
                        else:
                            logger.info('Connection confirmed.')

                            if gates:
                                logger.info(f"{len(gates)} remote-only directories found.")

                                try:
                                    rm_remdir(conn, gates) # delete remote-only[s] from server
                                    logger.info('Removed remote-only directories.')

                                except (ConnectionError, Exception) as c:
                                    logger.critical(f"Exception while altering database: {c}. Will attempt to roll back on exit.", exc_info=True)
                                    raise

                            if caves: 
                                # when uploading to server, order of when to upload new directories is not as sensitive 
                                # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                                logger.info(f"{len(caves)} local-only directories found.")

                                try:
                                    upload_dirs(conn, caves) # upload local-only[s] to server
                                    logger.info('Local-only directories uploaded.')
                        
                                except (ConnectionError, Exception) as c:
                                    logger.critical(f"Exception while altering database: {c}. Will attempt to roll back on exit.", exc_info=True)
                                    raise

                        if any(f_delta): # if hash discrepancies or list differences were found:
                            # try:
                            #     # reconfirm connection again; meat & potatoes are here
                            #     conn.ping(reconnect=True, attempts=3, delay=0.5)

                            # except (ConnectionError, Exception) as mce:
                            #     logger.critical(f"Connection to server is faulty: {mce}.", exc_info=True)
                            #     raise
                            # else:
                            #     logger.info('Connection confirmed.')

                            if cherubs:
                                logger.info(f"{len(cherubs)} remote-only files found.")

                                try:
                                    rm_remfile(conn, cherubs) # delete remote-only file[s]
                                    logger.info('Removed remote-only files.')

                                except (ConnectionError, Exception) as c:
                                    logger.critical(f"Exception while deleting remote-only files: {c}. Will attempt roll back on exit.", exc_info=True)
                                    raise

                            if souls:
                                # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                                # souls_ = sorted(souls, key=str.lower)
                                # souls_ = sorted(souls)
                                souls_ = [item['frp'] for item in souls]
                                soulss = sorted(souls_, key=str.lower)
                                soul_batches = collect_info(soulss, abs_path) # returns batches
                                for batch in soul_batches:
                                    # batch by batch, collect the content/hashes/relative paths into memory . . .
                                    bg = time.perf_counter()
                                    soul_data = collect_data(batch, abs_path, conn)
                                    # end = time.perf_counter()
                                    # btime = end - bg
                                    # logging.info(f"Wrote batch for upload in {btime:.4f} seconds.")

                                    if soul_data:
                                        # logger.info('Obtained batched data for files with hash discrepancies.')
                                        try:
                                            # and upload the batch tp the server; repeat
                                            # nd = time.perf_counter()
                                            upload_edited(conn, soul_data)
                                            fnl = time.perf_counter()
                                            logging.info(f"Wrote batch to server in {(fnl - nd):.4f} seconds.")

                                        except (ConnectionError, Exception) as c:
                                            logger.critical(f"Exception while uploading altered files: {c}. Will attempt roll back on exit.", exc_info=True)
                                            raise

                            if serpents:
                                # identical to souls upload block
                                # serpents_ = sorted(serpents, key=str.lower)
                                # serpents_ = sorted(serpents)
                                serpents_ = [item['frp'] for item in serpents]
                                serpentss = sorted(serpents_, key=str.lower)
                                serpent_batches = collect_info(serpentss, abs_path)

                                # collect_info2(serpentss, abs_path, batch_sz, conn)

                                for batch in serpent_batches:
                                    bg = time.perf_counter()
                                    serpent_data = collect_data(batch, abs_path, conn)

                                    # end = time.perf_counter()
                                    # btime = end - bg
                                    # logging.info(f"Wrote batch for upload in {btime:.4f} seconds.")

                                    if serpent_data:
                                        # logger.info('Obtained batched data for local-only files.')
                                        try:
                                            # upload the current batch
                                            upload_created(conn, serpent_data)
                                            fnl = time.perf_counter()
                                            logging.info(f"Wrote batch to server in {(fnl - bg):.4f} seconds.")
                                            # logger.info('Wrote batch to server.')

                                        except (ConnectionError, Exception) as c:
                                            logger.critical(f"Exception while uploading local-only files: {c}. Will attempt roll back on exit.", exc_info=True)
                                            raise

                    if start:
                        # end = datetime.datetime.now(datetime.UTC).timestamp()
                        end = time.perf_counter()
                        proc_time = end - start
                        if proc_time > 60:
                            min_time = proc_time / 60
                            logger.info(f"Upload time [in minutes] for rosa [give]: {min_time:.3f}.")
                        else:
                            logger.info(f"Upload time [in seconds] for rosa [give]: {proc_time:.3f}.")

                except (ConnectionError, Exception) as c:
                    logger.critical('Exception encountered while attempting to upload data.', exc_info=True)
                    raise
                else:
                    confirm(conn)
                    logger.info('Uploaded data to server.')

            else: # if no diff, wrap it up
                logger.info('No discrepancies found; All set.')

        else: # if server is empty, let us know
            logger.info('Server is devoid of data.')

    logger.info('[give] complete.')

    # if start:
    #     end = datetime.datetime.now(datetime.UTC).timestamp()
    #     proc_time = end - start
    #     if proc_time > 60:
    #         mins = proc_time / 60
    #         logger.info(f"Total processing time [in minutes] for rosa [give]: {mins:.3f}.")
    #     else:
    #         logger.info(f"Total processing time [in seconds] for rosa [give]: {proc_time:.3f}.")

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
    # init_logger()
    # logging.info('Rosa [give] executed.')
    # start = datetime.datetime.now(datetime.UTC).timestamp()
    # if start:
    #     logging.info('[give] timer started.')
    main()
    # if start:
    #     end = datetime.datetime.now(datetime.UTC).timestamp()
    #     proc_time = end - start
    #     if proc_time > 60:
    #         mins = proc_time / 60
    #         logging.info(f"Total processing time [in minutes] for rosa [give]: {mins:.3f}.")
    #     else:
    #         logging.info(f"Total processing time [in seconds] for rosa [give]: {proc_time:.3f}.")
