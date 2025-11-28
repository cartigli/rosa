#!/usr/bin/env python3
import sys
import time
import logging
from pathlib import Path
# import datetime

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

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
        if not heaven_data[0]: # functionally usless; need to implement logic for when heaven is deserted
            # print(heaven_data)
            logger.info('Heaven\'s empty.')

            local_dir = LOCAL_DIR

            blk_list = ['.DS_Store', '.git', '.obsidian'] 
            abs_path = Path(local_dir).resolve()
            # hasher = xxhash.xxh64()

            serpents = []
            caves = []

            if abs_path.exists():
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
                            # logger.info(f"Recorded path for directory: {item.name}.")
                        else:
                            continue
            else:
                logger.info('Local directory does not exist; nothing to give.')
                sys.exit(0)

            logger.info('Collected local paths.')

            start = time.perf_counter()
            if start:
                logger.info('[give] timer started.')

            try:
                if caves: 
                    upload_dirs(conn, caves) # upload local-only[s] to server
                    logger.info('Local-only directories uploaded.')

                if serpents:
                    serpents_ = sorted(serpents, key=str.lower)

                    # for batch in collect_info2(serpents_, abs_path): # alt method
                    #     beg = time.perf_counter()
                    #     upload_created(conn, batch)
                    #     end = time.perf_counter()
                    #     logging.info(f"Wrote batch to serer in {(end - beg):.4f} seconds.")

                    # serpent_batches = collect_info(serpents_, abs_path # original method
                    # for batch in serpent_batches:

                    with logging_redirect_tqdm(loggers=[logger]): # tqdm method
                        # with tqdm(serpent_batches, unit="batches", leave=False) as pbar:
                        with tqdm(collect_info(serpents_, abs_path), unit="batches", leave=False) as pbar:
                            for batch in pbar:
                                # bg = time.perf_counter()
                                serpent_data = collect_data(batch, abs_path, conn)

                                if serpent_data:
                                    upload_created(conn, serpent_data)
                                    # pbar.update(1)
                                    # fnl = time.perf_counter()
                                    # logging.info(f"Wrote batch to server in {(fnl - bg):.4f} seconds.")

                if start:
                    end = time.perf_counter()
                    proc_time = end - start
                    if proc_time > 60:
                        min_time = proc_time / 60
                        logger.info(f"Upload time [in minutes] for rosa [give]: {min_time:.3f}.")
                    else:
                        logger.info(f"Upload time [in seconds] for rosa [give]: {proc_time:.3f}.")

            except ConnectionError as c:
                logger.critical('Exception encountered while attempting to upload data.', exc_info=True)
                sys.exit(1)
            except KeyboardInterrupt as k:
                logger.warning('Boss killed it; aborting.')
                sys.exit(1)
            else:
                confirm(conn)
                logger.info('Uploaded data to server.')

        else: # if server is empty, let us know
            logger.info('Server contains data; truncate tables before attempting again. Aborting [give] [all].')

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
    init_logger()
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
