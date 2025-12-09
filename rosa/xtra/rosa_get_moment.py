#!/usr/bin/env python3
import time
import logging
from pathlib import Path

if __name__!="__main__":
    from rosa.guts.config import *
    from rosa.guts.queries import SNAP
    from rosa.guts.lib import fat_boy, calc_batch, download_batches2, phone_duty, init_logger, mini_ps

"""
Downloads the servers contents from a given moment using recorded UTC timestamps.
"""


def download_batches2(flist, conn, batch_size, tmp_): # get
	"""Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
	dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
	This function passes the found data to the wr_data function, which writes the new data structure to the disk.
	"""
	paths = [item[0] for item in flist]
	params = ', '.join(['%s']* len(paths))

	offset = 0

	with conn.cursor() as cursor:
		try:
			while True:
				query = f"SELECT frp, content FROM notes WHERE frp IN ({params}) LIMIT {batch_size} OFFSET {offset};"

				try:
					cursor.execute(query, paths)
					batch = cursor.fetchall()

				except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
					logger.warning(f"error while trying to download data: {c}.", exc_info=True)
					raise
				else:
					if batch:
						wr_batches(batch, tmp_)

					if len(batch) < batch_size:
						break

					offset += batch_size

		except: # tout de monde
			logger.critical(f"{RED}err while attempting batched atomic write{RESET}", exc_info=True)
			raise


def get_snap(conn, SNAP):
    """Collect the data from the given moment of the server's data."""
    logger = logging.getLogger()

    with conn.cursor() as cursor:
        logger.info('Getting a date to grab a snapshot from.')
        moment = input(f"Please provide a time to get the state of your server from that moment (ex: [1999-12-31 24:60:60]): ")

        moments = (moment,)*6 # need to parse variable into query 6 individual times
        try:
            cursor.execute(SNAP, moments)

        except (ConnectionError, Exception) as c:
            logger.error(f"{RED}exception encountered while attempting to get moment:{RESET} {c}.")
            raise
        else:
            logger.info('Executed query to get moment.')
            snapshot = cursor.fetchall()

            if snapshot:
                logger.info('Collected snapshot of database.')
                return snapshot
            else:
                logger.warning('No data returned from query. Did your notes exist at that time?')


def get_dest(LOCAL_DIR):
    """Ask the user for a path to write the moment's contents to. Default is LOCAL_DIR from config."""
    logger = logging.getLogger()

    snap_dest = Path( LOCAL_DIR / 'moment' )
    upath = input(f"Where do you want this written to? The default is: {snap_dest} and will overwrite your files. If you want another location, enter here: ")
    if upath:
        logger.info('User provided a new path.')
        return Path(upath).resolve()
    else:
        logger.info(f"User did not provide a path; {snap_dest} is the chosen destination.")
        try:
            snap_dest.mkdir(parents=True, exist_ok=True)
        except:
            raise
        else:
            return snap_dest


def main(args):
    logger, force, prints, start = mini_ps(args, NOMIC)

    # logger.info('Rosa [get] [moment] executed.')
    # start = time.perf_counter()
    # if start:
    #     logger.info('[get] [moment] timer started.')

    abs_path = Path(LOCAL_DIR).resolve()

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        logger.info('conn is connected; pinging heaven...')
        try:
            snapshot = get_snap(conn, SNAP)
            if snapshot:
                if not force:
                    upath = get_dest(LOCAL_DIR)
                if force is True:
                    upath = Path(abs_path / 'moment').resolve()

                with fat_boy(upath) as (tmp_, backup):
                    batch_size = calc_batch(conn)

                    if batch_size:
                        logger.info(f"Found batch size: {batch_size}.")
                    try:
                        download_batches2(snapshot, conn, batch_size, tmp_)

                    except (PermissionError, FileNotFoundError, Exception) as e:
                        logger.error(f"{RED}exception encountered while attempting atomic wr for [get] [moment]:{RESET} {e}.", exc_info=True)
                        raise
                    else:
                        logger.info('No exceptions caught during atomic wr for [get] [moment].')
                
                logger.info('Fat boy closed & cleaned.')
            
            else:
                logger.warning('No data returned from moment; do records exist at this time?')
        
        except (ConnectionError, Exception) as e:
            logger.error(f"{RED}error encountered while obtaining moment:{RESET} {e}.", exc_info=True)
            raise
        else:
            logger.info('[moment] main function complete without exception.')

    if start:
        end = time.perf_counter()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"Processing time [in minutes] for rosa [get] [moment]: {min_time:.4f}.")
        else:
            logger.info(f"Processing time [in seconds] for rosa [get] [moment]: {proc_time:.4f}.")

    logger.info('[get] [moment] complete.')

    if prints is True:
        print('All set.')


if __name__=="__main__":
    from config import *
    from queries import SNAP
    from lib import fat_boy, calc_batch, download_batches2, phone_duty, init_logger, mini_ps

    main(args=None)