#!/usr/bin/env python3
import logging
import datetime

from rosa.abilities.config import *
from rosa.abilities.queries import SNAP
from rosa.abilities.lib import fat_boy, calc_batch, download_batches2, phone_duty #, init_conn #, wr_data4

"""
Downloads the servers contents from a given moment using recorded UTC timestamps.
"""

logger = logging.getLogger(__name__)

def get_snap(conn, SNAP):
    """
    Collect the data from the given moment of the server's data.
    """
    # logger = logging.getLogger(__name__)

    with conn.cursor() as cursor:
        logger.info('Getting a date to grab a snapshot from.')
        moment = input(f"Please provide a time to get the state of your server from that moment (ex: [1999-12-31 24:60:60]): ")

        moments = (moment,)*6 # need to parse variable into query 6 individual times
        try:
            cursor.execute(SNAP, moments)

        except (ConnectionError, Exception) as c:
            logger.error(f"Exception encountered while attempting to get moment: {c}.")
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
    # logger = logging.getLogger(__name__)

    snap_dest = Path(LOCAL_DIR).resolve()
    upath = input(f"Where do you want this written to? The default is: {snap_dest} and will overwrite your files. If you want another location, enter here: ")
    if upath:
        logger.info('User provided a new path.')
        return Path(upath).resolve()
    else:
        logger.info(f"User did not provide a path; {snap_dest} is the chosen destination.")
        return snap_dest


# if __name__ == "__main__":
def main():
    # logger = logger.getLogger(__name__)

    logger.info('Rosa [get] [moment] executed.')

    start = datetime.datetime.now(datetime.UTC).timestamp()
    if start:
        logger.info('[get] [moment] timer started.')

    abs_path = Path(LOCAL_DIR).resolve()

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            snapshot = get_snap(conn, SNAP)
            if snapshot:
                upath = get_dest(LOCAL_DIR)
                with fat_boy(upath) as (tmp_, backup):
                    batch_size = calc_batch(conn)

                    if batch_size:
                        logger.info(f"Found batch size: {batch_size}.")
                    try:
                        download_batches2(snapshot, conn, batch_size, tmp_)

                    except (PermissionError, FileNotFoundError, Exception) as e:
                        logger.error(f"Exception encountered while attempting atomic wr for [get] [moment]: {e}.", exc_info=True)
                        raise
                    else:
                        logger.info('No exceptions caught during atomic wr for [get] [moment].')
                
                logger.info('Fat boy closed & cleaned.')
            
            else:
                logger.warning('No data returned from moment; do records exist at this time?')
        
        # except (mysql.connector.Error, ConnectionError, Exception) as e:
        except (ConnectionError, Exception) as e:
            logger.error(f"Error encountered while obtaining moment: {e}.", exc_info=True)
            raise
        else:
            logger.info('[moment] main function complete without exception.')
    
    logger.info('[get] [moment] complete.')

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"Processing time [in minutes] for rosa [get] [moment]: {min_time:.4f}.")
        else:
            logger.info(f"Processing time [in seconds] for rosa [get] [moment]: {proc_time:.4f}.")

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
    )


if __name__=="__main__":
    init_logger()
    # logging.info('Rosa [get] [moment] executed.')
    # start = datetime.datetime.now(datetime.UTC).timestamp()
    # if start:
    #     logging.info('[get] [moment] timer started.')
    main()
    # if start:
    #     end = datetime.datetime.now(datetime.UTC).timestamp()
    #     proc_time = end - start 
    #     if proc_time > 60:
    #         min_time = proc_time / 60
    #         logging.info(f"Processing time [in minutes] for rosa [get] [moment]: {min_time:.4f}.")
    #     else:
    #         logging.info(f"Processing time [in seconds] for rosa [get] [moment]: {proc_time:.4f}.")