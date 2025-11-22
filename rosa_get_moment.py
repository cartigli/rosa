import sys
import logging
# from config import *
# import mysql.connector
from pathlib import Path
from queries import SNAP
from contextlib import closing

import mysql.connector

from config import *
from rosa_lib import fat_boy, download_batches, phone_duty #, init_conn #, wr_data4

# f_handler = logging.FileHandler('rosa.log', mode='a')
# f_handler.setLevel(logging.DEBUG)

# cons_handler = logging.StreamHandler()
# cons_handler.setLevel(logging.ERROR)

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[f_handler, cons_handler]
# )

def get_snap(conn, SNAP):
    """
    Collect the data from the given moment of the server's data.
    """
    with conn.cursor() as cursor:
        # moments = {} # x

        logging.info('Getting a date to grab a snapshot from.')
        moment = input(f"Please provide a time to get the state of your server from that moment (ex: [1999-12-31 24:60:60]): ")

        # for i in range(6): # x
        #     secs['moment']=moment # x

        moments = (moment,)*6
        try:
            cursor.execute(SNAP, moments)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logging.error(f"Exception encountered while attempting to get moment: {c}.")
            raise
        else:
            logging.info('Executed query to get moment.')
            snapshot = cursor.fetchall()

            if snapshot:
                logging.info('Collected snapshot of database.')
                return snapshot
            else:
                logging.warning('No data returned from query. Did your notes exist at that time?')


def get_dest(LOCAL_DIR):
    """
    Ask the user for a moment to get a snapshot from; pass it forward to get_snap.
    """
    snap_dest = Path( LOCAL_DIR / snapshot ).resolve()
    upath = input(f"Where do you want this written to? The default is: {snap_dest} and will overwrite your files. If you want another location, enter here: ")
    if upath:
        logging.info('User provided a new path.')
        return Path(upath).resolve()
    else:
        logging.info(f"User did not provide a path; {snap_dest} is the chosen destination.")
        return snap_dest


# if __name__ == "__main__":
def main():
    logging.info('Rosa [get] [moment] executed.')
    start = datetime.datetime.now(datetime.UTC).timestamp()
    logging.info('Timer started.')

    abs_path = Path(LOCAL_DIR).resolve()

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            snapshot = get_snap(conn, SNAP)
            if snapshot:
                upath = get_dest(LOCAL_DIR)
                with fat_boy(upath) as (tmp_, backup):
                    try:
                        conn.ping(reconnect=True, attempts=3, delay=0.5)

                    except (mysql.connector.Error, ConnectionError, Exception) as e:
                        logging.error(f"Exception encountered while attempting atomic wr for [get] [moment]: {e}.", exc_info=True)
                        raise
                    else:
                        try:
                            download_batches(snapshot, tmp_)

                        except (PermissionError, FileNotFoundError, Exception) as e:
                            logging.error(f"Exception encountered while attempting atomic wr for [get] [moment]: {e}.", exc_info=True)
                            raise
                        else:
                            logging.info('No exceptions caught during atomic wr for [get] [moment].')
                
                logging.info('Fat boy closed & cleaned.')
            
            else:
                logging.warning('No data returned from moment; do records exist at this time?')
        
        except (mysql.connector.Error, ConnectionError, Exception) as e:
            logging.error(f"Error encountered while obtaining moment: {e}.", exc_info=True)
            raise
        else:
            logging.info('[moment] main function complete without exception.')

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start

        logging.info(f"Processing time for rosa [get] [moment]: {proc_time}.")
    
    logging.info('[get] [moment] complete.')
    print('All set.')


def init_logger():
    f_handler = logging.FileHandler('rosa.log', mode='a')
    f_handler.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    cons_handler.setLevel(logging.ERROR)

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[f_handler, cons_handler]
    )


if __name__=="__main__":
    init_logger()
    main()