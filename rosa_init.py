import os
import sys
import logging
import datetime
from contextlib import closing

import mysql.connector

from config import *
from queries import *
from rosa_lib import phone_duty, confirm, init_conn

# f_handler = logging.FileHandler('rosa.log', mode='a')
# f_handler.setLevel(logging.DEBUG)

# cons_handler = logging.StreamHandler()
# # cons_handler.setLevel(logging.INFO)
# cons_handler.setLevel(LOGGING_LEVEL.upper())

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[f_handler, cons_handler]
# ) # DEBUG, INFO, WARNING, ERROR, CRITICAL

"""
Helper for executing the queries for:
    Dealing with tables:
        - Creating tables
        - Truncating data from all tables
        - Dropping all tables
    Dealing with triggers:
        - Creating triggers
        - Dropping triggers
        - Replacing tables
Asks to user to confirm before committing to the server, just like rosa_give.
"""

def table_helper(conn):
    with conn.cursor() as cursor:
        cursor.execute(T_CHECK)
        qtables = cursor.fetchall()

    if qtables:
        logging.info('Obtained table data from db.')
        decis = input(f"Found these tables: {qtables} in the db; do you want to [t] truncate data, [d] drop tables, or [p] pass? ")
        
        if decis in ('t', 'T', 'truncate', 'Truncate', 'TRUNCATE'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(TRUNC)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logging.error(f"Connection Error encountered while attempting to truncate tables: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logging.info('Tables truncated.')

        elif decis in ('d', 'D', 'drop', 'Drop', 'DROP'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(DROP)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logging.error(f"Connection Error encountered while attempting to drop tables: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logging.info('Tables dropped.')
        else:
            logging.info('No selection made.')
            print('Moving on.')
            pass
    else:
        decis1 = input(f"Found no tables in the db; would you like to [i] iniate the db or [p] pass? ")
        
        if decis1 in ('i', 'I', 'init', 'Init', 'INIT', 'initiate', 'Initiate', 'INITIATE'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(INITIATION)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logging.error(f"Connection Error encountered while attempting to initiate database: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logging.info('Created tables.')
        else:
            logging.info('No selection made.')
            print('For sure.')


def trigger_helper(conn):
    with conn.cursor() as cursor:
        cursor.execute(TRIG_CHECK)
        trigs = cursor.fetchall()
        
        if trigs:
            decis2 = input(f"Found triggers: {trigs}. [r] Replace, [e] erase, or [p] pass? ")
            
            if decis2 in ('r', 'R', 'replace', 'Replace', 'REPLACE'):
                try:
                    for x in trigs:
                        f = f"DROP TRIGGER {x[0]}"
                        cursor.execute(f)

                except ConnectionError as c:
                    logging.error(f"Connection Error encountered while attempting to replace triggers: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    cursor.execute(EDIT_TRIGGER)
                    cursor.execute(DELETE_TRIGGER)
                    logging.info('Dropped & replaced triggers.')
            
            elif decis2 in ('e', 'E', 'erase', 'Erase', 'ERASE'):
                try:
                    for x in trigs:
                        f = f"DROP TRIGGER {x[0]}"
                        cursor.execute(f)

                except ConnectionError as c:
                    logging.error(f"Connection Error encountered while attempting to erase triggers: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logging.info('Dropped triggers.')

            else:
                logging.info('No selection made.')
                print('Cool.')

        else:
            decis3 = input('Found no triggers; would you like to create them? y/n: ')

            if decis3 in ('y', 'Y', 'yes', 'Yes', 'YES', 'yeah', 'i guess', 'I guess', 'i suppose', 'I suppose'):
                try:
                    cursor.execute(EDIT_TRIGGER)
                    cursor.execute(DELETE_TRIGGER)

                except ConnectionError as c:
                    logging.error(f"Connection Error encountered while attempting to create triggers: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logging.info('Triggers created.')

            elif decis3 in ('n', 'N', 'no', 'No', 'NO', 'nope', 'hell no', 'naw'):
                print('Got it.')
            else:
                print('Couldn\'t catch that.')


# if __name__=="__main__":
def main():
    logging.info('Rosa [init] executed.')
    start = datetime.datetime.now(datetime.UTC).timestamp()
    logging.info('Timer started.')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            table_helper(conn)
            trigger_helper(conn)
        
            confirm(conn)
            logging.info('Decision made, and relayed to the server.')

        except (mysql.connector.Error, ConnectionError, Exception) as e:
            logging.error(f"Exception encountered while initiating server: {e}.", exc_info=True)
            raise
        else:
            logging.info('Initiation faced no exceptions.')

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start

        logging.info(f"Processing time for rosa [init]: {proc_time}.")

    logging.info('[init] complete.')
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
    init_logger()
    main()