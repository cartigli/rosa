#!/usr/bin/env python3
import logging
import datetime

from rosa.abilities.config import *
from rosa.abilities.queries import *
from rosa.abilities.lib import phone_duty, confirm, init_conn


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
    logger = logging.getLogger(__name__)

    with conn.cursor() as cursor:
        cursor.execute(T_CHECK)
        qtables = cursor.fetchall()

    if qtables:
        logger.info('Obtained table data from db.')
        decis = input(f"Found these tables: {qtables} in the db; do you want to [t] truncate data, [d] drop tables, or [p] pass? ")
        
        if decis in ('t', 'T', 'truncate', 'Truncate', 'TRUNCATE'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(TRUNC)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"Connection Error encountered while attempting to truncate tables: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logger.info('Tables truncated.')

        elif decis in ('d', 'D', 'drop', 'Drop', 'DROP'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(DROP)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"Connection Error encountered while attempting to drop tables: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logger.info('Tables dropped.')
        else:
            logger.info('No selection made.')
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
                    logger.error(f"Connection Error encountered while attempting to initiate database: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logger.info('Created tables.')
        else:
            logger.info('No selection made.')
            print('For sure.')


def trigger_helper(conn):
    logger = logging.getLogger(__name__)

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
                    logger.error(f"Connection Error encountered while attempting to replace triggers: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    cursor.execute(EDIT_TRIGGER)
                    cursor.execute(DELETE_TRIGGER)
                    logger.info('Dropped & replaced triggers.')
            
            elif decis2 in ('e', 'E', 'erase', 'Erase', 'ERASE'):
                try:
                    for x in trigs:
                        f = f"DROP TRIGGER {x[0]}"
                        cursor.execute(f)

                except ConnectionError as c:
                    logger.error(f"Connection Error encountered while attempting to erase triggers: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logger.info('Dropped triggers.')

            else:
                logger.info('No selection made.')
                print('Cool.')

        else:
            decis3 = input('Found no triggers; would you like to create them? y/n: ')

            if decis3 in ('y', 'Y', 'yes', 'Yes', 'YES', 'yeah', 'i guess', 'I guess', 'i suppose', 'I suppose'):
                try:
                    cursor.execute(EDIT_TRIGGER)
                    cursor.execute(DELETE_TRIGGER)

                except ConnectionError as c:
                    logger.error(f"Connection Error encountered while attempting to create triggers: {c}. Rolling back.", exc_info=True)
                    raise
                else:
                    logger.info('Triggers created.')

            elif decis3 in ('n', 'N', 'no', 'No', 'NO', 'nope', 'hell no', 'naw'):
                print('Got it.')
            else:
                print('Couldn\'t catch that.')


def main():
    logger = logging.getLogger(__name__)
    logger.info('Rosa [init] executed.')

    start = datetime.datetime.now(datetime.UTC).timestamp()
    if start:
        logger.info('[init] timer started.')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            table_helper(conn)
            trigger_helper(conn)
        
            confirm(conn)
            logger.info('Decision made, and relayed to the server.')

        # except (mysql.connector.Error, ConnectionError, Exception) as e:
        except (ConnectionError, Exception) as e:
            logger.error(f"Exception encountered while initiating server: {e}.", exc_info=True)
            raise
        else:
            logger.info('Initiation faced no exceptions.')

    logger.info('[init] complete.')

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start
        if proc_time > 60:
            mins = proc_time / 60
            logger.info(f"Total processing time [in minutes] for rosa [init]: {mins:.3f}.")
        else:
            logger.info(f"Total processing time [in seconds] for rosa [init]: {proc_time:.3f}.")

    print('All set.')


def init_logger():
    f_handler = logging.FileHandler('rosa.log', mode='a')
    f_handler.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    cons_handler.setLevel(LOGGING_LEVEL.upper())

    logger.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[f_handler, cons_handler]
    ) # DEBUG, INFO, WARNING, ERROR, CRITICAL


if __name__=="__main__":
    init_logger()
    # logging.info('Rosa [init] executed.')

    # start = datetime.datetime.now(datetime.UTC).timestamp()
    # if start:
    #     logging.info('[init] timer started.')

    main()

    # if start:
    #     end = datetime.datetime.now(datetime.UTC).timestamp()
    #     proc_time = end - start
    #     if proc_time > 60:
    #         mins = proc_time / 60
    #         logging.info(f"Total processing time [in minutes] for rosa [init]: {mins:.3f}.")
    #     else:
    #         logging.info(f"Total processing time [in seconds] for rosa [init]: {proc_time:.3f}.")