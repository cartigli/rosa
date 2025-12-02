#!/usr/bin/env python3
import time
import datetime

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.queries import *
    from rosa.abilities.lib import phone_duty, confirm, init_conn, init_logger, mini_ps

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

def table_helper(conn, logger):
    # logger = log()

    with conn.cursor() as cursor:
        cursor.execute(T_CHECK)
        qtables = cursor.fetchall()

    if qtables:
        logger.info('obtained table data from db.')
        decis = input(f"found these tables: {qtables} in the db; do you want to [t] truncate data, [d] drop tables, or [p] pass? ").lower()
        if decis in ('t', 'trunc', 'truncate'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(TRUNC)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to truncate tables:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('tables truncated')

        elif decis in ('d', 'drop'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(DROP)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to drop tables:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('tables dropped')
        else:
            logger.info('no selection made')
            pass
    else:
        decis1 = input(f"found no tables in the db; would you like to [i] iniate the db or [p] pass? ").lower()
        
        if decis1 in ('i', 'init', 'initiate'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(INITIATION)

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to initiate database:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('created tables')
        else:
            logger.info('no selection made')


def trigger_helper(conn, logger):
    with conn.cursor() as cursor:
        cursor.execute(TRIG_CHECK)
        trigs = cursor.fetchall()
        
        if trigs:
            decis2 = input(f"found triggers: {trigs}. [r] replace, [e] erase, or [p] pass? ").lower()
            if decis2 in ('r', 'replace'):
                try:
                    for x in trigs:
                        f = f"DROP TRIGGER {x[0]}"
                        cursor.execute(f)

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to replace triggers:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    cursor.execute(EDIT_TRIGGER)
                    cursor.execute(DELETE_TRIGGER)
                    logger.info('dropped & replaced triggers')
            
            elif decis2 in ('e', 'erase'):
                try:
                    for x in trigs:
                        f = f"DROP TRIGGER {x[0]}"
                        cursor.execute(f)

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to erase triggers:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('dropped triggers')

            else:
                logger.info('no selection made')

        else:
            decis3 = input('found no triggers; would you like to create them? y/n: ').lower()
            if decis3 in ('y', 'yes', 'yeah', 'i guess', 'i suppose'):
                try:
                    cursor.execute(EDIT_TRIGGER)
                    cursor.execute(DELETE_TRIGGER)

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to create triggers:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('triggers created')

            elif decis3 in ('n', 'no', 'nope', 'hell no', 'naw'):
                logger.info('got it')
            else:
                logger.info('couldn\'t catch that')


def force_initiation(conn, logger):
    # logger = log()
    init_cmds = [INITIATION, EDIT_TRIGGER, DELETE_TRIGGER]

    with conn.cursor() as cursor:
        try:
            for cmd in init_cmds:
                cursor.execute(cmd)
        except (ConnectionError, TimeoutError, Exception) as e:
            raise
        else:
            try:
                conn.commit()
            except:
                raise
            else:
                logger.info('forced database initiation w.o exception')


def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)

    logger.info('rosa [init] executed')

    start = time.perf_counter()
    if start:
        logger.info('[init] timer started')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        if force:
            try:
                force_initiation(conn)
            except (ConnectionError, TimeoutError, Exception) as e:
                raise
        elif not force:
            try:
                table_helper(conn, logger)
                trigger_helper(conn, logger)
            
                confirm(conn)
                logger.info('decision made, and relayed to the server')

            except (ConnectionError, Exception) as e:
                logger.error(f"{RED}exception encountered while initiating server:{RESET} {e}", exc_info=True)
                raise
            else:
                logger.info('initiation faced no exceptions')

    logger.info('[init] complete')

    if start:
        end = time.perf_counter()
        proc_time = end - start
        if proc_time > 60:
            mins = proc_time / 60
            logger.info(f"total processing time [in minutes] for rosa [init]: {mins:.3f}")
        else:
            logger.info(f"total processing time [in seconds] for rosa [init]: {proc_time:.3f}")
    if prints:
        print('All set.')


if __name__=="__main__":
    from config import *
    from queries import *
    from lib import phone_duty, confirm, init_conn, init_logger

    main(args=None)