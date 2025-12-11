#!/usr/bin/env python3
"""Interacting with and initiating the database.

Can create, truncate, or drop tables. 
It can also create, drop, or replace triggers.
Queries server for tables/triggers, asks user what to do.
Can create and initiate the index? / 
No, should wipe index if user wipes tables, 
delete it if user drops tables, 
and create it if they make tables.
"""

import sys
import time
import logging
from pathlib import Path

from rosa.confs import *
from rosa.lib import (
    phones, confirm, 
    mini_ps, finale
)

logger = logging.getLogger('rosa.log')

NOMIC = "[init]"

def table_helper(conn, force=False):
    """Asks the user what they would like to do about the databases' tables based on the returns from the assessment queries. 

    If none are returned, asks to initiate the database with tables (y/n). 
    If tables are returned, asks to [t] truncate, [d] drop, or [p] pass.

    Args:
        conn: Connection object.
        force (=False): Using --force (-f), skips the ask & attempts to initiate. Default is False.
    
    Returns:
        None
    """
    if force is True:
        return

    with conn.cursor() as cursor:
        cursor.execute(T_CHECK) # find all the tables currently in the server
        qtables = cursor.fetchall()

    if qtables:
        logger.info('obtained table data from db.')
        decis = input(f"found these tables: {qtables} in the db; do you want to [t] truncate data, [d] drop tables, or [p] pass? ").lower()
        if decis in ('t', ' t', 't ', 'trunc', 'truncate'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(TRUNC) # truncate every table, one by one

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to truncate tables:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('tables truncated')

        elif decis in ('d', ' d', 'd ', 'drop'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(DROP) # drop each table, one by one

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
        
        if decis1 in ('i', ' i', 'i ', 'init', 'initiate'):
            with conn.cursor() as cursor:
                try:
                    cursor.execute(INITIATION) # create each table, one by one

                    while cursor.nextset():
                        pass

                except ConnectionError as c:
                    logger.error(f"{RED}conn err encountered while attempting to initiate database:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                    raise
                else:
                    logger.info('created tables')
        else:
            logger.info('no selection made')

def trigger_helper(conn, force=False):
    """Asks the user what they would like to do about the databases' triggers based on the returns from the assessment queries. 
    
    If none returned, asks to create them (y/n). 
    If triggers are returned, asks to [r] replace, [d] drop, or [p] pass.

    Args:
        conn: Connection object.
        force (=False): Using --force (-f), skips the ask. Default is False.
    
    Returns:
        None
    """
    if force is True:
        return
    else:
        with conn.cursor() as cursor:
            cursor.execute(TRIG_CHECK)
            trigs = cursor.fetchall()
            
            if trigs:
                decis2 = input(f"found triggers: {trigs}. [r] replace, [d] drop, or [p] pass? ").lower()
                if decis2 in ('r', ' r', 'r ', 'replace'):
                    try:
                        for x in trigs:
                            f = f"DROP TRIGGER {x[0]}" # drop each trigger (f-string in query; yikes)
                            cursor.execute(f)

                    except ConnectionError as c:
                        logger.error(f"{RED}conn err encountered while attempting to replace triggers:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                        raise
                    else:
                        cursor.execute(EDIT_TRIGGER) # make the edit trigger...
                        cursor.execute(DELETE_TRIGGER) # make the delete trigger
                        logger.info('dropped & replaced triggers')
                
                elif decis2 in ('d', ' d', 'd ', 'drop'):
                    try:
                        for x in trigs:
                            f = f"DROP TRIGGER {x[0]}" # same action here as replace but no second step
                            cursor.execute(f)

                    except ConnectionError as c:
                        logger.error(f"{RED}conn err encountered while attempting to erase triggers:{RESET} {c}. {RED}abandoning init{RESET}", exc_info=True)
                        raise
                    else:
                        logger.info('dropped triggers')

                else:
                    logger.info('no selection made')

            else:
                decis3 = input('found no triggers; would you like to create them? y/n: ').lower()
                if decis3 in ('y', ' y', 'y ', 'yes', 'yeah', 'i guess', 'i suppose'):
                    try:
                        cursor.execute(EDIT_TRIGGER) # no triggers found, so making them
                        cursor.execute(DELETE_TRIGGER)

                    except ConnectionError as c:
                        logger.error(f"{RED}conn err encountered while attempting to create triggers:{RESET} {c}. {RED}rolling back{RESET}", exc_info=True)
                        raise
                    else:
                        logger.info('triggers created')

                elif decis3 in ('n', ' n', 'n ', 'no', 'nope', 'hell no', 'naw'):
                    logger.info('got it') # do nothing
                else:
                    logger.info('couldn\'t catch that')

def force_initiation(conn, force=False):
    """Forced initiation of the database (not recommended). 
    
    Running 'rosa init -f' makes this block attempt to make the tables and triggers. 
    The queries contain 'IF NOT EXISTS' so it is fairly safe, but not recommended.

    Args:
        conn: Connection object.
        force (=False): Using --force (-f), skips the ask. Default is False.
    
    Returns:
        None
    """
    if force is True:
        force_init_cmds = [INITIATION, EDIT_TRIGGER, DELETE_TRIGGER]

        with conn.cursor() as cursor:
            try:
                for cmd in force_init_cmds:
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
    else:
        return

def main(args=None):
    """Main function for initiating the database. 
    
    Assesses its current state, asks choice to make, truncate, or drop tables.
    The same query and response is done for the triggers.
        If nothing in server, asks if user wants to initiate (y/n).

        If there are tables, choices are [t] truncate, [d] drop, or [p] pass.
        If there are triggers, choices are [r] replace, [d] drop, or [p] pass.
    """
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        try:
            table_helper(conn, force)
            trigger_helper(conn, force)

            force_initiation(conn, force)
        
            confirm(conn, force)
            logger.info('decision made and relayed to the server')

        except (ConnectionError, Exception) as e:
            logger.error(f"{RED}exception encountered while initiating server:{RESET} {e}", exc_info=True)
            raise
        else:
            logger.info('initiation faced no exceptions')
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()