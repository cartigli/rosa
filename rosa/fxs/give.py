#!/usr/bin/env python3
"""Upload local state to the database.

Upload local-only files/directories, 
delete remote-only files/directories, 
and update altered files. 
Abandon if the server or local directory are empty.
"""

import sys
import time
from pathlib import Path

from rosa.confs import *
from rosa.lib import (
    diffr, phones, upload_dirs, 
    rm_remfile, rm_remdir, confirm, 
    mini_ps, counter, finale, collector
)

NOMIC = "[give]"

def main(args=None):
    """Forces the local state onto the server. 
    
    Uploads new and altered files to the server. 
    Removes files/directories not found locally.
    Quits if server or local directory is empty.
    """
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        data, diff = diffr(conn)

    if diff is True:
        file_data, dir_data = data

        cherubs, souls, stags, serpents = file_data
        gates, caves, ledeux = dir_data

        with phones() as conn:
            if gates:
                logger.info('removing remote-only directory[s] from server...')
                rm_remdir(conn, gates) # delete remote-only[s] from server

            if caves:
                logger.info('uploading local-only directory[s] to server...')
                upload_dirs(conn, caves) # upload local-only[s] to server

            if cherubs: # now this file uses all lists for uploading files, no dictionaries
                logger.info('removing remote-only file[s]...')

                cherub_params = [(cherub,) for cherub in cherubs]
                rm_remfile(conn, cherub_params) # delete remote-only file[s]

            if souls:
                key = "altered_file"
                # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                logger.info('uploading altered file[s] to the server...')
                collector(conn, souls, LOCAL_DIR, key) # REVISED; OFFLOADED

            if serpents:
                key = "new_file"
                # twin to souls upload block
                logger.info('uploading serpents to the server...')
                collector(conn, serpents, LOCAL_DIR, key)

            counter(start, NOMIC) # one before confirm so user input doesn't get timed

            confirm(conn, force)

    finale(NOMIC, start, prints) # and one after for the final assessment 

if __name__=="__main__":
    main()