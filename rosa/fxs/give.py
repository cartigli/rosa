#!/usr/bin/env python3
import sys
import time
from pathlib import Path

from rosa.confs import *
from rosa.lib import diffr, phones, collect_info, collect_data, upload_created, upload_edited, upload_dirs, rm_remfile, rm_remdir, confirm, mini_ps, counter, finale, _collector_

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
Takes serpents and souls out of dictionary format, but not cherubs.
Neither caves nor gates are taken out of their dictionaires.
Ignored stags and ledeux.
"""

NOMIC = "[give]"

def main(args=None):
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        data, diff = diffr(conn)

    if diff is True:
        file_data, dir_data = data

        cherubs, souls, stags, serpents = file_data
        gates, caves, ledeux = dir_data

        with phones() as conn:
            # try: # phones() already catches this KeyboardInterrupt
            if gates:
                logger.info('removing remote-only directory[s] from server...')

                # gates_ = [(item['drp'],) for item in gates]

                rm_remdir(conn, gates) # delete remote-only[s] from server

            if caves:
                # when uploading to server, order of when to upload new directory[s] is not as sensitive 
                # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                logger.info('uploading local-only directory[s] to server...')

                # caves_ = [(item['drp'],) for item in caves]

                upload_dirs(conn, caves) # upload local-only[s] to server


            if cherubs: # now this file uses all lists for uploading files, no dictionaries
                logger.info('removing remote-only file[s]...')

                cherub_params = [(cherub,) for cherub in cherubs]

                rm_remfile(conn, cherub_params) # delete remote-only file[s]

            if souls:
                key = "altered_file"
                # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                logger.info('uploading altered file[s] to the server...')

                _collector_(conn, souls, LOCAL_DIR, key) # REVISED; OFFLOADED

            if serpents:
                key = "new_file"
                # twin to souls upload block
                logger.info('uploading serpents to the server...')

                _collector_(conn, serpents, LOCAL_DIR, key)

            counter(start, NOMIC) # one before confirm so user input doesn't get timed

            confirm(conn, force)

    finale(NOMIC, start, prints) # and one after for the final assessment 

if __name__=="__main__":
    main()