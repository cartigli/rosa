#!/usr/bin/env python3
import sys
import time
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm as tqdm_

# if __name__=="__main__":
#     cd = Path(__file__).resolve().parent.parent
#     if str(cd) not in sys.path:
#         sys.path.insert(0, str(cd))

from rosa.confs.config import *

from rosa.lib.analyst import diffr
from rosa.lib.opps import counter, mini_ps, finale
from rosa.lib.dispatch import phones, confirm
from rosa.lib.technician import collect_info, collect_data, upload_created, upload_edited, upload_dirs, rm_remfile, rm_remdir

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

NOMIC = "[give]"


def main(args=None):
    logger, force, prints, start = mini_ps(args, NOMIC)

    data, diff = diffr()
    if diff is True:

        cherubs = data[0][0]
        souls = data[0][1]
        stags = data[0][2]
        serpents = data[0][3]

        gates = data[1][0]
        caves = data[1][1]
        ledeux = data[1][2]

        with phones() as conn:
            # try: # phones() already catches this KeyboardInterrupt
            if gates:
                logger.info('removing remote-only directory[s] from server...')
                rm_remdir(conn, gates) # delete remote-only[s] from server

            if caves:
                # when uploading to server, order of when to upload new directory[s] is not as sensitive 
                # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                logger.info('uploading local-only directory[s] to server...')
                upload_dirs(conn, caves) # upload local-only[s] to server

            if cherubs:
                logger.info('removing remote-only file[s]...')
                rm_remfile(conn, cherubs) # delete remote-only file[s]

            if souls:
                # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                logger.info('uploading altered file[s] to the server...')
                souls_ = [item['frp'] for item in souls]

                with tqdm_(loggers=[logger]):
                    with tqdm(collect_info(souls_, LOCAL_DIR)) as pbar:
                        for batch in pbar:
                            soul_data = collect_data(batch, LOCAL_DIR)
                            upload_edited(conn, soul_data)

            if serpents:
                # twin to souls upload block
                logger.info('uploading serpents to the server...')
                serpents_ = [item['frp'] for item in serpents]

                with tqdm_(loggers=[logger]):
                    with tqdm(collect_info(serpents_, LOCAL_DIR)) as pbar:
                        for batch in pbar:
                            serpent_data = collect_data(batch, LOCAL_DIR)
                            upload_created(conn, serpent_data)

            counter(start, NOMIC) # one before confirm so user input doesn't get timed

            confirm(conn, force)

    finale(NOMIC, start, prints) # and one after for the final assessment 

if __name__=="__main__":
    main()