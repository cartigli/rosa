#!/usr/bin/env python3
import sys
import time
from pathlib import Path

if __name__=="__main__":
    cd = Path(__file__).resolve().parent.parent
    if str(cd) not in sys.path:
        sys.path.insert(0, str(cd))

from rosa.configurables.config import *

from rosa.guts.analyst import diffr
from rosa.guts.dispatch import phones, _safety
from rosa.guts.contractor import scope_sz
from rosa.guts.technician import collect_info, collect_data, upload_created, upload_edited, upload_dirs, confirm, counter, rm_remfile, rm_remdir

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

NOMIX = "[give]"

def main(args=None):
    data, diff, mini = diffr(args, NOMIX)

    logger = mini[0]
    force = mini[1]
    prints = mini[2]
    start = mini[3]

    if diff is True:

        cherubs = data[0][0]
        souls = data[0][1]
        stags = data[0][2]
        serpents = data[0][3]

        gates = data[1][0]
        caves = data[1][1]
        ledeux = data[1][2]

        batch_sz = scope_sz(LOCAL_DIR)

        with phones() as conn:
            try:
                if gates:
                    logger.info('removing remote-only directory[s] from server...')
                    rm_remdir(conn, gates) # delete remote-only[s] from server
                    # logger.info('removed directory[s]')

                if caves: 
                    # when uploading to server, order of when to upload new directory[s] is not as sensitive 
                    # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                    logger.info('uploading local-only directory[s] to server...')
                    upload_dirs(conn, caves) # upload local-only[s] to server
                    # logger.info('directory[s] uploaded')

                if cherubs:
                    logger.info('removing remote-only file[s]...')
                    rm_remfile(conn, cherubs) # delete remote-only file[s]
                    # logger.info('removed file[s]')

                if souls:
                    # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                    logger.info('uploading altered file[s] to the server...')
                    souls_ = [item['frp'] for item in souls]

                    soul_batches = collect_info(souls_, LOCAL_DIR) # returns batches
                    logger.info('formatted batches; uploading iteratively...')

                    for batch in soul_batches:
                        soul_data = collect_data(batch, LOCAL_DIR, conn)
                        if soul_data:
                            try:
                                upload_edited(conn, soul_data)
                            except:
                                raise

                if serpents:
                    # twin to souls upload block
                    logger.info('uploading serpents to the server...')
                    serpents_ = [item['frp'] for item in serpents]

                    serpent_batches = collect_info(serpents_, LOCAL_DIR)
                    logger.info('formatted batches; uploading iteratively...')

                    for batch in serpent_batches:
                        serpent_data = collect_data(batch, LOCAL_DIR, conn)
                        if serpent_data:
                            try:
                                upload_created(conn, serpent_data)
                            except:
                                raise

            except KeyboardInterrupt as ko:
                logger.warning('going to try manual rollback for this one; standby')
                _safety(conn)
                sys.exit(0)
            else:
                counter(start, NOMIX)

                confirm(conn, force)

    logger.info('[give] complete')

    counter(start, NOMIX)

    if prints is True:
        print('All set.')


if __name__=="__main__":
    main()