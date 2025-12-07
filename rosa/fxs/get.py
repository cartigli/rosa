#!/usr/bin/env python3
import sys
import time
from pathlib import Path

# if __name__=="__main__":
#     cd = Path(__file__).resolve().parent.parent
#     if str(cd) not in sys.path:
#         sys.path.insert(0, str(cd))

from rosa.confs.config import *

from rosa.lib.analyst import diffr
from rosa.lib.dispatch import phones
from rosa.lib.opps import counter, finale, mini_ps
from rosa.lib.contractor import fat_boy, download_batches5, mk_rrdir, save_people, calc_batch

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

NOMIC = "[get]"

def main(args=None):
    logger, force, prints, start = mini_ps(args, NOMIC)

    data, diff = diffr()
    # logger = mini[0]
    # force = mini[1]
    # prints = mini[2]
    # start = mini[3]
    if diff is True:
        cherubs = data[0][0]
        souls = data[0][1]
        stags = data[0][2]
        serpents = data[0][3]

        gates = data[1][0]
        caves = data[1][1]
        ledeux = data[1][2]
        try:
            with phones() as conn:
                batch_size, row_size = calc_batch(conn)
                logger.info('batch size returned')
                with fat_boy(LOCAL_DIR) as (tmp_, backup): # context manager [atomic]
                    try: # all the changes made on the disk are inside this try catch block - if outside, data's 'safe'
                        # deal with local file data first; then make directory[s], then make new and altered files
                        if ledeux:
                            logger.info('copying orignal directory structure from unchanged directory[s]')
                            mk_rrdir(ledeux, tmp_)

                        if stags:
                            logger.info('hard-linking unchanged files from backup to tmp_')
                            save_people(stags, backup, tmp_) # hard link unchanged files - fast[er]

                        # if serpents:
                            # files only found locally can be ignored; they will remain in the backup dir
                            # and be deleted when the atomic wr completes w.o exception

                        if gates:
                            logger.info('writing new directory[s] to disk')

                            gates_ = [gate['frp'] for gate in gates]

                            mk_rrdir(gates_, tmp_) # write directory heirarchy to tmp_ directory
                            # logger.info('new directory[s] [gates] written to disk')

                        # if caves:
                        #     logger.debug(f"Ignoring local-only directory[s] [caves].")

                        if cherubs:
                            logger.info('pulling cherubs...')
                            cherubs_ = [cherub['frp'] for cherub in cherubs]
                            download_batches5(cherubs_, conn, batch_size, row_size, tmp_)
                            # handles pulling new file data, giving it batch by batch

                        if souls:
                            logger.info('pulling souls...')
                            souls_ = [soul['frp'] for soul in souls]
                            download_batches5(souls_, conn, batch_size, row_size, tmp_)
                            # same here as w.cherubs but for altered file[s] (hash discrepancies)

                    except KeyboardInterrupt as ko:
                        logger.warning('boss killed it; wrap it up')
                        raise
                    except (PermissionError, Exception) as e:
                        raise
                    else:
                        logger.info('atomic downlaod & write completed without exception')
                
                logger.info('fat boy closed')
            
            logger.debug('hung up phone')

        except KeyboardInterrupt as ko:
            conn.close()
            logger.warning('no edits have been made; irish goodbye')
            sys.exit(0)
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()