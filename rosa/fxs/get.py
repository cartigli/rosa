#!/usr/bin/env python3
import sys
import time
from pathlib import Path

from rosa.confs import *
from rosa.lib import diffr, phones, calc_batch, fat_boy, download_batches5, mk_rrdir, save_people, mini_ps, counter, finale

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.

Cherubs and souls are both taken out of dictionary format; serpents are ignored & deleted from backup upon succesful write.
Gates are taken out of dictionary format, caves are ignored.
Stags & ledeux are not formatted after returning.
"""

NOMIC = "[get]"

def main(args=None):
    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        data, diff = diffr(conn)

    if diff is True:
        file_data, dir_data = data

        cherubs, souls, stags, serpents = file_data

        gates, caves, ledeux = dir_data

        # cherubs = data[0][0]
        # souls = data[0][1]
        # stags = data[0][2]
        # serpents = data[0][3]

        # gates = data[1][0]
        # caves = data[1][1]
        # ledeux = data[1][2]
        try:
            with phones() as conn:
                batch_size, row_size = calc_batch(conn)
                logger.info('batch size returned')
                with fat_boy(LOCAL_DIR) as (tmp_, backup): # context manager inside phones() so it handles errors first
                    try: # all the changes made on the disk are inside this try catch block - if outside, data's 'safe'
                        if ledeux: # deal with local file data first; then make directory[s], then make new and altered files
                            logger.info('copying orignal directory structure from unchanged directory[s]')
                            mk_rrdir(ledeux, tmp_)

                        if stags: # neither stags nor ledeux are given as dicts; ok
                            logger.info('hard-linking unchanged files from backup to tmp_')
                            save_people(stags, backup, tmp_) # hard link unchanged files - fast[er]

                        # if serpents:
                            # files only found locally can be ignored; they will remain in the backup dir
                            # and be deleted when the atomic wr completes w.o exception

                        if gates: # gates removes dict formatting, serpents are ignored, and stags/ledeux never get dictionary format regardless
                            logger.info('writing new directory[s] to disk')
                            # gates_ = [gate['frp'] for gate in gates]

                            mk_rrdir(gates, tmp_) # write directory heirarchy to tmp_ directory
                            # logger.info('new directory[s] [gates] written to disk')

                        # if caves:
                        #     logger.debug(f"Ignoring local-only directory[s] [caves].")

                        if cherubs: # all the files use lists and no dictionaries here
                            logger.info('pulling cherubs...')
                            # cherubs_ = [cherub['frp'] for cherub in cherubs]

                            download_batches5(cherubs, conn, batch_size, row_size, tmp_)
                            # handles pulling new file data, giving it batch by batch

                        if souls:
                            logger.info('pulling souls...')
                            # souls_ = [soul['frp'] for soul in souls]

                            download_batches5(souls, conn, batch_size, row_size, tmp_)
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