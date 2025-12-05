#!/usr/bin/env python3
import sys
import time

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import (
        scope_loc, hash_loc, scope_rem, 
        ping_cass, contrast, compare, 
        init_logger, calc_batch, 
        download_batches5, fat_boy,
        save_people, mk_rrdir, mk_dir, 
        phones, diffr
    )

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

NOMIC = "[get]"

def main(args):
    x = False
    data, diff, start, mini, x = diffr(args, NOMIC, x)

    cherubs = data[0][0]
    souls = data[0][1]
    stags = data[0][2]
    serpents = data[0][3]

    gates = data[1][0]
    caves = data[1][1]
    ledeux = data[1][2]

    logger = mini[0]
    force = mini[1]
    prints = mini[2]

    if diff == True:
        try:
            with phones() as conn:
                batch_size, row_size = calc_batch(conn)
                logger.info('optimal batch size returned')
                with fat_boy(LOCAL_DIR) as (tmp_, backup): # context manager [atomic]
                    try: # all the changes are made inside this try catch block
                        # deal with local file data first; then make directory[s], then make new and altered files
                        if ledeux:
                            logger.info('copying orignal directory structure from unchanged directory[s]')
                            beg = time.perf_counter()
                            mk_rrdir(ledeux, tmp_)
                            end = time.perf_counter()
                            logger.info(f"wrote unchanged directory[s] to tmp_ in {(end - beg):.4f}")

                        if stags:
                            logger.info('hard-linking unchanged files from backup to tmp_')
                            beg = time.perf_counter()
                            save_people(stags, backup, tmp_) # hard link unchanged files - fast[er]
                            end = time.perf_counter()
                            logger.info(f"linked unchanged files [stags] to tmp_ dir in {(end - beg):.4f}")

                        # if serpents:
                            # files only found locally can be ignored; they will remain in the backup dir
                            # and be deleted when the atomic wr completes w.o exception

                        if gates:
                            logger.info('writing new directory[s] to disk')
                            mk_dir(gates, tmp_) # write directory heirarchy to tmp_ directory
                            logger.info('new directory[s] [gates] written to disk')

                        # if caves:
                        #     logger.debug(f"Ignoring local-only directory[s] [caves].")

                        if cherubs:
                            logger.info('pulling cherubs')
                            cherubs_ = [cherub['frp'] for cherub in cherubs]
                            download_batches5(cherubs_, conn, batch_size, row_size, tmp_)
                            # handles pulling new file data, giving it batch by batch
                            # to the wr batches function, and continuing until list is empty

                        if souls:
                            logger.info('pulling souls')
                            souls_ = [soul['frp'] for soul in souls]
                            download_batches5(souls_, conn, batch_size, row_size, tmp_)
                            # same here as w.cherubs but for altered file[s] (hash discrepancies)

                    except KeyboardInterrupt as ko:
                        logger.warning('boss killed it; wrap it up')
                        raise
                    except (PermissionError, Exception) as e:
                        logger.error(f"{RED}error encountered while attempting atomic write:{RESET} {e}", exc_info=True)
                        raise
                    else:
                        logger.info('atomic downlaod & write completed without exception')
                
                logger.info('fat boy closed')

        except KeyboardInterrupt as ko:
            conn.close()
            logger.warning('no edits have been made; irish goodbye')
            sys.exit(0)

    else:
        logger.info('no discrepancies found')

    logger.info('[get] completed')

    if start:
        end = time.perf_counter()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"processing time [in minutes] for rosa [get]: {min_time:.4f}")
        else:
            logger.info(f"processing time [in seconds] for rosa [get]: {proc_time:.4f}")

    else:
        if prints:
            print('All set.')


if __name__=="__main__":
    from config import *
    from lib import (scope_loc, hash_loc, mini_ps,
        scope_rem, ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, fat_boy, 
        save_people, mk_rrdir, mk_dir, phones, diffr
    )

    main(args=None)