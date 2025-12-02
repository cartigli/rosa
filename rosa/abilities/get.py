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
        phone_duty, mini_ps
    )

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)

    try:
        logger.info('[get] executed')

        start = time.perf_counter()
        if start:
            logger.info('[get] timer started')

        with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn: # context manager for peace of mind
            logger.info('conn is connected; pinging heaven...')
            try: # this will catch all exceptions and just dip; all other keyboard interrupts are caught and handled; this avoids extraneous server rollback
                raw_heaven = scope_rem(conn) # raw remote files & hash_id's
                logger.info('raw heaven returned')
                heaven_dirs = ping_cass(conn) # raw remote dirs' rpath's
                logger.info('heavenly directory[s] returned')

                if raw_heaven or heaven_dirs:
                    logger.info(f"data was returned from heaven; collecting info from {LOCAL_DIR}...")

                    raw_paths, hell_dirs, abs_path = scope_loc(LOCAL_DIR)
                    if any(raw_paths) or any(hell_dirs):
                        logger.info('data was returned from local directory; hashing file[s] found...')
                    raw_hell = hash_loc(raw_paths, abs_path)

                    logger.info('found files hashed; proceeding to compare & contrast...')

                    cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # new, old, unchanged, changed file[s]
                    logger.info('files contrasted')
                    f_delta = [cherubs, serpents, souls] # f_delta = altered file data

                    gates, caves, ledeux = compare(heaven_dirs, hell_dirs) # new directory[s], old directory[s]
                    logger.info('directories compared')
                    d_delta = [gates, caves] # altered directory data

                    if any(f_delta) or any(d_delta): # if file or folder data has been changed, continue to processing
                        logger.info('discrepancies identified; proceeding to processing...')
                        batch_size, row_size = calc_batch(conn)
                        logger.info('optimal batch size returned')
                        with fat_boy(abs_path) as (tmp_, backup): # context manager [atomic]
                            try: # all the changes are made inside this try catch block
                                # deal with local file data first; then make directories, then make new and altered files
                                if ledeux:
                                    logger.info('copying orignal directory structure from unchanged directories')
                                    beg = time.perf_counter()
                                    mk_rrdir(ledeux, tmp_)
                                    end = time.perf_counter()
                                    logger.info(f"wrote unchanged directories to tmp_ in {(end - beg):.4f}")

                                if stags:
                                    logger.info('hard-linking unchanged files from backup to tmp_')
                                    beg = time.perf_counter()
                                    save_people(stags, backup, tmp_) # hard link unchanged files - fast[er]
                                    end = time.perf_counter()
                                    logger.info(f"linked unchanged files [stags] to tmp_ dir in {(end - beg):.4f}")

                                # if serpents:
                                    # files only found locally can be ignored; they will remain in the backup dir
                                    # and be deleted when the atomic wr completes w.o exception

                                if any(d_delta):

                                    if gates:
                                        logger.info('writing new directories to disk')
                                        mk_dir(gates, tmp_) # write directory heirarchy to tmp_ directory
                                        logger.info('new directories [gates] written to disk')

                                    # if caves:
                                    #     logger.debug(f"Ignoring local-only directories [caves].")

                                if any(f_delta):
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
                    else:
                        logger.info('no discrepancies found')
                else:
                    logger.info('server is devoid of data')

            except KeyboardInterrupt as ko:
                conn.close()
                logger.warning('no edits have been made; irish goodbye')
                sys.exit(0)

        logger.info('[get] completed')

        if start:
            end = time.perf_counter()
            proc_time = end - start 
            if proc_time > 60:
                min_time = proc_time / 60
                logger.info(f"processing time [in minutes] for rosa [get]: {min_time:.4f}")
            else:
                logger.info(f"processing time [in seconds] for rosa [get]: {proc_time:.4f}")

    except KeyboardInterrupt as e:
        logger.warning('boss killed it; wrap it up')
        sys.exit(0)
    except (SystemError, Exception) as e:
        logger.error(f"{RED}error encountered with [get] [main]:{RESET} {e}", exc_info=True)
        sys.exit(1)
    else:
        if prints:
            print('All set.')


if __name__=="__main__":
    from config import *
    from lib import (scope_loc, hash_loc, mini_ps,
        scope_rem, ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, fat_boy, 
        save_people, mk_rrdir, mk_dir, phone_duty
    )

    main(args=None)