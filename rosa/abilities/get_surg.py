#!/usr/bin/env python3
import sys
import time

if __name__!="__main__":
    from rosa.abilities.lib import (scope_loc, hash_loc, 
        scope_rem, ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, fat_boy, mini_ps,
        save_people, mk_rrdir, mk_dir, phone_duty
    )

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)

    logger.info('rosa [get] executed')

    start = time.perf_counter()
    if start:
        logger.info('rosa [get] timer started')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn: # context manager for peace of mind
        try: # this will catch all exceptions and just dip; all other keyboard interrupts are caught and handled; this avoids extraneous server rollback
            raw_heaven = scope_rem(conn) # raw remote files & hash_id's
            heaven_dirs = ping_cass(conn) # raw remote dirs' rpath's

            heaven_data = [raw_heaven, heaven_dirs]
            if heaven_data[0] or heaven_data[0]: # remote hashes & relative paths
                logger.info('Data returned from heaven; checking out local directory')

                raw_paths, hell_dirs, abs_path = scope_loc(LOCAL_DIR)
                raw_hell = hash_loc(raw_paths, abs_path)

                logger.info('contrasting and comparing')
                cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # new, old, unchanged, changed file[s]
                f_delta = [cherubs, serpents, souls] # f_delta = altered file data

                gates, caves, ledeux = compare(heaven_dirs, hell_dirs) # new directory[s], old directory[s]
                d_delta = [gates, caves] # altered directory data

                if any(f_delta) or any(d_delta): # if file or folder data has been changed, continue to processing
                    logger.info('discrepancies found; proceeding to processing')

                    altered = len(cherubs) + len(serpents) + len(souls)
                    un_altered = len(stags) + len(ledeux)
                    total = altered + un_altered
                    ratio = (total - altered) / total * 100

                    logger.info(f"{ratio:.4f}% unaltered files in {abs_path}.")
                    if ratio >= 80:
                        # dumb fat boy replacement
                        batch_size, row_size = calc_batch(conn)
                        patient = abs_path.resolve()

                        try: # all the changes are made inside this try catch block
                            if any(d_delta):

                                if gates:
                                    # logger.debug(f"{len(gates)} remote-only directories found.")
                                    lgates = [gate['frp'] for gate in gates]
                                    mk_dir(lgates, patient) # write directory heirarchy to tmp_ directory
                                    logger.info('new directories [gates] written to disk')

                                # if caves:
                                #     logger.debug(f"Ignoring local-only directories [caves].")

                            if any(f_delta):
                                if cherubs:
                                    logger.info('pulling cherubs')
                                    cherubs_ = [cherub['frp'] for cherub in cherubs]
                                    download_batches5(cherubs_, conn, batch_size, row_size, patient)
                                    # handles pulling new file data, giving it batch by batch
                                    # to the wr batches function, and continuing until list is empty

                                if souls:
                                    logger.info('pulling souls')
                                    souls_ = [soul['frp'] for soul in souls]
                                    download_batches5(souls_, conn, batch_size, row_size, patient)
                                    # same here as w.cherubs but for altered file[s] (hash discrepancies)

                        except KeyboardInterrupt as ko:
                            logger.warning('boss killed it; wrap it up')
                            raise
                        except (PermissionError, Exception) as e:
                            logger.error('exception encountered while attempting atomic write', exc_info=True)
                            raise

                    else:
                        logger.info('ratio of altered files too great; run "rosa get"')
            else:
                logger.info('server is devoid of data')

        except KeyboardInterrupt as ko:
            conn.close()
            logger.warning('no edits have been made; irish goodbye')
            sys.exit(0)

    if start:
        end = time.perf_counter()
        proc_time = end - start 
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"processing time [in minutes] for rosa [get] [surgic]: {min_time:.4f}.")
        else:
            logger.info(f"processing time [in seconds] for rosa [get] [surgic]: {proc_time:.4f}.")

    logger.info('[get] [surgic] completed')

    if prints:
        print('All set.')


if __name__=="__main__":
    from lib import (scope_loc, hash_loc, scope_rem, 
        ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, fat_boy, 
        save_people, mk_rrdir, mk_dir, phone_duty, mini_ps
    )
    main(args=None)