#!/usr/bin/env python3
import sys
import time
from pathlib import Path

if __name__!="__main__":
    from rosa.abilities.config import LOCAL_DIR
    from rosa.abilities.lib import (scope_loc, hash_loc, 
        scope_rem, ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, fat_boy, mini_ps,
        save_people, mk_rrdir, mk_dir, phones, diffr
    )

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

NOMIC = "[get][surgic]"

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
            altered = len(cherubs) + len(serpents) + len(souls)
            un_altered = len(stags) + len(ledeux)
            total = altered + un_altered
            ratio = (total - altered) / total * 100

            logger.info(f"{ratio:.4f}% unaltered files in {LOCAL_DIR}")
            if ratio >= 80:
                with phones() as conn:
                    # dumb fat boy replacement
                    batch_size, row_size = calc_batch(conn)
                    patient = Path(LOCAL_DIR).resolve()

                    try: # all the changes are made inside this try catch block
                        if gates:
                            # logger.debug(f"{len(gates)} remote-only directories found.")
                            lgates = [gate['frp'] for gate in gates]
                            mk_dir(lgates, patient) # write directory heirarchy to tmp_ directory
                            logger.info('new directories [gates] written to disk')

                        # if caves:
                        #     logger.debug(f"Ignoring local-only directories [caves].")

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
                        logger.error(f"{RED}exception encountered while attempting atomic write{RESET}", exc_info=True)
                        raise

            else:
                logger.info('ratio of altered files too great; run "rosa get"')

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