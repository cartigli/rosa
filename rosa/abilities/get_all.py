#!/usr/bin/env python3
import sys
import time
import shutil
from pathlib import Path

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import (scope_loc, scope_rem,
        ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, mini_ps,
        fat_boy, save_people,
        mk_rdir, phone_duty
    )

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

def tmper(tmp_, force, logger):
    if force:
        try:
            shutil.rmtree(tmp_)
        except e:
            try:
                logger.info('5 sec pause before retry recursive delete; standby')
            except:
                logger.warning('Retry unsucessful')
                sys.exit(1)
            else:
                logger.info('Retry sucessful')
        else:
            if tmp_.exists():
                logger.debug('Gah fucking lee; tmp_ is still there, I quit')
                raise
            else:
                logger.info(f"{tmp_} deleted")
    if tmp_.exists():
        decis = input(f"{tmp_} was started, but not finished. Keep the half-baked directory? y/n: ").lower()
        if decis0 in ('yes', 'y', ' y', 'y ', 'ye', 'yeah', 'sure'):
            try:
                shutil.rmtree(tmp_)
            except e:
                try:
                    logger.info('5 sec pause before retry recursive delete; standby')
                except:
                    logger.warning('retry unsucessful')
                    sys.exit(1)
                else:
                    logger.info('retry sucessful')
            else:
                if tmp_.exists():
                    logger.debug('gah fucking lee; tmp_ is still there, I quit')
                    raise
                else:
                    logger.info(f"{tmp_} deleted")

        elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
            logger.info('heard; won\'t touch it')
            pass

def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)
    force
    logger.info('rosa [get] executed')

    start = time.perf_counter()
    if start:
        logger.info('[get] [all] timer started')
    try:
        abs_path = Path( LOCAL_DIR ).resolve()

        if abs_path.exists():
            if not force:
                if abs_path.is_dir():
                    dwarn = input(f"A folder already exists @{abs_path}. Unless specified now with 'n', it will be written over. Return to continue. decision: ").lower()
                    if dwarn in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
                        logger.warning('abandoning rosa [get] [all]')
                        sys.exit(0)
                    else:
                        shutil.rmtree(abs_path)
                        logger.warning(f"{abs_path} was deleted from your disk.")
                else:
                    fwarn = input(f"a file already exists @{abs_path}; unless specified now with 'n', it will be written over. Return to continue. Decision: ").lower()
                    if fwarn in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
                        logger.warning('abandoning rosa [get] [all]')
                        sys.exit(0)
                    else:
                        abs_path.unlink()
                        logger.warning(f"{abs_path} was deleted from your disk.")
            elif force:
                logger.info(f"staying silent & deleting {abs_path}.")
                if abs_path.is_dir():
                    shutil.rm_tree(abs_path)
                    if abs_path.exists():
                        logger.info('shutil failed; retrying in 5 seconds')
                        time.sleep(5)
                        shutil.rmtree(abs_path)
                        if abs_path.exists():
                            logger.error(f"could not delete {abs_path}.")
                            sys.exit(1)
                    else:
                        logger.info(f"deleted {abs_path} silenetly.")
                if abs_path.is_file():
                    abs_path.unlink()
                    if abs_path.exists():
                        logger.info(f"couldn't delete {abs_path}.")
                        sys.exit(1)

        with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
            logger.info('conn is connected')
            raw_heaven = scope_rem(conn) # raw remote files & hash_id's
            logger.info('raw heaven returned')
            souls = [s[0] for s in raw_heaven]

            heaven_dirs = ping_cass(conn) # raw remote dirs' rpath's
            logger.info('heavenly dirs returned')
            soul_dirs = [h[0] for h in heaven_dirs]

            if soul_dirs or heaven_dirs: # remote hashes & relative paths
                logger.info(f"data was returned from heaven; going directly to processing...")

                tmp_ = abs_path.resolve()
                batch_size, row_size = calc_batch(conn)
                logger.info('optimal batch size returned')
                # try:
                if soul_dirs:
                    logger.info('...downloading directories...')
                    mk_rdir(soul_dirs, tmp_) # write directory heirarchy to tmp_ directory
                    logger.info('new directories written to disk')

                if souls:
                    logger.info('...downloading files...')
                    download_batches5(souls, conn, batch_size, row_size, tmp_)
                    logger.info('new files written to disk')
            
            else:
                logger.info('server is devoid of data')
                sys.exit(0)

        logger.info('[get] [all] completed')

        if start:
            end = time.perf_counter()
            proc_time = end - start 
            if proc_time > 60:
                min_time = proc_time / 60
                logger.info(f"processing time [in minutes] for rosa [get] [all]: {min_time:.4f}.")
            else:
                logger.info(f"processing time [in seconds] for rosa [get] [all]: {proc_time:.4f}.")

    except KeyboardInterrupt as ko:
        logger.warning('boss killed it; wrap it up')
        tmper(tmp_, force, logger)
        sys.exit(0)
    except Exception as e:
        logger.warning(f"exception encountered during [get] [all] [main]: {e}.", exc_info=True)
        tmper(tmp_, force, logger)
        sys.exit(1)
    else:
        if prints:
            print('All set.')


if __name__=="__main__":
    from config import *
    from lib import (scope_loc, scope_rem, mini_ps,
        ping_cass, contrast, compare, init_logger,
        calc_batch, download_batches5, fat_boy, 
        save_people, mk_rdir, phone_duty
    )

    main(args=None)