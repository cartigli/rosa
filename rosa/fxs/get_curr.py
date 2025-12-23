#!/usr/bin/env python3
"""Retrieves the current version from the server.

Used to sync before another version can be uploaded.
If the local and remote versions do not match, i.e.,
there has been an update from another machine since you pulled,
you cannot give. Sync before trying to give.
"""

import sys
import time
import shutil
import logging
from pathlib import Path

# LOCAL_DIR used once (besides import)
from rosa.confs import LOCAL_DIR
from rosa.lib import (
    phones, calc_batch, 
    sfat_boy, mk_rrdir, mini_ps, finale
)

NOMIC = "[get][current]"

def rm_origin(abs_path, force=False):
    """Deletes the LOCAL_DIR if it exists AND EITHER A. The user confirms this or B. The command is run with --force (-f).

    Args:
        abs_path (Path): The LOCAL_DIR's full path.
        force (=False): Force flag, if present, is passed and skips the user's confirmation step.
    
    Returns:
        None
    """
    logger = logging.getLogger('rosa.log')

    if force is True:
        logger.info(f"staying silent & deleting {abs_path}.")
        if abs_path.is_dir():
            shutil.rmtree(abs_path)
            if abs_path.exists():
                logger.info('shutil failed; retrying in 5 seconds')
                time.sleep(5)
                shutil.rmtree(abs_path)
                if abs_path.exists():
                    logger.warning(f"{RED}could not delete {abs_path}.{RESET}")
                    sys.exit(1)
            else:
                logger.info(f"deleted {abs_path} silenetly.")
        if abs_path.is_file():
            abs_path.unlink()
            if abs_path.exists():
                logger.warning(f"couldn't delete {abs_path}.")
                sys.exit(1)
    else:
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


def main(args=None):
    """Syncs directory to latest version.""" # has no actual logging; need to add that
    logger, force, prints, start = mini_ps(args, NOMIC)

    abs_path = Path( LOCAL_DIR ).resolve()

    if not abs_path.exists():
        abs_path.mkdir()

    with phones() as conn:
        with sfat_boy(abs_path) as tmpd: # sfat_boy inside phones ensures sfat_boy catches errors before phones
            # wait, why the fuck am I using sfat_boy instead of fat_boy? This function needs to restore the given directory on failure.
            dquery = "SELECT rp FROM directories;"
            with conn.cursor(buffered=False) as cursor:
                cursor.execute(dquery)
                c_dirs = cursor.fetchall()
            
            mk_rrdir(c_dirs, tmpd)

            batch_size, row_size = calc_batch(conn)
            cquery = "SELECT rp, content FROM files;"

            with conn.cursor(buffered=False) as cursor:
                cursor.execute(cquery)

                while True:
                    fdata = cursor.fetchmany(batch_size)

                    if not fdata:
                        break

                    for rp, content in fdata:
                        fp = tmpd / rp
                        with open(fp, 'wb') as f:
                            f.write(content)

            # # logger.info('conn is connected')
            # raw_heaven = ping_rem(conn) # raw remote files & hash_id's

            # logger.info('raw heaven returned')

            # heavenly_dirs = ping_cass(conn) # raw remote dirs' rpath's
            # logger.info('heavenly dirs returned')

            # if any((raw_heaven, heavenly_dirs)): # remote hashes & relative paths
            #     logger.info(f"data was returned from heaven; proceeding to processing...")

            #     logger.info('getting batch size...')
            #     batch_size, row_size = calc_batch(conn)

            #     if heavenly_dirs:
            #         logger.info('...downloading directories...')
            #         mk_rrdir(heavenly_dirs, _abs_path) # write directory heirarchy to abs_path directory

            #     if raw_heaven:
            #         logger.info('...downloading files...')
            #         download_batches5(raw_heaven, conn, batch_size, row_size, _abs_path)
            # else:
            #     logger.info('server is devoid of data')
            #     sys.exit(0)

    finale(NOMIC, start, prints)

    logger.info('All set.')

if __name__=="__main__":
    main()