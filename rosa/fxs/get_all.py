#!/usr/bin/env python3
import sys
import time
import shutil
import logging
from pathlib import Path

# if __name__=="__main__":
#     cd = Path(__file__).resolve().parent.parent
#     if str(cd) not in sys.path:
#         sys.path.insert(0, str(cd))

from rosa.confs.config import *

from rosa.lib.dispatch import phones
from rosa.lib.analyst import ping_rem, ping_cass
from rosa.lib.opps import mini_ps, counter, finale
from rosa.lib.contractor import calc_batch, download_batches5, sfat_boy, save_people, mk_rrdir

"""
Scan local directory, collect data from server, and compare all contents. Download/make/write all files not present but seen in 
server, download/write all hash discrepancies, and delete all files not found in the server. Make parent directories if needed & 
delete old ones.
"""

NOMIC = "[get][all]"

def log():
    logger = logging.getLogger('rosa.log')
    return logger

def rm_origin(abs_path, force=False):
    logger = log()

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
    logger, force, prints, start = mini_ps(args, NOMIC)

    abs_path = Path( LOCAL_DIR ).resolve()

    if abs_path.exists():
        rm_origin(abs_path, force)

    # with sfat_boy(abs_path) as _abs_path:

    with phones() as conn:
        with sfat_boy(abs_path) as _abs_path: # sfat_boy inside phones ensures sfat_boy catches errors before phones
            logger.info('conn is connected')
            raw_heaven = ping_rem(conn) # raw remote files & hash_id's

            logger.info('raw heaven returned')
            souls = {s[0] for s in raw_heaven}

            heavenly_dirs = ping_cass(conn) # raw remote dirs' rpath's
            logger.info('heavenly dirs returned')

            if any((souls, heavenly_dirs)): # remote hashes & relative paths
                logger.info(f"data was returned from heaven; proceeding to processing...")

                logger.info('getting batch size...')
                batch_size, row_size = calc_batch(conn)
                # logger.info('optimal batch size returned')

                if heavenly_dirs:
                    logger.info('...downloading directories...')
                    mk_rrdir(heavenly_dirs, _abs_path) # write directory heirarchy to abs_path directory

                if souls:
                    logger.info('...downloading files...')
                    download_batches5(souls, conn, batch_size, row_size, _abs_path)
            else:
                logger.info('server is devoid of data')
                sys.exit(0)

    finale(NOMIC, start, prints)
    # logger.info('[get] [all] completed')
    # counter(start, NOMIC)
    # if prints is True:
    #     print('All set.')

if __name__=="__main__":
    main()