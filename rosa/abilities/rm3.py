#!/usr/bin/env python3
import sys
import time
import logging
import shutil
from pathlib import Path
# import xxhash
# import datetime

from config import *
# from lib import(scope_loc, scope_rem, 
#     ping_cass, contrast, compare, rm_remdir, 
#     rm_remfile, collect_info, collect_info2, collect_data, 
#     upload_dirs, upload_created,
#     upload_edited, confirm, phone_duty
# )

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

logger = logging.getLogger(__name__)


def main():
    local_dir = LOCAL_DIR
    # logger.info(f"{"\x1b[31;1m"}Rosa [give] executed.")

    print(f"{"\x1b[31;1m"}[rm3] executed.")
    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    abs_path = Path(local_dir).resolve()

    item_no = 0
    dir_no = 0
    try:
        if abs_path.exists():
            for item in abs_path.rglob('*'):
                path_str = item.resolve().as_posix()
                if any(blocked in path_str for blocked in blk_list):
                    continue # skip item if blkd item in path
                else:
                    if item.is_file():
                        item_no += 1
                        if item_no % 3 == 0:
                            os.unlink(item)
                            print('Deleted one file.')
                    if item.is_dir():
                        dir_no += 1
                        if dir_no % 5 == 0:
                            shutil.rmtree(item)
                            print('Deleted one directory.')
                    else:
                        continue
        else:
            logger.info('Local directory does not exist; repair the config or run "rosa get all".')
            sys.exit(0)
            # return raw_hell, hell_dirs, abs_path

    except (KeyboardInterrupt, PermissionError) as e:
        logger.error(f"Encountered something while hashing local files: {e}. Aborting.", exc_info=True)
        # decis = input('Continue processing files or abort? (abort recommended) y/n: ')
        # if decis in ('a', 'A', ' a'):
        sys.exit(1)

    # avg_sz = tsz / cnt

    logger.info('Collected local paths and hashes.')
    # return raw_hell, hell_dirs, abs_path, avg_sz

if __name__=="__main__":
    logger = logging.getLogger(__name__)
    main()