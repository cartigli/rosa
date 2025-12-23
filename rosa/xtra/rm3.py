#!/usr/bin/env python3
import sys
import time
import shutil
import logging
import subprocess
import contextlib
import mysql.connector
from pathlib import Path

import xxhash
# import datetime

from rosa.confs.config import *

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

logger = logging.getLogger('rosa.log')

def main(args=None):
    local_dir = LOCAL_DIR

    print(f"{RED}[rm3] executed{RESET}")
    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    abs_path = Path(local_dir).resolve()
    hasher = xxhash.xxh64()

    item_no = 0
    dir_no = 0

    # try:
    if abs_path.exists():
        for item in abs_path.rglob('*'):
            path_str = item.resolve().as_posix()
            if any(blocked in path_str for blocked in blk_list):
                continue # skip item if blkd item in path
            else:
                # # counts files
                if item.is_file():
                    item_no += 1

                # # removes empty directories
                # if item.is_dir():
                #     for file in item.glob('*'):
                #         files = 0
                #         if file.is_file():
                #             files += 1
                #         else:
                #             continue
                #     if files == 0:
                #         shutil.rmtree(item)

                # hash alter-er [1 in 777] & file delete-r [1 in 8]
                if item.is_file():
                    item_no += 1
                    # if item_no % 107 == 0:
                    #     with open(item, 'a', encoding='utf-8') as f:
                    #         f.write("hello, world")

                    # remover
                    if item_no % 101 == 0: 
                        item.unlink()
                        print(f"{RED}deleted a file{RESET}")
                    # renamer
                    elif item_no % 103 == 0:
                        # item.rename("pandas")
                        # p = item.parent.parent
                        # new_title = p / "pandas.txt"
                        # item.rename(new_title)
                        item.touch()

                # # removes remote files
                # if item.is_file():
                #     item_no += 1
                #     if item_no % 127 == 0:
                #         if item.exists():
                #             frp = item.relative_to(abs_path).as_posix()
                #             # q = f"DELETE FROM notes WHERE frp = ({frp});"
                #             q = "DELETE FROM notes WHERE frp = %s;"
                #             with phones() as conn:
                #                 with conn.cursor() as cursor:
                #                     try:
                #                         # cursor.execute(q)
                #                         cursor.execute(q, (frp,))
                #                         conn.commit()
                #                         print(f"{RED}DELETED_FILE_REMOTE{RESET}")
                #                     except:
                #                         print('prolly already gone from server :(')
                #                     else:
                #                         printrosa (f"{RED}DELETED_FILE_REMOTE{RESET}")

                # removes 1 in 5 directories
                # if item.is_dir():
                #     dir_no += 1
                #     if dir_no % 5 == 0:
                #         shutil.rmtree(item)
                #         print(f"{RED}deleted one directory{RESET}")

                # if item.is_dir():
                #     dir_no += 1
                #     if dir_no % 317 == 0:
                #         # item.rename(str(item_no))
                #         dirp = item.parent
                #         subprocess.run(["mv", f"{item}", f"{dirp}/{item_no}" ])

                #         # shutil.rmtree(item)
                #         print(f"{RED}deleted one directory{RESET}")
                else:
                    continue

    else:
        logger.info('Local directory does not exist; repair the config or run "rosa get all".')
        sys.exit(0)
        # return raw_hell, hell_dirs, abs_path
    # avg_sz = tsz / cnt

    logger.info('Collected local paths and hashes.')
    print(item_no)
    # return raw_hell, hell_dirs, abs_path, avg_sz

if __name__=="__main__":
    logger = logging.getLogger('rosa.log')
    main()