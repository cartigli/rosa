#!/usr/bin/env python3
import sys
import time
import logging
import shutil
import mysql.connector
from pathlib import Path
import contextlib
import subprocess
import xxhash
# import datetime

from rosa.confs.config import *

LOCAL_DIR = "/Volumes/HomeXx/compuir/texts11"

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

# logger = logging.getLogger('rosa.log')

XCONFIG = os.getenv('XCONFIG',
            {
    'user': 'root',
    'pswd': 'koipo222',
    'name': 'notation',
    'addr': 'local_host'
    }
)

@contextlib.contextmanager
def phones():
	"""Context manager for the mysql.connector connection object."""
	conn = None
	logger.debug('...phone_duty() called; connecting...')
	try:
		conn = init_conn(XCONFIG['user'], XCONFIG['pswd'], XCONFIG['name'], XCONFIG['addr'])
		# if conn and conn.is_connected():
		if conn.is_connected():
			stats = conn.cmd_statistics()
			# if conn.connected:
			#     print('conn.connected')
			logger.debug(f"cmd_statistics: {stats} (connection object yielded to main)")
			yield conn
		else:
			logger.warning('connection object lost')
			try:
				conn.ping(reconnect=True, attempts=3, delay=1)
				if conn.is_connected():
					logger.info('connection obj. recovered after failed connect')
					yield conn
				else:
					logger.warning('reconnection failed; abandoning')
					sys.exit(1)
			except:
				raise
			else:
				logger.info('connection obj. recovered w.o exception [after exception was caught]')

	except KeyboardInterrupt as ko:
		logger.error('boss killed it; wrap it up')
		# _safety(conn)
	except (ConnectionRefusedError, TimeoutError, Exception) as e:
		logger.error(f"error encountered while connecting to the server:{RESET} {e}.", exc_info=True)
		# _safety(conn)
	else:
		logger.debug('phone_duty() executed w.o exception')
	finally:
		if conn:
			if conn.is_connected():
				conn.close()
				logger.info('phone_duty() closed conn [finally]')


def init_conn(db_user, db_pswd, db_name, db_addr): # used by all scripts
	"""Initiate the connection to the server. If an error occurs, [freak out] raise."""
	config = {
		'unix_socket': '/tmp/mysql.sock',
		# 'host': db_addr,
		'user': db_user,
		'password': db_pswd,
		'database': db_name,
		'autocommit': False,
		# 'use_pure': False # 5.886, 5.902, 5.903 | not worth the lib
		'use_pure': True # 5.122, 5.117, 5.113 | seems faster regardless
		# 'use_unicode': False # 3.213 (after use_pure: 3.266)
		# 'pool_size': 5
		# 'raw': True
	}
	try:
		conn = mysql.connector.connect(**config)
	except:
		raise
	else:
		logger.info("connection object initialized w.o exception")
		return conn


def main():
    local_dir = LOCAL_DIR

    print(f"{RED}[rm3] executed{RESET}")
    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    abs_path = Path(local_dir).resolve()
    hasher = xxhash.xxh64()

    item_no = 0
    dir_no = 0

    try:
        if abs_path.exists():
            with phones() as conn:
                for item in abs_path.rglob('*'):
                    path_str = item.resolve().as_posix()
                    if any(blocked in path_str for blocked in blk_list):
                        continue # skip item if blkd item in path
                    else:
                        # # counts files
                        # if item.is_file():
                        #     item_no += 1

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
                            if item_no % 137 == 0:
                                content = item.read_bytes()
                                hasher.reset()
                                hasher.update(content)
                                before = hasher.digest()
                                with open(item, 'a', encoding='utf-8') as f:
                                    f.write("hello, world")
                                a_content = item.read_bytes()
                                hasher.reset()
                                hasher.update(a_content)
                                after = hasher.digest()
                                if before == after:
                                    print("WE FUCKD UP")
                                else:
                                    print(f"{RED}we fuckd that one up (good){RESET}")
                            elif item_no % 4 == 0: # deletes abt 98% of files in a 17300 ish file directory
                                item.unlink()
                                print(f"{RED}deleted a file{RESET}")

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
                        #         print('Deleted one directory.')
                        # else:
                        #     continue

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
    print(item_no)
    # return raw_hell, hell_dirs, abs_path, avg_sz

if __name__=="__main__":
    logger = logging.getLogger('rosa.log')
    main()