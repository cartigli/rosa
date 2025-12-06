import os
import sys
import time
import shutil
import logging
import tempfile
# import hashlib
import subprocess
import contextlib
from pathlib import Path
from itertools import batched

# these three are the only external packages required
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import xxhash # this one is optional and can be replaced with hashlib which is more secure & in the native python library
import mysql.connector # to connect with the mysql server - helps prevent injection while building queries as well
# from mysql.connector impor
# import zstandard as zstd # compressor for files before uploading and decompressing after download

from rosa.configurables.queries import ASSESS2
from rosa.configurables.config import LOGGING_LEVEL, LOCAL_DIR, XCONFIG, MAX_ALLOWED_PACKET, RED, GREEN, YELLOW, RESET

"""
logger and connection management, recovery, and initiation
"""

# logging / er

logger = logging.getLogger('rosa.log')

def init_logger(logging_level):
	if logging_level:
		file_ = Path(__file__)
		log_dest = file_.parent.parent / "rosa.log"
		
		# init loggers
		logger = logging.getLogger('rosa.log')
		logger.setLevel(logging.DEBUG)

		logger_mysql = logging.getLogger('mysql.connector')
		logger_mysql.setLevel(logging.DEBUG)

		# clear thei handlers if present
		if logger.hasHandlers():
			logger.handlers.clear()
		
		if logger_mysql.hasHandlers():
			logger_mysql.handlers.clear()

		# init handlers
		file_handler = logging.FileHandler(log_dest, mode='a')
		file_handler.setLevel(logging.DEBUG)
	
		console_handler = logging.StreamHandler()
		console_handler.setLevel(logging_level.upper())

		mysql_console_handler = logging.StreamHandler()
		mysql_console_handler.setLevel(logging_level.upper())

		# define formatting - file loggers share format
		mysql_cons = "[%(levelname)s][%(name)s]: %(message)s"
		console_ = "[%(levelname)s][%(module)s:%(lineno)s]: %(message)s"
		file_ = "[%(asctime)s][%(levelname)s][%(module)s:%(lineno)s]: %(message)s"

		file_format = logging.Formatter(file_)
		console_format = logging.Formatter(console_)

		mysql_console_format = logging.Formatter(mysql_cons)

		# apply formatting
		file_handler.setFormatter(file_format)
		console_handler.setFormatter(console_format)

		mysql_console_handler.setFormatter(mysql_console_format)

		# add handlers to loggers
		logger.addHandler(file_handler)
		logger.addHandler(console_handler)

		logger_mysql.addHandler(file_handler)
		logger_mysql.addHandler(mysql_console_handler)

		logger.propagate = False
		logger_mysql.propagate = False

		return logger
	else:
		logger.warning("logger not passed; maybe config isn't configured?")
		sys.exit(1)

def doit_urself():
	cd = Path(__file__)
	rosa = cd.parent.parent
	rosa_log = rosa / "rosa.log"
	rosa_records = rosa / "_rosa_records"

	rosasz = os.path.getsize(rosa_log)
	rosakb = rosasz / 1024

	rosa_records_max = 5

	if rosakb >= 64.0:
		if rosa_records.exists():
			if rosa_records.is_file():
				logger.error(f"there is a file named rosa_records where a logging record should be; abandoning")
			elif rosa_records.is_dir():
				npriors = 0
				previous = []
				for file_ in sorted(rosa_records.glob('*')):
					if file_.is_file():
						previous.append(file_)
						npriors += 1

				if npriors > rosa_records_max:
					difference = npriors - rosa_records_max
					wanted = previous[difference:(rosa_records_max + difference)]

					for unwanted in previous:
						if unwanted not in wanted:
							unwanted.unlink()
							logger.debug(f"rosa_records reached capacity; oldest record deleted (curr. max log files recorded: {rosa_records_max} | curr. max log file sz: {rosakb})")
				else:
					ctime = f"{time.time():.2f}"
					subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

					logger.debug('backed up & replaced rosa.log')
		else:
			rosa_records.mkdir(parents=True, exist_ok=True)
			ctime = f"{time.time():.2f}"
			subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

			logger.debug('backed up & replaced rosa.log')
	else:
		logger.info('rosa.log: [ok]')

def mini_ps(args, nomix): # mini_parser for arguments/flags passed to the scripts
	force = False # no checks - force
	prints = False # no prints - prints

	if args:
		if args.force:
			force = True

		if args.silent:
			logging_level= "critical".upper()
			logger = init_logger(logging_level)
		elif args.verbose: # can't do verbose & silent
			logging_level = "debug".upper()
			logger = init_logger(logging_level)
			prints = True
		else:
			logger = init_logger(LOGGING_LEVEL.upper())

	else:
		logger = init_logger(LOGGING_LEVEL.upper())
	
	start = time.perf_counter()

	logger.debug(f"[rosa]{nomix} executed & timer started")
	return logger, force, prints, start


# connection & management thereof


@contextlib.contextmanager
def phones():
	"""Context manager for the mysql.connector connection object."""
	conn = None
	logger.debug('...phone call, connecting...')
	try:
		conn = init_conn(XCONFIG['user'], XCONFIG['pswd'], XCONFIG['name'], XCONFIG['addr'])

		if conn.is_connected():
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
		_safety(conn)
	except (ConnectionRefusedError, TimeoutError, Exception) as e:
		logger.error(f"error encountered while connecting to the server:{RESET} {e}.", exc_info=True)
		_safety(conn)
	except:
		logger.critical('uncaught exception found by phones; abandoning & rolling back')
		_safety(conn)
	else:
		logger.debug('phones executed w.o exception')
	finally:
		if conn:
			if conn.is_connected():
				conn.close()
				logger.info('phones closed conn [finally]')

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
	}

	try:
		conn = mysql.connector.connect(**config)
	except:
		raise
	else:
		return conn

def _safety(conn):
	"""Handles rollback of the server on err from phone_duty."""
	logger.warning('_safety called to rollback server due to err')

	if conn and conn.is_connected():
		try:
			conn.rollback()
		except ConnectionRefusedError as cre:
			logger.error(f"{RED}_safety failed due to connection being refused:{RESET} {cre}")
			sys.exit(3)
		except:
			logger.error(f"{RED}_safety failed; abandoning{RESET}")
			if conn:
				conn.ping(reconnect=True, attempts=3, delay=1)
				if conn and conn.is_connected():
					conn.rollback()
					logger.warning('conn is connnected & server rolled back (after caught exception & reconnection)')

				else:
					logger.warning('could not ping server; abandoning')
					sys.exit(1)
			else:
				logger.warning('conn object completely lost; abandoning')
				sys.exit(1)
		else:
			logger.warning('_safety recovered w.o exception')
	else:
		logger.warning('couldn\'t rollback due to faulty connection; abandoning')
		sys.exit(1)
		