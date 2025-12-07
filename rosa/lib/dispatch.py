import sys
import time
import logging
import contextlib

import mysql.connector

from rosa.confs.config import XCONFIG, RED, RESET

"""
logger and connection management, recovery, and initiation.
This file could be made strictly for connections and error handling, moving logging to operations.
"""

# logging / er

logger = logging.getLogger('rosa.log')

# CONN & CONN MANAGER

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
	"""Initiate the connection to the server. If an error occurs, [freak out]."""
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


def confirm(conn, force=False): # to dispatch? or could be to opps?
	"""Double checks that user wants to commit any changes made to the server. Asks for y/n response and rolls-back on any error or [n] no."""
	if force is True:
		try:
			logger.debug('forcing commit...')
			conn.commit()
		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to --force commit to server:{RESET} {c}", exc_info=True)
			raise
		else:
			logger.info('--forced commit to server')
	else:

		confirm = input("commit changes to server? y/n: ").lower()
		if confirm in ('y', ' y', 'y ', ' y ', 'yes', 'yeah', 'i guess', 'i suppose'):
			try:
				conn.commit()

			except (mysql.connector.Error, ConnectionError, Exception) as c:
				logger.error(f"{RED}err encountered while attempting to commit changes to server:{RESET} {c}", exc_info=True)
				raise
			else:
				logger.info('commited changes to server')

		elif confirm in ('n', ' n', 'n ', ' n ', 'no', 'nope', 'hell no', 'naw'):
			try:
				conn.rollback()

			except (mysql.connector.Error, ConnectionError, Exception) as c:
				logger.error(f"{RED}err encountered while attempting to rollback changes to server:{RESET} {c}", exc_info=True)
				raise
			else:
				logger.info('changes rolled back')
		else:
			logger.error('unknown response; rolling server back')
			raise