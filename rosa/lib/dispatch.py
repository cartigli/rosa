import sys
import time
import logging
import contextlib

import mysql.connector

from rosa.confs import XCONFIG, ASSESS2, MAX_ALLOWED_PACKET, RED, RESET

"""
Manages the connection object be for connection initiation, closing, and error handling.

[functions]
(contextmanager) phones(), 
init_conn(db_user, db_pswd, db_name, db_addr), 
calc_batch(conn), 
_safety(conn), 
confirm(conn, force=False)
"""

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

	conn = mysql.connector.connect(**config)

	return conn # thinking direct is better

def calc_batch(conn): # this one as referenced on analyst, should be in dispatch
	"""Get the average row size of the notes table to estimate optimal batch size for downloading. ASSESS2 is 1/100 the speed of ASSESS"""
	batch_size = 5 # default
	row_size = 10 # don't divide by 
	with conn.cursor() as cursor:
		try:
			cursor.execute(ASSESS2)
			row_size = cursor.fetchone()
		except (ConnectionError, TimeoutError) as c:
			logger.error(f"error encountered while attempting to find avg_row_size: {c}", exc_info=True)
			raise
		else:
			if row_size and row_size[0]:
				try:
					batch_size = max(1, int((0.94*MAX_ALLOWED_PACKET) / row_size[0]))
				except ZeroDivisionError:
					logger.warning("returned row_size was 0, can't divide by 0! returning default batch_sz")
					return batch_size, row_size
				else:
					logger.debug(f"batch size: {batch_size}")
					return batch_size, row_size
			else:
				logger.warning("ASSESS2 returned nothing usable, returning default batch size")
				return batch_size, row_size

def _safety(conn):
	"""Handles rollback of the server on err from phone_duty."""
	logger.warning('_safety called to rollback server due to err')

	if conn and conn.is_connected():
		try:
			conn.rollback()
		except (mysql.connector.Error, ConnectionRefusedError, TimeoutError):
			logger.error('connection could not rollback server; auto_commit is off regardless so abandoning connection')
			return
		else:
			logger.warning('rollback completed without exception')
			return
	else:
		logger.warning('connection lost; relying on autocommit=False (reconnection would be fruitless)')
		return

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
			_safety(conn)
		else:
			logger.error('unknown response; rolling server back')
			raise