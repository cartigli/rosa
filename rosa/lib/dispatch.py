"""Manages the connection object.

Context manager for the connection object.
Initiaton of the connection object.
Error handling, server rollback, committing.
"""

import sys
import logging
import contextlib

import sqlite3
import mysql.connector
from mysql.connector import errorcode

from rosa.confs import XCONFIG, ASSESS2, MAX_ALLOWED_PACKET

logger = logging.getLogger('rosa.log')

# CONN & CONN MANAGER

@contextlib.contextmanager
def phones():
	"""Context manager for the mysql.connector connection object.

	Args:
		None

	Yields: 
		conn (mysql): Connection object.
	"""
	conn: mysql.connector.MySQLConnection | None = None

	try:
		conn = init_conn(XCONFIG['user'], XCONFIG['pswd'], XCONFIG['name'], XCONFIG['addr'])

		if conn:
			if conn.is_connected():
				logger.debug('...phone call, connecting...')
				yield conn
			else:
				logger.warning('connection object is not connected')
		else:
			logger.warning('no connection object was returned')

	except KeyboardInterrupt as ko:
		logger.error('boss killed it; wrap it up')
		_safety(conn)
	except mysql.connector.Error as mse:
		if mse.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			logger.error('connection failed: invalid username/password')
			sys.exit(1)
		elif mse.errno == errorcode.ER_BAD_DB_ERROR:
			logger.error('database does not exist; run [init] or repair the config')
			sys.exit(1)
		elif mse.errno == errorcode.CR_CONN_HOST_ERROR:
			logger.error('connection failed; is the server running?')
			sys.exit(1)
		else:
			logger.error(f"unknown error caught by mysql: {mse}")
			raise

	except (ConnectionRefusedError, TimeoutError) as e:
		logger.error(f"error encountered while connecting to the server: {e}.", exc_info=True)
		_safety(conn)
	else:
		logger.debug('phones executed w.o exception')
		if conn:
			if conn.is_connected():
				conn.commit()
			else:
				logger.error('connection object is not connected; cannot commit')
		else:
			logger.error('connection object lost; cannot commit')
	finally:
		if conn:
			if conn.is_connected():
				conn.close()
				logger.debug('phones closed conn [finally]')
			else:
				logging.debug('connection did not make it back to phones')
		else:
			logging.debug('connection did not make it back to phones')

def init_conn(db_user: str = "", db_pswd: str = "", db_name: str = "", db_addr: str = ""):
	"""Initiate the connection to the server. If an error occurs, [freak out].

	Args:
		db_user (str): Database's user username
		db_pswd (str): Username's password
		db_name (str): Database's name
		db_addr (str): IP address of the server [or machine running the server]
	
	Returns:
		conn (mysql): Connection object.
	"""
	conn: mysql.connector.MySQLConnection | None = None
	config: dict = {
		# 'unix_socket': '/tmp/mysql.sock',
		'host': db_addr,
		'user': db_user,
		'password': db_pswd,
		'database': db_name,
		'autocommit': False,
		'use_pure': True
	}

	conn = mysql.connector.connect(**config)

	return conn

@contextlib.contextmanager
def landline(local: str = ""):
	"""Creates connection to a SQLite3 db file.
	
	Args:
		local (str): Path to a .db file.
	
	Yields:
		sconn (SQLite3): Connection object.
	"""
	sconn: sqlite3.Connection | None = None

	try:
		with sqlite3.connect(local) as sconn:
			if sconn:
				logger.debug('...landline, connecting...')
				yield sconn
			else:
				logger.error('no connection object returned')

	except KeyboardInterrupt as ki:
		logger.warning('Boss killed it; wrap it up')
		_emerg(sconn)
	except sqlite3.OperationalError as oe:
		if "unable to open database file" in str(oe):
			logger.error('unable to find the sqlite\'s database file')
			raise
		else:
			logger.error('unknown error caught by landline')
			_emerg(sconn)
			raise
	else:
		logger.debug('landline caught no exceptions; commiting...')
		sconn.commit()
	finally:
		if sconn:
			sconn.close()
		logger.debug('landline hung up [finally]')

def _emerg(sconn: sqlite3 | None = None):
	"""Rolls the sqlite3 database back on Error.

	Args:
		sconn (sqlite3): Connection object.

	Returns:
		None
	"""
	if sconn:
		logger.debug('rolling the local db back')
		sconn.rollback()
	else:
		logger.error('no sconn to rollback with')

def calc_batch(conn: MySQL | None = None):
	"""Get the average row size of the notes table to estimate optimal batch size for downloading. 

	Args:
		conn (mysql): Connection object.
	
	Returns:
		A 2-element tuple containing:
			batch_size (int): Calculated batch size from MAX_ALLOWED_PACKET & average_row_size of the table.
			row_size (single-element tuple): Table's average row size; useful for getting batch size in bytes.
	"""
	batch_size: int = 5 # default
	row_size: int = 10 # don't divide by 0

	with conn.cursor() as cursor:
		try:
			cursor.execute(ASSESS2)
			row_size: tuple = cursor.fetchone()

		except (ConnectionError, TimeoutError) as c:
			logger.error(f"error encountered while attempting to find avg_row_size: {c}", exc_info=True)
			raise
		else:
			if row_size and row_size[0]:
				try:
					batch_size: int = max(1, int((MAX_ALLOWED_PACKET) / row_size[0]))

				except ZeroDivisionError:
					logger.warning("returned row_size was 0, can't divide by 0! returning default batch_sz")
					return batch_size, row_size
				else:
					logger.debug(f"batch size: {batch_size}")
					return batch_size, row_size
			else:
				logger.warning("ASSESS2 returned nothing usable, returning default batch size")
				return batch_size, row_size

def _safety(conn: MySQL | None = None):
	"""Handles rollback of the server on err from phone_duty.

	If the connection object is connected, roll the incomplete upload back.
	If not, return. Autocommit = False, so reconnecting for rollback is pointless.

	Args:
		conn (mysql): Conneciton object.
	
	Returns:
		None
	"""
	logger.warning('_safety called to rollback server due to err')

	if conn:
		if conn.is_connected():
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
	else:
		logger.warning('connection lost; relying on autocommit=False (reconnection would be fruitless)')
		return

def confirm(conn: MySQL | None = None, force: bool = False):
	"""Double checks that user wants to commit any changes made to the server. 
	
	Asks for y/n response and rolls-back on any error or [n] no.

	Args:
		conn (mysql): Connection object to query the server with.
		force (=False): If True, does *not* ask for user confirmation before attempting to commit.
	
	Returns:
		None
	"""
	if force is True:
		try:
			logger.debug('forcing commit...')
			conn.commit()
		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Error encountered while attempting to --force commit to server: {c}", exc_info=True)
			raise
		else:
			logger.info('--forced commit to server')
	else:
		confirm = input("commit changes to server? y/n: ").lower()
		if confirm in ('y', ' y', 'y ', ' y ', 'yes', 'yeah', 'i guess', 'i suppose'):
			try:
				conn.commit()

			except (mysql.connector.Error, ConnectionError, Exception) as c:
				logger.error(f"Error encountered while attempting to commit changes to server: {c}", exc_info=True)
				raise
			else:
				logger.info('commited changes to server')

		elif confirm in ('n', ' n', 'n ', ' n ', 'no', 'nope', 'hell no', 'naw'):
			_safety(conn)
		else:
			logger.error('unknown response; rolling server back')
			_safety(conn)