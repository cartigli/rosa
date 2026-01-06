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

from rosa.confs import XCONFIG, ASSESS2, MAX_ALLOWED_PACKET, RED, RESET


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
	conn = None
	logger.debug('...phone call, connecting...')
	try:
		conn = init_conn(XCONFIG['user'], XCONFIG['pswd'], XCONFIG['name'], XCONFIG['addr'])

		if conn:
			if conn.is_connected():
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
			# sys.exit(7)
			raise
		elif mse.errno == errorcode.ER_BAD_DB_ERROR:
			logger.error('database does not exist; run [init] or repair the config')
			# sys.exit(7)
			raise
		elif mse.errno == errorcode.CR_CONN_HOST_ERROR:
			logger.error('connection failed; is the server running?')
			# sys.exit(7)
			raise
		# elif mse.errno == errorcode.CR_UNKOWN_HOST:
		# 	logger.error('unkown host; is the IP accurate?')
		# 	sys.exit(7) # this one doesn't get triggered as far as I can tell;
		# 	# changing the config's IP just results in a CR_CONN_HOST_ERROR
		# 	# also mysql.connector doesn't know it, so bin it
		else:
			logger.error(f"unknown error caught by mysql: {mse}")
			raise

	except (ConnectionRefusedError, TimeoutError, Exception) as e:
		logger.error(f"error encountered while connecting to the server:{RESET} {e}.", exc_info=True)
		_safety(conn)
	else:
		logger.debug('phones executed w.o exception')
		if conn:
			if conn.is_connected():
				conn.commit()
			else:
				logger.error('connection object is not conncted; cannot commit')
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

def init_conn(db_user, db_pswd, db_name, db_addr): # used by all scripts
	"""Initiate the connection to the server. If an error occurs, [freak out].

	Args:
		db_user: Database's user username
		db_pswd: Username's password
		db_name: Database's name
		db_addr: IP address of the server [or machine running the server]
	
	Returns:
		conn (mysql): Connection object.
	"""
	config = {
		# 'unix_socket': '/tmp/mysql.sock',
		'host': db_addr,
		'user': db_user,
		'password': db_pswd,
		'database': db_name,
		'autocommit': False,
		'use_pure': True
	}

	conn = mysql.connector.connect(**config)

	return conn # thinking direct is better

@contextlib.contextmanager
def landline(local):
	sconn = None

	logger.debug('...landline, connecting...')
	try:

		with sqlite3.connect(local) as sconn:
			if sconn:
				yield sconn
			else:
				logger.error('no connection object returned')

	except KeyboardInterrupt as ki:
		logger.warning('Boss killed it; wrap it up')
		_emerg(sconn)
		raise
	except sqlite3.OperationalError as oe:
		if "unable to open database file" in str(oe):
			logger.error('unable to find the sqlite\'s database file')
			# sys.exit(7)
			raise
		else:
			logger.error('unknown error caught by landline')
			_emerg(sconn)
			# sys.exit(7)
			raise
	else:
		logger.debug('landline caught no exceptions; commiting...')
		sconn.commit()
	finally:
		sconn.close()
		logger.debug('landline hung up [finally]')


def _emerg(sconn):
	if sconn:
		logger.debug('rolling the local db back')
		sconn.rollback()
	else:
		logger.error('no sconn to rollback with')


def calc_batch(conn): # this one as referenced on analyst, should be in dispatch
	"""Get the average row size of the notes table to estimate optimal batch size for downloading. 
	
	ASSESS2 is 1/100 the speed of ASSESS, especially with large datasets.

	Args:
		conn (mysql): Connection object.
	
	Returns:
		A 2-element tuple containing:
			batch_size (int): Calculated batch size from MAX_ALLOWED_PACKET & average_row_size of the table.
			row_size (single-element tuple): Table's average row size; useful for getting batch size in bytes.
	"""
	batch_size = 5 # default
	row_size = 10 # don't divide by 0

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
					batch_size = max(1, int((MAX_ALLOWED_PACKET) / row_size[0]))

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
	"""Handles rollback of the server on err from phone_duty.

	If the connection object is connected, roll the incomplete upload back.
	If not, return. Autocommit = False, so reconnecting for rollback is pointless.

	Args:
		conn (mysql): Conneciton object.
	
	Returns:
		None
	"""
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
	"""Double checks that user wants to commit any changes made to the server. 
	
	Asks for y/n response and rolls-back on any error or [n] no.

	Args:
		conn (mysql): Connection object to query the server with.
		force (=False): If True, does *not* ask for user confirmation before attempting to commit. Default is False.
	
	Returns:
		None
	"""
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
			_safety(conn)