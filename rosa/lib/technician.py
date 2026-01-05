"""Handles editing data in the server.

Uploads to, updates in, and deletes data from the server.
"""

# check complete 

import os
import logging
from pathlib import Path
from itertools import batched
from datetime import datetime, UTC

import mysql.connector # to connect with the mysql server
import xxhash # can be replaced w.native hashlib

from rosa.confs import MAX_ALLOWED_PACKET, LOCAL_DIR, RED, RESET, INIT2


logger = logging.getLogger('rosa.log')

# INITIATE SERVER

def init_remote(conn, drps, frps):
	"""Initiates the first upload to and creation of the database.

	Args:
		conn (mysql): Connection obj.
		drps (list): Relative paths of all the directories.
		frps (list): Relative paths of all the files.
	
	Returns:
		None
	"""
	# _path = Path(LOCAL_DIR)
	message = "INITIAL"
	version = 0

	with conn.cursor() as cursor:
		# make all the tables first
		cursor.execute(INIT2)

		while cursor.nextset():
			pass

		# start with the bulk file upload
		collector(conn, frps, LOCAL_DIR, version, key="new_files")
		# then upload the directories
		upload_dirs(conn, drps, version)
		# upload the new version no & message last (lightest & least data rich)
		remote_records(conn, version, message)

def remote_records(conn, version, message):
	"""Uploads the messave and new version.

	Args:
		conn (mysql): Connection obj.
		version (int): Current version.
		message (str): If any, it is the given message for the version.
	
	Returns:
		None
	"""
	moment = datetime.now(UTC).timestamp()

	with conn.cursor() as cursor:
		cursor.execute("INSERT INTO interior (moment, message, version) VALUES (%s, %s, %s);", (moment, message, version))

def upload_patches(conn, patches, xversion, oversions):
	"""Uploads the reverse patches generated for altered files.

	Args:
		conn (mysql): Connection obj.
		patches (dmp): Reverse patches as text.
		xversion (int): Previous version of the file.
		oversion (int): Current version.
	
	Returns:
		None
	"""
	query = "INSERT INTO deltas (rp, patch, xversion, oversion) VALUES (%s, %s, %s, %s);"
	values = []
	for rp, patch in patches:
		oversion = oversions[rp]
		values.append((rp, patch, xversion, oversion))
	
	with conn.cursor(prepared=True) as cursor:
		for val in values:
			cursor.execute(query, val)

# EDIT SERVER

def avg(_list, abs_path):
	"""Finds abatch size for the files passed.

	Args:
		_list (list): Relative paths of files.
		abs_path (Path): Pathlib path of the LOCAL_DIR.
	
	Returns:
		batch_count (int): Packet size divided by average file size.
	"""
	# paths = Path(abs_path)
	tsz = 0

	for path in _list:
		# fp = paths / path

		fp = os.path.join(abs_path, path)

		# tsz += fp.stat().st_size
		tsz += os.stat(fp).st_size
	
	avg = tsz / len(_list)
	
	batch_count = int(MAX_ALLOWED_PACKET / avg)

	return batch_count

def collector(conn, _list, abs_path, version, key=None):
	"""Manages the batched uploading to the server.

	Args:
		conn (mysql): Connection object.
		_list (list): Relative paths, for uploading.
		abs_path (str): Path to the LOCAL_DIR.
		version (int): Current version.
		key (var): Specifies files as new or altered.
	
	Returns:
		None
	"""
	batch_count = avg(_list[:17], abs_path)
	batches = list(batched(_list, batch_count))

	for _batch in batches:
		collect_data(conn, _batch, abs_path, version, key)

def collect_info(dicts_, _abs_path): # should use sizes in the dictionary; faster & less I/O
	"""Creates batches for uploading.

	Adds items to the batch_items until the size limit is met.
	Appends batch_items to all_batches and resets batch_items.
	Repeats for all files in the list passed.

	Args:
		dicts_ (list): List of files' relative paths.
		_abs_path (Path): Original path of LOCAL_DIR.

	Returns:
		all_batches (list): List of lists, each inner-list containing one batches' files for uploading.
	"""
	# cmpr = zstd.ZstdCompressor(level=3)
	curr_batch = 0

	abs_path = Path(_abs_path).resolve()

	batch_items = []
	all_batches = []
	
	for i in dicts_:
		size = 0
		item = Path( abs_path / i )

		size = item.stat().st_size

		if size > MAX_ALLOWED_PACKET:
			logger.error(f"{RED}a single file is larger than the maximum packet size allowed:{RESET} {item}")
			raise

		elif (curr_batch + size) > MAX_ALLOWED_PACKET:
			all_batches.append((batch_items,))

			batch_items = [i]
			curr_batch = size
		else:
			batch_items.append(i)
			curr_batch += size
	
	if batch_items:
		all_batches.append((batch_items,))
	
	logger.debug('all batches collected')
	return all_batches

def collect_data(conn, dicts_, abs_path, version, key=None):
	"""Collects details about the batch passed to it.

	Args:
		dicts_ (list): Batch's relative paths.
		abs_path (str): Path to the LOCAL_DIR.

	Returns:
		item_data (list): Tuples containing each files' content, hash, and relative path from the files in the given list.
	"""
	# abs_path = Path(_abs_path)
	item_data = []

	# cmpr = zstd.ZstdCompressor(level=3)
	# hasher = hashlib.sha256()
	hasher = xxhash.xxh64()

	for path in dicts_:
		# item = ( abs_path / path ).resolve()
		item = os.path.join(abs_path, path)

		# content = item.read_bytes()
		# c_content = cmpr.compress(content)

		with open(item, 'rb') as f:
			content = f.read()

		hasher.reset()
		hasher.update(content)
		hash_id = hasher.digest()

		item_data.append((content, hash_id, version, path))

	if key == "new_files":
		upload_created(conn, item_data)
	elif key == "altered_files":
		upload_edited(conn, item_data)

# UPLOAD THE COLLECTED

def rm_remdir(conn, gates, xversion):
	"""Removes directories from the server via DELETE.

	DML so executemany().
	
	Args: 
		conn: Connection object.
		gates (list): Single-element tuples of remote-only directories' relative paths.
	
	Returns:
		None
	"""
	logger.debug('...deleting remote-only drectory[s] from server...')

	query = "INSERT INTO depr_directories (rp, xversion, oversion) VALUES (%s, %s, %s);"
	oquery = "SELECT version FROM directories WHERE rp = %s;"

	xquery = "DELETE FROM directories WHERE rp = %s;"
	xvals = [(gate[0],) for gate in gates]

	with conn.cursor() as cursor:
		try:
			for gate in gates:
				cursor.execute(oquery, (gate,))
				oversion = cursor.fetchone()

				values = (gate[0], xversion, oversion[0])
				cursor.execute(query, values)

			cursor.executemany(xquery, xvals)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}error encountered when trying to delete directory[s] from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only directory[s] from server w.o exception')

def rm_remfile(conn, cherubs):
	"""Removes files from the server via DELETE.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		cherubs (list): Single-element tuple of the remote-only files' relative paths.
	
	Returns:
		None
	"""
	logger.debug('...deleting remote-only file[s] from server...')
	ovquery = "SELECT version FROM files WHERE rp = %s;"
	query = "DELETE FROM files WHERE rp = %s;"
	doversions = {}

	with conn.cursor(prepared=True) as cursor:
		try:
			for cherub in cherubs:
				cursor.execute(ovquery, (cherub,))
				oversion = cursor.fetchone()
				doversions[cherub] = oversion[0]

				cursor.execute(query, (cherub,))

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered when trying to delete file[s] from server:{RESET} {c}", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only file[s] from server w.o exception')
			return doversions

def upload_dirs(conn, drps, version):
	"""Uploads directories to the server via INSERT.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		drps (list): Lists of remote-only directories' relative paths.

	Returns:
		None
	"""
	query = "INSERT INTO directories (rp, version) VALUES (%s, %s);"
	values = [(rp, version) for rp in drps]

	with conn.cursor(prepared=True) as cursor:
		try:
			cursor.executemany(query, values)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"err encountered while attempting to upload [new] directory[s] to server: {c}", exc_info=True)
			raise

def upload_created(conn, serpent_data):
	"""Uploads new files into the database via INSERT.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		serpent_data (list): 3-element tuples containing each new files' new content, new hash, and new relative path for every file in the batch.

	Returns:
		None
	"""
	i = "INSERT INTO files (content, hash, version, rp) VALUES (%s, %s, %s, %s);"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(i, serpent_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"{RED}err encountered while attempting to upload [new] file[s] to server:{RESET} {c}", exc_info=True)
		raise


def upload_edited(conn, soul_data):
	"""Uploads altered files into the database via UPDATE.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		soul_data (list): 3-element tuples containing each altered files' new content, new hash, and relative path for every file in the batch.
	
	Returns:
		None
	"""
	j = "UPDATE files SET content = %s, hash = %s, version = %s WHERE rp = %s;"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(j, soul_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"err encountered while attempting to upload altered file to server:{RESET} {c}", exc_info=True)
		raise