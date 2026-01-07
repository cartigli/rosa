"""Handles editing data in the server.

Uploads to, updates in, and deletes data from the server.
"""


import os
import logging
from itertools import batched
from datetime import datetime, UTC

import mysql.connector
import xxhash

from rosa.confs import MAX_ALLOWED_PACKET, RED, RESET, INIT2


logger = logging.getLogger('rosa.log')

# INITIATE SERVER

def init_remote(conn, core, drps, frps):
	"""Initiates the first upload to and creation of the database.

	Args:
		conn (mysql): Connection obj.
		core (str): Source directory.
		drps (list): Relative paths of all the directories.
		frps (list): Relative paths of all the files.
	
	Returns:
		None
	"""
	message = "INITIAL"
	version = 0

	with conn.cursor() as cursor:
		# make all the tables first
		cursor.execute(INIT2)

		while cursor.nextset():
			pass

		# start with the bulk file upload
		collector(conn, frps, core, version, key="new_files")

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

def avg(files, abs_path):
	"""Finds abatch size for the files passed.

	Args:
		files (list): Relative paths of files.
		abs_path (str): Source directory.
	
	Returns:
		batch_count (int): Packet size divided by average file size.
	"""
	tsz = 0

	for path in files:
		fp = os.path.join(abs_path, path)

		tsz += os.stat(fp).st_size
	
	avg = tsz / len(files)
	
	batch_count = int(MAX_ALLOWED_PACKET / avg)

	return batch_count

def collector(conn, files, abs_path, version, key=None):
	"""Manages the batched uploading to the server.

	Args:
		conn (mysql): Connection object.
		files (list): Relative paths, for uploading.
		abs_path (str): Path to the given directory.
		version (int): Current version.
		key (var): Specifies files as new or altered.
	
	Returns:
		None
	"""
	batch_count = avg(files[:50], abs_path)
	batches = list(batched(files, batch_count))

	total = len(batches)
	length = 100
	fin = 0

	fill = '%'
	none = '-'

	# try:
	for _batch in batches:
		collect_data(conn, _batch, abs_path, version, key)

		fin += 1
		i = int((fin / total )*length)

		base = f"[{fill*i}{none*(length - i)}] uploading batch {fin}/{total}"
		print(base, end='\r', flush=True)

	# print(f"[{fill*length}] upload complete! {total}/{total}", flush=True)
	print("\x1b[2K\r", end="", flush=True)


def collect_data(conn, dicts_, abs_path, version, key=None):
	"""Collects details about the batch passed to it.

	Args:
		dicts_ (list): Batch's relative paths.
		abs_path (str): Path to the given directory.

	Returns:
		item_data (list): Tuples containing each files' content, hash, and relative path from the files in the given list.
	"""
	item_data = []

	hasher = xxhash.xxh64()

	for path in dicts_:
		item = os.path.join(abs_path, path)

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

def rm_remdir(conn, sconn, gates, xversion):
	"""Removes directories from the server via DELETE.

	DML, so executemany().
	
	Args: 
		conn (mysql): Connection object.
		sconn (sqlite3): Index's connection object.
		gates (list): Single-element tuples of remote-only directories' relative paths.
		xversion (int): Version of deletion for the files (Current version).

	Returns:
		None
	"""
	logger.debug('...deleting remote-only drectory[s] from server...')

	query = "INSERT INTO depr_directories (rp, xversion, oversion) VALUES (%s, %s, %s);"
	oquery = "SELECT version FROM directories WHERE rp = %s;"
	soquery = "SELECT version FROM directories WHERE rp = ?;"

	xquery = "DELETE FROM directories WHERE rp = %s;"
	xvals = [(gate[0],) for gate in gates]

	with conn.cursor() as cursor:
		try:
			for gate in gates:
				# cursor.execute(oquery, (gate,))
				# oversion = cursor.fetchone()

				oversion = sconn.execute(soquery, (gate,)).fetchone()

				values = (gate[0], xversion, oversion[0])
				cursor.execute(query, values)

			cursor.executemany(xquery, xvals)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}error encountered when trying to delete directory[s] from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only directory[s] from server w.o exception')

def rm_remfile(conn, sconn, cherubs):
	"""Removes files from the server via DELETE.

	DML so executemany().

	Args:
		conn (mysql): Connection object to query the server.
		sconn (sqlite3): Index's connection object.
		cherubs (list): Single-element tuple of the remote-only files' relative paths.
	
	Returns:
		None
	"""
	logger.debug('...deleting remote-only file[s] from server...')
	ovquery = "SELECT version FROM files WHERE rp = %s;"
	sovquery = "SELECT version FROM records WHERE rp = ?;"
	query = "DELETE FROM files WHERE rp = %s;"
	doversions = {}

	with conn.cursor(prepared=True) as cursor:
		try:
			for cherub in cherubs:
				# cursor.execute(ovquery, (cherub,))
				# oversion = cursor.fetchone()

				oversion = sconn.execute(sovquery, (cherub,)).fetchone()

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
		conn (mysql): Connection object to query the server.
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
		conn (mysql): Connection object to query the server.
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
		conn (mysql): Connection object to query the server.
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