"""Handles editing data in the server.

Uploads to, updates in, and deletes data from the server.
"""

import os
import logging
from itertools import batched
from datetime import datetime, UTC

import mysql.connector
import xxhash

from rosa.confs import MAX_ALLOWED_PACKET, INIT2
from rosa.lib import encoding

logger = logging.getLogger('rosa.log')

# INITIATE SERVER

def init_remote(conn: MySQL | None = None, core: str = None, drps: list = [], frps: list = []):
	"""Initiates the first upload to and creation of the database.

	Args:
		conn (mysql): Connection obj.
		core (str): Source directory.
		drps (list): Relative paths of all the directories.
		frps (list): Relative paths of all the files.
	
	Returns:
		None
	"""
	message: str = "INITIAL"
	version: int = 0

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

def remote_records(conn: MySQL | None = None, version: int = None, message: str = ""):
	"""Uploads the messave and new version.

	Args:
		conn (mysql): Connection obj.
		version (int): Current version.
		message (str): Message for the version.

	Returns:
		None
	"""
	moment: float = datetime.now(UTC).timestamp()

	with conn.cursor() as cursor:
		cursor.execute("INSERT INTO interior (moment, message, version) VALUES (%s, %s, %s);", (moment, message, version))

def upload_patches(conn: MySQL | None = None, patches: list = [], to_version: int = None, details: dict = None):
	"""Uploads the reverse patches generated for altered files.

	Args:
		conn (mysql): Connection obj.
		patches (dmp): Reverse patches as text.
		to_version (int): Previous version of the file.
		details (dict): Relative path keyed to encoding & versions.
	
	Returns:
		None
	"""
	query: str = "INSERT INTO deltas (rp, patch, original_version, to_version, from_version) VALUES (%s, %s, %s, %s, %s);"
	values: list = []
	for rp, patch in patches:
		original_version: int = details[rp][0]
		from_version: int = details[rp][1]
		values.append((rp, patch, original_version, to_version, from_version))

	with conn.cursor(prepared=True) as cursor:
		for val in values:
			cursor.execute(query, val)

# EDIT SERVER

def avg(files: list = [], dirx: str = ""):
	"""Finds abatch size for the files passed.

	Args:
		files (list): Relative paths of files.
		dirx (str): Source directory.

	Returns:
		batch_count (int): Packet size divided by average file size.
	"""
	tsz: int = 0

	for path in files:
		fp: str = os.path.join(dirx, path)

		tsz: int += os.stat(fp).st_size

	avg: int = tsz / len(files)

	batch_count: int = int(MAX_ALLOWED_PACKET / avg)

	return batch_count

def collector(conn: MySQL | None = None, files: list = [], abs_path: str = "", version: int = None, key: bool = None):
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
	batch_count: int = avg(files[:50], abs_path)
	batches: list = list(batched(files, batch_count))

	total: int = len(batches)
	length: int = 100
	fin: int = 0

	fill: str = '%'
	none: str = '-'

	for _batch in batches:
		collect_data(conn, _batch, abs_path, version, key)

		fin: int += 1
		i: int = int((fin/total)*length)

		base: str = f"[{fill*i}{none*(length - i)}] uploading batch {fin}/{total}"
		print(base, end='\r', flush=True)

	print("\x1b[2K\r", end="", flush=True)


def collect_data(conn: MySQL | None = None, dicts_: list = [], abs_path: str = "", version: int = None, key: bool = None):
	"""Collects details about the batch passed to it.

	Args:
		conn (mysql): Connection object.
		dicts_ (list): Batch's relative paths.
		abs_path (str): Path to the given directory.
		version (int): Current version.
		key (var): Specifies files as new or altered.

	Returns:
		item_data (list): Tuples containing each files' content, hash, and relative path from the files in the given list.
	"""
	item_data: list = []

	hasher = xxhash.xxh64()

	if key == "altered_files":
		for path in dicts_:
			item: str = os.path.join(abs_path, path)

			with open(item, 'rb') as f:
				content: bytes = f.read()

			hasher.reset()
			hasher.update(content)
			hash_id: bytes = hasher.digest()

			item_data.append((content, hash_id, version, path))

		upload_edited(conn, item_data)

	if key == "new_files":
		for path in dicts_:
			item: str = os.path.join(abs_path, path)

			with open(item, 'rb') as f:
				content: bytes = f.read()

			hasher.reset()
			hasher.update(content)
			hash_id: bytes = hasher.digest()

			track: str = encoding(item)

			item_data.append((content, hash_id, version, version, path, track))

		upload_created(conn, item_data)

# UPLOAD THE COLLECTED

def rm_remdir(conn: MySQL | None = None, sconn: SQLite3 | None = None, gates: list = [], to_version: int = None):
	"""Removes directories from the server via DELETE.

	DML, so executemany().
	
	Args: 
		conn (mysql): Connection object.
		sconn (sqlite3): Index's connection object.
		gates (list): Remote-only directories' relative paths.
		to_version (int): Version of deletion for the files (Current version).

	Returns:
		None
	"""
	logger.debug('...deleting remote-only drectory[s] from server...')

	query: str = "INSERT INTO depr_directories (rp, to_version, from_version) VALUES (%s, %s, %s);"
	oquery: str = "SELECT version FROM directories WHERE rp = %s;"
	soquery: str = "SELECT version FROM directories WHERE rp = ?;"

	xquery: str = "DELETE FROM directories WHERE rp = %s;"
	xvals: list = [(gate[0],) for gate in gates]

	with conn.cursor() as cursor:
		try:
			for gate in gates:
				from_version: int = sconn.execute(soquery, (gate,)).fetchone()[0]

				values:tuple = (gate[0], to_version, from_version)
				cursor.execute(query, values)

			cursor.executemany(xquery, xvals)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"error encountered when trying to delete directory[s] from server: {c}.", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only directory[s] from server w.o exception')

def rm_remfile(conn: MySQL | None = None, sconn: SQLite3 | None = None, cherubs: list = []):
	"""Removes files from the server via DELETE.

	DML so executemany().

	Args:
		conn (mysql): Connection object to the server.
		sconn (sqlite3): Connection object to the index.
		cherubs (list): Remote-only files' relative paths.
	
	Returns:
		Three-element tuple containing:
			doversions (dict): Previous versions.
			dogversions (dict): Original versions.
			dotrack (dict): Tracking values.
	"""
	logger.debug('...deleting remote-only file[s] from server...')
	ovquery: str = "SELECT original_version FROM files WHERE rp = %s;"
	sovquery: str = "SELECT from_version, track FROM records WHERE rp = ?;"
	query: str = "DELETE FROM files WHERE rp = %s;"
	doversions: dict = {}
	dogversions: dict = {}
	dotrack: dict = {}

	with conn.cursor(prepared=True) as cursor:
		try:
			for cherub in cherubs:
				cursor.execute(ovquery, (cherub,)) # this is the sqlite index as well, no?
				original_version: int = cursor.fetchone()[0]

				dogversions[cherub] = original_version

				index_data: tuple = sconn.execute(sovquery, (cherub,)).fetchone()

				doversions[cherub] = index_data[0]

				dotrack[cherub] = index_data[1]

				cursor.execute(query, (cherub,))

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"error encountered when trying to delete file[s] from server: {c}", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only file[s] from server w.o exception')
			return doversions, dogversions, dotrack

def upload_dirs(conn: MySQL | None = None, drps: list = [], version: int = None):
	"""Uploads directories to the server via INSERT.

	DML so executemany().

	Args:
		conn (mysql): Connection object to query the server.
		drps (list): Remote-only directories' relative paths.
		version (int): Current version.

	Returns:
		None
	"""
	query: str = "INSERT INTO directories (rp, version) VALUES (%s, %s);"
	values: list = [(rp, version) for rp in drps]

	with conn.cursor(prepared=True) as cursor:
		try:
			cursor.executemany(query, values)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"err encountered while attempting to upload [new] directory[s] to server: {c}", exc_info=True)
			raise

def upload_created(conn: MySQL | None = None, serpent_data: list = []):
	"""Uploads new files into the database via INSERT.

	DML so executemany().

	Args:
		conn (mysql): Connection object to query the server.
		serpent_data (list): 3-element tuples containing each new files' new content, new hash, and relative path.

	Returns:
		None
	"""
	query: str = "INSERT INTO files (content, hash, original_version, from_version, rp, track) VALUES (%s, %s, %s, %s, %s, %s);"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(query, serpent_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"error encountered while attempting to upload [new] file[s] to server: {c}", exc_info=True)
		raise


def upload_edited(conn: MySQL | None = None, soul_data: list = []):
	"""Uploads altered files into the database via UPDATE.

	DML so executemany().

	Args:
		conn (mysql): Connection object to query the server.
		soul_data (list): 3-element tuples containing each altered files' new content, new hash, and relative path.
	
	Returns:
		None
	"""
	j: str = "UPDATE files SET content = %s, hash = %s, from_version = %s WHERE rp = %s;"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(j, soul_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"err encountered while attempting to upload altered file to server: {c}", exc_info=True)
		raise