"""Handles editing data in the server.

Uploads to, updates in, and deletes data from the server.
"""

import logging
from pathlib import Path
# from itertools import batched
from datetime import datetime, UTC

import mysql.connector # to connect with the mysql server
import xxhash # can be replaced w.native hashlib

from rosa.confs import MAX_ALLOWED_PACKET, LOCAL_DIR, RED, RESET, INIT


logger = logging.getLogger('rosa.log')

# INITIATE SERVER

def init_remote(conn, drps, frps):
	message = "INITIAL"
	version = 0

	with conn.cursor() as cursor:
		cursor.execute(INIT)

		while cursor.nextset():
			pass

		collector(conn, frps, LOCAL_DIR, version, key="new_files")
		remote_records(conn, version, message)
		upload_dirs(conn, drps, version)

def remote_records(conn, version, message):
	moment = datetime.now(UTC).timestamp()

	with conn.cursor() as cursor:
		cursor.execute("INSERT INTO interior (moment, message, version) VALUES (%s, %s, %s);", (moment, message, version))

def upload_patches(conn, patches, version):
	query = "INSERT INTO deltas (rp, patch, version) VALUES (%s, %s, %s);"
	pversion = version - 1 # patch *to get to* version [n-1]
	values = []
	for rp, patch in patches:
		values.append((rp, patch, pversion))
	
	with conn.cursor(prepared=True) as cursor:
		for val in values:
			cursor.execute(query, val)

# EDIT SERVER

def collector(conn, _list, abs_path, version, key=None):
	"""Manages the batched uploading to the server.

	Sorts the batches with collect_info.
	Passes each resulting set to collect_data, 
	And the corresponding upload function.
	Both queries %s with 3 values in the same order.

	Args:
		conn: Connection object.
		_list (list): The files [local_only, deltas] for uploading.
		abs_path (Path): Original path of the LOCAL_DIR.
		key (var): String for specifying upload created() or edited().
	
	Returns:
		None
	"""
	for batch in collect_info(_list, abs_path):

		batch_data = collect_data(batch, abs_path, version)
		if batch_data:

			if key == "new_files": # INSERT[S]
				upload_created(conn, batch_data)
			elif key == "altered_files": # UPDATE[S]
				upload_edited(conn, batch_data)


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
	# logger.debug('...collecting info on file[s] sizes to upload...')
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
			# logger.debug("collected one batch's items")
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

def collect_data(dicts_, _abs_path, version):
	"""Collects details about the batch passed to it.

	For every file passed, it adds the content, hash, and relative path to a tuple.
	Each one is appended to item_data and returned.

	Args:
		dicts_ (list): List of the batch's relative paths, created by collect_info() passed by collector().
		_abs_path (Path): Original path of the LOCAL_DIR.

	Returns:
		item_data (list): Tuples containing each files' content, hash, and relative path from the files in the given list.
	"""
	# logger.debug('...collecting data on file[s] to upload...')
	abs_path = Path(_abs_path)
	item_data = []

	# cmpr = zstd.ZstdCompressor(level=3)
	# hasher = hashlib.sha256()
	hasher = xxhash.xxh64()

	for tupled_batch in dicts_:
		for path in tupled_batch:
			item = ( abs_path / path ).resolve()

			content = item.read_bytes()
			# c_content = cmpr.compress(content)

			hasher.reset()
			hasher.update(content)
			hash_id = hasher.digest()

			item_data.append((content, hash_id, version, path))

	return item_data

# UPLOAD THE COLLECTED

def rm_remdir(conn, gates, version):
	"""Removes directories from the server via DELETE.

	DML so executemany().
	
	Args: 
		conn: Connection object.
		gates (list): Single-element tuples of remote-only directories' relative paths.
	
	Returns:
		None
	"""
	logger.debug('...deleting remote-only drectory[s] from server...')

	query = "INSERT INTO depr_directories (rp, version) VALUES (%s, %s);"
	values = [(gate[0], version) for gate in gates]

	xquery = "DELETE FROM directories WHERE rp = %s;"
	xvals = [(gate[0],) for gate in gates]

	with conn.cursor() as cursor:
		try:
			cursor.executemany(query, values)
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
	f = "DELETE FROM files WHERE rp = %s;"

	with conn.cursor(prepared=True) as cursor:
		try:
			for cherub in cherubs:
				cursor.execute(f, (cherub,))
			# cursor.executemany(f, cherubs)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered when trying to delete file[s] from server:{RESET} {c}", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only file[s] from server w.o exception')

def upload_dirs(conn, drps, version):
	"""Uploads directories to the server via INSERT.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		drps (list): Lists of remote-only directories' relative paths.

	Returns:
		None
	"""
	# logger.debug('...uploading local-only directory[s] to server...')
	query = "INSERT INTO directories (rp, version) VALUES (%s, %s);"
	values = [(rp, version) for rp in drps]

	with conn.cursor(prepared=True) as cursor:
		try:
			cursor.executemany(query, values)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"err encountered while attempting to upload [new] directory[s] to server: {c}", exc_info=True)
			raise
		# else:
		#     logger.debug('local-only directory[s] written to server w.o exception')

def upload_created(conn, serpent_data):
	"""Uploads new files into the database via INSERT.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		serpent_data (list): 3-element tuples containing each new files' new content, new hash, and new relative path for every file in the batch.

	Returns:
		None
	"""
	# logger.debug('...writing new file[s] to server...')
	i = "INSERT INTO files (content, hash, version, rp) VALUES (%s, %s, %s, %s);"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(i, serpent_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"{RED}err encountered while attempting to upload [new] file[s] to server:{RESET} {c}", exc_info=True)
		raise
	# else:
	#     logger.debug('wrote new file[s] to server w.o exception')

def upload_edited(conn, soul_data):
	"""Uploads altered files into the database via UPDATE.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		soul_data (list): 3-element tuples containing each altered files' new content, new hash, and relative path for every file in the batch.
	
	Returns:
		None
	"""
	# logger.debug('...writing altered file[s] to server...')
	j = "UPDATE files SET content = %s, hash = %s, version = %s WHERE rp = %s;"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
		# with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(j, soul_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"err encountered while attempting to upload altered file to server:{RESET} {c}", exc_info=True)
		raise
	# 	logger.debug("wrote altered file[s]'s contents & new hashes to server w,o exception")
	# else: