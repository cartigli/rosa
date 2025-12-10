"""Handles editing data in the server.

Uploads to, updates in, and deletes data from the server.
"""

import logging
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm as tqdm_
import mysql.connector # to connect with the mysql server
import xxhash # can be replaced w.native hashlib

from rosa.confs import MAX_ALLOWED_PACKET, RED, RESET


logger = logging.getLogger('rosa.log')

# EDIT SERVER

def collector(conn, _list, abs_path, key):
	"""Manages the batched uploading to the server.

	Sorts the batches with collect_info.
	Passes each resulting set to collect_data, 
	And the corresponding upload function.
	Both queries %s with 3 values in the same order.
	Uses tqdm for progress bar.

	Args:
		conn: Connection object.
		_list (list): The files [local_only, deltas] for uploading.
		abs_path (Path): Original path of the LOCAL_DIR.
		key (var): String for specifying upload created() or edited().
	
	Returns:
		None
	"""
	with tqdm_(loggers=[logger]):
		with tqdm(collect_info(_list, abs_path)) as pbar:

			for batch in pbar:
				batch_data = collect_data(batch, abs_path)
				if batch_data:

					if key == "new_file":
						upload_created(conn, batch_data)
					elif key == "altered_file":
						upload_edited(conn, batch_data)


def collect_info(dicts_, _abs_path):
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

def collect_data(dicts_, _abs_path):
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
			hasher.reset()

			content = item.read_bytes()
			# c_content = cmpr.compress(content)

			hasher.update(content)
			hash_id = hasher.digest()

			item_data.append((content, hash_id, path))

	return item_data

# UPLOAD THE COLLECTED

def rm_remdir(conn, gates):
	"""Removes directories from the server via DELETE.

	DML so executemany().
	
	Args: 
		conn: Connection object.
		gates (list): Single-element tuples of remote-only directories' relative paths.
	
	Returns:
		None
	"""
	logger.debug('...deleting remote-only drectory[s] from server...')
	g = "DELETE FROM directories WHERE drp = %s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(g, gates)

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
	f = "DELETE FROM notes WHERE frp = %s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(f, cherubs)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered when trying to delete file[s] from server:{RESET} {c}", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only file[s] from server w.o exception')

def upload_dirs(conn, caves):
	"""Uploads directories to the server via INSERT.

	DML so executemany().

	Args:
		conn: Connection object to query the server.
		caves (list): Single-element tuples of remote-only directories' relative paths.

	Returns:
		None
	"""
	# logger.debug('...uploading local-only directory[s] to server...')
	h = "INSERT INTO directories (drp) VALUES (%s);"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(h, caves)

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
	i = "INSERT INTO notes (content, hash_id, frp) VALUES (%s, %s, %s);"

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
	j = "UPDATE notes SET content = %s, hash_id = %s WHERE frp = %s;"

	try:
		with conn.cursor(prepared=True, buffered=False) as cursor:
			cursor.executemany(j, soul_data)

	except (mysql.connector.Error, ConnectionError, Exception) as c:
		logger.error(f"err encountered while attempting to upload altered file to server:{RESET} {c}", exc_info=True)
		raise
	# 	logger.debug("wrote altered file[s]'s contents & new hashes to server w,o exception")
	# else: