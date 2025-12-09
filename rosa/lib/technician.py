import logging
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm as tqdm_
import mysql.connector # to connect with the mysql server
import xxhash # can be replaced w.native hashlib

from rosa.confs import MAX_ALLOWED_PACKET, RED, RESET

"""
Counter-component to contractor: editing and uploading to the server. 
Slightly more complicated than downloading, hence the name.

[functions]
rm_remdir(conn, gates),
rm_rem_file(conn, cherubs),
collect_info(dicts_, _abs_path),
collect_data(dicts_, _abs_path),
upload_dirs(conn, caves),
upload_created(conn, serpent_data),
upload_edited(conn, soul_data)
"""

logger = logging.getLogger('rosa.log')

# EDIT SERVER

def _collector_(conn, _list, abs_path, key):

	with tqdm_(loggers=[logger]):
		with tqdm(collect_info(_list, abs_path)) as pbar:

			for batch in pbar:
				batch_data = collect_data(batch, abs_path)
				if batch_data:

					if key == "new_file":
						upload_created(conn, batch_data)
					elif key == "altered_file":
						upload_edited(conn, batch_data)


def collect_info(dicts_, _abs_path): # give - a
	"""For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
	This is passed to the upload functions as required. Both functions [upload_edited(), upload_created()] use the same three variables, 
	so their batches can all be defined with this function. Order is irrelevant for the individual items if using the %(variable)s method 
	with executemany(). For batched uploads, the queries the files' metadata for its disk size in bytes. Makes one big list for all the 
	batches & files needed, and returns this single item. [give] calls this twice.
	"""
	# logger.debug('...collecting info on file[s] sizes to upload...')
	# cmpr = zstd.ZstdCompressor(level=3)
	curr_batch = 0

	abs_path = Path(_abs_path).resolve()

	batch_items = []
	all_batches = []
	
	for i in dicts_:
		size = 0
		item = Path( abs_path / i[0] ) # [tuple management]

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

def collect_data(dicts_, _abs_path): # lol why would this need conn
	"""For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
	This is pased to the upload functions as required. Both this fx and collect_info() use the same three variables for every file, so 
	they can all be built with this function. Order is irrelevant for the individual items when using the %(variable)s method with 
	executemany(). Works with the output of the collect_info function in terms of data type & format.
	"""
	# logger.debug('...collecting data on file[s] to upload...')
	abs_path = Path(_abs_path)
	item_data = []

	# cmpr = zstd.ZstdCompressor(level=3)
	# hasher = hashlib.sha256()
	hasher = xxhash.xxh64()

	for tupled_batch in dicts_:
		for path in tupled_batch:
			item = ( abs_path / path[0] ).resolve() # [tuple management]
			hasher.reset()

			content = item.read_bytes()
			# c_content = cmpr.compress(content)

			hasher.update(content)
			hash_id = hasher.digest()

			item_data.append((content, hash_id, path[0])) # [tuple management]

	return item_data

# UPLOAD THE COLLECTED

def rm_remdir(conn, gates):
	"""Remove remote-only directories from the server."""
	logger.debug('...deleting remote-only drectory[s] from server...')
	g = "DELETE FROM directories WHERE drp = %s;"
	# g = "DELETE FROM directories WHERE drp = %(drp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(g, gates)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}error encountered when trying to delete directory[s] from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only directory[s] from server w.o exception')

def rm_remfile(conn, cherubs):
	"""Remove remote-only files from the server. Paths [cherubs] passed as a list of dictionaries for executemany()."""
	logger.debug('...deleting remote-only file[s] from server...')
	f = "DELETE FROM notes WHERE frp = %s;"
	# f = "DELETE FROM notes WHERE frp = %(frp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(f, cherubs)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered when trying to delete file[s] from server:{RESET} {c}", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only file[s] from server w.o exception')

def upload_dirs(conn, caves): # give
	"""Insert into the directories table any local-only directories found. DML so executemany()."""
	# logger.debug('...uploading local-only directory[s] to server...')
	h = "INSERT INTO directories (drp) VALUES (%s);"
	# h = "INSERT INTO directories (drp) VALUES (%(drp)s);"
	with conn.cursor() as cursor:
		try:
			cursor.executemany(h, caves)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"err encountered while attempting to upload [new] directory[s] to server: {c}", exc_info=True)
			raise
		# else:
		#     logger.debug('local-only directory[s] written to server w.o exception')

def upload_created(conn, serpent_data): # give
	"""Insert into the notes table the new record for local-only files that do not exist in the server. DML again."""
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

def upload_edited(conn, soul_data): # only give 3.0
	"""Update the notes table to show the current content for a note that was altered, or whose hash did not show identical contents. *This function 
	triggers the on_update_notes trigger which will record the previous version of the file's contents and the time of changing.
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