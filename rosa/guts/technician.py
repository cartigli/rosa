import os
import sys
import time
import shutil
import logging
import tempfile
# import hashlib
import subprocess
import contextlib
from pathlib import Path
from itertools import batched

# these three are the only external packages required
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import xxhash # this one is optional and can be replaced with hashlib which is more secure & in the native python library
import mysql.connector # to connect with the mysql server - helps prevent injection while building queries as well
# from mysql.connector impor
# import zstandard as zstd # compressor for files before uploading and decompressing after download

from rosa.configurables.queries import ASSESS2
from rosa.configurables.config import LOGGING_LEVEL, LOCAL_DIR, XCONFIG, MAX_ALLOWED_PACKET, RED, GREEN, YELLOW, RESET

logger = logging.getLogger('rosa.log')

# EDIT SERVER

def rm_remdir(conn, gates): # only give 3.0
	"""Remove remote-only directories from the server. Paths [gates] passed as list of dictionaries for executemany(). This, and every other call to 
	make an edit on the database, is rolled back on errors. Its only for specific calls to change the information in them. The default with mysql - conn
	is to roll back on disconnect, so its an ok safety net, but its supposed to be all over this shit.
	"""
	logger.debug('...deleting remote-only drectory[s] from server...')
	g = "DELETE FROM directories WHERE drp = %(drp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(g, gates)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}error encountered when trying to delete directory[s] from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only directory[s] from server w.o exception')

def rm_remfile(conn, cherubs): # only give 3.0
	"""Remove remote-only files from the server. Paths [cherubs] passed as a list of dictionaries for executemany()."""
	logger.debug('...deleting remote-only file[s] from server...')
	f = "DELETE FROM notes WHERE frp = %(frp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(f, cherubs)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered when trying to delete file[s] from server:{RESET} {c}", exc_info=True)
			raise
		else:
			logger.debug('removed remote-only file[s] from server w.o exception')

def collect_info(dicts_, _abs_path): # give - a
	"""For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
	This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
	be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany().
	For batched uploads, the script reads the files to get their size so it can optimize queries-per-execution within the 
	limitation for packet sizes. Pretty inneficient because reading every file just for its size when we already have the content 
	in memory is a waste.
	"""
	# logger.debug('...collecting info on file[s] sizes to upload...')
	# cmpr = zstd.ZstdCompressor(level=3)
	curr_batch = 0

	abs_path = Path(_abs_path).resolve()

	batch_items = []
	all_batches = []
	
	for i in dicts_:
		size = 0
		item = ( abs_path / i )

		size = os.path.getsize(item) 

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

def collect_data(dicts_, _abs_path, conn): # give - redundant
	"""For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
	This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
	be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany(). Works with the 
	output of the collect_info function in terms of data type & format.
	"""
	# logger.debug('...collecting data on file[s] to upload...')
	abs_path = Path(_abs_path)
	item_data = []

	# cmpr = zstd.ZstdCompressor(level=3)
	# hasher = hashlib.sha256()
	hasher = xxhash.xxh64()

	for tupled_batch in dicts_:
		for paths in tupled_batch:
			item = ( abs_path / paths ).resolve()
			hasher.reset()

			content = item.read_bytes()
			# c_content = cmpr.compress(content)

			hasher.update(content)
			hash_id = hasher.digest()

			item_data.append((content, hash_id, paths))

	return item_data

def upload_dirs(conn, caves): # give
	"""Insert into the directories table any local-only directories found."""
	# logger.debug('...uploading local-only directory[s] to server...')
	h = "INSERT INTO directories (drp) VALUES (%(drp)s);"
	with conn.cursor() as cursor:
		try:
			cursor.executemany(h, caves)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"err encountered while attempting to upload [new] directory[s] to server: {c}", exc_info=True)
			raise
		# else:
		#     logger.debug('local-only directory[s] written to server w.o exception')

def upload_created(conn, serpent_data): # give
	"""Insert into the notes table the new record for local-only files that do not exist in the server. *This function triggers no actions in the database*."""
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
	# else:
	# 	logger.debug("wrote altered file[s]'s contents & new hashes to server w,o exception")


# USER INPUT & HANDLING


def counter(start, nomic):
    if start:
        end = time.perf_counter()
        duration = end - start
        if duration > 60:
            duration_minutes = duration / 60
            logger.info(f"time [in minutes] for rosa {nomic}: {duration_minutes:.3f}")
        else:
            logger.info(f"time [in seconds] for rosa {nomic}: {duration:.3f}")

def confirm(conn, force=False): # give
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