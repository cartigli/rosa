import os
import sys
import shutil

import zstandard as zstd
# import hashlib
import xxhash
import logging
import tempfile
import datetime

import contextlib
import mysql.connector
from pathlib import Path

from queries import ASSESS
from config import MAX_ALLOWED_PACKET # why am I not importing variables from the config directly into here? No point in having a middle-man - or is it better for tracing errors? Ig not seeing what is being passed could be sketch, but for the conn it does not change.


"""
Library of functions used in the management scripts. Many functions overlap and share uses, so moving them here is much easier for 
maintainability. There is also queries for longer MySQL queries. Some scripts, like get_moment, hold their own unique functions.
"""

# logging

logger = logging.getLogger(__name__)

# connection & management thereof

@contextlib.contextmanager
def phone_duty(db_user, db_pswd, db_name, db_addr):
	"""Context manager for the mysql.connector connection object."""
	conn = None

	try:
		conn = init_conn(db_user, db_pswd, db_name, db_addr)
		yield conn

	except (ConnectionError, mysql.connector.Error, Exception) as e:
		logger.critical(f"Error encountered while connecting to the server: {e}.", exc_info=True)
		try:
			_safety(conn)
		finally:
			raise

	finally:
		try:
			if conn and conn.is_connected():
				conn.close()
				logger.info('Connection closed.')

		except (mysql.connector.Error, ConnectionError, Exception) as mce:
			logger.critical(f"Error encountered while closing connection: {mce}.", exc_info=True)
			try:
				_safety(conn)
			finally:
				raise


def _safety(conn):
	"""Handles rollback of the server on err from phone_duty."""
	try:
		if conn and conn.is_connected():
			conn.rollback()
			logger.info('Server rolled back.')
		elif conn:
			try:
				conn.ping(reconnect=True, attempts=3, delay=1)
			except:
				logger.critical('Connection object lost and unable to reconnect to.', exc_info=True)
			else:
				conn.rollback()
				logger.warning('Server rolled back.')
		else:
			logger.info('Connection lost & rollback is crippled.')
	except (mysql.connector.Error, ConnectionError, Exception) as e:
		logger.critical(f"Error encountered while attempting rollback on connection error: {e}.", exc_info=True)
		raise
	else:
		logger.info('_safety completed without exception.')


def init_conn(db_user, db_pswd, db_name, db_addr): # used by all scripts
	"""Initiate the connection to the server. If an error occurs, freak out."""
	try:
		config = {
			'host': db_addr,
			'user': db_user,
			'password': db_pswd,
			'database': db_name,
			'autocommit': False, 
			'use_pure': False
		}

		try:
			conn = mysql.connector.connect(**config)

		except (ImportError) as ie:
			logger.error(f"Error establishing C Extension-connection: {ie}.", exc_info=True)
			try:
				config['use_pure']=True # switch back to pure_python
				conn = mysql.connector.connect(**config)
	
			except (ConnectionRefusedError, ConnectionError, mysql.connector.Error) as ce:
				logger.critical(f"Error establishing pure python connection: {ce}.", exc_info=True)
				raise
			else:
				logger.info('Fellback to pure_python connection & established connection.')
				return conn
		else:
			logger.info('C Extension connection established w/o exception.')
			return conn

	except (ConnectionRefusedError, TimeoutError) as e:
		logger.critical(f"Error encountered while attempting to establish connection: {e}", exc_info=True)
		raise
	except (ConnectionError, mysql.connector.Error) as c:
		logger.critical(f"Connection Error encountered while trying to establish connection: {c}.", exc_info=True)
		raise

# collecting local info for comparison

def scope_loc(local_dir): # all
	"""Collect the relative path and 256-bit hash for every file in the given directory. Ignore any files with paths that contain
	the '.DS_Store', '.git', or '.obsidian'. Record the hash in BINARY format. Hex for python ( and UNHEX for the sql queries ). 
	Every file that meets the criteria has its relative path and hash generated and recorded. Returns the pairs of paths/hashes 
	in a tuple to match data returned from the server.
	"""
	blk_list = ['.DS_Store', '.git', '.obsidian'] 
	abs_path = Path(local_dir).resolve()

	raw_hell = []
	hell_dirs = []

	if abs_path.exists():
		for item in abs_path.rglob('*'):
			path_str = item.resolve().as_posix()
			if any(blocked in path_str for blocked in blk_list):
				continue # skip item if blkd item in path
			else:
				if item.is_file():
					frp = item.relative_to(abs_path).as_posix()
					# hasher = hashlib.sha256() # 512 technically would b faster - slower in the db though
					hasher = xxhash.xxh64()

					with open(item, 'rb') as f:
						while chunk := f.read(8192):
							hasher.update(chunk) # stream

					hash_id = hasher.digest() # - BINARY(32)

					raw_hell.append((frp, hash_id)) # hash_id.hex()
					logger.info(f"Recorded path & hash for file: {item.name}.")

				elif item.is_dir():
					drp = item.relative_to(abs_path).as_posix()

					hell_dirs.append((drp,)) # keep the empty list of dirs
					logger.info(f"Recorded path for directory: {item.name}.")
				else:
					continue
	else:
		logger.info('Local directory does not exist.')
		return raw_hell, hell_dirs, abs_path

	logger.info('Collected local paths and hashes.')
	return raw_hell, hell_dirs, abs_path

# collecting server data for comparison

def scope_rem(conn): # all
	"""Select and return every single relative path and hash from the notes table. Returned as a list of tuples (rel_path, hash_id)."""
	q = f"SELECT frp, hash_id FROM notes;"

	with conn.cursor() as cursor:
		try:
			cursor.execute(q)

		except ConnectionError as c:
			logger.error(f"Connection Error encountered while attempting to collect file data from server: {c}.", exc_info=True)
			sys.exit(1)
		else:
			raw_heaven = cursor.fetchall()
			logger.info('Collected remote paths & hashes.')
			
			return raw_heaven


def ping_cass(conn): # all
	"""Ping the kid Cass because you just need a quick hand with the directories. If a directory is empty or contais only subdirectories and no files,
	Cass is the kid to clutch it. He returns a list of directories as tuples containing their relative paths.
	"""
	q = "SELECT * FROM directories;" # drp's

	with conn.cursor() as cursor:
		try:
			cursor.execute(q)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Exception encountered while attempting to collect directory data from server: {c}.", exc_info=True)
			raise
		else:
			heaven_dirs = cursor.fetchall()
			logger.info('Collected directories from server.')

			return heaven_dirs

# COMPARING

def contrast(raw_heaven, raw_hell): # unfiform for all scripts
	"""Accepts two lists of tupled pairs which each hold a files relative path and hash. It makes a list of the first item for every item in each list; 
	every file's relative path. It compares these lists to get the files that are remote-only and local-only and makes each one into a dictionary with 
	the same key for every item: 'frp'. Then, for the files that are in both locations, they ar emade into a new dictionary containing each file's 
	respective hash and relative path. Using their key values, each item in the local directory's hash is compared to the remote file's hash. If a 
	discrepancy is found, it is added to the same dictionary key values as the first two result sets: 'frp'. 'frp' is the substitution key for the 
	mysql queries these lists of dictionaries will be used for.
	"""
	heaven_souls = {s[0] for s in raw_heaven}
	hell_souls = {d[0] for d in raw_hell}

	cherubs = [{'frp':cherub} for cherub in heaven_souls - hell_souls] # get - cherubs as a dict: 'frp'
	serpents = [{'frp':serpent} for serpent in hell_souls - heaven_souls]

	people = heaven_souls & hell_souls # those in both - unaltered
	souls = []
	stags = []

	heaven = {lo: id for lo, id in raw_heaven if lo in people}
	hell = {lo: id for lo, id in raw_hell if lo in people}

	for key in hell:
		if hell[key] != heaven[key]:
			souls.append({'frp': key}) # get - souls as a dict: 'frp'
		else:
			stags.append(key)

	logger.info('Contrasted collections and id\'d discrepancies.')
	return cherubs, serpents, stags, souls # files in server but not present, files present not in server, files, in both, in both but with hash discrepancies


def compare(heaven_dirs, hell_dirs): # all
	"""Makes a set of each list of directories and formats them each into a dictionary. It compares the differences and returns a list of remote-only and local-only directories."""
	heaven = set(heaven_dirs)
	hell = set(hell_dirs)

	gates = [{'drp':gate[0]} for gate in heaven - hell]
	caves = [{'drp':cave[0]} for cave in hell - heaven]

	logger.info('Compared directories & id\'d discrepancies.')
	return gates, caves # dirs in heaven not found locally, dirs found locally not in heaven

# [atomically] edit local data; rosa_get mostly

@contextlib.contextmanager
def fat_boy(abs_path):
	"""Context manager for temporary directory and backup."""
	tmp_ = None
	backup = None

	try:
		tmp_, backup = configure(abs_path)
		yield tmp_, backup # return these & freeze in place

	except (Exception, KeyboardInterrupt, ConnectionError) as e:
		logger.critical(f"Error encountered while attempting atomic wr: {e}.", exc_info=True)
		try:
			_lil_guy(abs_path, backup, tmp_)
		finally:
			raise
	else:
		try:
			apply_atomicy(tmp_, abs_path, backup)
			logger.info('Atomic write complete.')

		except:
			logger.critical('Error encountered while attempting to apply atomicy.', exc_info=True)
			try:
				_lil_guy(abs_path, backup, tmp_)
			finally:
				raise


def _lil_guy(abs_path, backup, tmp_):
	"""Handles recovery on error for the context manager fat_boy."""
	try:
		if backup and backup.exists():
			if abs_path.exists():
				shutil.rmtree(abs_path)
				logger.info('Removed damaged attempt.')
			backup.rename(abs_path)
			logger.info('Moved backup back to original location.')
		if tmp_ and tmp_.exists():
			shutil.rmtree(tmp_)
			logger.info('Removed damaged temporary directory.')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"Replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
		raise
	else:
		logger.info('Cleanup complete.')


def calc_batch(conn):
	"""Get the average row size of the notes table to estimate optimal batch size for downloading."""
	batch_size = 5

	with conn.cursor() as cursor:
		try:
			cursor.execute(ASSESS)
			row_size = cursor.fetchone()

		except ConnectionError as c:
			logger.error('Connection Error encountered while attempting to find avg_row_size.', exc_info=True)
			raise
		else:
			if row_size:
				if row_size[0]:
					batch = int( MAX_ALLOWED_PACKET / row_size[0] )
					batch_size = max(1, batch)
					logger.info(f"Batch size determined: {batch_size}.")
					return batch_size
				else:
					logger.info('Using default batch size of 5.')
					return batch_size
			else:
				logger.info('Using default batch size of 5.')
				return batch_size

			# if row_size[0] or row_size:
			#     batch_size = int( MAX_ALLOWED_PACKET / row_size[0] ) + 1
			#     # batch_size = int( 0.25 * 1024 * 1024 / row_size[0] )
			#     logger.info('Determined batch size.')
			#     return batch_size
			# else:
			#     batch_size = 20
			#     return batch_size


def configure(abs_path):
	"""Configure the temporary directory & move the original to a backup location. 
	Returns the _tmp directory's path.
	"""
	try:
		if abs_path.exists():
			tmp_ = Path(tempfile.mkdtemp(dir=abs_path.parent))
			backup = Path( (abs_path.parent) / f"Backup_{datetime.datetime.now(datetime.UTC).timestamp()}" )

			abs_path.rename(backup)
			logger.info('Local directory renamed to backup.')
		else:
			abs_path.mkdir(parents=True, exist_ok=True)

			tmp_ = Path(tempfile.mkdtemp(dir=abs_path.parent))
			backup = Path( (abs_path.parent) / f"Backup_{datetime.datetime.now(datetime.UTC).timestamp()}" )

			abs_path.rename(backup) # return empty dir for consistency
			logger.info('Local directory not found so placeholder made for consistency.')
	
	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"Exception encountered while trying move {abs_path} to a backup location: {e}.", exc_info=True)
		raise
	else:
		logger.info('Temporary directory created & original directory moved to backup without exception.')
		return tmp_, backup


def save_people(people, backup, tmp_):
	"""Hard-links unchanged files present in the server and locally from the backup directory (original) 
	to the _tmp directory. Huge advantage over copying because the file doesn't need to move."""
	for person in people:
		curr = Path( backup / person )
		tmpd = Path( tmp_ / person )
		try:
			os.link(curr, tmpd)

		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.critical(f"Exception encountered while attempting to link unchanged files: {e}.", exc_info=True)
			raise
		else:
			logger.info('Linked unchanged file without exception.')


def download_batches(flist, conn, batch_size, tmp_): # get
	"""Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
	dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
	This function passes the found data to the wr_data function, which writes the new data structure to the disk.
	"""
	paths = [item['frp'] for item in flist]
	params = ', '.join(['%s']* len(paths))

	batch_size = batch_size
	offset = 0

	with conn.cursor() as cursor:
		try:
			while True:
				query = f"SELECT frp, content FROM notes WHERE frp IN ({params}) LIMIT {batch_size} OFFSET {offset};"

				try:
					cursor.execute(query, paths)
					batch = cursor.fetchall()
					logger.info('Got one batch of data.')

				except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
					logger.critical(f"Error while trying to download data: {c}.", exc_info=True)
					raise
				else:
					if batch:
						wr_batches(batch, tmp_)
						logger.info('Wrote batch to disk.')

					if len(batch) < batch_size:
						break

					offset += batch_size

		except: # tout de monde
			logger.critical('Error while attempting batched atomic write.', exc_info=True)
			raise
		else:
			logger.info('Atomic wr w batched download complete.')


def wr_batches(data, tmp_):
	"""Writes each batch to the _tmp directory as they are pulled. Each file has it and its parent directory flushed from memory for assurance of atomicy."""
	dcmpr = zstd.ZstdDecompressor() # init outside of loop; duh
	unflushed = set() # only flush each dir once; no repeats

	for frp, content in data:
		try:
			t_path = Path ( tmp_ / frp ) #.resolve()
			(t_path.parent).mkdir(parents=True, exist_ok=True)

			d_content = dcmpr.decompress(content)

			with open(t_path, 'wb') as t:
				t.write(d_content)
				t.flush()
				os.fsync(t.fileno())
				unflushed.add(t_path.parent)

				# if os.name == 'posix': # MOVED OUTSIDE OF LOOP
				# 	try: # test 1 time: 10.563706874847412 seconds - compared to outside-of-loop flush: 10.129662036895752.
				# 		idp = os.open(t_path.parent, os.O_RDONLY)
				# 		os.fsync(idp)
				# 	except:
				# 		logger.critical('Exception while writing batches to disk.', exc_info=True)
				# 		raise
				# 	else:
				# 		logger.info(f"Wrote & flushed {t_path}'s file & parent without exception.")
				# 	finally:
				# 		os.close(idp)

		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.critical(f"Exception encountered while attempting atomic wr: {e}.", exc_info=True)
			raise
		except:
			logger.critical('Exception encountered while attempting atomic wr.', exc_info=True)
			raise

	# moving this outside of the file loop sped up writing during downloading a lot
	# not a great test bc this pc also hosts the server, but it wrote 3 gb in under 10s
	for parent in unflushed: # for every [not repeated] p_dir in unflushed: flush it
		if os.name == 'posix':
			try:
				idp = os.open(parent, os.O_RDONLY)
				os.fsync(idp)
			except:
				logger.critical('Exception occured while attempting atomic wr; aborting.')
				raise
			else:
				logger.info('Parent of newly written file flushed w.o exception.')
			finally:
				if idp:
					os.close(idp)


def apply_atomicy(tmp_, abs_path, backup):
	"""If the download and write batches functions both complete entirely without error, this function moved the _tmp directory back to the original abs_path. 
	If this completes without error, the backup is deleted.
	"""
	try:
		tmp_.rename(abs_path)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"Exception encountered while attempting atomic write: {e}.", exc_info=True)
		raise
	except:
		logger.critical(f"System Error encountered while attempting atomic write.", exc_info=True)
		raise
	else:
		logger.info('Temporary directory renamed without exception.')
		if backup.exists():
			shutil.rmtree(backup)
			logger.info('Removed backup after execution without exception.')


def mk_dir(gates, abs_path):
	"""Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
	try:
		for gate in gates:
			path = gate['drp']
			fdpath = (abs_path / path ).resolve()
			fdpath.mkdir(parents=True, exist_ok=True)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(F"Permission Error when tried to make directories: {e}.", exc_info=True)
		raise
	else:
		logger.info('Created directory structure on disk without exception.')

# EDIT SERVER - rosaGIVE

# deletes

def rm_remdir(conn, gates): # only give 3.0
	"""Remove remote-only directories from the server. Paths [gates] passed as list of dictionaries for executemany(). This, and every other call to 
	make an edit on the database, is rolled back on errors. Its only for specific calls to change the information in them. The default with mysql - conn
	is to roll back on disconnect, so its an ok safety net, but its supposed to be all over this shit.
	"""
	g = "DELETE FROM directories WHERE drp = %(drp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(g, gates)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered when trying to delete directory[s] from server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Removed remote-only directory[s] from server.')


def rm_remfile(conn, cherubs): # only give 3.0
	"""Remove remote-only files from the server. Paths [cherubs] passed as a list of dictionaries for executemany()."""
	f = "DELETE FROM notes WHERE frp = %(frp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(f, cherubs)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered when trying to delete file[s] from server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Removed remote-only file[s] from server.')

# uploads

def collect_info(dicts_, abs_path): # give - a
	"""For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
	This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
	be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany().
	For batched uploads, the script reads the files to get their size so it can optimize queries-per-execution within the 
	limitation for packet sizes. Pretty inneficient because reading every file just for its size when we already have the content 
	in memory is a waste.
	"""
	curr_batch = 0
	total_size = 0

	batch_items = []
	all_batches = []
	
	for i in dicts_:
		frp = i['frp']
		item = ( abs_path / frp ).resolve()

		if item.is_file():
			# content = item.read_bytes() # this is just so ass
			# # could i do a faster t/f check if the file is over a certain size?
			# # dif measurement or format of measure like blocks of storage or maybe a meta_data obj I can ping easier
			# # only thing is this allows the cpu to read one file at a time; if I appended the content and passed it through for the collect_data function, 
			# # then every single files' content would need to be loaded into memory at once
			# size = len(content)# sys.getsizeof(content)
			size = os.path.getsize(item)

			total_size += size

			if size > MAX_ALLOWED_PACKET:
				logger.error(f"A single file is larger than the maximum packet size allowed: {item}.")
				raise

			elif (curr_batch + size) > MAX_ALLOWED_PACKET:
				all_batches.append((batch_items,))

				batch_items = [i]
				curr_batch = size

			else:
				batch_items.append(i)
				curr_batch += size
		else:
			continue
	
	if batch_items:
		all_batches.append((batch_items,))

	return all_batches, total_size


def collect_data(dicts_, abs_path): # give - redundant
	"""For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
	This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
	be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany(). Works with the 
	output of the collect_info function in terms of data type & format.
	"""
	c_size = 0
	item_data = []

	for x in dicts_: # for tuple in list of tuples generated by collect_info
		for i in x: # for relative path in the list of relative paths from the tuple
			frp = i['frp']
			item = ( abs_path / frp ).resolve()

			if item.is_file():
				content = item.read_bytes()

				# cmpr = zstd.ZstdCompressor(level=1) # upload time [3 gb]: 40.107
				# cmpr = zstd.ZstdCompressor(level=2) # upload time [3 gb]: 42.877
				cmpr = zstd.ZstdCompressor(level=3) # upload time [3 gb]: 37.825
				# cmpr = zstd.ZstdCompressor(level=4) # upload time [3 gb]: 38.387
				c_content = cmpr.compress(content)

				size = len(c_content)
				c_size += size # this and the line above are only for the size comparison; this should be moved to the collect_info fx for DRY

				# hasher = hashlib.sha256()
				hasher = xxhash.xxh64()

				with open(item, 'rb') as f:
					while chunk := f.read(8192):
						hasher.update(chunk)

				hash_id = hasher.digest()

				item_data.append({
					'content': c_content,
					'hash_id': hash_id,
					'frp': frp
				})

				logger.info('Generated query to upload local-only file.')
			else:
				continue

	return item_data, c_size


def upload_dirs(conn, caves): # give
	"""Insert into the directories table any local-only directories found."""
	h = "INSERT INTO directories (drp) VALUES (%(drp)s);"
	with conn.cursor() as cursor:
		try:
			cursor.executemany(h, caves)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered while attempting to upload [new] directory[s] to server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Uploaded local-only directory[s] to server.')


def upload_created(conn, serpent_data): # give
	"""Insert into the notes table the new record for local-only files that do not exist in the server. *This function triggers no actions in the database*."""
	# i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, UNHEX(%(content)s), UNHEX(%(hash_id)s));" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));"
	i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, %(content)s, %(hash_id)s);" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));" # don't need to hex/unhex for binary regardless

	with conn.cursor() as cursor:
		try:
			cursor.executemany(i, serpent_data)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered while attempting to upload [new] file[s] to server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Uploaded local-only file[s] to server.')


def upload_edited(conn, soul_data): # only give 3.0
	"""Update the notes table to show the current content for a note that was altered, or whose hash did not show identical contents. *This function 
	triggers the on_update_notes trigger which will record the previous version of the file's contents and the time of changing.
	"""
	# j = "UPDATE notes SET content = UNHEX(%(content)s), hash_id = UNHEX(%(hash_id)s) WHERE frp = %(frp)s;"
	j = "UPDATE notes SET content = %(content)s, hash_id = %(hash_id)s WHERE frp = %(frp)s;"

	with conn.cursor() as cursor:
		try:
			cursor.executemany(j, soul_data)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered while attempting to upload altered file to server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Uploaded altered file[s]\'s content & new hash.')

# USER INPUT & HANDLING

def confirm(conn): # give
	"""Double checks that user wants to commit any changes made to the server. Asks for y/n response and rolls-back on any error or [n] no."""
	confirm = input("Commit changes to server? y/n: ")

	if confirm in ('y', 'Y', 'yes', 'Yes', 'YES', 'yeah', 'i guess', 'I guess', 'i suppose', 'I suppose'):
		try:
			conn.commit()

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered while attempting to commit changes to server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Commited changes to server.')

	elif confirm in ('n', 'N', 'no', 'No', 'NO', 'nope', 'hell no', 'naw'):
		try:
			conn.rollback()

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"Connection Error encountered while attempting to rollback changes to server: {c}.", exc_info=True)
			raise
		else:
			logger.info('Changes rolled back.')
	else:
		logger.error('Unknown response; rolling server back.')
		raise