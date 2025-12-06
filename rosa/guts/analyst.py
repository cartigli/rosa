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

# COLLECTING LOCAL DATA

logger = logging.getLogger('rosa.log')

def scope_loc(local_dir): # all
	"""Collect the relative path and 256-bit hash for every file in the given directory. Ignore any files with paths that contain
	the '.DS_Store', '.git', or '.obsidian'. Record the hash in BINARY format. Hex for python ( and UNHEX for the sql queries ). 
	Every file that meets the criteria has its relative path and hash generated and recorded. Returns the pairs of paths/hashes 
	in a tuple to match data returned from the server.
	"""
	blk_list = ['.DS_Store', '.git', '.obsidian'] 
	abs_path = Path(local_dir).resolve()

	raw_paths = []
	hell_dirs = []

	try:
		if abs_path.exists():
			logger.debug('...scoping local directory...')
			for item in abs_path.rglob('*'):
				path_str = item.resolve().as_posix()
				if any(blocked in path_str for blocked in blk_list):
					# logger.debug(f"{item} was rejected due to blocked_list (config)")
					continue # skip item if blkd item in its path
				elif item.is_file():
					# logger.debug('log jam')
					raw_paths.append(item) # the full paths 

				elif item.is_dir():
					drp = item.relative_to(abs_path).as_posix()
					hell_dirs.append((drp,)) # ditto for the dirs but in tuples & rel_paths
		else:
			logger.warning('local directory does not exist')
			sys.exit(1)

	except Exception as e:
		logger.error(f"{RED}err:{RESET} {e} {RED}while scoping locally; aborting{RESET}", exc_info=True)
		raise
	except KeyboardInterrupt as ko:
		logger.warning("boss killed it; wrap it up")
		sys.exit(0)

	logger.debug('finished collecting local paths')
	return raw_paths, hell_dirs, abs_path

def hash_loc(raw_paths, abs_path):
	hasher = xxhash.xxh64()
	raw_hell = []

	logger.debug('...hashing...')

	with tqdm(raw_paths, unit="hashes", leave=True) as pbar:
		for item in pbar:
			hasher.reset()
			hasher.update(item.read_bytes())
			hash_id = hasher.digest()

			frp = item.relative_to(abs_path).as_posix()

			raw_hell.append((frp, hash_id))

	return raw_hell


# COLLECTING SERVER DATA


def scope_rem(conn): # all
	"""Select and return every single relative path and hash from the notes table. Returned as a list of tuples (rel_path, hash_id)."""
	q = "SELECT frp, hash_id FROM notes;"

	with conn.cursor() as cursor:
		try:
			logger.debug('...scoping remote files...')
			cursor.execute(q)
			raw_heaven = cursor.fetchall()
			if raw_heaven:
				logger.debug("server returned data from query")
			else:
				logger.warning("server returned raw_heaven as an empty set")

		except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_heaven

def ping_rem(conn): # all
	"""Select and return every single relative path and hash from the notes table. Returned as a list of tuples (rel_path, hash_id)."""
	q = "SELECT frp FROM notes;"

	with conn.cursor() as cursor:
		try:
			logger.debug('...scoping remote files...')
			cursor.execute(q)
			raw_heaven = cursor.fetchall()
			if raw_heaven:
				logger.debug("server returned data from query")
			else:
				logger.warning("server returned raw_heaven as an empty set")

		except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_heaven

def ping_cass(conn): # all
	"""Ping the kid Cass because you just need a quick hand with the directories. If a directory is empty or contais only subdirectories and no files,
	Cass is the kid to clutch it. He returns a list of directories as tuples containing their relative paths.
	"""
	q = "SELECT * FROM directories;" # drp's

	with conn.cursor() as cursor:
		try:
			logger.debug('...scoping remote directory[s]...')
			cursor.execute(q)
			heaven_dirs = cursor.fetchall()

			if heaven_dirs:
				logger.debug("server returned data from query")
			else:
				logger.warning("server returned heaven dirs as an empty set")

		except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to collect directory data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return heaven_dirs


# COMPARING


def contrast(remote_raw, local_raw): # unfiform for all scripts
	"""Accepts two lists of tupled pairs which each hold a files relative path and hash. It makes a list of the first item for every item in each list; 
	every file's relative path. It compares these lists to get the files that are remote-only and local-only and makes each one into a dictionary with 
	the same key for every item: 'frp'. Then, for the files that are in both locations, they ar emade into a new dictionary containing each file's 
	respective hash and relative path. Using their key values, each item in the local directory's hash is compared to the remote file's hash. If a 
	discrepancy is found, it is added to the same dictionary key values as the first two result sets: 'frp'. 'frp' is the substitution key for the 
	mysql queries these lists of dictionaries will be used for.
	"""
	remote = {file_path: hash_id for file_path, hash_id in remote_raw}
	local = {file_path: hash_id for file_path, hash_id in local_raw}

	remote_files = set(remote.keys())
	local_files = set(local.keys())

	remote_only = [{'frp':cherub} for cherub in remote_files - local_files] # get - cherubs as a dict: 'frp'
	local_only = [{'frp':serpent} for serpent in local_files - remote_files]

	both = remote_files & local_files # those in both - unaltered

	logger.info(f"found {len(remote_only)} cherubs, {len(local_only)} serpents, and {len(both)} people. comparing each persons' hash now")

	deltas = []
	nodiffs = []

	for file_path in both:
		if local.get(file_path) == remote.get(file_path):
			nodiffs.append(file_path)
		else:
			deltas.append({'frp': file_path})

	logger.info(f"found {len(deltas)} altered files [failed hash verification] and {len(nodiffs)} unchanged file[s] [hash verified]")

	return remote_only, deltas, nodiffs, local_only # files in server but not present, files present not in server, files in both, files in both but with hash discrepancies

def compare(heaven_dirs, hell_dirs): # all
	"""Makes a set of each list of directories and formats them each into a dictionary. It compares the differences and returns a list of remote-only and local-only directories."""
	heaven = set(heaven_dirs)
	hell = set(hell_dirs)

	gates = [{'drp':gate[0]} for gate in heaven - hell]
	caves = [{'drp':cave[0]} for cave in hell - heaven]

	ledeux = heaven & hell

	logger.debug(f"found {len(gates)} gates [server-only], {len(caves)} caves [local-only], and {len(ledeux)} ledeux's [found in both]")

	logger.debug('compared directories & id\'d discrepancies')
	return gates, caves, ledeux # dirs in heaven not found locally, dirs found locally not in heaven

def diffr(args, nomic):
	from rosa.guts.dispatch import mini_ps, phones

	diff = False
	data = ([], [])

	mini = mini_ps(args, nomic)
	logger = mini[0]

	with phones() as conn:
		logger.info('conn is connected; pinging heaven...')
		try:
			logger.info('...pinging heaven...')
			raw_heaven = scope_rem(conn)

			logger.info('...pinging cass...')
			heaven_dirs = ping_cass(conn)

			if any((raw_heaven, heaven_dirs)):
				logger.info('confirmed data was returned from heaven; processing...')
				raw_paths, hell_dirs, abs_path = scope_loc(LOCAL_DIR)

				if any(raw_paths):
					logger.info('...data returned from local directory; hashing file[s] found...')
					raw_hell = hash_loc(raw_paths, abs_path)

					# logger.info("file[s] hashed; proceeding to compare & contrast...")

					logger.info('contrasting file[s]...')
					remote_only, deltas, nodiffs, local_only = contrast(raw_heaven, raw_hell)

					logger.info('comparing directory[s]...')
					gates, caves, ledeux = compare(heaven_dirs, hell_dirs)

					if any((remote_only, local_only, deltas, gates, caves)):
						diff = True
						logger.info('discrepancies discovered')

						file_data = [remote_only, deltas, nodiffs, local_only]
						dir_data = [gates, caves, ledeux]
						data = (file_data, dir_data)

					else:
						logger.info('no diff!')
				else:
					logger.error(f"no paths returned from scan of {abs_path}. does it have any files?")
					sys.exit(1)
			else:
				logger.info('no heaven data; have you uploaded?')
				sys.exit(1)

		except (ConnectionError, KeyboardInterrupt, Exception) as e:
			logger.error(f"{RED}err caught while diff'ing directories:{RESET} {e}.", exc_info=True)
			sys.exit(1)

	return data, diff, mini

