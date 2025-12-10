"""Assesses the states and compares contents.

Scopes the local directory for hashes & paths.
Queries the server for the same & compare result sets.
If exist in both, compare their hashes. Return results.
"""

import sys
import time
import logging
import datetime
from pathlib import Path

import xxhash # can be replaced with hashlib
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm as tqdm_

from rosa.confs import LOCAL_DIR, RED, RESET


logger = logging.getLogger('rosa.log')

# COLLECTING LOCAL DATA

def scope_loc(local_dir):
	"""Walks the given directory through and records all the files (full paths) and directories (relative paths).

	Args:
		local_dir: The LOCAL_DIR variable from config.py is usually passed here; full path of a directory on the local machine.
	
	Returns:
		A 3-element tuple containing:
			raw_paths (list): Every files' full path from the given directory.
			hells_dirs (list): Single-element tuple of every sub-directories' relative path within the local_dir.
			abs_path (Path): The LOCAL_DIR's full path.
	"""
	# blk_list = ['.DS_Store', '.git', '.obsidian'] # should be imported from the config.py file
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
	else:
		logger.debug('scoping of local directory completed')
		return raw_paths, hell_dirs, abs_path

def hash_loc(raw_paths, abs_path):
	"""For every file, hash it, and tuple it with the relative path.
	
	Args:
		raw_paths (list): Every files' full path (collected / passed from scope_loc().
		abs_path (Path): LOCAL_DIR's full path as a Pathlib object.
	
	Returns:
		raw_hell (list): Tupled (relative paths, hashes) for every file found in scope_loc().
	"""
	hasher = xxhash.xxh64()
	raw_hell = []

	logger.debug('...hashing...')

	with tqdm_(loggers=[logger]):
		with tqdm(raw_paths, unit="hashes", leave=True) as pbar:
			for item in pbar:
				hasher.reset()
				hasher.update(item.read_bytes())

				hash_id = hasher.digest()
				frp = item.relative_to(abs_path).as_posix()

				raw_hell.append((frp, hash_id))

	return raw_hell

def track_loc(raw_paths, abs_path):
	"""For every file, hash it and tuple it with the files' st_mtime.
	
	Args:
		raw_paths (list): Every files' full path (collected / passed from scope_loc().
		abs_path (Path): LOCAL_DIR's full path as a Pathlib object.
	
	Returns:
		raw_local_times (list): Tupled (relative paths, st_ctimes) for every file found in scope_loc().
	"""
	raw_local_times = []

	logger.debug('...hashing...')

	with tqdm_(loggers=[logger]):
		with tqdm(raw_paths, unit="hashes", leave=True) as pbar:
			for item in pbar:
				dob = item.stat().st_ctime

				dob_str = datetime.datetime.fromtimestamp(dob)
				# dob_str = dob.strftime('%Y-%m-%D-%H:%M:%S')

				frp = item.relative_to(abs_path).as_posix()

				raw_local_times.append((frp, dob_str))

	return raw_local_times

# COLLECTING REMOTE DATA [AND SOME]

def scope_rem(conn):
	"""SELECTS every relative path and hash_id currently recorded in the table.

	Args:
		conn: Connection object.
	
	Returns:
		raw_heaven (list): Tupled (relative paths, hash_ids) for every file recorded in the table.
	"""
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

		except (ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_heaven

def ping_rem(conn):
	"""SELECTS every relative path currently recorded in the table.

	Args:
		conn: Connection object.
	
	Returns:
		raw_heaven (list): Single-item tuples (relative paths,) for every file recorded in the table.
	"""
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

		except (ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_heaven

def track_rem(conn):
	"""SELECTS every file's time-of-last_edit currently recorded in the table.

	Args:
		conn: Connection object.

	Returns:
		raw_remote_times (list): Tupled (relative_paths, time-of-last_edits) for every file in the table.
	"""
	q = "SELECT frp, tol_edit FROM notes;"

	with conn.cursor() as cursor:
		try:
			logger.debug('...scoping remote files...')
			cursor.execute(q)
			raw_remote_times = cursor.fetchall()
			if raw_remote_times:
				logger.debug("server returned data from query")
			else:
				logger.warning("server returned raw_remote_times as an empty set")

		except (ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_remote_times

def ping_cass(conn):
	"""SELECTS * (everything) from the directories table.

	Args:
		conn: Connection object.

	Returns:
		heaven_dirs (list): Single-item tuples (relative paths,) for every directory in the directories table.
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

		except (ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to collect directory data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return heaven_dirs

# COMPARING

def contrast(remote_raw, local_raw):
	"""Takes lists of tupled pairs (relative paths, hashes) for every file found locally and remotely, and compares them. 
	
	Uses sets for uniqueness & dictionaries for accuracy (paths as the keys because they are unique).

	Args:
		remote_raw (list): Tupled (relative file paths, hashes) from scope_rem().
		local_raw (list): Tupled (relative file paths, hashes) from scope_loc() and hash_loc().

	Returns:
		A 4-element tuple containing:
			remote_only (set): Relative paths found by subtracting remote_keys from local_keys
			deltas (list): Files present in both (local_set & remote_set) but whose hashes were inequal.
			nodiffs (list): Files present in both sets but whose hashes were equal.
			local_only (set): Relative paths found by subtracting local_keys from remote_keys
	"""
	remote = {file_path: hash_id for file_path, hash_id in remote_raw} # map each file to its hash in a dictionary
	local = {file_path: hash_id for file_path, hash_id in local_raw} # makes comparison easier

	remote_files = set(remote.keys()) # get a set of the keys() in the dictionaries
	local_files = set(local.keys()) # which are just both sets of files' relative paths

	remote_only = remote_files - local_files
	local_only = local_files - remote_files

	both = remote_files & local_files # those in both (people) # unchanged from original

	logger.debug(f"found {len(remote_only)} cherubs, {len(local_only)} serpents, and {len(both)} people. comparing each persons' hash now")

	deltas = []
	nodiffs = []

	for rel_path in both:
		if local.get(rel_path) == remote.get(rel_path):
			nodiffs.append(rel_path) # unchanged, hash verified
		else:
			deltas.append(rel_path)

	logger.debug(f"found {len(deltas)} altered files [failed hash verification] and {len(nodiffs)} unchanged file[s] [hash verified]")

	return remote_only, deltas, nodiffs, local_only

def compare(heaven_dirs, hell_dirs):
	"""Take the lists of tupled-directories (relative_paths,) and compare their sets.

	Args:
		heaven_dirs (list): Single-item tuples containing their relative paths.
		hell_dirs (list): Single-item tuples containing their relative paths.

	Returns:
		A 3-element tuple containing:
			gates (list): Single-item tuples containing remote-only directories' relative paths.
			caves (list): Single-item tuples containing local-only directories' relative paths.
			ledeux (set): Directories' relative paths if present in the server & locally.
	"""
	heaven = set(heaven_dirs)
	hell = set(hell_dirs)

	gates = [((gate[0],)) for gate in heaven - hell] # used by both so tuple format is useful
	caves = [((cave[0],)) for cave in hell - heaven]

	ledeux = heaven & hell # present in both (ledeux)

	logger.debug(f"found {len(gates)} gates [server-only], {len(caves)} caves [local-only], and {len(ledeux)} ledeux's [found in both]")
	return gates, caves, ledeux # dirs in heaven not found locally, dirs found locally not in heaven

def track(raw_remote_times, raw_local_times):
	"""Takes the list of tuples (relative paths, edit-times) and compares them to identify timing differences and possible file discrepancies.

	Args:
		raw_remote_times (list): Tupled (relative paths, time-of-last_edits) for every file in the table.
		raw_local_times (list): Tupled (relative paths, st_ctimes) for every file in the local directory.
	
	Returns:
		None; purely logging output made from this assessment, no functionality incorporated.
	"""
	remote = {rel_path: tole for rel_path, tole in raw_remote_times}
	local = {rel_path: ctime for rel_path, ctime in raw_local_times}

	remotes = remote.keys()
	locales = local.keys()

	remote_only = remotes - locales
	local_only = locales - remotes

	ledeux = remotes & locales

	logger.info(f"initial assessment revealed {len(remote_only)} remote only files, {len(local_only)} local only files, and {len(ledeux)} files in both places")

	remote_mods = []
	local_mods = []
	nodiffs = []

	for rel_path in ledeux:
		if remote[rel_path] > local[rel_path]:
			remote_mods.append(rel_path)
		elif remote[rel_path] < local[rel_path]:
			local_mods.append(rel_path)
		else:
			nodiffs.append(rel_path)
	
	if (len(remote_mods) + len(local_mods)) > 0:
		logger.info(f"tracking times revealed {len(remote_mods)} (files with server updates not seen locally), {len(local_mods)} (unbacked up local changes), and {len(nodiffs)} (unchanged files (via ctime))")
	else:
		logger.info('timer detected no changes between the sources')

def diffr(conn): # requires conn as argument so phones doesn't need to be imported
	"""Main diff'ing engine. 
	
	Uses the connection and environment variables in config.py to assess changes and identify discrepancies.
	Returns changes found as relative paths in lists of the specific change found; see Returns.

	Args:
		conn: Connection object.

	Returns:
		A 2-element tuple containing:
			data (2-element tuple): Contains two tupled lists:
				file_data (tuple): (remote_only (set), deltas (list), nodiffs (list), local_only(set))
					containing relative paths of files, each identifying a type of discrepancy
				dir_data (tuple): (remote_only (list), local_only (list), ledeux (set))
					containing relative paths of directories, identified by the same.
			diff (bool): Variable stating if discrepancies were discovered or not.
	"""
	diff = False
	data = ([], [])
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

				logger.info('contrasting file[s]...')
				remote_only, deltas, nodiffs, local_only = contrast(raw_heaven, raw_hell)

				logger.info('comparing directory[s]...')
				gates, caves, ledeux = compare(heaven_dirs, hell_dirs)

				discoveries = ((remote_only, local_only, deltas, gates, caves))

				if any(discoveries):
					diff = True
					logger.info('discrepancies discovered')

					file_data = remote_only, deltas, nodiffs, local_only
					dir_data = gates, caves, ledeux
					data = file_data, dir_data

				else:
					logger.info('no diff!')
			else:
				logger.error(f"no paths returned from scan of {abs_path}. does it have any files?")
				sys.exit(1)
		else:
			logger.info('no heaven data; have you uploaded?')
			sys.exit(1)

	except (ConnectionError, KeyboardInterrupt, Exception) as e:
		logger.error(f"{RED}err caught while diff'ing data:{RESET} {e}.", exc_info=True)
		sys.exit(1)

	return data, diff #, mini

def timer(conn):
	"""Test function to assess changes based on filesystem metadata (st_mtime/ctime).

	Args:
		conn: Connection object.
	
	Returns:
		None
	"""
	raw_paths, hell_dirs, abs_path= scope_loc(LOCAL_DIR)
	raw_local_times = track_loc(raw_paths, abs_path)

	raw_remote_times = track_rem(conn)

	track(raw_remote_times, raw_local_times)