import sys
import time
import logging
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm as tqdm_
import mysql.connector
import xxhash # can be replaced with hashlib

from rosa.confs import LOCAL_DIR, RED, RESET

"""
Assesses the state of the local directory, does the same for the server, and compares 
the two. [ diffr() ] is an engine for all three steps. It is used by: [get, give, & diff].

[functions]
scope_loc(local_dir),
scope_rem(conn), ping_cass(conn), and ping_rem(conn),
hash_loc(raw_paths, abs_path),
contrast(remote_raw, local_raw),
compare(heaven_dirs, hell_dirs),
diffr(conn)
"""

logger = logging.getLogger('rosa.log')

# COLLECTING LOCAL DATA

def scope_loc(local_dir): # all
	"""Collects the full paths of all files and the relative path of all directories from the given directory. The simplified blocking
	logic just skips the item if its path has *any of the blocked items [config.py] within it. So if a file is in .git, it, and everything
	else in there, will be ignored because their paths contain the blocked item. Same for files like .DS_Store, if the file has '.DS_Store'
	anywhere in its path, it will be ignored. Returns the files' paths, directories' relative paths, and the directory's pathlib object.
	*This used to also generate and then hex the hash, but hexing is no longer used & hashing logic is now segregated from file-searching.
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
	"""For every file, hash it and tuple it with the relative path."""
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

# COLLECTING REMOTE DATA [AND SOME]

def scope_rem(conn): # thinking all fx's that use conn to do _ w.the server should be together in a file (except most of technician & contractor's downloads because they are specialties of those scripts]
	"""
	Select and return every single relative path and hash from the 
	notes table. Returned as a list of tuples: (rel_path, hash_id).
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

		except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_heaven

def ping_rem(conn): # ditto
	"""
	Select and return every single files' relative path from the 
	notes table. Returned as a list of single-item tuples: (frp,).
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

		except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"{RED}err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			return raw_heaven

def ping_cass(conn): # tritto
	"""
	Ping the kid Cass because you just need a quick hand with the directories. If a directory is empty or contais only subdirectories and no 
	files, Cass is the kid to clutch it. He returns a list of directories as tuples containing their relative paths. He solved a problem no 
	longer present, but the function is still useful and implemented for assurance the directory tree is a 1:1 replica of the local directory.
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
	"""
	Takes the raw tuples returned from scope_rem() & ping_cass(). There is an initial sorting of the paired items into a dictionary for the remote 
	and local files, which makes comparison much easier. The initial comparison is done by pulling the .keys() from both dictionaries and finding which
	files, if any, are missing from the server AND/OR the local directory. Remote-only files' relative path was not in the directory's set, and vice 
	versa for the local-only files. For all the files' relative paths present in the server AND the local directory, the original dictionary uses their 
	path to 'look up' the files' hash and identifies files whose hashes' were unequal.
	Remote, local, and unverified files get output as a list of dictionaries for mysql_connector formatting, uploading, or updating.
	"""
	remote = {file_path: hash_id for file_path, hash_id in remote_raw} # map each file to its hash in a dictionary
	local = {file_path: hash_id for file_path, hash_id in local_raw} # makes comparison easier

	remote_files = set(remote.keys()) # get a set of the keys() in the dictionaries
	local_files = set(local.keys()) # which are just both sets of files' relative paths

	remote_only = [((cherub,)) for cherub in remote_files - local_files] # will have to double check that tuples are needed here
	local_only = [((serpent,)) for serpent in local_files - remote_files]

	# TUPLED 
	# remote_only = [(cherub,) for cherub in remote_files - local_files] # will have to double check that tuples are needed here
	# local_only = [(serpent,) for serpent in local_files - remote_files]
	both = remote_files & local_files # those in both (people) # unchanged from original

	# remote_only = [{'frp':cherub} for cherub in remote_files - local_files] # remote-only (cherubs) # original
	# local_only = [{'frp':serpent} for serpent in local_files - remote_files] # local-only (serpents)

	logger.debug(f"found {len(remote_only)} cherubs, {len(local_only)} serpents, and {len(both)} people. comparing each persons' hash now")

	deltas = []
	nodiffs = []

	for rel_path in both:
		if local.get(rel_path) == remote.get(rel_path):
			nodiffs.append((rel_path,)) # unchanged, hash verified
		else:
			deltas.append((rel_path,))

	# for file_path in both:
	# 	if local.get(file_path) == remote.get(file_path):
	# 		nodiffs.append(file_path) # unchanged, present in both (stags)
	# 	else:
	# 		deltas.append({'frp': file_path}) # transient, like water, buddy (souls)

	logger.debug(f"found {len(deltas)} altered files [failed hash verification] and {len(nodiffs)} unchanged file[s] [hash verified]")

	return remote_only, deltas, nodiffs, local_only


def compare(heaven_dirs, hell_dirs): # all
	"""
	Makes a set of each list of directories and formats them each into a dictionary very similarly to contrast. It 
	compares the differences and returns a list of remote-only and local-only directories as a list of dictionaries.
	"""
	heaven = set(heaven_dirs)
	hell = set(hell_dirs)

	gates = [((gate[0],)) for gate in heaven - hell]
	caves = [((cave[0],)) for cave in hell - heaven]

	# gates = [{'drp':gate[0]} for gate in heaven - hell] # remote-only (gates)
	# caves = [{'drp':cave[0]} for cave in hell - heaven] # local-only (caves)

	ledeux = heaven & hell # present in both (ledeux)

	logger.debug(f"found {len(gates)} gates [server-only], {len(caves)} caves [local-only], and {len(ledeux)} ledeux's [found in both]")

	logger.debug('compared directories & id\'d discrepancies')
	return gates, caves, ledeux # dirs in heaven not found locally, dirs found locally not in heaven


def diffr(conn): # requires conn as argument so phones doesn't need to be imported
	"""
	[get], [give], and [diff] use this as their main fx. It was being repeated across many files and moved here for uniformity across the scripts.
	Having nomix passed to it makes the logging much cleaner and much clearer. Only weird thing is mini_ps() needs to be imported here, so this
	file will likely receive additional fx's or this fx will be moved, TBD. Sike, this design sucks, too weird to follow and not worth the trouble.
	"""
	diff = False
	data = ([], [])

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

				discoveries = ((remote_only, local_only, deltas, gates, caves))

				for returns in discoveries: # quick assert
					assert all(isinstance(returned, tuple) for returned in returns)

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
		logger.error(f"{RED}err caught while diff'ing directories:{RESET} {e}.", exc_info=True)
		sys.exit(1)

	return data, diff #, mini