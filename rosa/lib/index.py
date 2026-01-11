"""Management and interactions with the index.

Sqlite3 database w.files' ctimes, 
sizes, and path. Used to detect
files that were changed or touched.
"""

import os
import sys
import time
import shutil
import logging
import subprocess
from datetime import datetime, UTC

import xxhash
import sqlite3

from rosa.confs import SINIT, CVERSION, BLACKLIST

logger = logging.getLogger('rosa.log')

def _config():
	"""Makes the directory for the index & path for SQLite database connection.

	Args:
		None

	Returns:
		index (str): Path to the SQLite's database file.
	"""
	curr: str = os.getcwd()

	ihome: str = os.path.join(curr, ".index")
	os.makedirs(ihome, exist_ok=True)

	index: str = os.path.join(ihome, "indeces.db")

	return index

def is_ignored(_str: str = ""):
	"""Checks file paths for blocked parents & files."""
	return any(blckd in _str for blckd in BLACKLIST)

def construct(sconn: sqlite3 | None = None):
	"""Makes the SQLite tables inside the database."""
	sconn.executescript(SINIT)

def copier(origin: str = "", originals: str = ""):
	"""Backs up the current directory to the index with the 'cp -r' unix command.

	Args:
		origin (str): Path to the source directory.
		originals (str): Path to the 'originals' directory.

	Returns:
		None
	"""
	os.makedirs(originals, exist_ok=True)

	for obj in os.scandir(origin):
		if not is_ignored(obj.path):
			destination: str = os.path.join(originals, obj.name)

			if obj.is_dir(): # root level directories
				shutil.copytree(obj, destination)

			elif obj.is_file(): # root level files
				# shutil.copy2(obj, destination)
				shutil.copyfile(obj, destination)

def r(dir_: str = ""):
	"""Recursive function for files & directory paths.

	Args:
		dir_ (str): Path to a directory.

	Yields:
		obj (str): Path to an entry found.
	"""
	for obj in os.scandir(dir_):
		yield obj.path

		if os.path.isdir(obj):
			yield from _r(obj.path)

		else:
			yield obj.path

def _r(dir_: str = ""):
	"""Recursive function for files.

	Args:
		dir_ (str): Path to a directory.

	Yields:
		obj (str): A file object found.
	"""
	for obj in os.scandir(dir_):

		if os.path.isdir(obj):
			yield from _r(obj.path)

		else:
			yield obj.path

def _rd(dirx: str = ""):
	"""Recursive function for finding directories.

	Args:
		dirx (str): Path to the given directory.
	
	Yields:
		d.path (str): A directory's path.
	"""
	for d in os.scandir(dirx):

		if d.is_dir():
			yield d.path

			yield from _rd(d.path)

def _surveyor(origin: str = ""):
	"""Collects metadata for initial indexing.

	Args:
		origin (str): Path pointing to the requested directory.

	Returns:
		inventory (list): Tupled relative paths, st_ctimes, and st_sizes for every file found.
	"""
	prefix: int = len(origin) + 1

	inventory: list = []

	for file in _r(origin):
		if not is_ignored(file):
			rp: str = file[prefix:]

			stats = os.stat(file)

			ctime: int = stats.st_ctime
			size: int = stats.st_size*(10**7)

			inventory.append((rp, ctime, size))

	return inventory

def _survey(origin: str = "", version: int = None):
	"""Collects metadata for initial indexing but includes versions for the index.
	
	Args:
		origin (str): Path to the requested directory.
		version (int): Current version for the index.
	
	Returns:
		inventory (list): Tupled relative pats, versions, st_ctimes and st_sizes for all the files found.
	"""
	inventory: list = []

	prefix: int = len(origin) + 1

	for file in _r(origin):
		if not is_ignored(file.path):
			rp: str = file.path[prefix:]

			stats = os.stat(file)

			ctime: int = stats.st_ctime
			size: int = stats.st_size*(10**7)

			track: str = encoding(file.path)

			inventory.append((rp, version, version, ctime, size, track))

	return inventory

def encoding(obj: str = ""):
	"""Checks if the first mb of a file can be encoded in utf-8.

	Args:
		obj (str): File path.
	
	Returns:
		utf (str): Single-character string: "T" or "F".
	"""
	utf: str = "T"

	with open(obj, 'rb') as f:
		raw: bytes = f.read(1024*1024)

	try:
		raw.decode('utf-8')
	except UnicodeDecodeError:
		utf: str = "F"
	
	return utf

def _dsurvey(origin: str = ""):
	"""Collects the subdirectories within the requested directory.

	Args:
		origin (str): Path to the requested directory.

	Returns:
		ldrps (list): Relative paths of every directory found.
	"""
	pfx: int = len(origin) + 1
	ldrps: list = []

	for obj in _rd(origin):
		if not is_ignored(obj):
			ldrps.append(obj[pfx:])

	return ldrps

def _surveyorx(origin: str = "", rps: list = []):
	"""Collects the new metadata for altered files from a list of files.

	Args:
		origin (str): Path to the given directory.
		rps (list): Relative paths of all the altered files.

	Returns:
		inventory (list): Tupled st_ctimes, st_sizes and relative paths of every file path in rps.
	"""
	query: str = "UPDATE records SET ctime = ?, bytes = ? WHERE rp = ?;"
	inventory: list = []

	for rp in rps:
		fp: str = os.path.join(origin, rp)

		stats = os.stat(fp)

		ctime: int = stats.st_ctime
		size: int = stats.st_size *(10**7)

		inventory.append((ctime, size, rp))

	return query, inventory

def historian(sconn: sqlite3 | None = None, version: int = None, message: str = ""):
	"""Records the version & message, if present, in the index.

	Args:
		sconn (sqlite3): Index's connection object.
		version (int): Current version of the update or initiation.
		message (str): Message, if present, to be recorded.

	Returns:
		None
	"""
	moment: int = datetime.now(UTC).timestamp()*(10**7)

	x: str = "INSERT INTO interior (moment, message, version) VALUES (?, ?, ?);"
	values: tuple = (moment, message, version)

	sconn.execute(x, values)

def _formatter(origin: str = ""):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file found.

	Args:
		origin (str): Path to the requested directory.

	Returns:
		rollcall (dict): Relative paths keyed to each files' st_ctime and st_size.
	"""
	inventory: list = _surveyor(origin)
	rollcall: dict = {rp:(ctime, size) for rp, ctime, size in inventory}

	return rollcall

def get_records(sconn: sqlite3 | None = None):
	"""Builds a dictionary of indexed files' st_ctimes and st_sizes.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		infrc_records (dict): Relative path keyed to each files' st_ctime and st_size.
	"""
	query: str = "SELECT rp, ctime, bytes FROM records;"

	records: list = sconn.execute(query).fetchall()
	index_records: dict = {rp:(ctime, size) for rp, ctime, size in records}

	return index_records

def init_index(sconn: sqlite3 | None = None, origin: str = "", parent: str = ""):
	"""Initiates a new index.

	Args:
		sconn (sqlite3): Index's connection object.
		origin (str): Target directory.
		parent (str): Index's parent directory.

	Returns:
		None
	"""
	message: str = "INITIAL"
	version: int = 0

	originals: str = os.path.join(parent, "originals")
	copier(origin, originals) # backup created first

	query: str = "INSERT INTO records (rp, original_version, from_version, ctime, bytes, track) VALUES (?, ?, ?, ?, ?, ?);"

	inventory: list = _survey(origin, version) # collect current files' metadata
	sconn.executemany(query, inventory)

	historian(sconn, version, message) # load the version into the local records table

def init_dindex(sconn: sqlite3 | None = None, drps: list = []):
	"""Initiates the table for the directories with version 0 data.

	Args:
		sconn (sqlite3): Index's connection object.
		drps (list): Subdirectories found.

	Returns:
		None
	"""
	version: int = 0

	query: str = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	values: list = [(rp, version) for rp in drps]

	sconn.executemany(query, values)

def qfdiffr(index_records: dict = {}, real_stats: dict = {}):
	"""Compares the indexed vs actual files & their metadata.

	Args:
		index_records (dict): Relative paths key paired to each file's st ctime & size.
		real_stats (dict): Relative paths key paired to each file's st ctime & size.

	Returns:
		new (list): Files created since the last commitment.
		deleted (list): Files deleted since the last commitment.
		diffs (list): Files whose metadata differs.
		unchanged (list): Unaltered files.
	"""
	all_indexes: set = set(index_records.keys())
	all_files: set = set(real_stats.keys())

	deleted: set = all_indexes - all_files
	new: set = all_files - all_indexes

	remaining: set = all_indexes & all_files

	diffs: list = []
	for rp in remaining:
		if index_records[rp][0] != real_stats[rp][0]:
			diffs.append(rp)

		elif index_records[rp][1] != real_stats[rp][1]:
			diffs.append(rp)

	diffs_: set = set(diffs)
	unchanged: set = remaining - diffs_

	return list(new), list(deleted), diffs, list(unchanged)

def query_index(conn: MySQL | None = None, sconn: sqlite3 | None = None, core: str = ""):
	"""Finds file discrepancies between indexed and actual files.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.
		core (str): Source directory.

	Returns:
		new (list): Files created since the last commitment.
		deleted (list): Files deleted since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (list): Unaltered files.
		diff (bool): Whether differences are present or not.
	"""
	diff: bool = False
	remaining: list = []

	real_stats: dict = _formatter(core)
	index_records: dict = get_records(sconn)

	new: list, deleted: list, diffs: list, remaining: list = qfdiffr(index_records, real_stats)

	failed: list, succeeded: list = verification(conn, diffs, core)

	passed: list = remaining + succeeded

	if any(failed) or any(new) or any(deleted):
		diff = True

	return new, deleted, failed, passed, diff

def verification(conn: MySQL | None or None, diffs: list = [], origin: str = ""):
	"""Checks actual vs. recorded hash for files with metadata discrepancies.

	Args:
		conn (mysql): Server's connection object.
		diffs (list): Files with metadata discrepancies.
		origin (str): The directory to search.

	Returns:
		failed (list): Files whose hash did not match.
		succeeded (list): Files whose hash matched.
	"""
	failed: list = []
	succeeded: list = []

	local_ids: dict = {}
	remote_ids: dict = {}

	hasher = xxhash.xxh64()

	for diff in diffs:
		hasher.reset()
		fp: str = os.path.join(origin, diff)

		with open(fp, 'rb') as f:
			content: bytes = f.read()

		hasher.update(content)
		_hash: bytes = hasher.digest()

		local_ids[diff] = _hash

	query: str = "SELECT hash FROM files WHERE rp = %s;"

	with conn.cursor() as cursor:
		for diff in diffs:
			cursor.execute(query, (diff,))
			rhash: bytes = cursor.fetchone()

			remote_ids[diff] = rhash

	for diff in diffs:
		if remote_ids[diff][0] != local_ids[diff]:
			failed.append(diff)
		else:
			succeeded.append(diff)

	return failed, succeeded

def query_dindex(sconn: MySQL | None = None, core: str = ""):
	"""Checks the actual directories against recorded.

	Args:
		sconn (sqlite3): Index's connection object.
		core (str): Source directory.

	Returns:
		newd (list): Directories created since the last recorded commitment.
		deletedd (list): Directories deleted since the last recorded commit.
		ledeux (list): Directories in both.
	"""
	query: str = "SELECT rp FROM directories;"
	idrps: list = sconn.execute(query).fetchall()
	xdrps: list = [i[0] for i in idrps]

	ldrps: list = _dsurvey(core)

	index_dirs: set = set(xdrps)
	real_dirs: set = set(ldrps)

	deletedd: set = index_dirs - real_dirs
	newd: set = real_dirs - index_dirs
	ledeux: set = index_dirs & real_dirs

	return list(newd), list(deletedd), list(ledeux)

def version_check(conn: MySQL | None = None, sconn: sqlite3 | None = None):
	"""Queries local and remote databases to compare the latest recorded version.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.

	Returns:
		vok (bool): Whether versions match (True) or not (False).
		lc_version (int): The current version, if verified.
	"""
	vok: bool = False

	lc_version: int = sconn.execute(CVERSION).fetchone()[0]

	with conn.cursor() as cursor:
		cursor.execute(CVERSION)
		rc_version: int = cursor.fetchone()[0]

	if rc_version and lc_version:
		if rc_version == lc_version:
			logger.info('versions: twinned')
			vok = True
		else:
			logger.error(f"versions misaligned: remote: {rc_version} | local: {lc_version}")

	return vok, lc_version

def local_audit_(sconn: sqlite3 | None = None, core: str = "", new: list = [], diffs: list = [], remaining: list = [], version: int = None, secure: tuple = ()):
	"""Reverts the current directory back to the latest locally recorded commit.

	Args:
		sconn (sqlite3): Index's connection object.
		core (str): Main target directory.
		new (list): Files created since the last commitment.
		diffs (list): Files whose recorded and actual hashes differ.
		remaining (list): Unaltered files.
		version (int): Current version.
		secure (Tuple): Contains the two paths to tmp & backup directories.

	Returns:
		None
	"""
	logger.debug('auditing the local index')

	tmpd: str, backup: str = secure

	inew: list = []
	idiffs: list = []

	logger.debug('recreating original\'s directory tree')
	prefix: int = len(backup) + 1

	for dirs in _rd(backup):
		if not is_ignored(dirs):
			rp: str = dirs[prefix:]
			ndir: str = os.path.join(tmpd, rp)

			os.makedirs(ndir, exist_ok=True)

	if remaining:
		logger.debug('hard-linking unchanged originals')
		for rem in remaining:
			origin: str = os.path.join(backup, rem)
			destin: str = os.path.join(tmpd, rem)

			os.makedirs(os.path.dirname(origin), exist_ok=True)
			os.makedirs(os.path.dirname(destin), exist_ok=True)

			os.link(origin, destin)

	if new:
		inew: list = xxnew(new, core, version, tmpd)
	if diffs:
		idiffs: list = xxdiff(diffs, core, version, tmpd)

	index_audit(sconn, inew, idiffs)

def xxnew(new: list = [], origin: str = "", version: int = None, tmpd: str = ""):
	"""Backs up new files to the 'originals' directory.

	Args:
		new (list): Files created since the last commitment.
		origin (str): The given directory.
		version (int): Current version.
		tmpd (str): The new directory.

	Returns:
		inew (list): Tuples containing relative path, 1, 1, version for every new file.
	"""
	logger.debug('copying new files over...')
	inew: list = []

	for rp in new:
		fp: str = os.path.join(origin, rp)
		bp: str = os.path.join(tmpd, rp)

		os.makedirs(os.path.dirname(bp), exist_ok=True)

		track: str = encoding(fp)

		# shutil.copy2(fp, bp)
		shutil.copyfile(fp, bp)

		inew.append((rp, 1, 1, track, version, version))

	return inew

def xxdiff(diffs: list = [], origin: str = "", version: int = None, tmpd: str = ""):
	"""Updates modified files' copies in the 'originals' directory.

	Args:
		diffs (list): Files whose contents were altered since the last commit.
		origin (str): The given directory.
		version (int): Current version.
		tmpd (str): The new directory.

	Returns:
		idiffs (list): Tuples containing each files' ctime, size, version and relative path.
	"""
	logger.debug('writing over dated files...')
	idiff: list = []

	for rp in diffs:
		fp: str = os.path.join(origin, rp)
		bp: str = os.path.join(tmpd, rp)

		os.makedirs(os.path.dirname(bp), exist_ok=True)

		with open(fp, 'rb') as m:
			modified: bytes = m.read()

		with open(bp, 'wb') as o:
			o.write(modified)

		stats = os.stat(fp)
		ctime: int = stats.st_ctime
		size: int = stats.st_size*(10**7)

		idiff.append((ctime, size, version, rp))

	return idiff

def index_audit(sconn: sqlite3 | None = None, new: list = [], diffs: list = []):
	"""Updates the current index to show metadata from recent changes.

	Args:
		sconn (sqlite3): Index's connection object.
		new (list): Tuples containing relative path, 1, 1, version for every new file.
		diffs (list): Tuples containing the ctime, size, version and relative path of each altered file.

	Returns:
		None
	"""
	if new:
		query: str = "INSERT INTO records (rp, ctime, bytes, track, original_version, from_version) VALUES (?, ?, ?, ?, ?, ?);"
		sconn.executemany(query, new)

	if diffs:
		query: str = "UPDATE records SET ctime = ?, bytes = ?, from_version = ? WHERE rp = ?;"
		sconn.executemany(query, diffs)

def xxdeleted(conn: MySQL | None = None, sconn: sqlite3 | None = None, deleted: list = [], to_version: int = None, secure: tuple = (), dodata: tuple = ()):
	"""Backs up deleted files to the server; deletes them from the index.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.
		deleted (list): Deleted files' relative paths.
		to_version (int): Version in wich the given file was deleted.
		secure (tuple): Temporary and backup directory's paths.
		dodata (tuple): Relevant deleted data in dictionaries.

	Returns:
		None
	"""
	logger.debug('archiving deleted files...')
	tmpd: str, backup: str = secure

	dov: dict, dog: dict, trk: dict = dodata

	query: str = "INSERT INTO deleted (rp, original_version, to_version, from_version, content, track) VALUES (%s, %s, %s, %s, %s, %s);"
	xquery: str = "DELETE FROM records WHERE rp = ?;"

	for rp in deleted:
		fp: str = os.path.join(backup, rp)

		with open(fp, 'rb') as d:
			dcontent: bytes = d.read()
		
		from_version: int = dov[rp]
		original_version: int = dog[rp]
		track: str = trk[rp]

		with conn.cursor(prepared=True) as cursor:
			deletedx: tuple = (rp, original_version, to_version, from_version, dcontent, track)
			cursor.execute(query, deletedx)

	data: list = [(rp,) for rp in deleted]
	sconn.executemany(xquery, data)

def local_daudit(sconn: sqlite3 | None = None, newd: list = [], deletedd: list = [], version: int = None):
	"""Refreshes the indexed directories to reflect the current contents.

	Args:
		sconn (sqlite3): Index's connection object.
		newd (list): Directories made since the latest commitment.
		deletedd (list): Directories deleted since the latest commitment.
		version (int): Current version.

	Returns:
		None
	"""
	nquery: str = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	dquery: str = "DELETE FROM directories WHERE rp = ?;"

	if newd:
		nvals: list = [(rp, version) for rp in newd]
		sconn.executemany(nquery, nvals)

	if deletedd:
		dvals: list = [(d,) for d in deletedd]
		sconn.executemany(dquery, dvals)

def scrape_dindex(sconn: sqlite3 | None = None):
	"""Gathers all directories from the directories' index.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		drps (list): Every directory currently indexed.
	"""
	query: str = "SELECT rp FROM directories;"
	drps: list = sconn.execute(query).fetchall()

	return drps

def refresh_index(sconn: sqlite3 | None = None, core: str = "", diffs: list = []):
	"""Refreshes the index to show currently accurate metadata of the files.

	Args:
		sconn (sqlite3): Index's connection object.
		core (str): Main target directory path.
		diffs (list): Relative paths of altered files.

	Returns:
		None
	"""
	query: str, inventory: list = _surveyorx(core, diffs)
	sconn.executemany(query, inventory)