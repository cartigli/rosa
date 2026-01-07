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
	"""Makes the directory for the index & path for SQLite db connection.

	Args:
		None

	Returns:
		index (str): Path to the SQLite's database file.
	"""
	curr = os.getcwd()

	ihome = os.path.join(curr, ".index")
	os.makedirs(ihome, exist_ok=True)

	index = os.path.join(ihome, "indeces.db")

	return index

def is_ignored(_str):
	return any(blckd in _str for blckd in BLACKLIST)

def construct(sconn):
	"""Makes the SQLite tables inside the database.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	sconn.executescript(SINIT)

def copier(origin, originals):
	"""Backs up the current directory to the index with the 'cp -r' unix command.

	Args:
		origin (str): Path to a chosen directory.
		originals (str): Path to the 'originals' directory.

	Returns:
		None
	"""
	os.makedirs(originals, exist_ok=True)

	for obj in os.scandir(origin):
		if not is_ignored(obj.path):
			destination = os.path.join(originals, obj.name)

			if obj.is_dir(): # root level directories
				shutil.copytree(obj, destination)

			elif obj.is_file(): # root level files
				# shutil.copy2(obj, destination)
				shutil.copyfile(obj, destination)

def _r(dir_):
	"""Recursive function for files.

	Args:
		dir_ (str): Path to a directory.

	Yields:
		obj (str): Path to a file found.
	"""
	for obj in os.scandir(dir_):
		if os.path.isdir(obj):
			yield from _r(obj.path)
		else:
			yield obj

def _rd(dirx):
	"""Recursive function for finding directories.

	Args:
		dirx (str): Path to the given directory.
	
	Yields:
		d.path (str): A directory found.
	"""
	for d in os.scandir(dirx):
		if d.is_dir():
			yield d.path
			yield from _rd(d.path)

def _surveyor(origin):
	"""Collects metadata for initial indexing.

	Args:
		origin (str): Path pointing to the requested directory.

	Returns:
		inventory (list): Tupled relative paths, st_ctimes, and st_sizes for every file found.
	"""
	prefix = len(origin) + 1

	inventory = []

	for file in _r(origin):
		if not is_ignored(file.path):
			rp = file.path[prefix:]

			stats = os.stat(file)

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, ctime, size))

	return inventory

def _survey(origin, version):
	"""Collects metadata for initial indexing but includes versions for the index.
	
	Args:
		origin (str): Path to the requested directory.
		version (int): Current version for the index.
	
	Returns:
		inventory (list): Tupled relative pats, versions, st_ctimes and st_sizes for all the files found.
	"""
	inventory = []

	prefix = len(origin) + 1

	for file in _r(origin):
		if not is_ignored(file.path):
			rp = file.path[prefix:]

			stats = os.stat(file)

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, version, ctime, size))

	return inventory

def _survey1(dir_, version):
	"""Collects metadata for initial indexing but includes versions for the index.
	
	Args:
		dir_ (str): Path to the requested directory.
		version (int): Current version for the index.
	
	Returns:
		inventory (list): Tupled relative pats, versions, st_ctimes and st_sizes for all the files found.
	"""
	inventory = []

	prefix = len(dir_.as_posix()) + 1

	for file in _r(dir_):
		if not is_ignored(file.path):
			rp = file.path[prefix:]

			stats = os.stat(file)

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, version, ctime, size))

	return inventory

def _dsurvey(origin):
	"""Collects the subdirectories within the requested directory.

	Args:
		origin (str): Path to the requested directory.

	Returns:
		ldrps (list): Relative paths of every directory found.
	"""
	pfx = len(origin) + 1
	ldrps = []

	for obj in _rd(origin):
		if not is_ignored(obj):
			ldrps.append(obj[pfx:])

	return ldrps


def _surveyorx(origin, rps):
	"""Collects the new metadata for altered files from a list of files.

	Args:
		origin (str): Path to the given directory.
		rps (list): Relative paths of all the altered files.

	Returns:
		inventory (list): Tupled st_ctimes, st_sizes and relative paths of revery file path in rps.
	"""
	inventory = []

	for rp in rps:
		fp = os.path.join(origin, rp)

		stats = os.stat(fp)

		ctime = stats.st_ctime
		size = stats.st_size *(10**7)

		inventory.append((ctime, size, rp))
	
	return inventory

def historian(sconn, version, message):
	"""Records the version & message, if present, in the index.

	Args:
		sconn (sqlite3): Index's connection object.
		version (int): Current version of the update or initiation.
		message (str): Message, if present, to be recorded.

	Returns:
		None
	"""
	moment = datetime.now(UTC).timestamp()*(10**7) # integer

	x = "INSERT INTO interior (moment, message, version) VALUES (?, ?, ?);"
	values = (moment, message, version)

	sconn.execute(x, values)

def _formatter(origin):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file found.

	Args:
		origin (str): Path to the requested directory.

	Returns:
		rollcall (dictionary): Every file's relative path keyed to its st_ctime and st_size.
	"""
	inventory = _surveyor(origin)

	rollcall = {rp:(ctime, size) for rp, ctime, size in inventory}

	return rollcall

def get_records(sconn):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file indexed.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		rollcall (dictionary): Every file's relative path keyed to its st_ctime and st_size.
	"""
	query = "SELECT rp, ctime, bytes FROM records;"

	records = sconn.execute(query).fetchall()

	index_records = {rp:(ctime, size) for rp, ctime, size in records}

	return index_records

def init_index(sconn, origin, parent):
	"""Initiates a new index.

	Args:
		sconn (sqlite3): Index's connection object.
		origin (str): Target directory.
		parent (str): Index's parent directory.

	Returns:
		None
	"""
	message = "INITIAL"
	version = 0

	originals = os.path.join(parent, "originals")

	copier(origin, originals) # backup created first

	inventory = _survey(origin, version) # collect current files' metadata

	query = "INSERT INTO records (rp, version, ctime, bytes) VALUES (?, ?, ?, ?);"

	sconn.executemany(query, inventory)

	historian(sconn, version, message) # load the version into the local records table

def init_dindex(drps, sconn):
	"""Initiates the table for the directories with version 0 data.

	Args:
		drps (list): All the subdirectories found at execution.

	Returns:
		None
	"""
	version = 0

	query = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	values = [(rp, version) for rp in drps]

	sconn.executemany(query, values)

def qfdiffr(index_records, real_stats):
	"""Compares the indexed vs actual files & their metadata.

	Args:
		index_records (dictionary): Relative paths key paired to the respective file's st ctime & size.
		real_stats (dictionary): Relative paths key paired to the respective file's st ctime & size.

	Returns:
		new (set): Files created since the last commitment.
		deleted (set): Files deleted since the last commitment.
		diffs (list): Files whose metadata differs.
		unchanged (set): Unaltered files.
	"""
	all_indexes = set(index_records.keys())
	all_files = set(real_stats.keys())

	deleted = all_indexes - all_files
	new = all_files - all_indexes

	remaining = all_indexes & all_files

	diffs = []
	for rp in remaining:
		if index_records[rp][0] != real_stats[rp][0]:
			diffs.append(rp)

		elif index_records[rp][1] != real_stats[rp][1]:
			diffs.append(rp)

	diffs_ = set(diffs)
	unchanged = remaining - diffs_

	return new, deleted, diffs, unchanged

def query_index(conn, sconn, core):
	"""Finds file discrepancies between indexed and actual files.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.
		core (str): Main target directory.

	Returns:
		new (set): Files created since the last commitment.
		deleted (set): Files deleted since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (list): Unaltered files.
		diff (bool): Whether differences are present or not.
	"""
	diff = False
	remaining = []

	real_stats = _formatter(core)
	index_records = get_records(sconn)

	new, deleted, diffs, remaining_ = qfdiffr(index_records, real_stats)

	failed, succeeded = verification(conn, diffs, core)

	passed = list(remaining_) + succeeded

	if any(failed) or any(new) or any(deleted):
		diff = True

	return new, deleted, failed, passed, diff

def verification(conn, diffs, origin):
	"""Checks actual vs. recorded hash for files with metadata discrepancies.

	Args:
		conn (mysql): Server's connection object.
		diffs (list): Files with metadata discrepancies.
		origin (str): The directory to search.

	Returns:
		failed (list): Files whose hash did not match.
		succeeded (list): Files whose hash matched.
	"""
	failed = []
	succeeded = []

	local_ids = {}
	remote_ids = {}

	hasher = xxhash.xxh64()

	for diff in diffs:
		hasher.reset()
		fp = os.path.join(origin, diff)

		with open(fp, 'rb') as f:
			content = f.read()

		hasher.update(content)
		_hash = hasher.digest()

		local_ids[diff] = _hash

	query = "SELECT hash FROM files WHERE rp = %s;"

	with conn.cursor() as cursor:
		for diff in diffs:
			cursor.execute(query, (diff,))
			rhash = cursor.fetchone()

			remote_ids[diff] = rhash
	
	for diff in diffs:
		if remote_ids[diff][0] != local_ids[diff]:
			failed.append(diff)
		else:
			succeeded.append(diff)
	
	return failed, succeeded

def query_dindex(sconn, core):
	"""Checks the actual directories against recorded.

	Args:
		sconn (sqlite3): Index's connection object.
		core (str): Main target directory.

	Returns:
		newd (set): Directories created since the last recorded commitment.
		deletedd (set): Directories deleted since the last recorded commit.
		ledeux (set): Directories in both.
	"""
	query = "SELECT rp FROM directories;"

	idrps = sconn.execute(query).fetchall()

	ldrps = _dsurvey(core)

	xdrps = [i[0] for i in idrps]

	index_dirs = set(xdrps)
	real_dirs = set(ldrps)

	deletedd = index_dirs - real_dirs
	newd = real_dirs - index_dirs

	ledeux = index_dirs & real_dirs

	return newd, deletedd, ledeux

def version_check(conn, sconn):
	"""Queries local and remote databases to compare the latest recorded version.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.

	Returns:
		vok (bool): Whether versions match (True) or not (False).
		lc_version[0] (int): The current version, if verified.
	"""
	vok = False

	lc_version = sconn.execute(CVERSION).fetchone()

	with conn.cursor() as cursor:
		cursor.execute(CVERSION)
		rc_version = cursor.fetchone()
	
	if rc_version and lc_version:
		if rc_version[0] == lc_version[0]:
			logger.info('versions: twinned')
			vok = True

		# if rc_version[0] != lc_version[0]:
		else:
			logger.error(f"versions misaligned: remote: {rc_version} | local: {lc_version}")

	return vok, lc_version[0]

def local_audit_(sconn, core, new, diffs, remaining, version, secure):
	"""Reverts the current directory back to the latest locally recorded commit.

	fat_boy uses Pathlib while fat_boy uses os.path, hence the change.

	Args:
		sconn (sqlite3): Index's connection object.
		core (str): Main target directory.
		new (set): Files created since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (set): Unaltered files.
		version (int): Current version.
		secure (Tuple): Contains the two paths to tmp & backup directories.

	Returns:
		None
	"""
	logger.debug('auditing the local index')

	tmpd, backup = secure

	inew = None
	idiffs = None

	logger.debug('recreating original\'s directory tree')
	prefix = len(backup) + 1

	for dirs in _rd(backup):
		# if is_ignored(dirs):
		# 	continue
		if not is_ignored(dirs):
			# if dirs.is_dir():
			rp = dirs.path[prefix:]
			ndir = os.path.join(tmpd, rp)

			os.makedirs(ndir, exist_ok=True)

	if remaining:
		logger.debug('hard-linking unchanged originals')
		for rem in remaining:
			origin = os.path.join(backup, rem)
			destin = os.path.join(tmpd, rem)

			os.makedirs(os.path.dirname(origin), exist_ok=True)
			os.makedirs(os.path.dirname(destin), exist_ok=True)

			os.link(origin, destin)

	if new:
		inew = xxnew(new, core, version, tmpd)
	if diffs:
		idiffs = xxdiff(diffs, core, version, tmpd)

	index_audit(sconn, inew, idiffs)

def xxnew(new, origin, version, tmpd):
	"""Backs up new files to the 'originals' directory.

	Args:
		new (set): Files created since the last commitment.
		origin (str): The given directory.
		version (int): Current version.
		tmpd (str): The new directory.

	Returns:
		inew (list): Tuples containing relative path, 1, 1, version for every new file.
	"""
	logger.debug('copying new files over...')
	inew = []

	for rp in new:
		fp = os.path.join(origin, rp)
		bp = os.path.join(tmpd, rp)

		os.path.makedirs(os.path.dirname(bp), exist_ok=True)

		# shutil.copy2(fp, bp)
		shutil.copyfile(fp, bp)

		inew.append((rp, 1, 1, version))

	return inew

def xxdiff(diffs, origin, version, tmpd):
	"""Updates modified files' copies in the 'originals' directory.

	Args:
		diffs (list): Files whose contents were altered since the last commit.
		origin (str): The given directory.
		version (int): Current version.
		tmpd (str): The new directory.

	Returns:
		idiffs (list): Tuples containing the ctime, size, version and relative path of each altered file.
	"""
	logger.debug('writing over dated files...')
	idiff = []

	for rp in diffs:
		fp = os.path.join(origin, rp)
		bp = os.path.join(tmpd, rp)

		os.path.makedirs(os.path.dirname(bp), exist_ok=True)
		bp.touch()

		with open(fp, 'rb') as m:
			modified = m.read()

		with open(bp, 'wb') as o:
			o.write(modified)

		stats = os.stat(fp)
		ctime = stats.st_ctime
		size = stats.st_size*(10**7)

		idiff.append((ctime, size, version, rp))

	return idiff

def index_audit(sconn, new, diffs):
	"""Updates the current index to show metadata from recent changes.

	Args:
		new (list): Tuples containing relative path, 1, 1, version for every new file.
		diffs (list): Tuples containing the ctime, size, version and relative path of each altered file.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	if new:
		query = "INSERT INTO records (rp, ctime, bytes, version) VALUES (?, ?, ?, ?);"
		sconn.executemany(query, new)

	if diffs:
		query = "UPDATE records SET ctime = ?, bytes = ?, version = ? WHERE rp = ?;"
		sconn.executemany(query, diffs)

def xxdeleted(conn, sconn, deleted, xversion, doversions, secure):
	"""Backs up deleted files to the server; deletes them from the index.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.
		deleted (set): Deleted files' relative paths.
		xversion (int): Version in wich the given file was deleted.
		doversion (int): Original (prior) version of the deleted file.
		secure (tuple): Temporary and backup directory's paths.

	Returns:
		None
	"""
	logger.debug('archiving deleted files...')
	tmpd, backup = secure

	query = "INSERT INTO deleted (rp, xversion, oversion, content) VALUES (%s, %s, %s, %s);"
	xquery = "DELETE FROM records WHERE rp = ?;"

	for rp in deleted:
		fp = os.path.join(backup, rp)

		with open(fp, 'rb') as d:
			dcontent = d.read()
		
		oversion = doversions[rp]

		with conn.cursor(prepared=True) as cursor:
			deletedx = (rp, xversion, oversion, dcontent)
			cursor.execute(query, deletedx)

	data = [(rp,) for rp in deleted]

	sconn.executemany(xquery, data)

def local_daudit(sconn, newd, deletedd, version):
	"""Refreshes the indexed directories to reflect the current contents.

	Args:
		sconn (sqlite3): Index's connection object.
		newd (set): Directories made since the latest commitment.
		deletedd (set): Directories deleted since the latest commitment.
		version (int): Current version.

	Returns:
		None
	"""
	nquery = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	dquery = "DELETE FROM directories WHERE rp = ?;"

	if newd:
		nvals = [(rp, version) for rp in newd]
		sconn.executemany(nquery, nvals)

	if deletedd:
		dvals = [(d,) for d in deletedd]
		sconn.executemany(dquery, dvals)

def scrape_dindex(sconn):
	"""Gathers all directories from the directories' index.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		drps (list): Every directory currently indexed.
	"""
	query = "SELECT rp FROM directories;"

	drps = sconn.execute(query).fetchall()

	return drps

def refresh_index(sconn, core, diffs):
	"""Refreshes the index to show currently accurate metadata of the files.

	Args:
		sconn (sqlite3): Index's connection object.
		core (str): Main target directory.
		diffs (list): Relative paths of altered files.

	Returns:
		None
	"""
	inventory = _surveyorx(core, diffs)

	query = "UPDATE records SET ctime = ?, bytes = ? WHERE rp = ?;"

	sconn.executemany(query, inventory)