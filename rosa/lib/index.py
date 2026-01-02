import os
import sys
import time
import xxhash
import shutil
import sqlite3
import logging
import subprocess
from pathlib import Path
from datetime import datetime, UTC

# LOCAL_DIR used 5 times (besides import)
# last step is pass connection obj. & import landline to every script that uses it
from rosa.confs import LOCAL_DIR, SINIT, CVERSION

logger = logging.getLogger('rosa.log')

def _config():
	"""Makes the directory for the index & path for SQLite db connection.

	Args:
		None

	Returns:
		index (Path): Pathlib path to the SQLite's database file.
	"""
	curr = Path.cwd()

	ihome = curr / ".index"
	ihome.mkdir(parents=True, exist_ok=True)

	index = ihome / "indeces.db"

	return index

def is_ignored(_str):
	blacklist = ['.index', '.git', '.obsidian', '.vscode', '.DS_Store']
	return any(blckd in _str for blckd in blacklist)

def construct(sconn):
	"""Makes the SQLite tables inside the database.

	Args:
		index (Path): Pathlib path to the SQLite's database file.

	Returns:
		None
	"""
	sconn.executescript(SINIT)


def copier(abs_path, ihome): # git does this on 'add' instead of on 'init'
	"""Backs up the current directory to the index with the 'cp -r' unix command.

	Args:
		abs_path (Path): Pathlib path to a chosen directory.

	Returns:
		None
	"""
	backup_lo = ihome / "originals"
	backup_lo.mkdir(parents=True, exist_ok=True)

	for xobj in abs_path.glob('*'):
		if is_ignored(xobj.as_posix()):
			continue
		else:
			destination = backup_lo / xobj.name

			if xobj.is_dir(): # root level directories
				shutil.copytree(xobj, destination)

			elif xobj.is_file(): # root level files
				shutil.copy2(xobj, destination)

def _r(dir_):
	"""Recursive function which is faster than rglob('*').

	Args:
		dir_ (Path): Pathlib path to a directory to recursively find files from.

	Yields:
		obj (Path): Pathlib path containing a file found in the given directory.
	"""
	for obj in os.scandir(dir_):
		if obj.is_dir():
			yield from _r(obj.path)
		else:
			yield obj

def _surveyor(dir_):
	"""Collects metadata for initial indexing.

	Args:
	dir (Path): Pathlib path pointing to the requested directory.

	Returns:
		inventory (list): Tupled relative paths, st_ctimes, and st_sizes for every file found.
	"""
	prefix = len(dir_.as_posix()) + 1

	inventory = []
	inter = []

	for file in _r(dir_):
		if is_ignored(file.path):
			continue
		else:
			rp = file.path[prefix:]
			stats = file.stat()

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, ctime, size))

	return inventory

def _survey(dir_, version):
	"""Collects metadata for initial indexing but includes versions for the index.
	
	Args:
		dir_ (Path): Pathlib path to the requested directory.
		version (int): Current version for the index.
	
	Returns:
		inventory (list): Tupled relative pats, versions, st_ctimes and st_sizes for all the files found.
	"""
	inventory = []
	inter = []

	prefix = len(dir_.as_posix()) + 1

	for file in _r(dir_):
		if is_ignored(file.path):
			continue
		else:
			rp = file.path[prefix:]
			stats = file.stat()

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, version, ctime, size))

	return inventory

def _dsurvey(dir_):
	"""Collects the subdirectories within the requested directory.

	Args:
		dir_ (Path): Pathlib path to the requested directory.

	Returns:
		ldrps (list): Relative paths of every directory found.
	"""
	pfx = len(dir_.as_posix()) + 1
	ldrps = []

	for dirx in dir_.rglob('*'):
		if is_ignored(dirx.as_posix()):
			continue

		elif dirx.is_dir():
			fp = dirx.as_posix()
			rp = fp[pfx:]

			ldrps.append(rp)

	return ldrps

def _surveyorx(dir_, rps):
	"""Collects the new metadata for altered files from a list of files.

	Args:
		dir_ (Path): Pathlib path to the requested directory.
		rps (list): Relative paths of all the altered files.

	Returns:
		inventory (list): Tupled st_ctimes, st_sizes and relative paths of revery file path in rps.
	"""
	inventory = []

	for rp in rps:
		fp = dir_ / rp
		stats = fp.stat()

		ctime = stats.st_ctime
		size = stats.st_size *(10**7)

		inventory.append((ctime, size, rp))
	
	return inventory

def historian(version, message, sconn):
	"""Records the version & message, if present, in the index.

	Args:
		version (int): Current version of the update or initiation.
		message (str): Message, if present, to be recorded.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	moment = datetime.now(UTC).timestamp()*(10**7) # integer

	x = "INSERT INTO interior (moment, message, version) VALUES (?, ?, ?);"
	values = (moment, message, version)

	# cursor = sconn.cursor()
	# cursor.execute(x, values)

	sconn.execute(x, values)

def _formatter(dir_):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file found.

	Args:
		dir_ (Path): Pathlib path to the requested directory.

	Returns:
		rollcall (dictionary): Every file's relative path keyed to its st_ctime and st_size.
	"""
	# rollcall = {}

	inventory = _surveyor(dir_)

	# for rp, ctime, size in inventory:
	# 	rollcall[rp] = (ctime, size)

	rollcall = {rp:(ctime, size) for rp, ctime, size in inventory}

	return rollcall

def get_records(sconn):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file indexed.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		rollcall (dictionary): Every file's relative path keyed to its st_ctime and st_size.
	"""
	# index_records = {}
	query = "SELECT rp, ctime, bytes FROM records;"

	# cursor = sconn.cursor()
	# cursor.execute("SELECT rp, ctime, bytes FROM records;")
	# records = cursor.fetchall()

	records = sconn.execute(query).fetchall()

	# for record in records:
	# 	index_records[record[0]] = (record[1], record[2])

	index_records = {rp:(ctime, size) for rp, ctime, size in records}
	# index_records = {record[0]:(record[1], record[2]) for record in records}

	return index_records

def init_index(sconn, ihome):
	"""Initiates a new index.

	Args:
		sconn (sqlite3): Index's connection object.
		ihome (Path): The index's parent directory.

	Returns:
		None
	"""
	message = "INITIAL"
	version = 0

	abs_path = Path(LOCAL_DIR)

	copier(abs_path, ihome) # backup created first
	inventory = _survey(abs_path, version) # collect current files' metadata

	# cursor = sconn.cursor()
	query = "INSERT INTO records (rp, version, ctime, bytes) VALUES (?, ?, ?, ?);"

	# for item in inventory:
	sconn.executemany(query, inventory)

	historian(version, message, sconn) # load the version into the local records table

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

	# cursor = sconn.cursor()
	# cursor.executemany(query, values)

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

def query_index(conn, sconn):
	"""Finds file discrepancies between indexed and actual files.

	Args:
		conn (mysql): Server's connection object.
		sconn (sqlite3): Index's connection object.

	Returns:
		new (set): Files created since the last commitment.
		deleted (set): Files deleted since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (list): Unaltered files.
		diff (bool): Whether differences are present or not.
	"""
	diff = False
	remaining = []

	abs_path = Path(LOCAL_DIR)

	real_stats = _formatter(abs_path)
	index_records = get_records(sconn)

	new, deleted, diffs, remaining_ = qfdiffr(index_records, real_stats)

	failed, succeeded = verification(conn, diffs, abs_path)

	for x in remaining_:
		remaining.append(x)
	
	for y in succeeded:
		remaining.append(y)

	if any(failed) or any(new) or any(deleted):
		diff = True

	return new, deleted, failed, remaining, diff

def verification(conn, diffs, dir_):
	"""Checks actual vs. recorded hash for files with metadata discrepancies.

	Args:
		conn (mysql): Server's connection object.
		diffs (list): Files with metadata discrepancies.
		dir_ (Path): The directory to search.

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
		fp = dir_ / diff

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
		if remote_ids[diff] != local_ids[diff]:
			failed.append(diff)
		else:
			succeeded.append(diff)
	
	return failed, succeeded

def query_dindex(sconn):
	"""Checks the actual directories against recorded.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		newd (set): Directories created since the last recorded commitment.
		deletedd (set): Directories deleted since the last recorded commit.
		ledeux (set): Directories in both.
	"""
	abs_path = Path(LOCAL_DIR)
	query = "SELECT rp FROM directories;"

	# cursor = sconn.cursor()
	# cursor.execute(query)
	# idrps = cursor.fetchall()

	idrps = sconn.execute(query).fetchall()
	
	ldrps = _dsurvey(abs_path)

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

	# cursor = sconn.cursor()
	# cursor.execute(CVERSION)
	# lc_version = cursor.fetchone()

	lc_version = sconn.execute(CVERSION).fetchone()

	with conn.cursor() as cursor:
		cursor.execute(CVERSION)
		rc_version = cursor.fetchone()
	
	if rc_version and lc_version:
		if rc_version[0] != lc_version[0]:
			print(f"versions misaligned: remote: {rc_version} | local: {lc_version}")
			vok = False

		elif rc_version[0] == lc_version[0]:
			vok = True

	return vok, lc_version[0]

def local_audit_(new, diffs, remaining, version, secure, sconn):
	"""Reverts the current directory back to the latest locally recorded commit.

	Args:
		new (set): Files created since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (set): Unaltered files.
		version (int): Current version.
		secure (Tuple): Contains the two paths to tmp & backup directories.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	logger.debug('auditing the local index')
	abs_path = Path(LOCAL_DIR) # nothing

	tmpd, backup = secure

	inew = None
	idiffs = None

	logger.debug('recreating original\'s directory tree')
	prefix = len(backup.as_posix()) + 1

	for dirs in backup.rglob('*'):
		if is_ignored(dirs.as_posix()):
			continue
		elif dirs.is_dir():
			rp = dirs.as_posix()[prefix:]
			ndir = Path(tmpd / rp).resolve()

			ndir.mkdir(parents=True, exist_ok=True)

	if remaining:
		logger.debug('hard-linking unchanged originals')
		for rem in remaining:
			origin = (backup / rem).resolve()
			destin = (tmpd / rem).resolve()

			origin.parent.mkdir(parents=True, exist_ok=True)
			destin.parent.mkdir(parents=True, exist_ok=True)

			destin.hardlink_to(origin)

	if new:
		inew = xxnew(new, abs_path, version, tmpd)
	if diffs:
		idiffs = xxdiff(diffs, abs_path, version, tmpd)

	index_audit(inew, idiffs, sconn)

def xxnew(new, abs_path, version, tmpd):
	"""Backs up new files to the 'originals' directory.

	Args:
		new (set): Files created since the last commitment.
		abs_path (Path): The given directory.
		version (int): Current version.
		tmpd (Path): The new directory.

	Returns:
		inew (list): Tuples containing relative path, 1, 1, version for every new file.
	"""
	logger.debug('copying new files over...')
	inew = []

	for rp in new:
		fp = abs_path / rp
		bp = tmpd / rp

		bp.parent.mkdir(parents=True, exist_ok=True)
		shutil.copy2(fp, bp)

		inew.append((rp, 1, 1, version))

	return inew

def xxdiff(diffs, abs_path, version, tmpd):
	"""Updates modified files' copies in the 'originals' directory.

	Args:
		diffs (slist): Files whose contents were altered since the last commit.
		abs_path (Path): The given directory.
		version (int): Current version.
		tmpd (Path): The new directory.

	Returns:
		idiffs (list): Tuples containing the ctime, size, version and relative path of each altered file.
	"""
	logger.debug('writing over dated files...')
	idiff = []

	for rp in diffs:
		fp = abs_path / rp
		bp = tmpd / rp # tmpd target; local_dir source (outdated)

		bp.parent.mkdir(parents=True, exist_ok=True)
		bp.touch()

		with open(fp, 'rb') as m:
			modified = m.read()

		with open(bp, 'wb') as o:
			o.write(modified)

		stats = fp.stat()
		ctime = stats.st_ctime
		size = stats.st_size*(10**7)

		idiff.append((ctime, size, version, rp))

	return idiff

def index_audit(new, diffs, sconn):
	"""Updates the current index to show metadata from recent changes.

	Args:
		new (list): Tuples containing relative path, 1, 1, version for every new file.
		diffs (list): Tuples containing the ctime, size, version and relative path of each altered file.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	# cursor = sconn.cursor()

	if new:
		query = "INSERT INTO records (rp, ctime, bytes, version) VALUES (?, ?, ?, ?);"
		# for n in new:
			# cursor.execute(q, n)

		sconn.executemany(query, new)

	if diffs:
		query = "UPDATE records SET ctime = ?, bytes = ?, version = ? WHERE rp = ?;"
		# for a in diffs:
			# cursor.execute(q, a)

		sconn.executemany(query, diffs)

def xxdeleted(conn, deleted, xversion, doversions, secure, sconn): # deleted should be its own logic
	"""Backs up deleted files to the server; deletes them from the index.

	Args:
		conn (mysql): Server's connection object.
		deleted (set): Deleted files' relative paths.
		xversion (int): Version in wich the given file was deleted.
		doversion (int): Original (prior) version of the deleted file.
		secure (tuple): Temporary and backup directory's paths.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	logger.debug('archiving deleted files...')
	tmpd, backup = secure

	query = "INSERT INTO deleted (rp, xversion, oversion, content) VALUES (%s, %s, %s, %s);"
	xquery = "DELETE FROM records WHERE rp = ?;"

	for rp in deleted:
		fp = backup / rp

		with open(fp, 'rb') as d:
			dcontent = d.read()
		
		oversion = doversions[rp]

		with conn.cursor(prepared=True) as cursor:
			deletedx = (rp, xversion, oversion, dcontent)
			cursor.execute(query, deletedx)

		# cursor = sconn.cursor()
		# cursor.execute(xquery, (rp,))

		# sconn.execute(xquery, (rp,))

	data = [(rp,) for rp in deleted] # might need

	sconn.executemany(xquery, data)

def local_daudit(newd, deletedd, version, sconn):
	"""Refreshes the indexed directories to reflect the current contents.

	Args:
		newd (set): Directories made since the latest commitment.
		deletedd (set): Directories deleted since the latest commitment.
		version (int): Current version.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	# this is also current with last change's version, so get oversion from here instead of remote
	nquery = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	dquery = "DELETE FROM directories WHERE rp = ?;"

	if newd:
		nvals = [(rp, version) for rp in newd]

		# cursor = sconn.cursor()
		# cursor.executemany(nquery, nvals)
		sconn.executemany(nquery, nvals)

	if deletedd:
		dvals = [(d,) for d in deletedd]

		# cursor = sconn.cursor()
		# cursor.executemany(dquery, dvals)
		sconn.executemany(dquery, dvals)

def scrape_dindex(sconn):
	"""Gathers all directories from the directories' index.

	Args:
		sconn (sqlite3): Index's connection object.

	Returns:
		drps (list): Every directory currently indexed.
	"""
	query = "SELECT rp FROM directories;"

	# cursor = sconn.cursor()
	# cursor.execute(query)
	# drps = cursor.fetchall()

	drps = sconn.execute(query).fetchall()

	return drps

def refresh_index(diffs, sconn):
	"""Refreshes the index to show currently accurate metadata of the files.

	Args:
		diffs (set): Relative paths of altered files.
		sconn (sqlite3): Index's connection object.

	Returns:
		None
	"""
	abs_path = Path(LOCAL_DIR)

	inventory = _surveyorx(abs_path, diffs)

	# cursor = sconn.cursor()

	query = "UPDATE records SET ctime = ?, bytes = ? WHERE rp = ?;"

	# for inv in inventory:
		# cursor.execute(query, inv)

	sconn.executemany(query, inventory)