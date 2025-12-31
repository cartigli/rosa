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
from rosa.confs import LOCAL_DIR, RECORDS, INTERIOR, DIRECTORIES, DIRECTORIES_INDEX, CVERSION

logger = logging.getLogger('rosa.log')

def _config():
	"""Makes the directory for the index & path for SQLite db connection.

	Args:
		None

	Returns:
		home (Path): Pathlib path to the SQLite's database file.
	"""
	# curr = Path(__file__).resolve()
	# dhome = curr.parent.parent / "index"

	# dhome.mkdir(parents=True, exist_ok=True)
	# home = dhome / "indeces.db"

	curr = Path.cwd()

	dhome = curr / ".index"
	dhome.mkdir(parents=True, exist_ok=True)

	home = dhome / "indeces.db"

	return home

def is_ignored(path_str):
	blacklist = ['.index', '.git', '.obsidian', '.vscode', '.DS_Store']
	return any(blckd in str_ for blckd in blacklist)

	# if any(blocked in path_str for blocked in BLACKLIST):
	# 	strx = None
	# else:
	# 	strx = path_str
	# return strx

def construct(index):
	"""Makes the SQLite tables inside the database.

	Args:
		home (Path): Pathlib path to the SQLite's database file.

	Returns:
		None
	"""
	with sqlite3.connect(index) as conn:
		cursor = conn.cursor()

		cursor.execute(RECORDS)
		cursor.execute(INTERIOR)
		cursor.execute(DIRECTORIES)
		cursor.execute(DIRECTORIES_INDEX)

		conn.commit()

def copier(abs_path, home): # git does this on 'add' instead of on 'init'
	"""Backs up the current directory to the index with the 'cp -r' unix command.

	Args:
		abs_path (Path): Pathlib path to a chosen directory.

	Returns:
		None
	"""
	backup_lo = home / "originals"
	backup_lo.mkdir(parents=True, exist_ok=True)

	# original_ = backup_lo / f"{abs_path.name}"
	# original_.mkdir(parents=True, exist_ok=True)

	for xobj in abs_path.glob('*'):
		# if any(blocked in xobj.as_posix() for blocked in BLACKLIST):
		# 	pass
		if is_ignored(xobj.as_posix()):
			continue
		else:
			destination = backup_lo / xobj.name

			if xobj.is_dir():
				shutil.copytree(xobj, destination)
				# subprocess.run(["cp", "-r", f"{xdir}", f"{backup_lo}"])
				# shutil.copy(xdir, original_)

			elif xobj.is_file():
				shutil.copy2(xobj, destination)

	# subprocess.run(["cp", "-r", f"{abs_path}", f"{backup_lo}"])

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
		# if any(blocked in file.path for blocked in BLACKLIST):
		# 	continue
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
		# if any(blocked in file.path for blocked in BLACKLIST):
		# 	continue
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
		# if dirx.as_posix() in BLACKLIST:
		# if any(blocked in dirx.as_posix() for blocked in BLACKLIST):
		# 	continue
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

	# for rps in rpsl:
	for rp in rps:
		fp = dir_ / rp
		stats = fp.stat()

		ctime = stats.st_ctime
		size = stats.st_size *(10**7)

		inventory.append((ctime, size, rp))
	
	return inventory

def historian(version, message, index):
	"""Records the version & message, if present, in the index.

	Args:
		version (int): Current version of the update or initiation.
		message (str): Message, if present, to be recorded.
		index (Path): The current index's path.

	Returns:
		None
	"""
	moment = datetime.now(UTC).timestamp()*(10**7) # integer

	if index.parent.exists() and index.exists():
		x = "INSERT INTO interior (moment, message, version) VALUES (?, ?, ?);"
		values = (moment, message, version)

		with sqlite3.connect(index) as conn:
			cursor = conn.cursor()

			cursor.execute(x, values)
			conn.commit()
	else:
		logger.info('there is no index; initiate or repair the config')
		sys.exit(4)

def _formatter(dir_):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file found.

	Args:
		dir_ (Path): Pathlib path to the requested directory.

	Returns:
		rollcall (dictionary): Every file's relative path keyed to its st_ctime and st_size.
	"""
	rollcall = {}
	inventory = _surveyor(dir_)

	for rp, ctime, size in inventory:
		rollcall[rp] = (ctime, size)

	return rollcall # dictionary of local files for comparison against index

def get_records(index):
	"""Builds a dictionary of indexed st_ctimes and st_sizes for every file indexed.

	Args:
		dir_ (Path): Pathlib path to the requested directory.

	Returns:
		rollcall (dictionary): Every file's relative path keyed to its st_ctime and st_size.
	"""
	index_records = {}

	with sqlite3.connect(index) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT rp, ctime, bytes FROM records;")
		records = cursor.fetchall()

	for record in records:
		index_records[record[0]] = (record[1], record[2])

	return index_records

def init_index():
	"""Initiates a new index.

	Args:
		None

	Returns:
		None
	"""
	message = "INITIAL"
	version = 0

	index = _config()
	abs_path = Path(LOCAL_DIR)

	copier(abs_path, index.parent) # backup created first
	inventory = _survey(abs_path, version) # collect current files' metadata

	with sqlite3.connect(index) as conn:
		cursor = conn.cursor()
		query = "INSERT INTO records (rp, version, ctime, bytes) VALUES (?, ?, ?, ?);"

		for item in inventory:
			cursor.execute(query, item)

		conn.commit()

	historian(version, message, index) # load the version into the local records table

def init_dindex(drps):
	"""Initiates the table for the directories with version 0 data.

	Args:
		drps (list): All the subdirectories found at execution.

	Returns:
		None
	"""
	version = 0
	index = _config()

	query = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	values = [(rp, version) for rp in drps]

	construct(index)

	with sqlite3.connect(index) as conn:
		cursor = conn.cursor()

		cursor.executemany(query, values)
		conn.commit()

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

def query_index(conn, index):
	"""Finds file discrepancies between indexed and actual files.

	Args:
		conn (mysql): Connection object.
		index (Path): The current index's path.

	Returns:
		new (set): Files created since the last commitment.
		deleted (set): Files deleted since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (list): Unaltered files.
		diff (bool): Whether differences are present or not.
	"""
	diff = False
	remaining = []

	if index.parent.exists() and index.exists():
		abs_path = Path(LOCAL_DIR)

		real_stats = _formatter(abs_path)
		index_records = get_records(index)

		new, deleted, diffs, remaining_ = qfdiffr(index_records, real_stats)

		failed, succeeded = verification(conn, diffs, abs_path)

		# remaining = list(remaining_)
		for x in remaining_:
			remaining.append(x)
		
		for y in succeeded:
			remaining.append(y)

		# remaining.append(succeeded)

		if any(failed) or any(new) or any(deleted):
			diff = True

		return new, deleted, failed, remaining, diff
	else:
		logger.warning('there is no index; initiate or repair the config')
		sys.exit(4)

def verification(conn, diffs, dir_):
	"""Checks actual vs. recorded hash for files with metadata discrepancies.

	Args:
		conn (mysql): Connection object.
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
		chash = hasher.digest()

		local_ids[diff] = chash

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

def query_dindex(index):
	"""Checks the actual directories against recorded.

	Args:
		index (Path): The current index's path.

	Returns:
		newd (set): Directories created since the last recorded commitment.
		deletedd (set): Directories deleted since the last recorded commit.
		ledeux (set): Directories in both.
	"""
	if index.parent.exists() and index.exists():
		abs_path = Path(LOCAL_DIR)
		query = "SELECT rp FROM directories;"

		with sqlite3.connect(index) as conn:
			cursor = conn.cursor()
			cursor.execute(query)

			idrps = cursor.fetchall()
		
		ldrps = _dsurvey(abs_path)

		xdrps = [i[0] for i in idrps]

		index_dirs = set(xdrps)
		real_dirs = set(ldrps)

		deletedd = index_dirs - real_dirs
		newd = real_dirs - index_dirs

		ledeux = index_dirs & real_dirs

		return newd, deletedd, ledeux
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def version_check(conn, index):
	"""Queries local and remote databases to compare the latest recorded version.

	Args:
		conn (mysql): Connection object.
		index (Path): The current index's path.

	Returns:
		vok (bool): Whether versions match (True) or not (False).
		lc_version[0] (int): The current version, if verified.
	"""
	vok = False

	if index.parent.exists() and index.exists():
		with conn.cursor() as cursor:
			cursor.execute(CVERSION)
			rc_version = cursor.fetchone()
		
		with sqlite3.connect(index) as conn:
			cursor = conn.cursor()

			cursor.execute(CVERSION)
			lc_version = cursor.fetchone()
		
		if rc_version and lc_version:
			if rc_version[0] != lc_version[0]:
				print(f"versions misaligned: remote: {rc_version} | local: {lc_version}")
				vok = False

			elif rc_version[0] == lc_version[0]:
				vok = True

		return vok, lc_version[0]
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def local_audit_(new, diffs, remaining, version, secure, index):
	"""Reverts the current directory back to the latest locally recorded commit.

	Args:
		new (set): Files created since the last commitment.
		failed (list): Files whose recorded and actual hashes differ.
		remaining (set): Unaltered files.
		version (int): Current version.
		secure (Tuple): Contains the two paths to tmp & backup directories.
		index (Path): The current index's path.

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
		# if any(blocked in dirs.as_posix() for blocked in BLACKLIST):
		# 	continue
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

	index_audit(inew, idiffs, index)

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

def index_audit(new, diffs, index):
	"""Updates the current index to show metadata from recent changes.

	Args:
		new (list): Tuples containing relative path, 1, 1, version for every new file.
		diffs (list): Tuples containing the ctime, size, version and relative path of each altered file.
		index (Path): The current index's path.

	Returns:
		None
	"""
	if index.parent.exists() and index.exists():
		with sqlite3.connect(index) as conn:
			cursor = conn.cursor()

			if new:
				q = "INSERT INTO records (rp, ctime, bytes, version) VALUES (?, ?, ?, ?);"
				for n in new:
					cursor.execute(q, n)

			if diffs:
				q = "UPDATE records SET ctime = ?, bytes = ?, version = ? WHERE rp = ?;"
				for a in diffs:
					cursor.execute(q, a)

			conn.commit()
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def xxdeleted(conn, deleted, xversion, doversions, secure, index): # deleted should be its own logic
	"""Backs up deleted files to the server; deletes them from the index.

	Args:
		conn (mysql): Connection object.
		deleted (set): Deleted files' relative paths.
		xversion (int): Version in wich the given file was deleted.
		doversion (int): Original (prior) version of the deleted file.
		secure (tuple): Temporary and backup directory's paths.
		index (Path): The current index's path.

	Returns:
		None
	"""
	logger.debug('archiving deleted files...')
	tmpd, backup = secure

	if index.parent.exists() and index.exists():
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

			with sqlite3.connect(index) as sconn: # becuase this conn is initiated here, commit has to happen here. 
				# I could initiate out of the func, but that doesn't fit sqlite strats. 
				# Although index audit does this. Yeesh. Might be huge design flaw. Will come back to this.
				cursor = sconn.cursor()
				cursor.execute(xquery, (rp,))

				sconn.commit()
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def local_daudit(newd, deletedd, version, index):
	"""Refreshes the indexed directories to reflect the current contents.

	Args:
		newd (set): Directories made since the latest commitment.
		deletedd (set): Directories deleted since the latest commitment.
		version (int): Current version.
		index (Path): The current index's path.

	Returns:
		None
	"""
	# this is also current with last change's version, so get oversion from here instead of remote
	nquery = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	dquery = "DELETE FROM directories WHERE rp = ?;"

	if index.parent.exists() and index.exists():

		if newd:
			nvals = [(rp, version) for rp in newd]

			with sqlite3.connect(index) as conn:
				cursor = conn.cursor()

				cursor.executemany(nquery, nvals)
				conn.commit()

		if deletedd:
			dvals = [(d,) for d in deletedd]

			with sqlite3.connect(index) as conn:
				cursor = conn.cursor()

				cursor.executemany(dquery, dvals)
				conn.commit()
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def scrape_dindex(index):
	"""Gathers all directories from the directories' index.

	Args:
		index (Path): The current index's path.

	Returns:
		drps (list): Every directory currently indexed.
	"""
	query = "SELECT rp FROM directories;"

	with sqlite3.connect(index) as conn:
		cursor = conn.cursor()
		cursor.execute(query)

		drps = cursor.fetchall()
	
	return drps

def refresh_index(diffs, index):
	"""Refreshes the index to show currently accurate metadata of the files.

	Args:
		diffs (set): Relative paths of altered files.
		index (Path): The current index's path.

	Returns:
		None
	"""
	abs_path = Path(LOCAL_DIR)

	inventory = _surveyorx(abs_path, diffs)

	with sqlite3.connect(index) as conn:
		cursor = conn.cursor()
		query = "UPDATE records SET ctime = ?, bytes = ? WHERE rp = ?;"

		for inv in inventory:
			cursor.execute(query, inv)

		conn.commit()