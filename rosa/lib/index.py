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
from rosa.confs import LOCAL_DIR, BLACKLIST, RECORDS, RECORDS_INDEX, INTERIOR, DIRECTORIES, DIRECTORIES_INDEX, CVERSION, _TRUNCATE, _DROP

logger = logging.getLogger('rosa.log')

def _config():
	curr = Path(__file__).resolve()
	dhome = curr.parent.parent / "index"

	dhome.mkdir(parents=True, exist_ok=True)
	home = dhome / "indeces.db"

	return home

def construct(home):
	with sqlite3.connect(home) as conn:
		cursor = conn.cursor()

		cursor.execute(RECORDS)
		cursor.execute(INTERIOR)
		cursor.execute(DIRECTORIES)
		cursor.execute(DIRECTORIES_INDEX)

		conn.commit()

def copier(abs_path): # git does this on 'add' instead of on 'init'
	b = Path(__file__).resolve().parent.parent / "index"

	backup_lo = b / "originals"
	if abs_path.exists():
		if backup_lo.exists():
			shutil.rmtree(backup_lo)
			time.sleep(1)

		subprocess.run(["cp", "-r", f"{abs_path}", f"{backup_lo}"])

def _r(dir_):
	for obj in os.scandir(dir_):
		if obj.is_dir():
			yield from _r(obj.path)
		else:
			yield obj

def _surveyor(dir_):
	"""Collects metadata for initial indexing."""
	prefix = len(dir_.as_posix()) + 1

	inventory = []
	inter = []

	for file in _r(dir_):
		if any(blocked in file.path for blocked in BLACKLIST):
			continue
		else:
			rp = file.path[prefix:]
			stats = file.stat()

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, ctime, size))

	return inventory

def _survey(dir_, version):
	"""Collects metadata for initial indexing."""
	inventory = []
	inter = []

	prefix = len(dir_.as_posix()) + 1
	# blk_list = ['.DS_Store', '.git', '.obsidian', 'index']

	for file in _r(dir_):
		if any(blocked in file.path for blocked in BLACKLIST):
			continue
		else:
			rp = file.path[prefix:]
			stats = file.stat()

			ctime = stats.st_ctime
			size = stats.st_size*(10**7)

			inventory.append((rp, version, ctime, size))

	return inventory

def _dsurvey(dir_):
	pfx = len(dir_.as_posix()) + 1
	ldrps = []

	for dirx in dir_.rglob('*'):
		if dirx.is_dir():
			fp = dirx.as_posix()
			rp = fp[pfx:]
			ldrps.append(rp)

	return ldrps

def _surveyorx(dir_, rps):
	inventory = []

	for rp in rps:
		fp = dir_ / rp
		stats = fp.stat()
		ctime = stats.st_ctime
		size = stats.st_size *(10**7)

		inventory.append((ctime, size, rp))
		# inventory.append((rp, ctime, size))
	
	return inventory

def _formatter(dir_):
	rollcall = {}
	inventory = _surveyor(dir_)

	for rp, ctime, size in inventory:
		rollcall[rp] = (ctime, size)

	return rollcall # dictionary of local files for comparison against index

def historian(version, message):
	moment = datetime.now(UTC).timestamp()*(10**7) # integer
	# home = _config()
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"

	if home.parent.exists() and home.exists():
		x = "INSERT INTO interior (moment, message, version) VALUES (?, ?, ?);"
		values = (moment, message, version)

		with sqlite3.connect(home) as conn:
			cursor = conn.cursor()
			cursor.execute(x, values)

			conn.commit()
	else:
		logger.info('there is no index; initiate or repair the config')
		sys.exit(4)

def get_records(home):
	index_records = {}

	with sqlite3.connect(home) as conn:
		cursor = conn.cursor()
		cursor.execute("SELECT rp, ctime, bytes FROM records;")
		records = cursor.fetchall()

	for record in records:
		index_records[record[0]] = (record[1], record[2])

	return index_records

def get_dirs():
	query = "SELECT rp FROM directories;"
	# home = _config()
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"
	
	if home.parent.exists() and home.exists():
		with sqlite3.connect(home) as conn:
			cursor = conn.cursor()
			cursor.execute(query)
			idrps = cursor.fetchall()
		
		return idrps
	else:
		logger.info('there is no index; initiate or repair the config')
		sys.exit(4)

def init_index():
	message = "INITIAL"
	version = 0

	home = _config()
	abs_path = Path(LOCAL_DIR)

	copier(abs_path) # backup created first
	inventory = _survey(abs_path, version) # collect current files' metadata

	with sqlite3.connect(home) as conn:
		cursor = conn.cursor()
		query = "INSERT INTO records (rp, version, ctime, bytes) VALUES (?, ?, ?, ?);"

		for item in inventory:
			cursor.execute(query, item)

		conn.commit()

	historian(version, message) # load the version into the local records table

def init_dindex(drps):
	version = 0
	home = _config()

	query = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	values = [(rp, version) for rp in drps]

	construct(home)

	with sqlite3.connect(home) as conn:
		cursor = conn.cursor()
		cursor.executemany(query, values)
		conn.commit()

def qfdiffr(index_records, real_stats):
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

def query_index(conn):
	diff = False
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"

	if home.parent.exists() and home.exists():
		abs_path = Path(LOCAL_DIR)

		real_stats = _formatter(abs_path)
		index_records = get_records(home)

		new, deleted, diffs, remaining = qfdiffr(index_records, real_stats)

		failed = verification(conn, diffs, abs_path)

		if any(failed) or any(new) or any(deleted):
			diff = True

		return new, deleted, failed, remaining, diff
	else:
		logger.warning('there is no index; initiate or repair the config')
		sys.exit(4)

def verification(conn, diffs, dir_):
	failed = []
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

	for diff in diffs:
		with conn.cursor() as cursor:
			cursor.execute(query, (diff,))
			rhash = cursor.fetchone()

			remote_ids[diff] = rhash
	
	for diff in diffs:
		if remote_ids[diff] != local_ids[diff]:
			failed.append(diff)
	
	return failed

def query_dindex():
	# home = _config()
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"

	if home.parent.exists() and home.exists():
		abs_path = Path(LOCAL_DIR)
		query = "SELECT rp FROM directories;"

		with sqlite3.connect(home) as conn:
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

def version_check(conn):
	# home = _config()
	vok = False
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"

	if home.parent.exists() and home.exists():
		with conn.cursor() as cursor:
			cursor.execute(CVERSION)
			rc_version = cursor.fetchone()
		
		with sqlite3.connect(home) as conn:
			cursor = conn.cursor()
			cursor.execute(CVERSION)
			lc_version = cursor.fetchone()
		
		if rc_version and lc_version:
			if rc_version[0] != lc_version[0]:
				vok = False
				print(f"versions misaligned: remote: {rc_version} - local: {lc_version}")
			elif rc_version[0] == lc_version[0]:
				vok = True

		return vok, lc_version[0], home
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def local_audit_(new, diffs, remaining, version, secure):
	logger.debug('auditing the local index')
	abs_path = Path(LOCAL_DIR) # nothing

	tmpd, backup = secure

	inew = None
	idiffs = None

	logger.debug('recreating original\'s directory tree')
	prefix = len(backup.as_posix()) + 1

	for dirs in backup.rglob('*'):
		if dirs.is_dir():
			rp = dirs.as_posix()[prefix:]
			ndir = Path(tmpd / rp).resolve()

			ndir.mkdir(parents=True, exist_ok=True)

	if remaining:
		logger.debug('hard-linking unchanged originals')
		for rem in remaining:
			origin = (backup / rem).resolve()
			destin = (tmpd / rem).resolve()

			destin.hardlink_to(origin)

	if new:
		inew = xxnew(new, abs_path, version, tmpd)
	if diffs:
		idiffs = xxdiff(diffs, abs_path, version, tmpd)

	index_audit(inew, idiffs)

def xxnew(new, abs_path, version, tmpd):
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

def index_audit(new, diffs):
	# home = _config() # versions are current in the local index; can get them here instead of remote :)
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"

	if home.parent.exists() and home.exists():
		with sqlite3.connect(home) as conn:
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


def xxdeleted(conn, deleted, xversion, doversions, secure): # deleted should be its own logic
	logger.debug('archiving deleted files...')
	tmpd, backup = secure
	# home = _config()
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"

	if home.parent.exists() and home.exists():

		ideleted = []

		query = "INSERT INTO deleted (rp, xversion, oversion, content) VALUES (%s, %s, %s, %s);"
		xquery = "DELETE FROM records WHERE rp = ?;"

		for rp in deleted:
			fp = backup / rp

			with open(fp, 'rb') as d:
				dcontent = d.read()
			
			# oversion = doversions.get(rp)
			oversion = doversions[rp]

			# deletedx = (rp, version, dcontent)

			with conn.cursor(prepared=True) as cursor:
				deletedx = (rp, xversion, oversion, dcontent)
				cursor.execute(query, deletedx)

			with sqlite3.connect(home) as sconn: # becuase this conn is initiated here, commit has to happen here. 
				# I could initiate out of the func, but that doesn't fit sqlite strats. 
				# Although index audit does this. Yeesh. Might be huge design flaw. Will come back to this.
				cursor = sconn.cursor()
				cursor.execute(xquery, (rp,))

				sconn.commit()
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def local_daudit(newd, deletedd, version):
	# this is also current with last change's version, so get oversion from here instead of remote
	nquery = "INSERT INTO directories (rp, version) VALUES (?, ?);"
	dquery = "DELETE FROM directories WHERE rp = ?;"
	# home = _config()
	curr = Path(__file__).resolve()
	home = curr.parent.parent / "index" / "indeces.db"
	if home.parent.exists() and home.exists():

		if newd:
			nvals = [(rp, version) for rp in newd]
			with sqlite3.connect(home) as conn:
				cursor = conn.cursor()
				cursor.executemany(nquery, nvals)

				conn.commit()

		if deletedd:
			dvals = [(d,) for d in deletedd]
			with sqlite3.connect(home) as conn:
				cursor = conn.cursor()
				cursor.executemany(dquery, dvals)

				conn.commit()
	else:
		logger.warning('there is no index; either initiate or correct the config')
		sys.exit(4)

def scrape_dindex():
	query = "SELECT rp FROM directories;"
	home = _config()

	with sqlite3.connect(home) as conn:
		cursor = conn.cursor()
		cursor.execute(query)

		drps = cursor.fetchall()
	
	return drps

def refresh_index(diffs):
	abs_path = Path(LOCAL_DIR)
	home = _config()

	inventory = _surveyorx(abs_path, diffs)

	with sqlite3.connect(home) as conn:
		cursor = conn.cursor()
		query = "UPDATE records SET ctime = ?, bytes = ? WHERE rp = ?;"

		for inv in inventory:
			cursor.execute(query, inv)

		conn.commit()

# def main(args=None):
#      start = time.perf_counter()
#      init_index()
#      end = time.perf_counter()
#      print(f"init took {(end - start):.4f} seconds.")