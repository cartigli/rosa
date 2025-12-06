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

"""
Library of functions used in the management scripts. Many functions overlap and share uses, so moving them here is much easier for 
maintainability. There are also queries for longer MySQL queries. Some scripts, like get_moment and contrast, hold their own unique functions.
"""

# logging / er

logger = logging.getLogger('rosa.log')

def init_logger(logging_level):
	if logging_level:
		file_ = Path(__file__)
		log_dest = file_.parent.parent / "rosa.log"
		
		# init loggers
		logger = logging.getLogger('rosa.log')
		logger.setLevel(logging.DEBUG)

		logger_mysql = logging.getLogger('mysql.connector')
		logger_mysql.setLevel(logging.DEBUG)

		# clear thei handlers if present
		if logger.hasHandlers():
			logger.handlers.clear()
		
		if logger_mysql.hasHandlers():
			logger_mysql.handlers.clear()

		# init handlers
		file_handler = logging.FileHandler(log_dest, mode='a')
		file_handler.setLevel(logging.DEBUG)
	
		console_handler = logging.StreamHandler()
		console_handler.setLevel(logging_level.upper())

		mysql_console_handler = logging.StreamHandler()
		mysql_console_handler.setLevel(logging_level.upper())

		# define formatting - file loggers share format
		mysql_cons = "[%(levelname)s][%(name)s]: %(message)s"
		console_ = "[%(levelname)s][%(module)s:%(lineno)s]: %(message)s"
		file_ = "[%(asctime)s][%(levelname)s][%(module)s:%(lineno)s]: %(message)s"

		file_format = logging.Formatter(file_)
		console_format = logging.Formatter(console_)

		mysql_console_format = logging.Formatter(mysql_cons)

		# apply formatting
		file_handler.setFormatter(file_format)
		console_handler.setFormatter(console_format)

		mysql_console_handler.setFormatter(mysql_console_format)

		# add handlers to loggers
		logger.addHandler(file_handler)
		logger.addHandler(console_handler)

		logger_mysql.addHandler(file_handler)
		logger_mysql.addHandler(mysql_console_handler)

		logger.propagate = False
		logger_mysql.propagate = False

		return logger
	else:
		logger.warning("logger not passed; maybe config isn't configured?")
		sys.exit(1)

def mini_ps(args, nomix): # mini_parser for arguments/flags passed to the scripts
	force = False # no checks - force
	prints = False # no prints - prints

	if args:
		if args.force:
			force = True

		if args.silent:
			logging_level= "critical".upper()
			logger = init_logger(logging_level)
		elif args.verbose: # can't do verbose & silent
			logging_level = "debug".upper()
			logger = init_logger(logging_level)
			prints = True
		else:
			logger = init_logger(LOGGING_LEVEL.upper())

	else:
		logger = init_logger(LOGGING_LEVEL.upper())
	
	start = time.perf_counter()

	logger.debug(f"[rosa]{nomix} executed & timer started")
	return logger, force, prints, start

def doit_urself():
	cd = Path(__file__)
	rosa = cd.parent.parent
	rosa_log = rosa / "rosa.log"
	rosa_records = rosa / "rosa_records"

	rosasz = os.path.getsize(rosa_log)
	rosakb = rosasz / 1024

	rosa_records_max = 5

	if rosakb >= 64.0:
		if rosa_records.exists():
			if rosa_records.is_file():
				logger.error(f"there is a file named rosa_records where a logging record should be; abandoning")
			elif rosa_records.is_dir():
				npriors = 0
				previous = []
				for file_ in sorted(rosa_records.glob('*')):
					if file_.is_file():
						previous.append(file_)
						npriors += 1

				if npriors > rosa_records_max:
					difference = npriors - rosa_records_max
					wanted = previous[difference:(rosa_records_max + difference)]

					for unwanted in previous:
						if unwanted not in wanted:
							unwanted.unlink()
							logger.debug(f"rosa_records reached capacity; oldest record deleted (curr. max log files recorded: {rosa_records_max} | curr. max log file sz: {rosakb})")
				else:
					ctime = f"{time.time():.2f}"
					subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

					logger.debug('backed up & replaced rosa.log')
		else:
			rosa_records.mkdir(parents=True, exist_ok=True)
			ctime = f"{time.time():.2f}"
			subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

			logger.debug('backed up & replaced rosa.log')
	else:
		logger.info('rosa.log: [ok]')


# connection & management thereof


@contextlib.contextmanager
def phones():
	"""Context manager for the mysql.connector connection object."""
	conn = None
	logger.debug('...phone call, connecting...')
	try:
		conn = init_conn(XCONFIG['user'], XCONFIG['pswd'], XCONFIG['name'], XCONFIG['addr'])

		if conn.is_connected():
			yield conn
		else:
			logger.warning('connection object lost')
			try:
				conn.ping(reconnect=True, attempts=3, delay=1)
				if conn.is_connected():
					logger.info('connection obj. recovered after failed connect')
					yield conn

				else:
					logger.warning('reconnection failed; abandoning')
					sys.exit(1)
			except:
				raise
			else:
				logger.info('connection obj. recovered w.o exception [after exception was caught]')

	except KeyboardInterrupt as ko:
		logger.error('boss killed it; wrap it up')
		_safety(conn)
	except (ConnectionRefusedError, TimeoutError, Exception) as e:
		logger.error(f"error encountered while connecting to the server:{RESET} {e}.", exc_info=True)
		_safety(conn)
	except:
		logger.critical('uncaught exception found by phones; abandoning & rolling back')
		_safety(conn)
	else:
		logger.debug('phones executed w.o exception')
	finally:
		if conn:
			if conn.is_connected():
				conn.close()
				logger.info('phones closed conn [finally]')

def init_conn(db_user, db_pswd, db_name, db_addr): # used by all scripts
	"""Initiate the connection to the server. If an error occurs, [freak out] raise."""
	config = {
		'unix_socket': '/tmp/mysql.sock',
		# 'host': db_addr,
		'user': db_user,
		'password': db_pswd,
		'database': db_name,
		'autocommit': False,
		# 'use_pure': False # 5.886, 5.902, 5.903 | not worth the lib
		'use_pure': True # 5.122, 5.117, 5.113 | seems faster regardless
		# 'use_unicode': False # 3.213 (after use_pure: 3.266)
		# 'pool_size': 5
	}

	try:
		conn = mysql.connector.connect(**config)
	except:
		raise
	else:
		return conn

def _safety(conn):
	"""Handles rollback of the server on err from phone_duty."""
	logger.warning('_safety called to rollback server due to err')

	if conn and conn.is_connected():
		try:
			conn.rollback()
		except ConnectionRefusedError as cre:
			logger.error(f"{RED}_safety failed due to connection being refused:{RESET} {cre}")
			sys.exit(3)
		except:
			logger.error(f"{RED}_safety failed; abandoning{RESET}")
			if conn:
				conn.ping(reconnect=True, attempts=3, delay=1)
				if conn and conn.is_connected():
					conn.rollback()
					logger.warning('conn is connnected & server rolled back (after caught exception & reconnection)')

				else:
					logger.warning('could not ping server; abandoning')
					sys.exit(1)
			else:
				logger.warning('conn object completely lost; abandoning')
				sys.exit(1)
		else:
			logger.warning('_safety recovered w.o exception')
	else:
		logger.warning('couldn\'t rollback due to faulty connection; abandoning')
		sys.exit(1)


# COLLECTING LOCAL DATA


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
					logger.debug('log jam')
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


# EDIT LOCAL DIRECTORY


@contextlib.contextmanager
def fat_boy(_abs_path):
	"""Context manager for temporary directory and backup."""
	tmp_ = None
	backup = None
	abs_path = Path(_abs_path)

	try:
		tmp_, backup = configure(abs_path)
		if tmp_ and backup:
			logger.debug(f"fat boy made {tmp_} and {backup}; yielding...")
			yield tmp_, backup # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		_lil_guy(abs_path, backup, tmp_)
		sys.exit(0)

	except (mysql.connector.Error, ConnectionError, Exception) as e:
		logger.error(f"{RED}err encountered while attempting atomic wr:{RESET} {e}.", exc_info=True)
		_lil_guy(abs_path, backup, tmp_)
		sys.exit(1)

	else:
		try:
			apply_atomicy(tmp_, abs_path, backup)

		except KeyboardInterrupt as c:
			logger.warning('boss killed it; wrap it up')
			_lil_guy(abs_path, backup, tmp_)
			sys.exit(0)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
			_lil_guy(abs_path, backup, tmp_)
			sys.exit(1)
		else:
			logger.debug("fat boy finished w.o exception")

def _lil_guy(abs_path, backup, tmp_):
	"""Handles recovery on error for the context manager fat_boy."""
	try:
		if backup and backup.exists():
			if tmp_.exists():
				shutil_fx(tmp_)
			try:
				backup.rename(abs_path)
			except:
				raise
			else:
				logger.warning("moved backup back to original location")
		else:
			if tmp_ and tmp_.exists():
				shutil_fx(tmp_)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
		raise
	else:
		logger.info("_lil_guy's cleanup complete")

def shutil_fx(dir_):
	if dir_.exists() and dir_.is_dir():
		try:
			shutil.rmtree(dir_)
		except:
			logger.warning('err for shutil fx, letting her relax and retrying')
			time.sleep(7)
			if dir_.exists():
				try:
					shutil.rmtree(dir_)
				except:
					logger.warning('failed twice, calling it')
					raise
		else:
			if dir_.exists():
				try:
					shutil.rmtree(dir_)
				except:
					logger.warning('failed twice, calling it')
					raise
	else:
		logger.warning('shutil_fx passed something that was not a directory')
	
	if dir_.exists():
		logger.warning(f"shutil_fx could not delete {dir_}")
	else:
		logging.debug(f"shutil_fx deleted {dir_}")

def configure(abs_path): # raise err & say 'run get all or fix config's directory; there is no folder here'
	"""Configure the temporary directory & move the original to a backup location. 
	Returns the _tmp directory's path.
	"""
	if abs_path.exists():
		try:
			tmp_ = Path(tempfile.mkdtemp(dir=abs_path.parent))
			backup = Path( (abs_path.parent) / f"Backup_{time.time():2f}" )

			abs_path.rename(backup)
			logger.debug('local directory moved to backup')

			if tmp_.exists() and backup.exists():
				logger.debug(f"{tmp_} and {backup} configured by [configure]")
	
		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"err encountered while trying move {abs_path} to a backup location: {e}.", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & original directory moved to backup w.o exception')
			return tmp_, backup
	else:
		logger.warning(f"{abs_path} doesn't exist; fix the config or run 'rosa get all'")
		sys.exit(1)

def calc_batch(conn):
	"""Get the average row size of the notes table to estimate optimal batch size for downloading. ASSESS2 is 1/100 the speed of ASSESS"""
	batch_size = 5 # default
	row_size = 10 # don't divide by 0

	with conn.cursor() as cursor:
		try:
			# beg = time.perf_counter()
			cursor.execute(ASSESS2)
			row_size = cursor.fetchone()
			# if row_size:
			# 	end = time.perf_counter()
			# 	logger.info(f"ASSESS2 took {(end - beg):.4f} seconds")
		except (ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"err encountered while attempting to find avg_row_size: {c}", exc_info=True)
			raise
		else:
			if row_size:
				if row_size[0] and row_size[0] != 0:
					batch_size = max(1, int((0.94*MAX_ALLOWED_PACKET) / row_size[0]))
					logger.debug(f"batch size: {batch_size}")
					return batch_size, row_size
				else:
					logger.warning(f"couldn't use row_size; defaulting to batch size = {batch_size}")
					return batch_size, row_size
			else:
				logger.warning(f"ASSESS2 returned nothing; defaulting to batch size = {batch_size}")
				return batch_size, row_size

def scope_sz(local_dir):
	blk_list = ['.DS_Store', '.git', '.obsidian']
	abs_path = Path(local_dir)

	files = 0
	tsz = 0

	for path in abs_path.rglob('*'):
		tsz += os.path.getsize(path)
		files += 1

	avg = tsz / files
	logger.info(f"found avg_size of local file[s] : {avg}")

	return int(avg)

# WRITING TO DISK

def save_people(people, backup, tmp_):
	"""Hard-links unchanged files present in the server and locally from the backup directory (original) 
	to the _tmp directory. Huge advantage over copying because the file doesn't need to move."""
	# try:
	with tqdm(people, unit="hard-links", leave=True) as pbar:
		for person in pbar:
			try:
				curr = Path( backup / person )
				tmpd = Path( tmp_ / person )

				os.link(curr, tmpd)

			except (PermissionError, FileNotFoundError, KeyboardInterrupt, Exception) as te:
				raise

def download_batches2(flist, conn, batch_size, tmp_): # get
	"""Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
	dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
	This function passes the found data to the wr_data function, which writes the new data structure to the disk.
	"""
	paths = [item[0] for item in flist]
	params = ', '.join(['%s']* len(paths))

	offset = 0

	with conn.cursor() as cursor:
		try:
			while True:
				query = f"SELECT frp, content FROM notes WHERE frp IN ({params}) LIMIT {batch_size} OFFSET {offset};"

				try:
					cursor.execute(query, paths)
					batch = cursor.fetchall()

				except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
					logger.warning(f"error while trying to download data: {c}.", exc_info=True)
					raise
				else:
					if batch:
						wr_batches(batch, tmp_)

					if len(batch) < batch_size:
						break

					offset += batch_size

		except: # tout de monde
			logger.critical(f"{RED}err while attempting batched atomic write{RESET}", exc_info=True)
			raise

def download_batches5(souls, conn, batch_size, row_size, tmp_): # get_all ( aggressive )
	"""Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
	dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
	This function passes the found data to the wr_data function, which writes the new data structure to the disk.
	"""
	batch_count = int(len(souls) / batch_size)
	if len(souls) % batch_size:
		batch_count += 1

	kbb = False
	# curr_count = 0
	batched_list = list(batched(souls, batch_size))

	logger.debug(f"split list into {batch_count} batches")

	batch_mbytes = (batch_size * row_size[0]) / (1024*1024)

	# bar = "{l_bar}{bar}| {n:.3f}/{total:.3f} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
	bar = "{l_bar}{bar}| {n:.0f}/{total:.0f} [{rate_fmt}{postfix}]"

	try:
		with logging_redirect_tqdm(loggers=[logger]):
			with tqdm(batched_list,
			desc=f"Pulling {batch_count} batches", unit=" batches", unit_scale=True, 
			unit_divisor=1024, colour="white", bar_format = bar) as pbar:
				for bunch in pbar:
					# batch = []
					actual = 0
					# cpr = 0

					current_rate = pbar.format_dict['rate']
					spd_str = "? mb/s"
					# cpr_str = "?:1"

					if current_rate:
						actual = current_rate * batch_mbytes
						spd_str = f"{actual:.2f}mb/s"

					with conn.cursor() as cursor:
						try:
							inputs = ', '.join(['%s']*len(bunch))
							query = f"SELECT frp, content FROM notes WHERE frp IN ({inputs});"

							cursor.execute(query, bunch)
							batch = cursor.fetchall()
							# logger.debug('got one batch of data')
							
							if batch:
								# logger.debug('...passing batch to wr_batches...')
								wr_batches(batch, tmp_)
								# uncpr = wr_batches(batch, tmp_)

								pbar.set_postfix_str(f"{spd_str}")
								# if cpr and uncpr:
								#     current_rate = uncpr / cpr
								#     cpr_str = f"{current_rate:.1f}:1"
									# wr_pace = current_rate * actual
									# pbar.set_postfix_str(f"{spd_str} | cmpr: {cpr_str}")
									# pbar.set_postfix_str(f"{spd_str} | cmpr: {cpr_str} | wr_rate: {wr_pace:.2f}mb/s")

						except KeyboardInterrupt as c:
							pbar.leave = False
							pbar.close()
							try:
								cursor.fetchall()
								cursor.close()
							except:
								pass
							logger.warning(f"{RED}boss killed it; deleting partial downlaod")
							raise
						except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
							logger.error(f"err while trying to downwrite data: {c}.", exc_info=True)
							pbar.leave = False
							pbar.close()
							try:
								cursor.fetchall()
								cursor.close()
							except:
								pass
							raise

	except KeyboardInterrupt as c:
		raise
	else:
		logger.debug('atomic wr w.batched download completed w.o exception')

def wr_batches(data, tmp_):
	"""Writes each batch to the _tmp directory as they are pulled. Each file has it and its parent directory flushed from memory for assurance of atomicy."""
	# logger.debug('...writing batch to disk...')
	# dcmpr = zstd.ZstdDecompressor() # init outside of loop; duh
	try:
		for frp, content in data:
			t_path = Path ( tmp_ / frp ) #.resolve()
			(t_path.parent).mkdir(parents=True, exist_ok=True)

			# d_content = dcmpr.decompress(content)
			with open(t_path, 'wb') as t:
				t.write(content)

	except KeyboardInterrupt as ki:
		raise
	except (PermissionError, FileNotFoundError, Exception) as e:
		raise
	# else:
		# logger.debug('wrote batch w.o exception')

def apply_atomicy(tmp_, abs_path, backup):
	"""If the download and write batches functions both complete entirely w.o error, this function moved the _tmp directory back to the original abs_path. 
	If this completes w.o error, the backup is deleted.
	"""
	try:
		tmp_.rename(abs_path)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		logger.debug('temporary directory renamed w.o exception')
		shutil_fx(backup)

def mk_rrdir(raw_directories, abs_path):
	"""Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
	logger.debug('...writing directory tree to disk...')
	directories = {dir_[0] for dir_ in raw_directories}
	# try:
	with logging_redirect_tqdm(loggers=[logger]):
		with tqdm(directories, desc=f"Writing {len(directories)} directories", unit="dirs") as pbar:
			try:
				for directory in pbar:
					fdpath = Path(abs_path / directory ).resolve()
					fdpath.mkdir(parents=True, exist_ok=True)

			except (PermissionError, FileNotFoundError, Exception) as e:
				pbar.leave = False
				pbar.close()
				logger.error(f"{RED}error when tried to make directories:{RESET} {e}.", exc_info=True)
				raise
			else:
				logger.debug('created directory tree on disk w.o exception')


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
            logger.info(f"upload time [in minutes] for rosa {nomic}: {duration_minutes:.3f}")
        else:
            logger.info(f"upload time [in seconds] for rosa {nomic}: {duration:.3f}")

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