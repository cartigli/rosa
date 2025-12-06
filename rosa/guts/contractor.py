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

logger = logging.getLogger('rosa.log')

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

