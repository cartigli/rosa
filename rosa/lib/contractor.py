import sys
import time
import shutil
import logging
import tempfile
import contextlib
from pathlib import Path
from itertools import batched

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm as tqdm_
import mysql.connector # only for error codes in this file

from rosa.confs import ASSESS2
from rosa.confs import LOCAL_DIR, MAX_ALLOWED_PACKET, RED, RESET

"""
Handles all the functions for downloading and writing data to the disk. [ fat_boy() ] is a key contextmanager that handles recovery 
on error. [ download_batches() ] is the most efficient downloader and writer, but it is not used by get_all due to formatting.
This lib file is fairly specific, it is simply long due to complex logic, iterations 
of functions, and lack of conciseness, especially in the error handling.

[functions]
(contextmanager)
fat_boy(_abs_path),
(contextmanager)
sfat_boy(abs_path),
_lil_guy(abs_path, backup, tmp_),
shutil_fx(dir_),
configure(abs_path),
apply_atomicy(tmp_, abs_path, backup),
save_people(people, backup, tmp_),
download_batches5(souls, conn, batch_size, row_size, tmp_),
wr_batches(data, tmp_),
mk_rrdir(raw_directories, abs_path)
"""

logger = logging.getLogger('rosa.log')

# SETUP EDITOR FOR LOCAL DISK

@contextlib.contextmanager
def fat_boy(_abs_path):
	"""
	Context manager for temporary directory and backup. Takes over if error 
	is caught or occurs while downloading & writing to disk. Fairly aggressive.
	"""
	tmp_ = None # ORIGINAL
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

	except (FileNotFoundError, PermissionError, Exception) as e:
		logger.error(f"{RED}err caught while backup & temporary directories:{RESET} {e}.", exc_info=True)
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

@contextlib.contextmanager
def sfat_boy(_abs_path):
	"""
	Context manager for the get_all because there is nothing to backup.
	"""
	tmp_ = Path(_abs_path)

	try:
		if tmp_:
			if tmp_.is_dir():
				pass
			else:
				tmp_.mkdir(parents=True, exist_ok=True)

			logger.debug(f"fat boy made {tmp_}; yielding...")
			yield tmp_ # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		shutil_fx(tmp_)
		sys.exit(0)
	except (mysql.connector.Error, ConnectionError, Exception) as e:
		logger.error(f"{RED}err encountered while attempting atomic wr:{RESET} {e}", exc_info=True)
		shutil_fx(tmp_)
		sys.exit(1)
	else:
		logger.info('sfat_boy caught no exeptions while writing_all')

def _lil_guy(abs_path, backup, tmp_):
	"""Handles recovery on error for the context manager fat_boy (don't have to rewrite it for every error caught)."""
	try:
		if backup and backup.exists():
			if tmp_ and tmp_.exists():
				shutil_fx(tmp_)
				backup.rename(abs_path)
				logger.warning("moved backup back to original location & deleted the temporary directory")
		elif tmp_ and tmp_.exists():
			shutil_fx(tmp_)
			logger.warning(f"_lil_guy called to recover on error but the backup was no where to  be found. deleted temporary directory")
		else:
			logger.debug('_lil_guy called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
		raise
	else:
		logger.info("_lil_guy's cleanup had no exceptions")

def shutil_fx(dirx):
	"""Handles the retry logic & check anytime a function needs to delete a directory (freaking silicon)"""
	if dirx.exists() and dirx.is_dir():
		try:
			shutil.rmtree(dirx)
		except:
			logger.warning('err for shutil fx, letting her relax and retrying')
			time.sleep(5)
			if dirx.exists():
				try:
					shutil.rmtree(dirx)
				except:
					logger.warning(f"failed to delete {dirx} twice, calling it")
					raise
			else:
				logger.debug(f"shutil_fx removed {dirx} on retry")
				return
		else:
			if dirx.exists():
				try:
					shutil.rmtree(dirx)
				except:
					logger.warning('failed twice, calling it')
					raise
			else:
				logger.debug(f"shutil_fx deleted {dirx}")
				return
	else:
		logger.warning('shutil_fx passed something that was not a directory')
	
	if dirx.exists():
		logger.warning(f"shutil_fx could not delete {dirx}")
	else:
		logging.debug(f"shutil_fx deleted {dirx}")

def configure(abs_path): # raise err & say 'run get all or fix config's directory; there is no folder here'
	"""Configure the temporary directory & move the original to a backup location. Returns the _tmp directory's path."""
	if abs_path.exists():
		parent = abs_path.parent
		try:
			tmpd = Path(tempfile.mkdtemp(dir=parent))
			backup = parent / f".{time.time():.0f}"

			abs_path.rename(backup)
			logger.debug('local directory moved to backup')

			if tmpd.exists() and backup.exists():
				logger.debug(f"{tmpd} and {backup} configured by [configure]")
	
		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {abs_path} to a backup location:{RESET} {e}.", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & original directory moved to backup w.o exception')
			return tmpd, backup
	else:
		logger.warning(f"{abs_path} doesn't exist; fix the config or run 'rosa get all'")
		sys.exit(1)

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
		logger.debug('temporary directory renamed w.o exception; removing backup of original')
		shutil_fx(backup)

# WRITING TO DISK

def save_people(people, backup, tmp_):
	"""Hard-links unchanged files present in the server and locally from the backup directory (original) 
	to the _tmp directory. Huge advantage over copying because the file doesn't need to move."""
	with tqdm_(loggers=[logger]):
		with tqdm(people, unit="hard-links", leave=True) as pbar:
			for person in pbar:
				try:
					curr = Path( backup / person ) # x[tuple management]
					tmpd = Path( tmp_ / person ) # x[tuple management]

					tmpd.hardlink_to(curr)

				except (PermissionError, FileNotFoundError, KeyboardInterrupt, Exception) as te:
					raise

def download_batches5(souls, conn, batch_size, row_size, tmp_): # get_all ( aggressive )
	"""Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
	dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
	This function passes the found data to the wr_data function, which writes the new data structure to the disk.
	This was the fastest form of many I tested, but obviously multitudes faster than download_batches2() 
	above this one, which is the WORST and only used by one ex_fxs, but this is due to procrastination. 
	Using Offset/Limit with 100,000+ files was a terrible idea and needs to be completely removed. 
	"""
	# souls = [soul[0] for soul in xsouls] # x[tuple management]

	batch_count = int(len(souls) / batch_size)
	if len(souls) % batch_size:
		batch_count += 1

	batched_list = list(batched(souls, batch_size))

	logger.debug(f"split list into {batch_count} batches")

	batch_mbytes = (batch_size * row_size[0]) / (1024*1024)

	# bar = "{l_bar}{bar}| {n:.3f}/{total:.3f} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
	bar = "{l_bar}{bar}| {n:.0f}/{total:.0f} [{rate_fmt}{postfix}]"

	try:
		with tqdm_(loggers=[logger]):
			with tqdm(batched_list,
			desc=f"Pulling {batch_count} batches", unit=" batches", unit_scale=True, 
			unit_divisor=1024, bar_format = bar) as pbar:
				for bunch in pbar:
					actual = 0

					current_rate = pbar.format_dict['rate']
					spd_str = "? mb/s"

					if current_rate:
						actual = current_rate * batch_mbytes
						spd_str = f"{actual:.2f}mb/s"

					with conn.cursor() as cursor:
						try:
							inputs = ', '.join(['%s']*len(bunch))
							query = f"SELECT frp, content FROM notes WHERE frp IN ({inputs});"

							cursor.execute(query, bunch)
							batch = cursor.fetchall()							
							if batch:
								# logger.debug('...passing batch to wr_batches...')
								wr_batches(batch, tmp_)

								pbar.set_postfix_str(f"{spd_str}")

						except KeyboardInterrupt as c:
							pbar.leave = False
							pbar.close()
							try:
								cursor.fetchall()
								cursor.close()
							except:
								pass
							logger.warning(f"{RED}boss killed it; deleting partial downlaod{RESET}")
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

def mk_rrdir(raw_directories, abs_path):
	"""Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
	logger.debug('...writing directory tree to disk...')

	# directories = {dir_[0] for dir_ in raw_directories} # set comprehension? tf

	with tqdm_(loggers=[logger]):
		with tqdm(raw_directories, desc=f"Writing {len(raw_directories)} directories", unit="dirs") as pbar:
			try:
				for directory in pbar:
					fdpath = Path(abs_path / directory[0] ).resolve() # [tuple management]
					fdpath.mkdir(parents=True, exist_ok=True)

			except (PermissionError, FileNotFoundError, Exception) as e:
				pbar.leave = False
				pbar.close()
				logger.error(f"{RED}error when tried to make directories:{RESET} {e}.", exc_info=True)
				raise
			else:
				logger.debug('created directory tree on disk w.o exception')