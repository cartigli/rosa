"""Handles data on the disk.

Configures directories for safe writing.
Downloads data, creates directories, and writes files.
Majority of the logic is for handling errors, and ensuring
the original source directory is not altered on failure.
"""


import os
import sys
import time
import shutil
import logging
import tempfile
import contextlib
from itertools import batched

import mysql.connector

from rosa.confs import RED, RESET, BLACKLIST

logger = logging.getLogger('rosa.log')

# SETUP EDITOR FOR LOCAL DISK

@contextlib.contextmanager
def fat_boy(dir_):
	"""Context manager for the temporary directory and backup of original. 
	
	Moves contents instead of renaming directory for C.W.D. preservation.

	Args:
		dir_ (str): Path of the source directory.

	Yields: 
		tmpd (str): The temporary directory.
		backup (str): The original source directory as a backup.
	"""
	tmpd = None
	backup = None

	try:
		tmpd, backup = configure(dir_)
		if tmpd and backup:

			logger.debug(f"fat boy1 yielding tmpd and backup...")
			yield tmpd, backup # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		lil_guy(dir_, backup, tmpd)
		sys.exit(0)

	except (FileNotFoundError, PermissionError, Exception) as e:
		logger.error(f"{RED}err caught while backup & temporary directories:{RESET} {e}.", exc_info=True)
		lil_guy(dir_, backup, tmpd)
		sys.exit(1)
	else:
		try:
			apply_atomicy(tmpd, backup, dir_)

		except KeyboardInterrupt as c:
			logger.warning('boss killed it; wrap it up')
			lil_guy(dir_, backup, tmpd)
			sys.exit(0)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
			lil_guy(dir_, backup, tmpd)
			sys.exit(1)
		else:
			logger.debug("fat boy finished w.o exception")

@contextlib.contextmanager
def fat_boy_o(dir_):
	"""Context manager for the temporary directory and backup of original. 
	
	Replaces corrupted directory with backup on error.

	Args:
		dir_ (str): Path of the Source directory.
	
	Yields: 
		tmpd (str): The temporary directory the new data is being downloaded and written to. Deleted on error, renamed as source directory if not.
		backup (str): The original source directory after being renamed to a backup location/path. Renamed to source directory on error, deleted if not.
	"""
	tmpd = None
	backup = None

	try:
		tmpd, backup = configure_o(dir_)
		if tmpd and backup:

			logger.debug(f"fat boy o yielding tmpd and backup...")
			yield tmpd, backup # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		_lil_guy_o(dir_, backup, tmpd)
		sys.exit(0)

	except (FileNotFoundError, PermissionError, Exception) as e:
		logger.error(f"{RED}err caught while backup & temporary directories:{RESET} {e}.", exc_info=True)
		_lil_guy_o(dir_, backup, tmpd)
		sys.exit(1)
	else:
		try:
			apply_atomicy_o(dir_, tmpd, backup)

		except KeyboardInterrupt as c:
			logger.warning('boss killed it; wrap it up')
			_lil_guy_o(dir_, backup, tmpd)
			sys.exit(0)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
			_lil_guy_o(dir_, backup, tmpd)
			sys.exit(1)
		else:
			logger.debug("fat boy finished w.o exception")

@contextlib.contextmanager
def sfat_boy(tmpd):
	"""Simplified context manager for rosa [get][all] because there is no original to backup. 

	Args:
		abs_path (str): Path of the desired directory.
	
	Yields: 
		tmpd: A new directory.
	"""
	try:
		if tmpd:
			if os.path.isdir(tmpd):
				logger.warning('A directory already exists where sfat_boy was pointed; deleting & recreating')
				shutil_fx(tmpd)

			os.makedirs(tmpd, exist_ok=True)

			logger.debug(f"sfat_boy yielding tmpd...")
			yield tmpd # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		shutil_fx(tmpd)
		sys.exit(0)
	except (mysql.connector.Error, ConnectionError, Exception) as e:
		logger.error(f"{RED}err encountered while attempting atomic wr:{RESET} {e}", exc_info=True)
		shutil_fx(tmpd)
		sys.exit(1)
	else:
		logger.debug('sfat_boy caught no exeptions while writing_all')

def lil_guy(dir_, backup, tmpd):
	"""Handles recovery if error is caught by fat_boy.

	Args:
		dir_ (str): Path of the source directory.
		backup (str): Path to the source directory.
		tmpd (str): Temporary directory.
	
	Returns:
		None
	"""
	try:
		if backup and os.path.exists(backup):
			if tmpd and os.path.exists(tmpd):
				shutil_fx(tmpd)

				for entry in os.scandir(backup):
					destin = os.path.join(dir_, entry.name)

					os.rename(entry.path, destin)

				logger.warning("moved backup back to original location & deleted the temporary directory")

		elif tmpd and os.path.exists(tmpd):
			shutil_fx(tmpd)
			logger.error(f"lil_guy called to recover on error and couldn't find the backup. deleted temporary directory")

		else:
			logger.error('lil_guy called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {dir_} and cleanup encountered an error: {e}.", exc_info=True)
		if os.path.exists(dir_):
			shutil_fx(dir_)
		os.rename(backup, dir_)
		raise
	else:
		logger.info("lil_guy's cleanup had no exceptions")
		shutil_fx(backup)

def _lil_guy_o(abs_path, backup, tmpd):
	"""Handles recovery if error is caught by fat_boy_o.

	Args:
		abs_path (str): Full path of the source directory from config.py
		backup (str): Original source directory renamed to a backup location while downloading and writing.
		tmpd (str): Temporary directory made to hold the updated source directory. Renamed to source directory if succeeds, deleted if error is caught.
	
	Returns:
		None
	"""
	try:
		if backup and os.path.exists(backup):
			if tmpd and os.path.exists(tmpd):
				os.rename(backup, abs_path)

				shutil_fx(tmpd)
				logger.warning("moved backup back to original location & deleted the temporary directory")

		elif tmpd and os.path.exists(tmpd):
			shutil_fx(tmpd)
			logger.error(f"_lil_guy_o called to recover on error and couldn't find the backup. deleted temporary directory")

		else:
			logger.error('_lil_guy_o called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
		raise
	else:
		logger.info("_lil_guy_o's cleanup had no exceptions")

def shutil_fx(dirx):
	"""Handles deletion of directories with retries.

	Args:
		dirx (str): Directory to be removed.
	
	Returns:
		None
	"""
	if dirx:
		if os.path.isdir(dirx):
			try:
				shutil.rmtree(dirx)
			except:
				logger.warning('err for shutil fx, letting her relax and retrying')
				time.sleep(5)

				if os.path.exists(dirx):
					try:
						shutil.rmtree(dirx)

					except:
						logger.warning(f"failed to delete {dirx} twice, calling it")
						raise
				else:
					logger.debug(f"shutil_fx removed {dirx} on retry")
					return
			else:
				if os.path.exists(dirx):
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
		
		if os.path.exists(dirx):
			logger.warning(f"shutil_fx could not delete {dirx}")
		else:
			logger.debug(f"shutil_fx deleted {dirx}")

def configure(dir_):
	"""Configures the backup and creates the tmpd.

	Moves content of the source directory to a backup.
	Creates a temporary directory.

	Args:
		dir_ (str): Path of the source directory.
	
	Returns:
		tmpd (str): Temporary directory.
		backup (str): Source directory renamed as a backup.
	"""
	dir_ = dir_.rstrip(os.sep)

	if os.path.exists(dir_):
		parent = os.path.dirname(dir_)
		try:
			tmpd = tempfile.mkdtemp(dir=parent)

			backup = os.path.join(parent, f".{time.time():.0f}")
			os.makedirs(backup, exist_ok=True)

			for entry in os.scandir(dir_):
				rp = entry.name # K.I.S.S.
				destin = os.path.join(backup, rp)

				os.rename(entry.path, destin)

			logger.debug('local directory moved to backup')

			if os.path.exists(tmpd) and os.path.exists(backup):
				logger.debug("tmpd and backup configured by [configure]")

		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {dir_} to a backup location:{RESET} {e}.", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & original directory moved to backup w.o exception')
			return tmpd, backup
	else:
		logger.warning(f"{dir_} doesn't exist; fix the config or pull a version")
		raise FileNotFoundError ('source directory does not exist')

def configure_o(abs_path):
	"""Configures the backup and creates the tmpd.

	Renames the source directory as a backup.
	Creates a temporary directory.

	Args:
		abs_path (str): Path of the source directory.
	
	Returns:
		tmpd (str): New (empty) temporary directory.
		backup (str): Source directory renamed to as a backup location/path.
	"""
	if os.path.exists(abs_path):
		parent = os.path.dirname(abs_path)
		try:
			tmpd = tempfile.mkdtemp(dir=parent)
			backup = os.path.join(parent, f".{time.time():.0f}")

			os.rename(abs_path, backup)
			logger.debug('local directory moved to backup')

		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {abs_path} to a backup location:{RESET} {e}", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & source directory contents moved to backup')
			return tmpd, backup
	else:
		logger.warning(f"{dir_} doesn't exist; fix the config or pull a version")
		raise FileNotFoundError ('source directory does not exist')

def is_ignored(_str):
	return any(blckd in _str for blckd in BLACKLIST)

def apply_atomicy(tmpd, backup, dir_):
	"""Cleans up the 'atomic' writing for fat_boy. 
	
	Renames all of tmpd's contents as backup's content.
	Deletes backup & tmpd if no errors are caught.

	Args:
		tmpd (str): Temporary directory.
		dir_ (str): Original path of the source directory.
		backup (str): Path to the backup.

	Returns:
		None
	"""
	try:
		for entry in os.scandir(tmpd):
			destin = os.path.join(dir_, entry.name)

			os.rename(entry.path, destin)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		logger.debug('temporary directory renamed w.o exception; removing backup & tmpd')
		shutil_fx(backup)
		shutil_fx(tmpd)

def apply_atomicy_o(abs_path, tmpd, backup):
	"""Cleans up the 'atomic' writing for fat_boy_o. 
	
	Renames tmpd as source directory & deletes backup *if no errors are caught.

	Args:
		tmpd (str): Temporary directory containing the updated directory.
		abs_path (str): Original path of the source directory [empty at this point].
		backup (str): Location/path the source directory was moved to.
	
	Returns:
		None
	"""
	try:
		os.rename(tmpd, abs_path)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		shutil_fx(backup)
		logger.debug('temporary directory renamed and backup removed')

# WRITING TO DISK

def save_people(people, backup, tmpd):
	"""Hard-links unchanges files from the original to the tmpd.

	Takes the relative paths passed and builds two new ones for each directory.
	Uses .hardlink_to() to make a 'hard-link' between the two directories.

	Args:
		people (list): List of relative paths of unchanged files.
		backup (str): Path of the source directory.
		tmpd (str): Path of the temporary directory.

	Returns:
		None
	"""
	for person in people:
		curr = os.path.join(backup, person)
		tmp_ = os.path.join(tmpd, person)

		os.makedirs(os.path.dirname(tmp_), exist_ok=True)
		try:
			os.link(curr, tmp_)

		except (PermissionError, FileNotFoundError, KeyboardInterrupt, Exception):
			raise

def wr_batches(data, tmpd):
	"""Writes each batch's data to the tmpd as they come. 
	
	Each files' parent is made if does not exist (faster than checking).

	Args:
		data (2-element tuple): Tupled list of pairs (relative paths, content).
		tmpd (str): Target directory.

	Returns:
		None
	"""
	try:
		for frp, content in data:
			t_path = os.path.join(tmpd, frp)
			os.makedirs(os.path.dirname(t_path), exist_ok=True)

			with open(t_path, 'w', encoding='utf-8') as t:
				t.write(content)

	except KeyboardInterrupt as ki:
		raise
	except (PermissionError, FileNotFoundError, Exception) as e:
		raise

def mk_rrdir(drps, tmpd):
	"""Writes remote directories to the disk.

	Args:
		raw_directories (list): Single-item tuples (relative paths,).
		tmpd (str): Target directory to write the directories in.
	
	Returns:
		None
	"""
	logger.debug('...correcting local directory tree...')

	for rp in drps:
		fp = os.path.join(tmpd, rp[0])
		os.makedirs(fp, exist_ok=True)