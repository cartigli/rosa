"""Handles data on the disk.

Configures directories for safe writing.
Downloads data, creates directories, and writes files.
Also deletes directories and files.
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
	"""Conext manager for the temporary directory and backup of original. 
	
	Takes over on error and ensures corrupted data is never kept.

	Args:
		abs_path (str): Path of the Source directory.
	
	Yields: 
		tmpd (Path): The temporary directory the new data is being downloaded and written to. Deleted on error, renamed as source directory if not.
		backup (Path): The original source directory after being renamed to a backup location/path. Renamed to source directory on error, deleted if not.
	"""
	tmpd = None # ORIGINAL
	backup = None

	abs_path = Path(dir_)
	try:

		tmpd, backup = configure(abs_path)
		if tmpd and backup:

			# logger.debug(f"fat boy made {tmpd} and {backup}; yielding...")
			yield tmpd, backup # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		_lil_guy(abs_path, backup, tmpd)
		sys.exit(0)

	except (FileNotFoundError, PermissionError, Exception) as e:
		logger.error(f"{RED}err caught while backup & temporary directories:{RESET} {e}.", exc_info=True)
		_lil_guy(abs_path, backup, tmpd)
		sys.exit(1)
	else:
		try:
			apply_atomicy(tmpd, abs_path, backup)

		except KeyboardInterrupt as c:
			logger.warning('boss killed it; wrap it up')
			_lil_guy(abs_path, backup, tmpd)
			sys.exit(0)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
			_lil_guy(abs_path, backup, tmpd)
			sys.exit(1)
		else:
			logger.debug("fat boy finished w.o exception")


@contextlib.contextmanager
def fat_boy_o(dir_):
	"""Conext manager for the temporary directory and backup of original. 
	
	Takes over on error and ensures corrupted data is never kept.

	Args:
		dir_ (str): Path of the Source directory.
	
	Yields: 
		tmpd (str): The temporary directory the new data is being downloaded and written to. Deleted on error, renamed as source directory if not.
		backup (str): The original source directory after being renamed to a backup location/path. Renamed to source directory on error, deleted if not.
	"""
	tmpd = None # ORIGINAL
	backup = None

	# dir_ = Path(dir_)
	try:

		tmpd, backup = configure_o(dir_)
		if tmpd and backup:

			# logger.debug(f"fat boy made {tmpd} and {backup}; yielding...")
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
def fat_boy1(dir_):
	"""Conext manager for the temporary directory and backup of original. 
	
	Takes over on error and ensures corrupted data is never kept.

	Args:
		dir_ (str): Path of the source directory.

	Yields: 
		tmpd (str): The temporary directory.
		backup (str): The original source directory as a backup.
	"""
	tmpd = None # ORIGINAL
	backup = None
	# cwd = Path(dir_).resolve()

	# abs_path = Path(dir_)
	try:

		tmpd, backup = configure1(dir_)
		if tmpd and backup:

			logger.debug(f"fat boy1 made tmpd and backup; yielding...")
			yield tmpd, backup # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		_lil_guy1(dir_, backup, tmpd)
		sys.exit(0)

	except (FileNotFoundError, PermissionError, Exception) as e:
		logger.error(f"{RED}err caught while backup & temporary directories:{RESET} {e}.", exc_info=True)
		_lil_guy1(dir_, backup, tmpd)
		sys.exit(1)
	else:
		try:
			apply_atomicy1(tmpd, backup, dir_)

		except KeyboardInterrupt as c:
			logger.warning('boss killed it; wrap it up')
			_lil_guy1(dir_, backup, tmpd)
			sys.exit(0)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
			_lil_guy1(dir_, backup, tmpd)
			sys.exit(1)
		else:
			logger.debug("fat boy finished w.o exception")

@contextlib.contextmanager
def sfat_boy(tmpd):
	"""Simplified context manager for rosa [get][all] because there is no original to backup. 

	[third piece of evidence]

	Args:
		abs_path (str): Path of the desired directory.
	
	Yields: 
		tmpd: A new directory.
	"""
	# tmpd = Path(abs_path)

	try:
		if tmpd:
			# if tmpd.is_dir():
			if os.path.isdir(tmpd):
				logger.warning('A directory already exists where sfat_boy was pointed; deleting & recreating')
				shutil_fx(tmpd)

			os.makedirs(tmpd, exist_ok=True)

			logger.debug(f"sfat_boy yielding {tmpd}...")
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

def _lil_guy(abs_path, backup, tmpd):
	"""Handles recovery if error occurs and is caught by fat_boy. 
	
	Mostly checking which directories exist at the time of the error, 
	And deleting/renaming accordingly.

	Args:
		abs_path (Path): Full path of the source directory from config.py
		backup (Path): Original source directory renamed to a backup location while downloading and writing.
		tmpd (Path): Temporary directory made to hold the updated source directory. Renamed to source directory if succeeds, deleted if error is caught.
	
	Returns:
		None
	"""
	try:
		if backup and backup.exists():
			if tmpd and tmpd.exists():
				shutil_fx(tmpd.as_posix())
				backup.rename(abs_path)
				logger.warning("moved backup back to original location & deleted the temporary directory")
		elif tmpd and tmpd.exists():
			shutil_fx(tmpd.as_posix())
			logger.warning(f"_lil_guy called to recover on error but the backup was no where to  be found. deleted temporary directory")
		else:
			logger.debug('_lil_guy called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
		raise
	else:
		logger.info("_lil_guy's cleanup had no exceptions")



def _lil_guy_o(abs_path, backup, tmpd):
	"""Handles recovery if error occurs and is caught by fat_boy. 
	
	Mostly checking which directories exist at the time of the error, 
	And deleting/renaming accordingly.

	Args:
		abs_path (str): Full path of the source directory from config.py
		backup (str): Original source directory renamed to a backup location while downloading and writing.
		tmpd (str): Temporary directory made to hold the updated source directory. Renamed to source directory if succeeds, deleted if error is caught.
	
	Returns:
		None
	"""
	try:
		# if backup and backup.exists():
		if backup and os.path.exists(backup):
			# if tmpd and tmpd.exists():
			if tmpd and os.path.exists(tmpd):
				# backup.rename(abs_path)
				os.rename(backup, abs_path)
				shutil_fx(tmpd)
				logger.warning("moved backup back to original location & deleted the temporary directory")
		# elif tmpd and tmpd.exists():
		elif tmpd and os.path.exists(tmpd):
			# shutil_fx(tmpd.as_posix())
			shutil_fx(tmpd)
			logger.warning(f"_lil_guy called to recover on error but the backup was no where to  be found. deleted temporary directory")
		else:
			logger.debug('_lil_guy called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
		raise
	else:
		logger.info("_lil_guy's cleanup had no exceptions")



def _lil_guy1(dir_, backup, tmpd):
	"""Handles recovery if error occurs and is caught by fat_boy. 
	
	Mostly checking which directories exist at the time of the error, 
	And deleting/renaming accordingly.

	Args:
		dir_ (str): Path of the source directory.
		backup (str): Path to the source directory.
		tmpd (str): Temporary directory.
	
	Returns:
		None
	"""
	try:
		# if backup and backup.exists():
		if backup and os.path.exists(backup):
			# if tmpd and tmpd.exists():
			if tmpd and os.path.exists(tmpd):
				shutil_fx(tmpd)

				# prefix = len(backup.as_posix()) + 1

				# for entry in backup.glob('*'):
				for entry in os.scandir(backup):
					# rp = entry.as_posix()[prefix:]

					# destin = dir_ / rp
					destin = os.path.join(dir_, entry.name)

					# entry.rename(destin)
					os.rename(entry.path, destin)

				logger.warning("moved backup back to original location & deleted the temporary directory")
		# elif tmpd and tmpd.exists():
		elif tmpd and os.path.exists(tmpd):
			shutil_fx(tmpd)
			logger.warning(f"_lil_guy called to recover on error but the backup was no where to  be found. deleted temporary directory")
		else:
			logger.debug('_lil_guy called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {dir_} and cleanup encountered an error: {e}.", exc_info=True)
		# if dir_.exists(): # basically fuck the cwd workarounds if an error occurs here
		if os.path.exists(dir_):
			shutil_fx(dir_) # too rare and messy to properly deal; just brute force it
		# backup.rename(dir_)
		os.rename(backup, dir_)
		raise
	else:
		logger.info("_lil_guy's cleanup had no exceptions")
		shutil_fx(backup)


def shutil_fx(dirx):
	"""Handles deletion of directories. 
	
	Since shutil.rmtree() is inconsistent in macOS silicon, 
	this offers basic retry logic if the directory exists after deletion, or similar.

	Args:
		dirx (str): Directory to be removed.
	
	Returns:
		None
	"""
	if dirx:
		# if dirx.exists() and dirx.is_dir():
		# if os.path.exists(dirx) and os.path.isdir(dirx):
		if os.path.isdir(dirx):
			try:
				shutil.rmtree(dirx)
			except:
				logger.warning('err for shutil fx, letting her relax and retrying')
				time.sleep(5)
				# if dirx.exists():
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
				# if dirx.exists():
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
		
		# if dirx.exists():
		if os.path.exists(dirx):
			logger.warning(f"shutil_fx could not delete {dirx}")
		else:
			logger.debug(f"shutil_fx deleted {dirx}")

def configure(abs_path):
	"""Configures the backup and creates the tmpd.

	Renames the source directory as a backup.
	Creates a temporary directory for keeping the backup clean.

	Args:
		abs_path (Path): Pathlib object of the source directory.
	
	Returns:
		tmpd (Path): New (empty) temporary directory.
		backup (Path): Source directory renamed to as a backup location/path.
	"""
	if abs_path.exists():
		parent = abs_path.parent
		try:
			tmpd = Path(tempfile.mkdtemp(dir=parent))
			backup = parent / f".{time.time():.0f}"

			abs_path.rename(backup)
			logger.debug('local directory moved to backup')

			# if tmpd.exists() and backup.exists():
			# 	logger.debug(f"{tmpd} and {backup} configured by [configure]")
	
		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {abs_path} to a backup location:{RESET} {e}.", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & source directory contents moved to backup')
			return tmpd, backup
	else:
		logger.warning(f"{abs_path} doesn't exist; fix the config or run 'rosa get all'")
		sys.exit(1)


def configure_o(abs_path):
	"""Configures the backup and creates the tmpd.

	Renames the source directory as a backup.
	Creates a temporary directory for keeping the backup clean.

	Args:
		abs_path (str): Path of the source directory.
	
	Returns:
		tmpd (str): New (empty) temporary directory.
		backup (str): Source directory renamed to as a backup location/path.
	"""
	# if abs_path.exists():
	if os.path.exists(abs_path):
		# parent = abs_path.parent
		parent = os.path.dirname(abs_path)
		try:
			tmpd = tempfile.mkdtemp(dir=parent)
			# backup = parent / f".{time.time():.0f}"
			backup = os.path.join(parent, f".{time.time():.0f}")

			# abs_path.rename(backup)
			os.rename(abs_path, backup)
			logger.debug('local directory moved to backup')

			# if tmpd.exists() and backup.exists():
			# 	logger.debug(f"{tmpd} and {backup} configured by [configure]")
	
		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {abs_path} to a backup location:{RESET} {e}", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & source directory contents moved to backup')
			return tmpd, backup
	else:
		logger.warning(f"{abs_path} doesn't exist; fix the config or run 'rosa get all'")
		sys.exit(1)


def configure1(dir_):
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
			# tmpd = Path(tempfile.mkdtemp(dir=parent))
			tmpd = tempfile.mkdtemp(dir=parent)

			# backup = parent / f".{time.time():.0f}"
			backup = os.path.join(parent, f".{time.time():.0f}")
			# backup.mkdir(parents=True, exist_ok=True)
			os.makedirs(backup, exist_ok=True)

			# dir_.rename(backup)
			# prefix = len(dir_.as_posix()) + 1
			# prefix = len(dir_) + 1

			# for entry in dir_.glob('*'):
			for entry in os.scandir(dir_):
				# rp = entry.as_posix()[prefix:]
				# rp = entry.path[prefix:]
				rp = entry.name # K.I.S.S.

				# destin = backup / rp
				destin = os.path.join(backup, rp)

				# entry.rename(destin)
				os.rename(entry.path, destin)

			logger.debug('local directory moved to backup')

			# if tmpd.exists() and backup.exists():
			if os.path.exists(tmpd) and os.path.exists(backup):
				logger.debug("tmpd and backup configured by [configure1]")

		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {dir_} to a backup location:{RESET} {e}.", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & original directory moved to backup w.o exception')
			return tmpd, backup
	else:
		logger.warning(f"{dir_} doesn't exist; fix the config or run 'rosa get all'")
		raise FileNotFoundError ('source directory does not exist')


def is_ignored(_str):
	# blacklist = ['.index', '.git', '.obsidian', '.vscode', '.DS_Store']
	return any(blckd in _str for blckd in BLACKLIST)


def apply_atomicy(tmpd, abs_path, backup):
	"""Cleans up the 'atomic' writing for fat_boy. 
	
	Only runs if no exceptions are caught by fat_boy. 
	Renames tmpd as source directory & deletes backup *if no errors are caught.

	Args:
		tmpd (str): Temporary directory containing the updated directory.
		abs_path (str): Original path of the source directory [empty at this point].
		backup (str): Location/path the source directory was moved to.
	
	Returns:
		None
	"""
	try:
		tmpd.rename(abs_path)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		shutil_fx(backup.as_posix())
		logger.debug('temporary directory renamed and backup removed')



def apply_atomicy_o(abs_path, tmpd, backup):
	"""Cleans up the 'atomic' writing for fat_boy. 
	
	Only runs if no exceptions are caught by fat_boy. 
	Renames tmpd as source directory & deletes backup *if no errors are caught.

	Args:
		tmpd (str): Temporary directory containing the updated directory.
		abs_path (str): Original path of the source directory [empty at this point].
		backup (str): Location/path the source directory was moved to.
	
	Returns:
		None
	"""
	try:
		# tmpd.rename(abs_path)
		os.rename(tmpd, abs_path)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		shutil_fx(backup)
		logger.debug('temporary directory renamed and backup removed')


def apply_atomicy1(tmpd, backup, dir_):
	"""Cleans up the 'atomic' writing for fat_boy. 
	
	Only runs if no exceptions are caught by fat_boy. 
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
		# tmpd.rename(dir_)
		# prefix = len(tmpd.as_posix()) + 1

		# for entry in tmpd.glob('*'):
		for entry in os.scandir(tmpd):
			# rp = entry.as_posix()[prefix:]

			# destin = dir_ / rp
			destin = os.path.join(dir_, entry.name)

			# entry.rename(destin)
			os.rename(entry.path, destin)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		logger.debug('temporary directory renamed w.o exception; removing backup & tmpd')
		shutil_fx(backup)
		shutil_fx(tmpd)


# WRITING TO DISK

def save_people(people, backup, tmpd):
	"""Hard-links unchanges files from the original to the tmpd.

	Takes the relative paths passed and builds two new ones for each directory.
	Uses .hardlink_to() to make a 'hard-link' between the two directories.
	Much faster than copying and effectively gets the files in both, 
	without altering the backup or its contents AND letting them be deleted w.o concern.

	Args:
		people (list): List of relative paths of unchanged files.
		backup (str): Path of the source directory.
		tmpd (str): Path of the temporary directory.

	Returns:
		None
	"""
	# for people in population:
	for person in people:
		# curr = Path( backup / person ).resolve()
		curr = os.path.join(backup, person)
		# tmp_ = Path( tmpd / person ).resolve()
		tmp_ = os.path.join(tmpd, person)

		# tmp_.parent.mkdir(parents=True, exist_ok=True)
		os.makedirs(os.path.dirname(tmp_), exist_ok=True)
		try:

			# tmp_.hardlink_to(curr)
			# os.link(tmp_, curr)
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
	# dcmpr = zstd.ZstdDecompressor() # init outside of loop; duh

	try:
		for frp, content in data:
			# t_path = Path ( tmpd / frp )
			t_path = os.path.join(tmpd, frp)
			# (t_path.parent).mkdir(parents=True, exist_ok=True)
			os.makedirs(os.path.dirname(t_path), exist_ok=True)

			# d_content = dcmpr.decompress(content)
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
		# rx = rp[0]
		# fp = tmpd / rx
		fp = os.path.join(tmpd, rp[0])
		# fp.mkdir(parents=True, exist_ok=True)
		os.makedirs(fp, exist_ok=True)