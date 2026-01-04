"""Handles data on the disk.

Configures directories for safe writing.
Downloads data, creates directories, and writes files.
Also deletes directories and files.
Majority of the logic is for handling errors, and ensuring
the original LOCAL_DIR is not altered on failure.
"""

import sys
import time
import shutil
import logging
import tempfile
import contextlib
from pathlib import Path
from itertools import batched

import mysql.connector # only for error codes in this file

from rosa.confs import RED, RESET, BLACKLIST


logger = logging.getLogger('rosa.log')

# SETUP EDITOR FOR LOCAL DISK

@contextlib.contextmanager
def fat_boy(dir_):
	"""Conext manager for the temporary directory and backup of original. 
	
	Takes over on error and ensures corrupted data is never kept.

	Args:
		abs_path (Path): Full path of the LOCAL_DIR from config.py
	
	Yields: 
		tmpd (Path): The temporary directory the new data is being downloaded and written to. Deleted on error, renamed as LOCAL_DIR if not.
		backup (Path): The original LOCAL_DIR after being renamed to a backup location/path. Renamed to LOCAL_DIR on error, deleted if not.
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
def fat_boy1(dir_):
	"""Conext manager for the temporary directory and backup of original. 
	
	Takes over on error and ensures corrupted data is never kept.

	Args:
		dir_ (Path): Full path of the LOCAL_DIR from config.py
	
	Yields: 
		tmpd (Path): The temporary directory the new data is being downloaded and written to. Deleted on error, renamed as LOCAL_DIR if not.
		backup (Path): The original LOCAL_DIR after being renamed to a backup location/path. Renamed to LOCAL_DIR on error, deleted if not.
	"""
	tmpd = None # ORIGINAL
	backup = None
	cwd = Path(dir_).resolve()

	# abs_path = Path(dir_)
	try:

		tmpd, backup = configure1(cwd)
		if tmpd and backup:

			logger.debug(f"fat boy made {tmpd} and {backup}; yielding...")
			yield tmpd, backup # return these & freeze in place

	except KeyboardInterrupt as e:
		logger.warning('boss killed it; wrap it up')
		_lil_guy1(cwd, backup, tmpd)
		sys.exit(0)

	except (FileNotFoundError, PermissionError, Exception) as e:
		logger.error(f"{RED}err caught while backup & temporary directories:{RESET} {e}.", exc_info=True)
		_lil_guy1(cwd, backup, tmpd)
		sys.exit(1)
	else:
		try:
			apply_atomicy1(tmpd, backup, cwd)

		except KeyboardInterrupt as c:
			logger.warning('boss killed it; wrap it up')
			_lil_guy1(cwd, backup, tmpd)
			sys.exit(0)

		except (mysql.connector.Error, ConnectionError, Exception) as c:
			logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
			_lil_guy1(cwd, backup, tmpd)
			sys.exit(1)
		else:
			logger.debug("fat boy finished w.o exception")

@contextlib.contextmanager
def sfat_boy(abs_path):
	"""Simplified context manager for rosa [get][all] because there is no original to backup. 

	[third piece of evidence]

	Args:
		abs_path (Path): Full path of the LOCAL_DIR from config.py
	
	Yields: 
		tmpd: The temporary directory the new data is being downloaded and written to. Deleted on error.
	"""
	tmpd = Path(abs_path)

	try:
		if tmpd:
			if tmpd.is_dir():
				pass
			else:
				tmpd.mkdir(parents=True, exist_ok=True)

			logger.debug(f"fat boy made {tmpd}; yielding...")
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
		abs_path (Path): Full path of the LOCAL_DIR from config.py
		backup (Path): Original LOCAL_DIR renamed to a backup location while downloading and writing.
		tmpd (Path): Temporary directory made to hold the updated LOCAL_DIR. Renamed to LOCAL_DIR if succeeds, deleted if error is caught.
	
	Returns:
		None
	"""
	try:
		if backup and backup.exists():
			if tmpd and tmpd.exists():
				shutil_fx(tmpd)
				backup.rename(abs_path)
				logger.warning("moved backup back to original location & deleted the temporary directory")
		elif tmpd and tmpd.exists():
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
		dir_ (Path): Full path of the LOCAL_DIR from config.py
		backup (Path): Original LOCAL_DIR renamed to a backup location while downloading and writing.
		tmpd (Path): Temporary directory made to hold the updated LOCAL_DIR. Renamed to LOCAL_DIR if succeeds, deleted if error is caught.
	
	Returns:
		None
	"""
	try:
		if backup and backup.exists():
			if tmpd and tmpd.exists():
				shutil_fx(tmpd)
				# backup.rename(dir_)
				prefix = len(backup.as_posix()) + 1

				for entry in backup.glob('*'):
					rp = entry.as_posix()[prefix:]

					destin = dir_ / rp

					entry.rename(destin)
				
				# shutil_fx(backup)

				logger.warning("moved backup back to original location & deleted the temporary directory")
		elif tmpd and tmpd.exists():
			shutil_fx(tmpd)
			logger.warning(f"_lil_guy called to recover on error but the backup was no where to  be found. deleted temporary directory")
		else:
			logger.debug('_lil_guy called on error but no directories to recover were found')

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.error(f"{RED}replacement of {dir_} and cleanup encountered an error: {e}.", exc_info=True)
		if dir_.exists(): # basically fuck the cwd workarounds if an error occurs here
			shutil_fx(dir_) # too rare and messy to properly deal; just brute force it
		backup.rename(dir_)
		raise
	else:
		logger.info("_lil_guy's cleanup had no exceptions")
		shutil_fx(backup)
		# shutil_fx(tmpd)



def shutil_fx(dirx):
	"""Handles deletion of directories. 
	
	Since shutil.rmtree() is inconsistent in macOS silicon, 
	this offers basic retry logic if the directory exists after deletion, or similar.

	Args:
		dirx (Path): The directory to be removed.
	
	Returns:
		None
	"""
	if dirx:
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
			logger.debug(f"shutil_fx deleted {dirx}")

def configure(abs_path):
	"""Configures the backup and creates the tmpd.

	Renames the LOCAL_DIR as a temporary backup name.
	Creates a temporary directory for keeping the backup clean.

	Args:
		abs_path (Path): Pathlib object of the LOCAL_DIR's full path.
	
	Returns:
		tmpd (Path): New (empty) temporary directory.
		backup (Path): LOCAL_DIR renamed to as a backup location/path.
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
			logger.debug('temporary directory created & original directory moved to backup w.o exception')
			return tmpd, backup
	else:
		logger.warning(f"{abs_path} doesn't exist; fix the config or run 'rosa get all'")
		sys.exit(1)


def configure1(dir_):
	"""Configures the backup and creates the tmpd.

	Renames the LOCAL_DIR as a temporary backup name.
	Creates a temporary directory for keeping the backup clean.

	Args:
		dir_ (Path): Pathlib object of the LOCAL_DIR's full path.
	
	Returns:
		tmpd (Path): New (empty) temporary directory.
		backup (Path): LOCAL_DIR renamed to as a backup location/path.
	"""
	if dir_.exists():
		parent = dir_.parent
		try:
			tmpd = Path(tempfile.mkdtemp(dir=parent))

			backup = parent / f".{time.time():.0f}"
			backup.mkdir(parents=True, exist_ok=True)

			# dir_.rename(backup)
			prefix = len(dir_.as_posix()) + 1

			for entry in dir_.glob('*'):
				# if is_ignored(entry.as_posix()):
				# 	continue

				# else: 
				# *we want a full backup here, not a selective few files/directories

				rp = entry.as_posix()[prefix:]
				destin = backup / rp

				entry.rename(destin)

					# if entry.is_dir():
						# shutil.copytree(entry, destin)

					# elif entry.is_file():
					# 	shutil.copy2(entry, destin)

			logger.debug('local directory moved to backup')

			if tmpd.exists() and backup.exists():
				logger.debug(f"{tmpd} and {backup} configured by [configure]")
	
		except (PermissionError, FileNotFoundError, Exception) as e:
			logger.error(f"{RED}err encountered while trying move {dir_} to a backup location:{RESET} {e}.", exc_info=True)
			raise
		else:
			logger.debug('temporary directory created & original directory moved to backup w.o exception')
			return tmpd, backup
	else:
		logger.warning(f"{dir_} doesn't exist; fix the config or run 'rosa get all'")
		sys.exit(1)


def is_ignored(_str):
	# blacklist = ['.index', '.git', '.obsidian', '.vscode', '.DS_Store']
	return any(blckd in _str for blckd in BLACKLIST)


def apply_atomicy(tmpd, abs_path, backup):
	"""Cleans up the 'atomic' writing for fat_boy. 
	
	Only runs if no exceptions are caught by fat_boy. 
	Renames tmpd as LOCAL_DIR & deletes original backup *if no errors occur/caught.

	Args:
		tmpd (Path): Temporary directory containing the updated directory.
		abs_path (Path): Original path of the LOCAL_DIR [empty at this point].
		backup (Path): Location/path the original LOCAL_DIR was moved to.
	
	Returns:
		None
	"""
	try:
		tmpd.rename(abs_path)

	except (PermissionError, FileNotFoundError, Exception) as e:
		logger.critical(f"{RED}exception encountered while attempting atomic write:{RESET} {e}.", exc_info=True)
		raise
	else:
		logger.debug('temporary directory renamed w.o exception; removing backup of original')
		shutil_fx(backup)


def apply_atomicy1(tmpd, backup, dir_):
	"""Cleans up the 'atomic' writing for fat_boy. 
	
	Only runs if no exceptions are caught by fat_boy. 
	Renames tmpd as LOCAL_DIR & deletes original backup *if no errors occur/caught.

	Args:
		tmpd (Path): Temporary directory containing the updated directory.
		dir_ (Path): Original path of the LOCAL_DIR [empty at this point].
		backup (Path): Location/path the original LOCAL_DIR was moved to.
	
	Returns:
		None
	"""
	try:
		# tmpd.rename(dir_)
		prefix = len(tmpd.as_posix()) + 1

		for entry in tmpd.glob('*'):
			rp = entry.as_posix()[prefix:]

			destin = dir_ / rp

			entry.rename(destin)

			# if entry.is_dir():
			# 	shutil.copytree(entry, destin)
			
			# elif entry.is_file():
			# 	shutil.copy2(entry, destin)


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
		backup (Path): Full path of the original LOCAL_DIR.
		tmpd (Path): Full path of the temporary directory.
	
	Returns:
		None
	"""
	# for people in population:
	for person in people:
		curr = Path( backup / person ).resolve()
		tmp_ = Path( tmpd / person ).resolve()
		tmp_.parent.mkdir(parents=True, exist_ok=True)
		try:

			tmp_.hardlink_to(curr)

		except (PermissionError, FileNotFoundError, KeyboardInterrupt, Exception) as te:
			raise

def wr_batches(data, tmpd):
	"""Writes each batch's data to the tmpd as they come. 
	
	Each files' parent is made if does not exist (faster than checking).

	Args:
		data (2-element tuple): Tupled list of pairs (relative paths, content).
		tmpd (Path): Target directory to write to.

	Returns:
		None
	"""
	# dcmpr = zstd.ZstdDecompressor() # init outside of loop; duh

	try:
		for frp, content in data:
			t_path = Path ( tmpd / frp )
			(t_path.parent).mkdir(parents=True, exist_ok=True)

			# d_content = dcmpr.decompress(content)
			with open(t_path, 'r', encoding='utf-8') as t:
				t.write(content)

	except KeyboardInterrupt as ki:
		raise
	except (PermissionError, FileNotFoundError, Exception) as e:
		raise

def mk_rrdir(drps, tmpd):
	"""Writes remote directories to the disk.

	Args:
		raw_directories (list): Single-item tuples (relative paths,).
		tmpd (Path): Target directory to write the new directories to/in.
	
	Returns:
		None
	"""
	logger.debug('...correcting local directory tree...')

	for rp in drps:
		rx = rp[0]
		fp = tmpd / rx
		fp.mkdir(parents=True, exist_ok=True)