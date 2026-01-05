"""Initiate the logging and other.

Format & add handlers for logger.
Cleanup large logs and delete oldest.
Timer, counter, wrap up for runtime info.
"""

import os
import sys
import time
import logging
import subprocess
from pathlib import Path
import diff_match_patch as dp_

from rosa.confs import LOGGING_LEVEL


logger = logging.getLogger('rosa.log')

# INITIATE LOGGER & RECORDS MANAGER

def init_logger(logging_level):
	"""Initiates the logger and configures their formatting.

	Args:
		logging_level (str): Logging level from the config file.
	
	Returns:
		logger (Logging): Logger.
	"""
	if logging_level:
		file = Path(__file__)
		log_dest = file.parent.parent / "rosa.log"
		
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

		# define formatting
		console_ = "%(message)s"
		file = "[%(asctime)s][%(levelname)s][%(module)s:%(lineno)s]: %(message)s"

		file_format = logging.Formatter(file)
		console_format = logging.Formatter(console_)

		# apply formatting
		file_handler.setFormatter(file_format)
		console_handler.setFormatter(console_format)

		# add handlers to loggers
		logger.addHandler(file_handler)
		logger.addHandler(console_handler)

		logger_mysql.addHandler(file_handler)

		logger.propagate = False
		logger_mysql.propagate = False

		return logger
	else:
		logger.warning("logger not passed; maybe config isn't configured?")
		sys.exit(1)

def doit_urself():
	"""Moves rosa.log to record of old logs if the size limit is met.

	Deletes oldest record if the file count reaches the limit (5).

	Args:
		None

	Returns:
		None
	"""
	cd = Path(__file__).resolve()
	rosa = cd.parent.parent

	rosa_log = rosa / "rosa.log"
	rosa_records = rosa / "_rosa_records"

	logger.debug(f"rosa log is @: {rosa_log}")
	logger.debug(f"rosa records are @: {rosa_records}")

	rosasz = rosa_log.stat().st_size
	rosakb = rosasz / 1024

	rosa_records_max = 5 # if 5 files when rosa_log is moved, delete oldest record
	if rosakb >= 64.0: # 32 kb, and then move it to records
		if rosa_records.resolve().exists():
			if rosa_records.is_dir():
				npriors = 1 # start w.one because this is only occurring when the log is larger than 64 kb
				previous = []
				for file in sorted(rosa_records.glob('*')):
					if file.is_file():
						previous.append(file)
						npriors += 1

				if npriors > rosa_records_max:
					diff = npriors - rosa_records_max
					unwanted = previous[0:diff]
					for x in unwanted:
						x.unlink()

					ctime = f"{time.time():.2f}"
					subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

					logger.debug('deleted record of log to make room, and moved current log to records')
				else:
					ctime = f"{time.time():.2f}"
					subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

					logger.debug('rosa_records: ok, backed up current log')
		else:
			rosa_records.mkdir(parents=True, exist_ok=True)
			ctime = f"{time.time():.2f}"
			subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

			logger.debug('backed up & replaced rosa.log')
	else:
		logger.debug('rosa.log: [ok]')

def mini_ps(args, nomix): # (operations)
	"""Mini parser for arguments passed from the command line (argparse).

	Args:
		args (argparse): Holds the flags present at execution; must unpack them from this object & assess/adjust.
		nomix (var): Name variable passed from each script for logging.
	
	Returns:
		A 4-element tuple containing:
			logger (logger): The logging object returned from init_logger (above).
			force (bool): A value depending on if the flag -f (--force) was present.
			prints (bool): A value depending on if the flag -v (--verbose) was present.
			start (float): A time.perf_counter() value obtained before the script's main function runs to time it.
	"""
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

def counter(start, nomix):
	"""Counts diff between end and start for timing functions.
	
	Args:
		start (int): A time.perf_counter() value obtained at the start of execution.
		nomix (var): Name variable passed from each script for logging.
	
	Returns:
		None
	"""
	if start:
		end = time.perf_counter()
		duration = end - start
		if duration > 60:
			duration_minutes = duration / 60
			logger.debug(f"time [in minutes] for rosa {nomix}: {duration_minutes:.3f}")
		else:
			logger.debug(f"time [in seconds] for rosa {nomix}: {duration:.3f}")

def finale(nomix, start, prints):
	"""Wraps up each files' execution & logging statements.
	
	Args:
		nomix (var): Name variable passed from each script for logging.
		start (float): A time.perf_counter value obtained at the start of execution.
		prints (bool): Variable specifying whether or not to print (verbosity).
	
	Returns:
		None
	"""
	doit_urself()
	counter(start, nomix)

	logger.info(f"rosa {nomix} complete")

	if prints is True:
		print('All set.')

def diff_gen(modified, originals, origin):
	"""Generates reverse patches between two files.

	Args:
		modified (list): Relative paths of the modified files.
		originals (str): Path to the 'originals' directory.
		origin (str): The LOCAL_DIR.
	
	Returns:
		patches (dmp patches): Generated patches as text.
		originals (Path)): Pathlib path to the folder with the originals.
	"""
	patches = []

	for rp in modified:
		# fp_original = originals / rp
		fp_original = os.path.join(originals, rp)
		with open(fp_original, 'r', encoding='utf-8', errors='replace') as f:
			original = f.read()

		fp_modified = os.path.join(origin, rp)
		with open(fp_modified, 'r', encoding='utf-8', errors='replace') as m:
			different = m.read()

		patch = patcher(original, different) # returned as text

		patches.append((rp, patch))
	
	return patches, originals

def patcher(old, new):
	"""Computes the reverse patch.

	Args:
		old (str): The content of the original modded file.
		new (str): The content of the edited file.
	
	Returns:
		p_txt (str): The computed patch as text.
	"""
	dmp = dp_.diff_match_patch()

	patches = dmp.patch_make(new, old)
	p_txt = dmp.patch_toText(patches)

	return p_txt

def find_index(cd):
	"""Finds the index from the current working directory.

	Args:
		cd (Path): Current working directory.
	
	Returns:
		index (Path): The index, if found.
	"""
	index = None

	for dir_ in [cd] + list(cd.parents):
		mb = dir_ / ".index" / "indeces.db"

		if mb.exists():
			index = mb
			break

	return index

def find_index00(cd):
	"""Finds the index from the given directory.

	Args:
		cd (str): Current directory.
	
	Returns:
		index (str): The index, if found.
	"""
	index = None
	parents = []
	parents.append(cd)

	while cd:
		xd = cd
		cd = os.path.dirname(cd)

		if xd == cd:
			break

		if cd:
			parents.append(cd)

	for dir_ in parents:
		mb = os.path.join(dir_, ".index", "indeces.db")

		if os.path.exists(mb):
			index = mb
			break

	return index

class Heart:

	def __init__(self, strict=True):
		# self.origin = Path.cwd()
		self.origin = os.getcwd()
		self.index = find_index00(self.origin)

		self.target = None
		self.originals = None

		if self.index:
			# self.target = self.index.parents[1] xPathlib
			self.target = os.path.dirname(os.path.dirname(self.index))
			# self.target = os.path.abspath(os.path.join(self.index, os.pardir, os.pardir))

			# self.originals = self.index.parent / "originals" xPathlib
			self.originals = os.path.join(os.path.dirname(self.index), "originals")

		else:
			if strict is True:
					logger.warning('not an indexed directory')
					sys.exit(7)