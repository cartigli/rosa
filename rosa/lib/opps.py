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

import diff_match_patch as dp_

from rosa.confs import LOGGING_LEVEL

logger = logging.getLogger('rosa.log')

def find_index(cd: str = ""):
	"""Finds the index from the given directory.

	Args:
		cd (str): Current directory.

	Returns:
		index (str): The index, if found.
	"""
	index: str = None
	parents: list = []
	parents.append(cd)

	while cd:
		xd: str = cd
		cd: str = os.path.dirname(cd)

		if xd == cd:
			break

		if cd:
			parents.append(cd)

	for dir_ in parents:
		mb: str = os.path.join(dir_, ".index", "indeces.db")

		if os.path.exists(mb):
			index: str = mb
			break

	return index

class Heart:
	"""Finds the index based off the C.W.D.

	Attributes:
		redirect (str): Optional replacement of the C.W.D (init, diff).
		strict (bool): Optional enforcement of found index (init).
	"""
	def __init__(self, redirect: str = "", strict: bool = True):
		"""Finds index and originals' paths.

		Args:
			redirect (str): Optional path.
			strict (bool): Optional force index returned.
		"""
		if redirect:
			self.origin: str = redirect
		else:
			self.origin: str = os.getcwd()

		self.index: str = find_index(self.origin)

		self.target: str = None
		self.originals: str = None

		if self.index:
			self.target: str = os.path.dirname(os.path.dirname(self.index))

			self.originals: str = os.path.join(os.path.dirname(self.index), "originals")

		else:
			self.target = self.origin

			if strict is True:
					logger.warning('not an indexed directory')
					sys.exit(7)

# INITIATE LOGGER & RECORDS MANAGER

def init_logger(logging_level: str = ""):
	"""Initiates the logger and configures their formatting.

	Args:
		logging_level (str): Logging level from the config file (overriden if flags present).
	
	Returns:
		logger (logging): Logger.
	"""
	if logging_level:
		file: str = os.path.abspath(__file__)
		log_dest: str = os.path.join(os.path.dirname(os.path.dirname(file)), "rosa.log")
		
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
		console_: str = "%(message)s"
		file: str = "[%(asctime)s][%(levelname)s][%(module)s:%(lineno)s]: %(message)s"

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
	cd: str = os.path.abspath(__file__)
	rosa: str = os.path.dirname(os.path.dirname(cd))

	rosa_log: str = os.path.join(rosa, "rosa.log")
	rosa_records: str = os.path.join(rosa, "_rosa_records")

	logger.debug(f"rosa log is @: {rosa_log}")
	logger.debug(f"rosa records are @: {rosa_records}")

	rosasz: float = os.stat(rosa_log).st_size
	rosakb: float = rosasz / 1024

	rosa_records_max: int = 5 # if 5 already stored, delete one
	if rosakb >= 64.0: # 32 kb, and then move it to records
		if os.path.exists(rosa_records):
			if os.path.isdir(rosa_records):
				npriors: int = 1
				previous: list = []
				for file in os.scandir(rosa_records):
					if file.is_file():
						previous.append(file.path)
						npriors += 1

				if npriors > rosa_records_max:
					diff: int = npriors - rosa_records_max
					unwanted: list = previous[0:diff]
					for x in unwanted:
						os.remove(x)

					xtime: str = f"{time.time():.2f}"
					os.rename(rosa_log, os.path.join(rosa_records, f"rosa_log_{xtime}_"))

					logger.debug('deleted record of log to make room, and moved current log to records')
				else:
					xtime: str = f"{time.time():.2f}"
					os.rename(rosa_log, os.path.join(rosa_records, f"rosa_log_{xtime}_"))

					logger.debug('rosa_records: ok, backed up current log')
		else:
			os.makedirs(rosa_records, exist_ok=True)
			xtime: str = f"{time.time():.2f}"

			os.rename(rosa_log, os.path.join(rosa_records, f"rosa_log_{xtime}_"))
			logger.debug('backed up & replaced rosa.log')
	else:
		logger.debug('rosa.log: [ok]')

def mini_ps(args: argparse = None, nomix: str = ""):
	"""Mini parser for arguments passed from the command line (argparse).

	Args:
		args (argparse): Holds the flags present at execution; must unpack them from this object & assess/adjust.
		nomix (str): Name variable passed from each script for logging.
	
	Returns:
		A 4-element tuple containing:
			logger (logger): The logging object returned from init_logger (above).
			force (bool): A value depending on if the flag -f (--force) was present.
			prints (bool): A value depending on if the flag -v (--verbose) was present.
			start (float): A time.perf_counter() value obtained before the script's main function runs to time it.
	"""
	force: bool = False # no checks - force
	prints: bool = False # no prints - prints

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

	start: float = time.perf_counter()

	logger.debug(f"[rosa]{nomix} executed & timer started")
	return logger, force, prints, start

def diff_gen(diffs: list = [], details: dict = {}, originals: str = "", origin: str = ""):
	"""Generates reverse patches between two files.

	Args:
		diffs (list): Relative paths of the modified files.
		details (dict): Relative paths keyed to encoding & versions.
		originals (str): Path to the 'originals' directory.
		origin (str): Target directory.
	
	Returns:
		patches (list): Patches generated as bytes.
	"""
	patches: list = []
	enc: bool = None

	for rp in diffs:
		fp_alt: str = os.path.join(originals, rp)
		fp_mod: str = os.path.join(origin, rp)

		if details[rp][2] == "T":
			with open(fp_alt, 'r', encoding="utf-8") as f:
				alt = f.read()
			with open(fp_mod, 'r', encoding="utf-8") as m:
				mod = m.read()

			patch: bytes = patcher(alt, mod) # returned as binary

			patches.append((rp, patch))

		else:
			with open(fp_alt, 'rb') as f:
				patch: bytes = f.read()

			patches.append((rp, patch))

	return patches

def patcher(old: str = "", new: str = ""):
	"""Computes the reverse patch.

	Args:
		old (str): The original content of the modded file.
		new (str): The content of the edited file.

	Returns:
		patch (bytes): The computed patch as bytes.
	"""
	dmp = dp_.diff_match_patch()

	patches = dmp.patch_make(new, old)
	ptxt: str = dmp.patch_toText(patches)

	patch: bytes = ptxt.encode("utf-8")

	return patch

def counter(start: float = None, nomix: str = ""):
	"""Counts diff between end and start for timing functions.

	Args:
		start (int): time.perf_counter() value.
		nomix (str): Name variable for logging.

	Returns:
		pace (str): Fromatted time with factor.
	"""
	pace: bool = None

	if start:
		end: float = time.perf_counter()

		duration: float = end - start
		factor: str = "seconds"

		if duration > 60:
			duration: float = duration / 60
			factor: str = "minutes"

		pace: str = f"{duration:.4f} {factor}"

	return pace

def finale(nomix: str = "", start: float = None, prints: bool = False):
	"""Wraps up each files' execution & logging statements.
	
	Args:
		nomix (str): Name variable passed from each script for logging.
		start (float): A time.perf_counter value obtained at the start of execution.
		prints (bool): Variable specifying whether or not to print (verbosity).
	
	Returns:
		None
	"""
	doit_urself()
	pace: str = counter(start, nomix)

	logger.info(f"rosa {nomix} complete [{pace}]")

	if prints is True:
		print('All set.')