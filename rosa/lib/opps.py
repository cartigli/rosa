"""Initiate the logging and other.

Format & add handlers for logger.
Cleanup large logs and delete oldest.
Timer, counter, wrap up for runtime info.
"""

import sys
import time
import logging
import subprocess
from pathlib import Path

from rosa.confs import LOGGING_LEVEL


logger = logging.getLogger('rosa.log')

# INITIATE LOGGER & RECORDS MANAGER

def init_logger(logging_level):
	"""Initiates the logger and configures the formatting.

	Two loggers & three handlers; one logger for mysql.connector and the other for the file & console. 
	One handler for the file, and two for the console, one for mysql & one for rosa's logging output.
	Mysql & the rosa logger both get the file handler added to them so the file records everything.

	Args:
		logging_level (str): Logging level configured in the config.py file, unless changed by mini_ps due to a flag.
	
	Returns:
		logger: Logging object.
	"""
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

def doit_urself():
	"""Moves rosa.log to record of old logs if the size limit is met and deletes oldest record if the file limit is reached.

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
	if rosakb >= 64.0: # 64 kb, and then move it to records
		if rosa_records.resolve().exists():
			if rosa_records.is_file():
				logger.error(f"there is a file named rosa_records where a logging record should be; abandoning")
			elif rosa_records.is_dir():
				npriors = 1 # start w.one because this is only occurring when the log is larger than 64 kb
				previous = []
				for file_ in sorted(rosa_records.glob('*')):
					if file_.is_file():
						previous.append(file_)
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

					logger.info('rosa_records: ok, backed up current log')
		else:
			rosa_records.mkdir(parents=True, exist_ok=True)
			ctime = f"{time.time():.2f}"
			subprocess.run(["mv", f"{rosa_log}", f"{rosa_records}/rosa.log_{ctime}_"])

			logger.info('backed up & replaced rosa.log')
	else:
		logger.info('rosa.log: [ok]')

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
			start (int): A time.perf_counter() value obtained before the script's main function runs to time it.
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
			logger.info(f"time [in minutes] for rosa {nomix}: {duration_minutes:.3f}")
		else:
			logger.info(f"time [in seconds] for rosa {nomix}: {duration:.3f}")

def finale(nomix, start, prints):
	"""Wraps up each files' execution & logging statements.
	
	Args:
		nomix (var): Name variable passed from each script for logging.
		start (int): A time.perf_counter value obtained at the start of execution.
		prints (bool): Variable specifying whether or not to print (verbosity).
	
	Returns:
		None
	"""
	logger = logging.getLogger('rosa.log')
	doit_urself()

	counter(start, nomix)

	logger.info(f"rosa {nomix} complete")

	if prints is True:
		print('All set.')