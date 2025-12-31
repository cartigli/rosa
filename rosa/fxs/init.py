#!/usr/bin/env python3
"""Initation of versioning.

Needs to be run before anything else.
This uploads the current files, indexes the 
entire directory and makes the copies.

Latest test was about 32 seconds for a 4.0 GB directory.
Majority of the time is always uploading.
Also, these are not genuine network speeds; purely functionality tests.
"""

import os
import sys
import time
import shutil
import logging
from pathlib import Path

from rosa.confs import LOCAL_DIR, BLACKLIST, TABLE_CHECK, _DROP
from rosa.lib import (
	phones, mini_ps, finale, _config,
	init_remote, init_index, _r, confirm,
	init_dindex, _safety, shutil_fx, find_index
)

logger = logging.getLogger('rosa.log')


NOMIC = "[init]"

def _r2(xdir):
	for obj in os.scandir(xdir):
		if obj.is_dir():
			yield obj
			yield from _r2(obj.path)
		else:
			yield obj

def r3(xdir):
	for obj in os.scandir(xdir):
		if obj.is_dir():
			yield from r3(obj.path)
		else:
			yield obj.path

def r4(xdir):
	for obj in os.scandir(xdir):
		if obj.is_dir():
			yield obj.path
			yield from r4(obj.path)

def scraper(dir_):
	"""'Scrapes' the given directory for every file and directory's relative paths.
	
	Args:
		dir_ (str): Path to the LOCAL_DIR as a string.
	
	Returns:
		drps (list): Relative paths of every directory.
		frps (list): Relative paths of every file.

	"""
	pfx = len(dir_) + 1
	dirx = Path(dir_)
	frps = []
	drps = []

	if dirx.exists():
		for obj in _r2(dir_):
			if any(blocked in obj.path for blocked in BLACKLIST):
				continue
			elif obj.is_file():
				rp = obj.path[pfx:]
				frps.append(rp)
			
			elif obj.is_dir():
				rp = obj.path[pfx:]
				drps.append(rp)

	else:
		logger.warning('local directory does not exist')
		sys.exit(1)
	
	return drps, frps

def main(args=None):
	"""Initiating the local index & remote database."""
	logger, force, prints, start = mini_ps(args, NOMIC)

	with phones() as conn:
		with conn.cursor() as cursor:
			cursor.execute(TABLE_CHECK)
			rez = cursor.fetchall()

	res = [rex[0] for rex in rez]

	index = find_index()
	# if index:
	# 	logger.warning('This is already an indexed directory; erase current or run give/get. Abandoning.')
	# 	sys.exit(1)
	# else:
	if any(res):
		logger.info(f"found these tables in the server {res}.")

		# if index.exists():
		# 	logger.info(f"local index's folder also exists: {index}")

		# 	dec = input("initiation appears to have been run already; do you want to [w] wipe everything? [Return to quit] ").lower()
		# 	if dec in ('w', 'wipe'):
		# 		start = time.perf_counter()
		# 		try:
		# 			with phones() as conn:
		# 				with conn.cursor() as cursor:
		# 					cursor.execute(_DROP)

		# 					while cursor.nextset():
		# 						pass
		# 		except:
		# 			logger.error('error occured while erasing db', exc_info=True)
		# 		else:
		# 			shutil_fx(index)
		# 			if index.exists():
		# 				logger.warning('shutil failed; retrying after 1 second')
		# 				time.sleep(1)
		# 				shutil_fx(index)
		# else:
		logger.info('the server has tables but the local index does not exist; the server needs to be erased.')
		dec = input('wipe now [w]? [Return to quit]: ').lower()
		
		if dec in('w', 'wipe', ' w', 'w '):
			try:
				with phones() as conn:
					with conn.cursor() as cursor:
						cursor.execute(_DROP)

						while cursor.nextset():
							pass
					if index:

						shutil_fx(index.parent)
						if index.exists():
							logger.warning('shutil failed; retrying after 1 second')
							time.sleep(1)
							shutil_fx(index.parent)

			except Exception as e:
				logger.info(f"failed to erase server due to: {e}", exc_info=True)
	
	elif index:
		if index.exists():
			logger.warning('the local index exists but the server has no tables; the index needs to be deleted')
			dec = input('delete [d] the index now? [Return to quit]: ').lower()

			if dec in('d', 'delete', 'd ', ' d'):
				shutil_fx(index.parent)
				if index.exists():
					logger.warning('shutil failed; retrying after 1 second')
					time.sleep(1)
					shutil_fx(index.parent)

	else:
		dec = input("[i] intiate? [Return to quit] ").lower()

		if dec in('i', 'init', 'initiate'):
			start = time.perf_counter()
			with phones() as conn:
				try:
					drps, frps = scraper(LOCAL_DIR)
					init_remote(conn, drps, frps)
					init_dindex(drps)
					init_index()

				except:
					logger.info(f"initiation failed due to error", exc_info=True)
					with conn.cursor() as cursor:
						cursor.execute(_DROP)

						while cursor.nextset():
							pass

					shutil_fx(index.parent)
					if index.exists():
						logger.warning('shutil failed; retrying after 1 second')
						time.sleep(1)
						shutil_fx(index.parent)
				else:
					conn.commit()

	finale(NOMIC, start, prints)
	# logger.info('All set.')

if __name__=="__main__":
	main()