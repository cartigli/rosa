#!/usr/bin/env python3
"""Initation of versioning.

Initiates local & remote databases & indexing.
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

from rosa.confs import LOCAL_DIR, TABLE_CHECK, _DROP
from rosa.lib import (
	phones, mini_ps, finale, _config,
	init_remote, init_index, _r, init_dindex, 
	_safety, shutil_fx, find_index, is_ignored,
	landline, construct, Heart
)

logger = logging.getLogger('rosa.log')


NOMIC = "[init]"

def r20(xdir):
	"""Recursively searches a directory for sub-directories & files.

	Args:
		xdir (Path): A directory's full path.
	
	Yields:
		obj (str): A found file or directories path.
	"""
	for obj in os.scandir(xdir):
		yield obj

		if obj.is_dir():
			yield from r20(obj.path)

def scraper(dir_):
	"""'Scrapes' the given directory for every file and directory's relative paths.
	
	Args:
		dir_ (str): Path to the LOCAL_DIR as a string.
	
	Returns:
		drps (list): Relative paths of every directory.
		frps (list): Relative paths of every file.
	"""
	pfx = len(dir_) + 1

	frps = []
	drps = []

	for obj in r20(dir_):
		if is_ignored(obj.path):
			continue

		elif obj.is_file():
			rp = obj.path[pfx:]
			frps.append(rp)
		
		elif obj.is_dir():
			rp = obj.path[pfx:]
			drps.append(rp)
	
	return drps, frps

def main(args=None):
	"""Initiating the local index & remote database.
	
	If they exist, partially or in whole, asks to delete.
	"""
	logger, force, prints, start = mini_ps(args, NOMIC)

	with phones() as conn:
		with conn.cursor() as cursor:
			cursor.execute(TABLE_CHECK)

			rez = cursor.fetchall()

	res = [rex[0] for rex in rez]

	local = Heart(strict=False) # only script to use =False (non-default value)

	if any(res):
		logger.info(f"found these tables in the server {res}.")

		if not local.index:
			logger.info('the server has tables but the local index does not exist; the server needs to be erased.')
			dec = input('wipe now [w]? [Return to quit]: ').lower()
			
			if dec in('w', 'wipe', ' w', 'w '):
				try:
					with phones() as conn:
						with conn.cursor() as cursor:
							cursor.execute(_DROP)

							while cursor.nextset():
								pass

						if local.index:
							shutil_fx(local.index.parent)

				except Exception as e:
					logger.info(f"failed to erase server due to: {e}", exc_info=True)

		elif local.index:
			logger.info('the server has tables and the local index exists; they both need to be erased.')
			dec = input('erase now [e]? [Return to quit]: ').lower()
			
			if dec in('e', 'erase', ' e', 'e '):
				try:
					with phones() as conn:
						with conn.cursor() as cursor:
							cursor.execute(_DROP)

							while cursor.nextset():
								pass

						if local.index:
							shutil_fx(local.index.parent)

				except Exception as e:
					logger.info(f"failed to erase server due to: {e}", exc_info=True)

	elif local.index:
		if local.index.exists():
			logger.warning('the local index exists but the server has no tables; the index needs to be deleted')
			dec = input('delete [d] the index now? [Return to quit]: ').lower()

			if dec in('d', 'delete', 'd ', ' d'):
				shutil_fx(local.index.parent)

	else:
		dec = input("[i] initiate? [Return to quit] ").lower()

		if dec in('i', 'init', 'initiate'):
			start = time.perf_counter()

			with phones() as conn:
				try:
					drps, frps = scraper(LOCAL_DIR)

					init_remote(conn, drps, frps)

					index = _config() # don't use the class's attributes bc they don't exist

					with landline(index) as sconn: # *they are None, but you get it
						construct(sconn)

						init_dindex(drps, sconn)
						print(os.path.dirname(index))
						init_index(sconn, os.path.dirname(index))
						# init_index(sconn, index.parent)

				except Exception as err:
					raise
				except KeyboardInterrupt as ki:
					logger.info(f"initiation failed due to: {ki}", exc_info=True)
					with conn.cursor() as cursor:
						cursor.execute(_DROP)

						while cursor.nextset():
							pass

					if index:
						shutil_fx(os.path.dirname(index))

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()