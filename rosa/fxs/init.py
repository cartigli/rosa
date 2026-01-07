#!/usr/bin/env python3
"""Initation of versioning.

Initiates local & remote databases & indexing.
Needs to be run before anything else.
Uploads the current files, indexes the 
entire directory and makes the copies.
"""


import os
import sys
import time
import shutil

from rosa.confs import TABLE_CHECK, _DROP
from rosa.lib import (
	phones, mini_ps, finale, _config,
	init_remote, init_index, _r, init_dindex, 
	_safety, shutil_fx, is_ignored,
	landline, construct, Heart
)

NOMIC = "[init]"

def r20(xdir):
	"""Recursively searches a directory for sub-directories & files.

	Args:
		xdir (str): The given directory's path.
	
	Yields:
		obj (str): A found file or directories path.
	"""
	for obj in os.scandir(xdir):
		yield obj

		if obj.is_dir():
			yield from r20(obj.path)

def scraper(origin):
	"""'Scrapes' the given directory for every file and directory's relative paths.
	
	Args:
		origin (str): Path of the source directory.
	
	Returns:
		drps (list): Relative paths of every directory.
		frps (list): Relative paths of every file.
	"""
	pfx = len(origin) + 1

	frps = []
	drps = []

	for obj in r20(origin):
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

	try:
		if any(res):
			logger.info(f"found these tables in the server {res}.")

			if not local.index:
				logger.info('the server has tables but the local index does not exist; the server needs to be erased.')
				dec = input('Wipe now [w]? [Return to quit]: ').lower()
				
				if dec in('w', 'wipe', ' w', 'w '):
					try:
						with phones() as conn:
							with conn.cursor() as cursor:
								cursor.execute(_DROP)

								while cursor.nextset():
									pass

							if local.index:
								shutil_fx(os.path.dirname(local.index))

					except Exception as e:
						logger.info(f"failed to erase server due to: {e}", exc_info=True)

			elif local.index:
				logger.info('the server has tables and the local index exists; they both need to be erased.')
				dec = input('Erase now [e]? [Return to quit]: ').lower()
				
				if dec in('e', 'erase', ' e', 'e '):
					try:
						with phones() as conn:
							with conn.cursor() as cursor:
								cursor.execute(_DROP)

								while cursor.nextset():
									pass

							if local.index:
								shutil_fx(os.path.dirname(local.index))

					except Exception as e:
						logger.info(f"failed to erase server due to: {e}", exc_info=True)

		elif local.index:
			if os.path.exists(local.index):
				logger.warning('the local index exists but the server has no tables; the index needs to be deleted')
				dec = input('Delete [d] the index now? [Return to quit]: ').lower()

				if dec in('d', 'delete', 'd ', ' d'):
					shutil_fx(os.path.dirname(local.index))

		else:
			dec = input("Initiate [i]? [Return to quit] ").lower()

			if dec in('i', 'init', 'initiate'):
				start = time.perf_counter()

				with phones() as conn:
					try:
						logger.info('scraping source directory...')
						drps, frps = scraper(local.target)

						logger.info('initiating the index...')
						index = _config() # don't use the class's attributes bc they don't exist

						with landline(index) as sconn: # *they are None, but you get it
							construct(sconn)

							init_dindex(drps, sconn)
							init_index(sconn, local.target, os.path.dirname(index))

						logger.info(f'initiating remote database...')
						init_remote(conn, local.target, drps, frps)

					except KeyboardInterrupt as ki:
						logger.info(f"\ninitiation process killed")
						if index:
							shutil_fx(os.path.dirname(index))

						with conn.cursor() as cursor:
							cursor.execute(_DROP)

							while cursor.nextset():
								pass
						sys.exit(1)

	except KeyboardInterrupt:
		logger.warning(f'\nboss killed the process; abandoning...')
		sys.exit(1)

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()