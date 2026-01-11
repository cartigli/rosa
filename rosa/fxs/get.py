#!/usr/bin/env python3
"""Rolls local directory back to state of latest commitment.

Does not query or connect to the server EXCEPT to verify the hashes.
"""

import os
import sys
import shutil
import subprocess

import sqlite3

from rosa.lib import (
	phones, fat_boy, mk_rrdir, 
	save_people, mini_ps, finale,
	query_index, _config, refresh_index,
	scrape_dindex, landline, Heart
)

NOMIC: str = "[get]"

def originals(replace: list = [], tmpd: str = "", backup: str = ""):
	"""Copies the originals of deleted or altered files to replace edits.

	Args:
		replace (list): Relative paths of files to 'replace'.
		tmpd (str): Path to the temporary directory.
		backup (str): Path to the source directory.

	Returns:
		None
	"""
	originals: str = os.path.join(backup, ".index", "originals")

	for rp in replace:
		fp: str = os.path.join(originals, rp)
		bp: str = os.path.join(tmpd, rp)

		os.makedirs(os.path.dirname(bp), exist_ok=True)

		# shutil.copy2(fp, bp)
		shutil.copyfile(fp, bp)

def finals(tmpd: str = "", backup: str = ""):
	"""Copies the index from the backup to the temporary directory.

	Args:
		tmpd (str): Path to the temporary directory.
		backup (str): Path to the source directory.

	Returns:
		None
	"""
	origin: str = os.path.join(backup, ".index")
	destin: str = os.path.join(tmpd, ".index")

	os.rename(origin, destin)

def main(args: argparse = None):
	"""Reverts the local state to the most recent commitment."""
	xdiff: bool = False

	logger, force: bool, prints: bool, start: bool = mini_ps(args, NOMIC)

	local = Heart()

	with phones() as conn:
		with landline(local.index) as sconn:
			new: list, deleted: list, diffs: list, remaining: list, xdiff: bool = query_index(conn, sconn, local.target)
			newd: list, deletedd: list, ledeux: list = query_dindex(sconn, local.target)

	if xdiff is True:
		logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")

		try:
			with fat_boy(local.target) as (tmp_, backup):

				logger.info('copying directory tree...')
				mk_rrdir(indexed_dirs, tmp_)

				logger.info(f'hard linking {len(remaining)} unchanged files...')
				save_people(remaining, backup, tmp_)

				# ignore new files

				diffs: list += deleted

				if diffs:
					logger.info('replacing files with deltas')
					originals(diffs, tmp_, backup) # (bad commenting)

				diffs: list += remaining

				logger.info('replacing index & originals')
				finals(tmp_, backup) # (more bad commenting)

			logger.info('refreshing the index')
			with landline(local.index) as sconn:
				refresh_index(sconn, local.target, diffs)

		except KeyboardInterrupt:
			logger.warning(f'\nboss killed the process; index could be corrupted; refreshing before exit...')
			sys.exit(1)

	else:
		logger.info('no diff!')

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()