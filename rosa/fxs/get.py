#!/usr/bin/env python3
"""Rolls local directory back to state of latest commitment.

Does not query or connect to the server EXCEPT to verify the hashes.
**^ I am lying; it gets the versions of altered/deleted files from the server.**
Hashes are only verified if the file's timestamp shows a discrepancy.

Name should be changed. get_curr should be get & this should be get_last or similar.
"""

import os
import shutil
import subprocess
from pathlib import Path

import sqlite3

# LOCAL_DIR used once (besides import)
from rosa.confs import LOCAL_DIR
from rosa.lib import (
	phones, fat_boy1, mk_rrdir, 
	save_people, mini_ps, finale,
	query_index, _config, refresh_index,
	scrape_dindex, find_index, landline, Heart
)

NOMIC = "[get]"

def originals(replace, tmpd, backup):
	"""Copies the originals of deleted or altered files to replace edits."""
	originals = backup / ".index" / "originals"

	for rp in replace:
		fp = originals / rp
		bp = tmpd / rp

		(bp.parent).mkdir(parents=True, exist_ok=True)

		shutil.copy2(fp, bp)

def finals(tmpd, backup):
	index = ".index"

	origin = backup / index
	destin = tmpd / index

	shutil.copytree(origin, destin)

def main(args=None):
	"""Reverts the local state to the most recent commitment."""
	xdiff = False
	logger, force, prints, start = mini_ps(args, NOMIC)

	local = Heart()

	with phones() as conn:
		with landline(local.index) as sconn:
			new, deleted, diffs, remaining, xdiff = query_index(conn, sconn)
			indexed_dirs = scrape_dindex(sconn)

	if xdiff is True:
		logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")

		with fat_boy1(LOCAL_DIR) as (tmp_, backup):

			logger.info('copying directory tree...')
			mk_rrdir(indexed_dirs, tmp_)

			logger.info('hard linking unchanged files...')
			save_people(remaining, backup, tmp_)

			# ignore new files

			for d in deleted:
				diffs.append(d)

			if diffs:
				logger.info('replacing files with deltas')
				originals(diffs, tmp_, backup)

			for r in remaining:
				diffs.append(r)
			
			logger.info('inserting index & originals')
			finals(tmp_, backup)

		logger.info('refreshing the index')
		with landline(local.index) as sconn:
			refresh_index(diffs, sconn)

	else:
		logger.info('no diff!')

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()