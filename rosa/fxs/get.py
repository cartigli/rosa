#!/usr/bin/env python3
"""Rolls local directory back to state of latest commitment.

Does not query or connect to the server EXCEPT to verify the hashes.
**^ I am lying; it gets the versions of altered/deleted files from the server.**
Hashes are only verified if the file's timestamp shows a discrepancy.

Name should be changed. get_curr should be get & this should be get_last or similar.
"""

# (incomplete commenting)

import os
import shutil
import subprocess
from pathlib import Path

import sqlite3

from rosa.lib import (
	phones, fat_boy1, mk_rrdir, 
	save_people, mini_ps, finale,
	query_index, _config, refresh_index,
	scrape_dindex, find_index, landline, Heart
)

NOMIC = "[get]"

def originals(replace, tmpd, backup):
	"""Copies the originals of deleted or altered files to replace edits.""" # fix this mf comment
	# originals = backup / ".index" / "originals"
	originals = os.path.join(backup, ".index", "originals")

	for rp in replace:
		# fp = originals / rp
		fp = os.path.join(originals, rp)
		# bp = tmpd / rp
		bp = os.path.join(tmpd, rp)

		# (bp.parent).mkdir(parents=True, exist_ok=True)
		os.makedirs(os.path.dirname(bp), exist_ok=True)

		shutil.copy2(fp, bp)

def finals(tmpd, backup):
	# index = ".index"

	# origin = backup / index
	origin = os.path.join(backup, ".index")
	# destin = tmpd / index
	destin = os.path.join(tmpd, ".index")

	shutil.copytree(origin, destin)

def main(args=None):
	"""Reverts the local state to the most recent commitment."""
	xdiff = False
	logger, force, prints, start = mini_ps(args, NOMIC)

	local = Heart()

	with phones() as conn:
		with landline(local.index) as sconn:
			new, deleted, diffs, remaining, xdiff = query_index(conn, sconn, local.target)
			indexed_dirs = scrape_dindex(sconn)

		if xdiff is True:
			logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")

			with fat_boy1(local.target) as (tmp_, backup): # CHECKED

				logger.info('copying directory tree...')
				mk_rrdir(indexed_dirs, tmp_) # checked

				logger.info('hard linking unchanged files...')
				save_people(remaining, backup, tmp_) # checked

				# ignore new files

				for d in deleted:
					diffs.append(d)

				if diffs:
					logger.info('replacing files with deltas')
					originals(diffs, tmp_, backup) # checked (bad commenting)

				for r in remaining:
					diffs.append(r)
				
				logger.info('inserting index & originals')
				finals(tmp_, backup) # checked (more bad commenting)

			logger.info('refreshing the index')
			with landline(local.index) as sconn:
				refresh_index(sconn, local.target, diffs)

		else:
			logger.info('no diff!')

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()