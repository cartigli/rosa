#!/usr/bin/env python3
"""Rolls local directory back to state of latest commitment.

Does not query or connect to the server EXCEPT to verify the hashes.
**^ I am lying; it gets the versions of altered/deleted files from the server.**
Hashes are only verified if the file's timestamp shows a discrepancy.

Name should be changed. get_curr should be get & this should be get_last or similar.
"""

import shutil
import sqlite3

# LOCAL_DIR used once (besides import)
from rosa.confs import LOCAL_DIR
from rosa.lib import (
	phones, fat_boy, mk_rrdir, 
	save_people, mini_ps, finale,
	query_index, _config, refresh_index,
	scrape_dindex, find_index, landline
)

NOMIC = "[get]"

def originals(replace, tmpd, origin):
	"""Copies the originals of deleted or altered files to replace edits."""
	originals = origin / "originals"

	for rp in replace:
		fp = originals / rp
		bp = tmpd / rp

		(bp.parent).mkdir(parents=True, exist_ok=True)

		shutil.copy2(fp, bp)

def main(args=None):
	"""Reverts the local state to the most recent commitment."""
	xdiff = False
	logger, force, prints, start = mini_ps(args, NOMIC)

	index = find_index()

	if not index:
		logger.info('not an indexed directory')
		finale(NOMIC, start, prints)
		sys.exit(2)

	with phones() as conn:
		with landline(index) as sconn:
			new, deleted, diffs, remaining, xdiff = query_index(conn, sconn)
			indexed_dirs = scrape_dindex(sconn)

	if xdiff is True:
		logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")

		with fat_boy(LOCAL_DIR) as (tmp_, backup):

			logger.info('copying directory tree...')
			mk_rrdir(indexed_dirs, tmp_)

			logger.info('hard linking unchanged files...')
			save_people(remaining, backup, tmp_)

			# ignore new files

			for d in deleted:
				diffs.append(d)

			logger.info('replacing files with deltas')
			originals(diffs, tmp_, index.parent) # NOT here

			for r in remaining:
				diffs.append(r)

		with landline(index) as sconn:
			refresh_index(diffs, sconn)

		# ^This is delicate and senstive, but not as much as 'give'. Since the new/deleted/altered files' content is not recorded, 
		# their new values are not in the index, and the local copy of the directory is left at the state of the original, the 
		# index only needs to be updated for the files that actually get touched during this procedure (local copy of directory 
		# remains untouched). The files who are touched (a.k.a. ctimes change) are the altered files, the deleted files, and the 
		# unchanged files. Altered files get overwritten, deleted files get replaced, and unchanged files get hard-linked, which 
		# alone doesn't doesn't change their ctimes, but upon deletion of the original path, the ctime does change (also meaning 
		# the index can't be updated until the original directory is deleted, a.k.a. when fat_boy closes). This means that the 
		# refresh_index won't break the system if it fails, but it will force it out of sync and will need to be resynced again. 
		# The local copy of the directory is untouched, so we don't need a backup of the orginal like fat_boy uses, but it will 
		# require retry on failure (or perfect code, which is not realistic). This also means refresh_index should get a list of 
		# modified, deleted, and unchanged files to update in the index.
		# Also, adjusting and using the index during fat_boy's context is difficult because the index gets moved from the original
		# location and correlations get obfuscated. It is much easier and more logical to update once fat_boy is completely closed.
	else:
		logger.info('no diff!')
	
	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()