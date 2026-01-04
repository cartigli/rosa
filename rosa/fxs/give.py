#!/usr/bin/env python3
"""Uploads changes to the server.

'Gives' new files, deletes deleted files,
and updates altered files. 
Updates the local index.
"""

# LOCAL_DIR used 3 times (besides import)
import sys

from rosa.confs import LOCAL_DIR, RED, RESET
from rosa.lib import (
	phones, rm_remfile, # confirm, 
	mini_ps, finale, collector,
	query_index, version_check, 
	diff_gen, remote_records, 
	upload_patches, local_audit_,
	historian, fat_boy, refresh_index,
	xxdeleted, rm_remdir, local_daudit,
	upload_dirs, query_dindex, find_index,
	landline, Heart
)

NOMIC = "[give]"

def main(args=None):
	"""Uploads the local state to the server. 

	Uploads new files, updates altered files, and 
	removes files/directories not found locally.
	"""
	xdiff = False

	logger, force, prints, start = mini_ps(args, NOMIC)

	local = Heart()

	with phones() as conn:
		with landline(local.index) as sconn:
			new, deleted, diffs, remaining, xdiff = query_index(conn, sconn)
			newd, deletedd, ledeux = query_dindex(sconn)

	# All the oversions I am querying the server for should be from the local index; 
	# they all existed here so ARE indexed & if they are new, they need no oversion here
	# hashes should and will always be retrieved from the server; no reason to bloat the lightweight local index
	# if i can't trust the hashes stored anyway. SQLite's db is open as a mf; the MySQL server requires preset users
	# and hash-verified passwords, so its contents are more convictable than the SQLite db, atleast IMO.

	if xdiff is True:
		logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")

		with phones() as conn:
			with landline(local.index) as sconn:
				vok, version = version_check(conn, sconn)

				if vok is True:
					logger.info('versions: twinned')
					cv = version + 1

					if force is True:
						message = f"upload v{version}"
					else:
						message = input("attach a message to this version (or enter for None): ") or None

					remote_records(conn, cv, message)

					if new:
						logger.info('uploading new files...')
						collector(conn, new, LOCAL_DIR, cv, key="new_files")

					if diffs:
						logger.debug('finding altered files\' previous versions')
						oversions = {}
						ovquery = "SELECT version FROM files WHERE rp = %s;"

						with conn.cursor() as cursor:
							for diff in diffs:
								# print(diff)
								cursor.execute(ovquery, (diff,))

								oversion = cursor.fetchone()
								oversions[diff] = oversion[0]

						logger.info('uploading altered files...')
						collector(conn, diffs, LOCAL_DIR, cv, key="altered_files") # updates altered

						logger.info('generating altered files\' patches')
						patches, originals = diff_gen(diffs, local.originals, LOCAL_DIR) # computes & returns patches

						logger.info('uploading altered files\' patches')
						upload_patches(conn, patches, cv, oversions) # uploads the patches to deltas

					if deleted:
						logger.info('removing deleted files from server')
						# needs to find the deleted file's original version first
						doversions = rm_remfile(conn, deleted)

						logger.info('updating remote directories')
						rm_remdir(conn, deletedd, cv)
						upload_dirs(conn, newd, cv)

					logger.info('updating local indexes')
					with fat_boy(originals) as secure:
						# for p in secure:
						# 	print(p)

						local_audit_(new, diffs, remaining, cv, secure, sconn)
						local_daudit(newd, deletedd, cv, sconn)

						if deleted:
							logger.info('backing up deleted files')
							xxdeleted(conn, deleted, cv, doversions, secure, sconn)

						logger.info('final confirmations')
						historian(cv, message, sconn)

				else:
					logger.critical(f"{RED}versions did not align; pull most recent upload from server before committing{RESET}")
					return

		updates = list(remaining)

		for n in new:
			updates.append(n) # new & remaining need to get updated

		with landline(local.index) as sconn:
			refresh_index(updates, sconn)

	else:
		logger.info('no diff!')

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()