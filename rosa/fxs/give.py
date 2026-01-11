#!/usr/bin/env python3
"""Uploads changes to the server.

'Gives' new files, deletes deleted files,
and updates altered files. 
Updates the local index.
"""

import os
import sys

from rosa.confs import RED, RESET
from rosa.lib import (
	phones, rm_remfile, mini_ps, 
	finale, collector, query_index, 
	version_check, diff_gen, remote_records, 
	upload_patches, local_audit_, historian, 
	rm_remdir, local_daudit, upload_dirs,
	fat_boy_o, refresh_index, xxdeleted, 
	query_dindex, landline, Heart
)

NOMIC: str = "[give]"

def main(args: argparse = None):
	"""Uploads the local state to the server."""
	xdiff: bool = False

	logger, force: bool, prints: bool, start: float = mini_ps(args, NOMIC)

	local = Heart()

	with phones() as conn:
		with landline(local.index) as sconn:
			new: list, deleted: list, diffs: list, remaining: list, xdiff: bool = query_index(conn, sconn, local.target)
			newd: list, deletedd: list, ledeux: list = query_dindex(sconn, local.target)

	if xdiff is True:
		logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")

		try:
			with phones() as conn:
				with landline(local.index) as sconn:
					vok: bool, version: int = version_check(conn, sconn)

					if vok is True:
						cv: int = version + 1

						if force is True:
							message: str = f"upload v{version}"
						else:
							message: str = input("attach a message to this version [Return for None]: ") or None

						logger.info('updating records...')
						remote_records(conn, cv, message)
						historian(sconn, cv, message)

						if new:
							logger.info('uploading new files...')
							collector(conn, new, local.target, cv, key="new_files")

						if diffs:
							logger.debug('getting altered files\' previous versions...')
							sovquery: str = "SELECT original_version, from_version, track FROM records WHERE rp = ?;"
							details: dict = {}

							with conn.cursor() as cursor:
								for diff in diffs:
									data: int = sconn.execute(sovquery, (diff,)).fetchall()[0]
									details[diff] = (data[0], data[1], data[2])

							logger.info('uploading altered files...')
							collector(conn, diffs, local.target, cv, key="altered_files") # updates altered

							logger.info('generating altered files\' patches...')
							patches: list = diff_gen(diffs, details, local.originals, local.target) # computes & returns patches

							logger.info('uploading altered files\' patches...')
							upload_patches(conn, patches, cv, details) # uploads the patches to deltas

						if deleted:
							logger.info('removing deleted files...')
							dodata: tuple = rm_remfile(conn, sconn, deleted)

						if deletedd or newd:
							logger.info('updating remote directories...')
							rm_remdir(conn, sconn, deletedd, cv)
							upload_dirs(conn, newd, cv)

						logger.info('updating local index...')
						with fat_boy_o(local.originals) as secure:
							local_audit_(sconn, local.target, new, diffs, remaining, cv, secure)
							local_daudit(sconn, newd, deletedd, cv)

							if deleted:
								logger.info('backing up deleted files...')
								xxdeleted(conn, sconn, deleted, cv, secure, dodata)

					else:
						logger.critical(f"{RED}versions did not align; pull most recent upload from server before committing{RESET}")
						return

			updates = remaining + new

			with landline(local.index) as sconn:
				refresh_index(sconn, local.target, updates)

		except KeyboardInterrupt:
			logger.warning(f'\nboss killed the process; index could be corrupted; refreshing before exit...')
			sys.exit(1)

	else:
		logger.info('no diff!')

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()