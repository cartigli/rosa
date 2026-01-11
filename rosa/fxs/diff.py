#!/usr/bin/env python3
"""Identify and present changes made since the latest commitment."""

import sys

from rosa.confs import RED, RESET
from rosa.lib import (
	phones, finale, mini_ps, query_index,
	phones, query_dindex, version_check, 
	landline, Heart
)

NOMIC: str = "[diff]"

def ask_to_share(diff_data: list = [], force: bool = False):
	"""Asks the user if they would like to see the details of the discrepancies found (specific files/directories).

	Args:
		diff_data (list): Dictionaries containing the details of differences found.
		force (bool): If true, ask user to see each types' files. If not, skips.
	
	Returns:
		None
	"""
	print('discrepancy[s] found between the server and local data:')

	for i in diff_data:
		title: str = i["type"]
		count: int = len(i["details"])
		descr: str = i["message"]

		if count > 0:
			if force is True:
				print(f"found: {count} {descr}")
				return
			else:
				decis0: str = input(f"found {count} {descr}. do you want details? y/n: ").lower()
				formatted: list = []

				if decis0 in ('yes', 'y', ' y', 'y ', 'ye', 'yeah','sure'):
					c: list = []
					[c.append(item) for item in i["details"]]

					[formatted.append(f"\n{item}") for item in c]
					print(f".../{title} ({descr}):\n{''.join(formatted)}")

				elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
					print('heard')

def main(args: argparse = None):
	"""Runs the diff'ing engine before asking to show what was found, if anything."""
	redirect: str = None
	xdiff: bool = False
	r: bool = False

	logger, force: bool, prints: bool, start: float = mini_ps(args, NOMIC)
	
	if args:
		if args.extra:
			r = True
		if args.redirect:
			redirect = args.redirect

	local = Heart(redirect)

	with phones() as conn:
		with landline(local.index) as sconn:
			new: list, deleted: list, diffs: list, remaining: list, xdiff: bool = query_index(conn, sconn, local.target)
			newd: list, deletedd: list, ledeux: list = query_dindex(sconn, local.target)

			if r:
				vok: bool, vers: int = version_check(conn, sconn)

				if vok is True:
					logger.info('versions: twinned')
				elif vok is False:
					logger.info(f"versions: {RED}twisted{RESET}")

	try:
		if xdiff is True:
			if any((new, deleted, diffs)):
				logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")
			
			if any((newd, deletedd)):
				logger.info(f"found {len(newd)} new directories & {len(deletedd)} deleted directories.")

			diff_data: list = []

			diff_data.append(
				{ # CHERUBS
					"type": "deleted files", 
					"details": deleted,
					"message": "file[s] that only exist in server"
				}
			)
			diff_data.append(
				{ # SERPENTS
					"type": "new files", 
					"details": new,
					"message": "local-only file[s]"
				}
			)
			diff_data.append(
				{ # SOULS
					"type": "altered files", 
					"details": diffs,
					"message": "file[s] with hash discrepancies"
				}
			)

			diff_data.append(
				{ # CAVES
					"type": "new directories", 
					"details": newd,
					"message": "new directories"
				}
			)
			diff_data.append(
				{ # GATES
					"type": "deleted directories", 
					"details": deletedd,
					"message": "deleted directories"
				}
			)

			if prints is True:
				ask_to_share(diff_data, force)

		else:
			logger.info('no diff!')

	except KeyboardInterrupt:
		logger.warning(f'\nboss killed the process; abandoning...')
		sys.exit(1)

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()