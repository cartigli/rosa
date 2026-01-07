#!/usr/bin/env python3
"""Identify and present changes made since the latest commitment.

This only tracks differences from the latest commitment made on *this* machine.
If the server and local index are on different versions, this will not tell you.
It will only identify the changes you made since the latest indexing.
"""


import sys
# import logging

from rosa.confs import RED, RESET
from rosa.lib import (
	phones, finale, 
	mini_ps, query_index,
	phones, query_dindex,
	version_check, find_index,
	landline, Heart
)

NOMIC = "[diff]"

def ask_to_share(diff_data, force=False):
	"""Asks the user if they would like to see the details of the discrepancies found (specific files/directories).

	Args:
		diff_data (list): Dictionaries containing the details of differences found.
		force (bool): If true, ask user to see each types' files. If not, skips.
	
	Returns:
		None
	"""
	# logger = logging.getLogger('rosa.log')

	if force is True:
		return

	print('discrepancy[s] found between the server and local data:')

	for i in diff_data:
		title = i["type"]
		count = len(i["details"])
		descr = i["message"]

		if count > 0:
			if force is True:
				print(f"found: {count} {descr}")
				return
			else:
				decis0 = input(f"found {count} {descr}. do you want details? y/n: ").lower()
				formatted = []

				if decis0 in ('yes', 'y', ' y', 'y ', 'ye', 'yeah','sure'):
					c = []
					[c.append(item) for item in i["details"]]

					[formatted.append(f"\n{item}") for item in c]
					print(f".../{title} ({descr}):\n{''.join(formatted)}")

				elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
					print('heard')
				# else:
				# 	logger.info('ok, freak')

def main(args=None):
	"""Runs the diff'ing engine before asking to show the user what was found, if anything."""
	xdiff = False
	r = False

	logger, force, prints, start = mini_ps(args, NOMIC)
	
	if args:
		if args.remote:
			r = True

	local = Heart()

	with phones() as conn:
		with landline(local.index) as sconn:
			new, deleted, diffs, remaining, xdiff = query_index(conn, sconn, local.target)
			newd, deletedd, ledeux = query_dindex(sconn, local.target)

			if r:
				vok, vers = version_check(conn, sconn)

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

			diff_data = []

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

			ask_to_share(diff_data, force)

		else:
			logger.info('no diff!')

	except KeyboardInterrupt:
		logger.warning(f'\nboss killed the process; abandoning...')
		sys.exit(1)

	finale(NOMIC, start, prints)

if __name__=="__main__":
	main()