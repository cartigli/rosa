#!/usr/bin/env python3
"""Compare local data to the server.

If difference, format it and show the user.
Should not effect local index or remote server.
"""

# import sys
# import time
import logging
# from pathlib import Path

# from rosa.confs import *
from rosa.lib import (
    phones, finale, 
    mini_ps, query_index,
    phones, query_dindex
)

NOMIC = "[diff]"

def ask_to_share(diff_data, force=False):
    """Asks the user if they would like to see the details of the discrepancies found (specific files/directories).

    Args:
        diff_data (list): Dictionaries containing the details, description, and title of the given files/directories, based on how they were found.
        force (=False): If passed, the function skips the ask-to-show and just prints the count as well as the title for any changes found. Defualt is false.
    
    Returns:
        None
    """
    logger = logging.getLogger('rosa.log')
    logger.info('discrepancy[s] found between the server and local data:')

    for i in diff_data:
        title = i["type"]
        count = len(i["details"])
        descr = i["message"]

        if count > 0:
            if force is True:
                logger.info(f"found: {count} {descr}")
                return
            else:
                decis0 = input(f"found {count} {descr}. do you want details? y/n: ").lower()
                formatted = []

                if decis0 in ('yes', 'y', ' y', 'y ', 'ye', 'yeah','sure'):
                    c = []
                    [c.append(item) for item in i["details"]]

                    [formatted.append(f"\n{item}") for item in c]
                    logger.info(f".../{title} ({descr}):\n{''.join(formatted)}")

                elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
                    logger.info('heard')
                else:
                    logger.info('ok, freak')

def main(args=None):
    """Runs the diff'ing engine and formats the data before asking to show the user whatever is found. 
    
    Function ran the most often.
    Using --force (-f) skips the ask-to-show.
    If no changes, it does not try to show.
    """
    xdiff = False
    logger, force, prints, start = mini_ps(args, NOMIC)

    newd, deletedd, ledeux = query_dindex()

    with phones() as conn:
        new, deleted, diffs, remaining, xdiff = query_index(conn)

    if prints is True:
        logger.info(f"found {len(newd)} new directories & {len(deletedd)} deleted directories.")

    if xdiff is True:
        logger.info(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} altered files.")
        logger.info(f"found {len(newd)} new directories & {len(deletedd)} deleted directories.")

        # if prints is True:

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

    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()