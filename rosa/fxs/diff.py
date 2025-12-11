#!/usr/bin/env python3
"""Compare local data to the server.

If difference, format it and show the user.
Should not effect local index or remote server.
"""

import sys
import time
import logging
from pathlib import Path

from rosa.confs import *
from rosa.lib import (
    diffr, phones, finale, 
    mini_ps, timer, query_index
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
    logger, force, prints, start = mini_ps(args, NOMIC)

    # new, deleted, diffs = query_index()
    
    with phones() as conn:
        data, diff = diffr(conn)

        if conn and conn.is_connected():
            conn.close()

    if diff is True:
        file_data, dir_data = data

        remote_only_files, altered_files, unchanged_files, local_only_files = file_data
        remote_only_directories, local_only_directories, unchanged_directories = dir_data

        diff_data = []

        diff_data.append(
            { # CHERUBS
                "type": "remote_only_files", 
                "details": remote_only_files,
                "message": "file[s] that only exist in server"
            }
        )
        diff_data.append(
            { # SERPENTS
                "type": "local_only_files", 
                "details": local_only_files,
                "message": "local-only file[s]"
            }
        )
        diff_data.append(
            { # SOULS
                "type": "altered_files", 
                "details": altered_files,
                "message": "file[s] with hash discrepancies"
            }
        )

        diff_data.append(
            { # GATES
                "type": "remote only directory[s]", 
                "details": remote_only_directories,
                "message": "directory[s] that only exist in the server"
            }
        )
        diff_data.append(
            { # CAVES
                "type": "local_only_directory", 
                "details": local_only_directories,
                "message": "directory[s] that are local only"
            }
        )

        ask_to_share(diff_data, force)

        # files altered:total
        tot = 0
        t_deltas = 0
        unchanged = 0
        
        t_deltas += len(remote_only_files) + len(local_only_files) + len(altered_files)
        unchanged += len(unchanged_files)
        tot += unchanged + t_deltas

        fratio = (t_deltas / tot)*100

        # directories altered:total
        d_tot = 0
        d_deltas = 0
        unchangedd = 0
        
        d_deltas += len(remote_only_directories) + len(local_only_directories)
        unchangedd += len(unchanged_directories)
        d_tot += unchangedd + d_deltas

        dratio = (d_deltas / d_tot)*100
    
        # ratio prints
        if prints is True:
            if fratio < 2:
                logger.info(f"{RED}{(fratio):.4f} %{RESET} of files were altered [failed hash verification]")
            elif fratio > 2:
                logger.info(f"{(100 - fratio):.4f} % of files were altered [failed hash verification]")
            else:
                logger.info(f"{fratio:.4f} % of files are unaltered [verified by hash]")

            if dratio < 2:
                logger.info(f"{RED}{(dratio):.4f} %{RESET} of directories were altered [failed hash verification]")
            elif dratio > 2:
                logger.info(f"{(100 - dratio):.4f} % of files were altered [failed hash verification]")
            else:
                logger.info(f"{dratio:.4f} % of files are unaltered [verified by hash]")

    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()