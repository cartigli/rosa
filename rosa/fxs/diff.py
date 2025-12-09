#!/usr/bin/env python3
import sys
import time
import logging
from pathlib import Path

from rosa.confs import *
from rosa.lib import diffr, phones, finale, doit_urself, mini_ps, counter

"""
Compare local data to server, report back.
"""

NOMIX = "[diff]"

def ask_to_share(diff_data, force):
    logger = logging.getLogger('rosa.log')
    logger.info('discrepancy[s] found between the server and local data:')

    for i in diff_data:
        title = i["type"]
        count = len(i["details"])
        descr = i["message"]
        dict_key = i["key"]

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
    logger, force, prints, start = mini_ps(args, NOMIX)
    
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
                "message": "file[s] that only exist in server", 
                "key": "frp"
            }
        )
        diff_data.append(
            { # SERPENTS
                "type": "local_only_files", 
                "details": local_only_files,
                "message": "local-only file[s]", 
                "key": "frp"
            }
        )
        diff_data.append(
            { # SOULS
                "type": "altered_files", 
                "details": altered_files,
                "message": "file[s] with hash discrepancies", 
                "key": "frp"
            }
        )

        diff_data.append(
            { # GATES
                "type": "remote only directory[s]", 
                "details": remote_only_directories,
                "message": "directory[s] that only exist in the server", 
                "key": "drp"
            }
        )
        diff_data.append(
            { # CAVES
                "type": "local_only_directory", 
                "details": local_only_directories,
                "message": "directory[s] that are local only", 
                "key": "drp"
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

    finale(NOMIX, start, prints)

if __name__=="__main__":
    main()