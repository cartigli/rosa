#!/usr/bin/env python3
import sys
import time
import logging
from pathlib import Path

# if __name__=="__main__":
#     cd = Path(__file__).resolve().parent.parent
#     if str(cd) not in sys.path:
#         sys.path.insert(0, str(cd))

from rosa.confs.config import *
from rosa.lib.analyst import diffr
from rosa.lib.opps import finale, doit_urself, mini_ps, counter

"""
Compare local data to server, report back.
"""

NOMIX = "[diff]"

logger = logging.getLogger('rosa.log')

def ask_to_share(diff_data, force):
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

                    if dict_key == "frp":
                        [c.append(item["frp"]) for item in i["details"]]
                    else:
                        [c.append(item["drp"]) for item in i["details"]]

                    [formatted.append(f"\n{item}") for item in c]
                    logger.info(f".../{title} ({descr}):\n{''.join(formatted)}")

                elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
                    logger.info('heard')
                else:
                    logger.info('ok, freak')


def main(args=None):
    logger, force, prints, start = mini_ps(args, NOMIX)
    data, diff = diffr()
    # logger = mini[0]
    # force = mini[1]
    # prints = mini[2]
    # start = mini[3]
    if diff is True:
        remote_only = data[0][0]
        deltas = data[0][1]
        nodiffs = data[0][2]
        local_only = data[0][3]

        remote_only_directories = data[1][0]
        local_only_directories = data[1][1]
        ledeux = data[1][2]

        diff_data = []

        diff_data.append(
            {
                "type": "remote_only", 
                "details": remote_only, 
                "message": "file[s] that only exist in server", 
                "key": "frp"
            }
        )
        diff_data.append(
            {
                "type": "local_only", 
                "details": local_only, 
                "message": "local-only file[s]", 
                "key": "frp"
            }
        )
        diff_data.append(
            {
                "type": "deltas", 
                "details": deltas, 
                "message": "file[s] with hash discrepancies", 
                "key": "frp"
            }
        )

        diff_data.append(
            {
                "type": "remote only directory[s]", 
                "details": remote_only_directories, 
                "message": "directory[s] that only exist in the server", 
                "key": "drp"
            }
        )
        diff_data.append(
            {
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
        
        t_deltas += len(remote_only) + len(local_only) + len(deltas)
        unchanged += len(nodiffs)
        tot += unchanged + t_deltas

        fratio = (t_deltas / tot)*100

        # directories altered:total
        d_tot = 0
        d_deltas = 0
        unchangedd = 0
        
        d_deltas += len(remote_only_directories) + len(local_only_directories)
        unchangedd += len(ledeux)
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

    finale(NOMIX, start, prints) # ops
    # doit_urself()
    # counter(start, NOMIX)
    # logger.info('[diff] completed') # these three
    # if prints is True:
    #     print('All set.')

if __name__=="__main__":
    main()