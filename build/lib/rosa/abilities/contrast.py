#!/usr/bin/env python3
import sys
import time
import logging

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import(mini_ps, diffr,
        scope_loc, hash_loc, scope_rem, ping_cass,
        contrast, compare, init_logger, doit_urself
    )

"""
Compare local data to server, report back.
"""

NOMIC = "[diff]"

def check(diff):
    logger = logging.getLogger()

    title = diff["type"]
    count = len(diff["details"])
    descr = diff["message"]
    dict_key = diff["key"]

    if count > 0:
        decis0 = input(f"found {count} {title} ({descr}). do you want details? y/n: ").lower()
        formatted = []
        if decis0 in ('yes', 'y', 'ye', 'yeah','sure', ' y', 'y '):
            c = []
            if dict_key == "frp":
                [c.append(item["frp"]) for item in diff["details"]]
            else:
                [c.append(item["drp"]) for item in diff["details"]]
            [formatted.append(f"\n{item}") for item in c]
            print(f".../{title} ({descr}):\n{''.join(formatted)}")

        elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
            pass
        else:
            print('ok freak')


def main(args):
    x = False
    data, diff, start, mini, x = diffr(args, NOMIC, x)

    cherubs = data[0][0]
    souls = data[0][1]
    stags = data[0][2]
    serpents = data[0][3]

    gates = data[1][0]
    caves = data[1][1]
    ledeux = data[1][2]

    logger = mini[0]
    force = mini[1]
    prints = mini[2]

    if diff == True:
        try:
            if force == True:
                logger.info('-force skipped ask-to-show')
                pass
            else:
                if prints == False:
                    logger.info('-silent skipped ask-to-show')
                    # pass
                else:
                    fdiff = []
                    ddiff = []

                    fdiff.append( {
                        "type": "cherubs",
                        "details": cherubs,
                        "message": "files not found locally [only exist in server]",
                        "key": "frp"
                        }
                    )
                    fdiff.append( {
                        "type": "souls",
                        "details": souls,
                        "message": "files whose contents have been altered [hash discrepancies]",
                        "key": "frp"
                        }
                    )
                    fdiff.append( {
                        "type": "serpents",
                        "details": serpents,
                        "message": "files not found in the server [local only]", 
                        "key": "frp"
                        }
                    )

                    ddiff.append( {
                        "type": "gates",
                        "details": gates,
                        "message": "directories not found locally [only exist in the server]", 
                        "key": "drp"
                        }
                    )
                    ddiff.append( {
                        "type": "caves",
                        "details": caves, 
                        "message": "directories not found in the server [local only]", 
                        "key": "drp"
                        }
                    )

                    for item in fdiff:
                        check(item)

                    for item in ddiff:
                        check(item)

            tot = 0
            altered = 0
            unchanged = 0
            
            altered += len(cherubs) + len(serpents) + len(souls)
            unchanged += len(stags)
            tot += unchanged + altered

            fratio = (((tot - altered) / tot)*100)
            # print(fratio)
            if fratio > 1:
                if fratio >= 50:
                    logger.info(f"{100 - fratio:.4f}% of files were altered")
                else:
                    logger.info(f"{fratio:.4f}% of files were unchanged")
            else:
                logger.info(f"less than 1% of files were altered: {100 - fratio:.3f}")

        except KeyboardInterrupt as ko:
            logger.error(f"boss killed it; wrap it up")
            sys.exit(1)

    else:
        logger.info('no dif')
    
    doit_urself()

    if start:
        end = time.perf_counter()
        proc_time = end - start
        if proc_time > 60:
            mins = proc_time / 60
            logger.info(f"total processing time [in minutes] for rosa [contrast]: {mins:.3f}")
        else:
            logger.info(f"total processing time [in seconds] for rosa [contrast]: {proc_time:.3f}")

    logger.info('[diff] completed')

    if prints:
        print('All set.')


if __name__=="__main__":
    from config import *
    from lib import(mini_ps,
        scope_loc, hash_loc, scope_rem, ping_cass,
        contrast, compare, phone_duty, init_logger
    )

    main(args=None)