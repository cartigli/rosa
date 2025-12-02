#!/usr/bin/env python3
import sys
import time

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import(mini_ps,
        scope_loc, hash_loc, scope_rem, ping_cass,
        contrast, compare, phone_duty, init_logger
    )

"""
Compare local data to server, report back.
"""

def check(fdiff):
    for tupl3 in fdiff:
        title = tupl3[0]
        count = len(tupl3[1])
        descr = tupl3[2]
        dict_key = tupl3[3]

        if count == 0:
            break

        decis0 = input(f"found {count} {title} ({descr}). do you want details? y/n: ")
        formatted = []
        if decis0.lower() in ('yes', 'y', 'ye', 'yeah','sure', ' y', 'y '):
            c = []
            if dict_key == 'frp':
                [c.append(item['frp']) for item in tupl3[1]]
            else:
                [c.append(item['drp']) for item in tupl3[1]]
            [formatted.append(f"\n{item}") for item in c]
            print(f"{title} ({descr}):\n{''.join(formatted)}")

        elif decis0.lower() in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
            pass


def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)

    logger.info('rosa [contrast] executed')
    start = time.perf_counter()
    if start:
        logger.info('rosa [contrast] timer started')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            1/0
            logger.info('...pinging heaven...')
            raw_heaven = scope_rem(conn)
            logger.info('...pinging cass...')
            heaven_dirs = ping_cass(conn)

            if any(raw_heaven) or any(heaven_dirs):
                logger.info('data returned from heaven; processing...')
                tot = 0
                altered = 0
                unchanged = 0
                fdiff = []
                ddiff = []

                begin = time.perf_counter()
                logger.info('...scanning local directory...')
                raw_paths, hell_dirs, abs_path = scope_loc(LOCAL_DIR)
                logger.info('...hashing local files...')
                raw_hell = hash_loc(raw_paths, abs_path)
                end = time.perf_counter()
                logger.info(f"scoped local directory in {end - begin:.4f} seconds")

                if begin and end:
                    hash_time = end - begin
                    if hash_time < 60:
                        logger.info(f"hash & path generation took {hash_time:.4f} seconds")
                    else:
                        hash_min = hash_time / 60
                        logger.info(f"hash & path generation took {hash_min:.4f} minutes")

                cherubs, souls, stags, serpents = contrast(raw_heaven, raw_hell)
                fdiff.append(("cherubs", cherubs, "files not found locally [only exist in server]", 'frp'),)
                fdiff.append(("souls", souls, "files whose contents have been altered [hash discrepancies]", 'frp'),)
                fdiff.append(("serpents", serpents, "files not found in the server [local only]", 'frp'),)

                gates, caves, ledeux = compare(heaven_dirs, hell_dirs)
                ddiff.append(("gates", gates, "directories not found locally [only exist in the server]", "drp"),)
                ddiff.append(("caves", caves, "directories not found in the server [local only]", "drp"),)

                if any(fdiff) or any(ddiff):
                    if not prints and not force:
                        logger.info('discrepancies found; showing to user')

                        if any(fdiff):
                            check(fdiff)

                        if any(ddiff):
                            check(ddiff)

                    elif prints or force:
                        logger.info('skipping ask-to-show')
                    
                    altered += len(cherubs) + len(serpents) + len(souls)
                    unchanged += len(stags)
                    tot += unchanged + altered

                    if unchanged > 1:
                        fratio = (((tot - altered) / tot)*100)
                        if fratio < 50:
                            logger.info(f"{100 - fratio:.4f}% of files were altered")
                        else:
                            logger.info(f"{fratio:.4f}% of files were unchanged")
                    else:
                        logger.info("less than 1% of files were altered")
                else:
                    logger.info('no dif')
            else:
                logger.info('no heaven data; have you uploaded?')

        except (ConnectionError, KeyboardInterrupt, Exception) as e:
            logger.error(f"{RED}err occured while contrasting directories:{RESET} {e}.", exc_info=True)
            sys.exit(1)

    if start:
        end = time.perf_counter()
        proc_time = end - start
        if proc_time > 60:
            mins = proc_time / 60
            logger.info(f"total processing time [in minutes] for rosa [contrast]: {mins:.3f}")
        else:
            logger.info(f"total processing time [in seconds] for rosa [contrast]: {proc_time:.3f}")

    logger.info('[contrast] completed')

    if prints:
        print('All set.')


if __name__=="__main__":
    from config import *
    from lib import(mini_ps,
        scope_loc, hash_loc, scope_rem, ping_cass,
        contrast, compare, phone_duty, init_logger
    )

    main(args=None)