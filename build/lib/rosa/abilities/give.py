#!/usr/bin/env python3
import sys
import time
from pathlib import Path

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import(scope_loc, hash_loc, scope_rem, 
        ping_cass, contrast, compare, rm_remdir, init_logger,
        rm_remfile, collect_info, collect_data, collect_data4, diffr, phones,
        upload_dirs, upload_created, upload_created2, _safety, scope_sz, 
        upload_edited, upload_edited2, confirm 
        )

"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

NOMIC = "[give]"

def main(args):
    x = True
    data, diff, start, mini, batch_sz = diffr(args, NOMIC, x)

    cherubs = data[0][0]
    souls = data[0][1]
    stags = data[0][2]
    serpents = data[0][3]

    # print('GIVE R .py')
    # print(cherubs)
    # print(type(cherubs))
    # print(souls)
    # print(type(souls))
    # print(serpents)
    # print(type(serpents))

    gates = data[1][0]
    caves = data[1][1]
    ledeux = data[1][2]

    logger = mini[0]
    force = mini[1]
    prints = mini[2]

    if diff == True:
        try:
            with phones() as conn:
                if gates:
                    logger.info('removing remote-only directory[s] from server...')
                    # gates_ = [(gate['drp'],) for gate in gates]
                    rm_remdir(conn, gates) # delete remote-only[s] from server
                    logger.info('removed directory[s]')

                if caves: 
                    # when uploading to server, order of when to upload new directory[s] is not as sensitive 
                    # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                    logger.info('uploading local-only directory[s] to server...')
                    upload_dirs(conn, caves) # upload local-only[s] to server
                    logger.info('directory[s] uploaded')

                if cherubs:
                    try:
                        logger.info('removing remote-only file[s]...')
                        rm_remfile(conn, cherubs) # delete remote-only file[s]
                        logger.info('removed file[s]')
                    except:
                        raise

                if souls:
                    logger.info('uploading altered file[s] to the server...')
                    # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                    souls_ = [item['frp'] for item in souls]

                    # soulss = sorted(souls_, key=str.lower)
                    soul_batches = collect_info(souls_, LOCAL_DIR) # returns batches
                    logger.info('formatted batches; uploading iteratively...')

                    logger.info('multi batches called for souls')
                    for batch in soul_batches:
                        soul_data = collect_data(batch, LOCAL_DIR, conn)
                        if soul_data:
                            try:
                                upload_edited(conn, soul_data)
                            except:
                                raise

                if serpents:
                    logger.info('uploading serpents to the server...')
                    # identical to souls upload block
                    serpents_ = [item['frp'] for item in serpents]

                    # serpentss = sorted(serpents_, key=str.lower)
                    serpent_batches = collect_info(serpents_, LOCAL_DIR)
                    logger.info('formatted batches; uploading iteratively...')

                    logger.info('multi batches called for serpents')
                    for batch in serpent_batches:
                        serpent_data = collect_data(batch, LOCAL_DIR, conn)
                        if serpent_data:
                            try:
                                upload_created(conn, serpent_data)
                            except:
                                raise

                if start:
                    end = time.perf_counter()
                    proc_time = end - start
                    if proc_time > 60:
                        min_time = proc_time / 60
                        logger.info(f"upload time [in minutes] for rosa [give]: {min_time:.3f}")
                    else:
                        logger.info(f"upload time [in seconds] for rosa [give]: {proc_time:.3f}")
                
                if force == True:
                    try:
                        conn.commit()
                        logger.info('commitment --forced')
                        logger.info(f"after conn.commit(): {conn.in_transaction}")
                        conn.close()
                        with phones() as conn:
                            try:
                                conn.ping(reconnect=False)
                            except:
                                pass
                    except Exception as e:
                        logger.critical(f"{RED}error on --forced commit:{RESET} {e}", exc_info=True)
                        sys.exit(3) # auto_commit: False, so error handling to rollback is not necessary
                    except:
                        raise
                    else:
                        logger.info('forced commit w.o exception')
                else:
                    try:
                        confirm(conn)
                    except:
                        raise
                    else:
                        logger.info('confirmation completed w.o exception')

        except KeyboardInterrupt as ko:
            logger.warning('going to try manual rollback for this one; standby')
            try:
                _safety(conn)
            except:
                raise
            else:
                logger.info('rollback was clean')
                sys.exit(0)
    else:
        logger.info('no diff')

    if start:
        end = time.perf_counter()
        proc_time = end - start
        if proc_time > 60:
            min_time = proc_time / 60
            logger.info(f"upload time [in minutes] for rosa [give]: {min_time:.3f}")
        else:
            logger.info(f"upload time [in seconds] for rosa [give]: {proc_time:.3f}")

    logger.info('[give] complete')

    if prints:
        print('All set.')


if __name__=="__main__":
    from config import *
    from lib import(scope_loc, hash_loc, scope_rem, 
        ping_cass, contrast, compare, rm_remdir, init_logger,
        rm_remfile, collect_info, collect_data, 
        upload_dirs, upload_created, _safety, scope_sz,
        upload_edited, confirm, phone_duty
    )

    main(args=None)