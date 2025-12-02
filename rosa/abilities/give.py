#!/usr/bin/env python3
import sys
import time

if __name__!="__main__":
    from rosa.abilities.config import *
    from rosa.abilities.lib import(scope_loc, hash_loc, scope_rem, 
        ping_cass, contrast, compare, rm_remdir, init_logger,
        rm_remfile, collect_info, collect_data, 
        upload_dirs, upload_created, _safety, scope_sz,
        upload_edited, confirm, phone_duty
    )


"""
Scan local directory, collect data from server, and compare all contents. Upload/insert files found locally but not in server, 
upload/update all files with hash discrepancies, and delete files not found locally but existing in server. Delete from the list
of directories if not found locally, and add new ones.
"""

def main(args):
    prints, force, logger = mini_ps(args, LOGGING_LEVEL)

    logger.info('rosa [give] executed.')

    start = time.perf_counter()
    if start:
        logger.info('[give] timer started.')

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            logger.info('conn is connected; pinging heaven...')
            raw_heaven = scope_rem(conn) # raw remote files & hash_id's
            logger.info('raw heaven returned')
            heaven_dirs = ping_cass(conn) # raw remote dirs' rpats
            logger.info('heavenly directory[s] returned')

            if raw_heaven or heaven_dirs:
                logger.info(f"data was returned from heaven; collecting info from {LOCAL_DIR}...")

                # logger.info('Contrasting and comparing.')
                raw_paths, hell_dirs, abs_path = scope_loc(LOCAL_DIR)  # files, directory[s], folder full path
                if any(raw_paths) or any(hell_dirs):
                    logger.info('data returned from local directory; hashing file[s] found...')
                raw_hell = hash_loc(raw_paths, abs_path)

                # logger.debug('getting average size for batch[es]...')
                # avg_size = scope_sz(raw_paths)

                logger.info('file[s] hashed; proceeding to compare and contrast...')
                cherubs, serpents, stags, souls = contrast(raw_heaven, raw_hell) # old, new, unchanged, changed file[s]
                logger.info('file[s] contrasted')
                f_delta = [cherubs, serpents, souls] # f_felta = altered file data

                gates, caves, ledeux = compare(heaven_dirs, hell_dirs) # old directory[s], new directory[s]
                logger.info('directory[s] compared')
                d_delta = [gates, caves] # d_delta = directory data

                if any(f_delta) or any(d_delta): # need context manager for connection
                    # logger.debug('getting average size for batch[es]...')
                    logger.info('discrepancies found; proceeding to processing...')
                    avg_size = scope_sz(raw_paths)
                    batch_sz = int(MAX_ALLOWED_PACKET / avg_size)
                    if batch_sz:
                        logger.info('optimal batch size returned')

                    if any(d_delta):
                        if gates:
                            try:
                                logger.info('removing remote-only directory[s] from server...')
                                # gates_ = [(gate['drp'],) for gate in gates]
                                rm_remdir(conn, gates) # delete remote-only[s] from server
                                logger.info('removed directory[s]')

                            except Exception as c:
                                raise

                        if caves: 
                            # when uploading to server, order of when to upload new directory[s] is not as sensitive 
                            # as rosa_get is when writing to disk (writing a file require's its parent to exist)
                            try:
                                logger.info('uploading local-only directory[s] to server...')
                                upload_dirs(conn, caves) # upload local-only[s] to server
                                logger.info('directory[s] uploaded')
                    
                            except Exception as c:
                                raise

                    if any(f_delta): # if hash discrepancies or list differences were found:
                        if cherubs:
                            try:
                                logger.info('removing remote-only file[s]...')
                                rm_remfile(conn, cherubs) # delete remote-only file[s]
                                logger.info('removed file[s]')

                            except Exception as c:
                                raise

                        if souls:
                            logger.info('uploading altered file[s] to the server...')
                            # create lists of files to upload based on their size & the MAX_ALLOWED_PACKET
                            souls_ = [item['frp'] for item in souls]
                            # soulss = sorted(souls_, key=str.lower)
                            soul_batches = collect_info(souls_, abs_path) # returns batches
                            logger.info('formatted batches; uploading iteratively...')
                            for batch in soul_batches:
                                # batch by batch, collect the content/hashes/relative paths into memory . . .
                                bg = time.perf_counter()
                                soul_data = collect_data(batch, abs_path, conn)

                                if soul_data:
                                    try:
                                        # and upload the batch tp the server; repeat
                                        upload_edited(conn, soul_data)
                                        fnl = time.perf_counter()
                                        logger.info(f"wrote batch to server in {(fnl - nd):.4f} seconds.")

                                    except Exception as c:
                                        raise

                        if serpents:
                            logger.info('uploading serpents to the server...')
                            # identical to souls upload block
                            serpents_ = [item['frp'] for item in serpents]
                            # serpentss = sorted(serpents_, key=str.lower)
                            serpent_batches = collect_info(serpents_, abs_path)
                            logger.info('formatted batches; uploading iteratively...')
                            for batch in serpent_batches:
                                bg = time.perf_counter()
                                serpent_data = collect_data(batch, abs_path, conn)

                                if serpent_data:
                                    try:
                                        upload_created(conn, serpent_data)
                                        fnl = time.perf_counter()
                                        logger.info(f"wrote batch to server in {(fnl - bg):.4f} seconds.")

                                    except Exception as c:
                                        raise

                    if start:
                        end = time.perf_counter()
                        proc_time = end - start
                        if proc_time > 60:
                            min_time = proc_time / 60
                            logger.info(f"upload time [in minutes] for rosa [give]: {min_time:.3f}")
                        else:
                            logger.info(f"upload time [in seconds] for rosa [give]: {proc_time:.3f}")
                    
                    if force:
                        try:
                            conn.commit()
                        except Exception as e:
                            logger.critical(f"{RED}error on --forced commit:{RESET} {e}", exc_info=True)
                            sys.exit(3) # auto_commit: False, so error handling to rollback is not necessary
                        else:
                            logger.info('forced commit w.o exception')
                    elif not force:
                        try:
                            confirm(conn)
                        except:
                            raise
                        else:
                            logger.info('confirmation completed w.o exception')

                else: # if no diff, wrap it up
                    logger.info('no discrepancies found')r

            else: # if server is empty, let us know
                logger.info('server is devoid of data')
        
        except KeyboardInterrupt as ko:
            logger.warning('going to try manual rollback for this one; standby')
            try:
                _safety(conn)
            except:
                raise
            else:
                logger.info('rollback was clean')
                sys.exit(0)

        logger.info('[give] complete')

        else:
            logger.info('rosa [give] complete')
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