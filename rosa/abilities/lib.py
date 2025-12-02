import os
import sys
import time
import shutil
import logging
import tempfile
# import hashlib
import contextlib
from pathlib import Path
from itertools import batched

# these three are the only external packages required
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import xxhash # this one is optional and can be replaced with hashlib which is more secure & in the native python library
import mysql.connector # to connect with the mysql server - helps prevent injection while building queries as well
# import zstandard as zstd # compressor for files before uploading and decompressing after download

from rosa.abilities.queries import ASSESS2
from rosa.abilities.config import MAX_ALLOWED_PACKET


"""
Library of functions used in the management scripts. Many functions overlap and share uses, so moving them here is much easier for 
maintainability. There is also queries for longer MySQL queries. Some scripts, like get_moment, hold their own unique functions.
"""

# logging

logger = logging.getLogger(__name__)
RED = "\x1b[31;1m"
RESET = "\x1b[0m"

def init_logger(logging_level):
    if logging_level:
        logger = logging.getLogger('rosa.log')
        logger.setLevel(logging.DEBUG)

        if logger.hasHandlers():
            if logging_level:
                logger.handlers.clear()

        file_handler = logging.FileHandler('rosa.log', mode='a')
        file_handler.setLevel(logging.DEBUG)

        error_handler = logging.FileHandler('rosa.errors', mode='a')
        error_handler.setLevel(logging.ERROR)
    
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging_level.upper())

        cons_ = "[%(levelname)s][%(filename)s:%(lineno)s]: %(message)s"
        file_ = "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s]: %(message)s"
        errs_ = "[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d]: %(message)s"

        console_format = logging.Formatter(cons_)
        file_format = logging.Formatter(file_)
        err_format = logging.Formatter(errs_)

        file_handler.setFormatter(file_format)
        console_handler.setFormatter(console_format)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger
    else:
        logger.warning("logger not passed; maybe config isn't configured?")
        sys.exit(1)

def mini_ps(args, logging_level):
    f = False # no checks - force
    p = True # no prints - prints

    if args:
        if args.silent:
            logging_level="critical".upper()
            logger = init_logger(logging_level)
            p = False
        elif args.verbose: # can't do verbose & silent
            logging_level = "debug".upper()
            logger = init_logger(logging_level)
            p = True
        else: # but can do verbose & force (or silent & force)
            logging_level = logging_level
            logger = init_logger(logging_level.upper())

        if args.force:
            f = True
    else:
        logger = init_logger(logging_level.upper())

    logger.debug('mini parser completed')
    return p, f, logger

# connection & management thereof

@contextlib.contextmanager
def phone_duty(db_user, db_pswd, db_name, db_addr):
    """Context manager for the mysql.connector connection object."""
    conn = None
    logger.debug('...phone_duty() received config; connecting...')
    try:
        conn = init_conn(db_user, db_pswd, db_name, db_addr)
        if conn:
            logger.debug('connection object created & yielded to main()')
            yield conn
        else:
            logger.warning('connection object lost')

    except KeyboardInterrupt as ko:
        logger.error('boss killed it; wrap it up')
        if conn and conn.is_connected():
            _safety(conn)
        sys.exit(1)
    except (ConnectionRefusedError, TimeoutError, RuntimeError, Exception) as e:
        logger.error(f"err encountered while connecting to the server: {e}.", exc_info=True)
        if conn:
            _safety(conn)
        sys.exit(1)
    else:
        logger.debug('phone_duty() executed w.o exception')
    finally:
        if conn:
            if conn.is_connected():
                conn.close()
                logger.info('phone_duty() closed conn [finally]')


def _safety(conn):
    """Handles rollback of the server on err from phone_duty."""
    try:
        logger.warning('_safety called rollback server due to err')
        conn.rollback()

    except (mysql.connector.Error, ConnectionRefusedError, ConnectionError, TimeoutError, Exception) as e:
        logger.error(f"_safety caught an error while trying to rollback on err: {e}", exc_info=True)
        raise
    else:
        logger.warning("_safety rolled server back w.o exception")


def init_conn(db_user, db_pswd, db_name, db_addr): # used by all scripts
    """Initiate the connection to the server. If an error occurs, [freak out] raise."""
    config = {
        'unix_socket': '/tmp/mysql.sock',
        # 'host': db_addr,
        'user': db_user,
        'password': db_pswd,
        'database': db_name,
        'autocommit': False,
        # 'use_pure': False # 5.886, 5.902, 5.903 | not worth the lib
        'use_pure': True # 5.122, 5.117, 5.113 | seems faster regardless
        # 'use_unicode': False # 3.213 (after use_pure: 3.266)
        # 'pool_size': 5
        # 'raw': True
    }
    try:
        conn = mysql.connector.connect(**config)
    except Exception as e:
        raise
    else:
        logger.info("connection object initialized w.o exception")
        return conn

# collecting local info for comparison

def scope_loc(local_dir): # all
    """Collect the relative path and 256-bit hash for every file in the given directory. Ignore any files with paths that contain
    the '.DS_Store', '.git', or '.obsidian'. Record the hash in BINARY format. Hex for python ( and UNHEX for the sql queries ). 
    Every file that meets the criteria has its relative path and hash generated and recorded. Returns the pairs of paths/hashes 
    in a tuple to match data returned from the server.
    """
    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    abs_path = Path(local_dir).resolve()

    raw_paths = []
    hell_dirs = []

    try:
        if abs_path.exists():
            logger.debug('...scoping local directory...')
            for item in abs_path.rglob('*'):
                path_str = item.resolve().as_posix()
                if any(blocked in path_str for blocked in blk_list):
                    # logger.debug(f"{item} was rejected due to blocked_list (config)")
                    continue # skip item if blkd item in its path
                elif item.is_file():
                    # logger.debug(f"file: {item} noted")
                    raw_paths.append(item) # only the full paths 

                elif item.is_dir():
                    logger.debug(f"directory: {item} is noted")
                    drp = item.relative_to(abs_path).as_posix()
                    hell_dirs.append((drp,)) # ditto for the dirs but in tuples
                else:
                    continue
        else:
            logger.warning('local directory does not exist')
            sys.exit(1)

    except Exception as e:
        logger.error(f"encountered: {e} while hashing; aborting.", exc_info=True)
        raise
    except KeyboardInterrupt as ko:
        logger.warning("boss killed it; wrap it up")
        sys.exit(0)

    logger.debug('finished collecting local paths')
    return raw_paths, hell_dirs, abs_path


def scope_sz(raw_paths):
    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    tsz = 0

    for path in raw_paths:
        tsz += os.path.getsize(path)    
    if tsz:
        avg = tsz / len(raw_paths)

    if avg:
        logger.info(f"found avg_size of local file[s] : {avg}")

    return int(avg)


def hash_loc(raw_paths, abs_path):
    hasher = xxhash.xxh64()
    raw_hell = []

    logger.debug('...hashing...')

    with tqdm(raw_paths, unit="hashes", leave=True) as pbar:
        for item in pbar:
            hasher.reset()

            hasher.update(item.read_bytes())
            hash_id = hasher.digest()

            frp = item.relative_to(abs_path).as_posix()

            raw_hell.append((frp, hash_id))
            logger.debug(f"hashed file: {item}")

    if any(raw_hell[1]):
        logger.debug('files are hashed')

    return raw_hell


# collecting server data for comparison

def scope_rem(conn): # all
    """Select and return every single relative path and hash from the notes table. Returned as a list of tuples (rel_path, hash_id)."""
    q = "SELECT frp, hash_id FROM notes;"

    with conn.cursor() as cursor:
        try:
            logger.debug('...scoping remote files...')
            cursor.execute(q)
            raw_heaven = cursor.fetchall()
            if raw_heaven:
                logger.debug("server returned data from query")
            else:
                logger.warning("server returned raw_heaven as an empty set")

        except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
            logger.error(f"err while getting data from server: {c}.", exc_info=True)
            raise
        else:
            logger.debug('remote file scoping completed w.o exception')
            # if raw_heaven:
            #     logger.info("server returned data from query")
            # else:
            #     logger.warning("server returned raw_heaven as an empty set")

    return raw_heaven


def ping_cass(conn): # all
    """Ping the kid Cass because you just need a quick hand with the directories. If a directory is empty or contais only subdirectories and no files,
    Cass is the kid to clutch it. He returns a list of directories as tuples containing their relative paths.
    """
    q = "SELECT * FROM directories;" # drp's

    with conn.cursor() as cursor:
        try:
            logger.debug('...scoping remote directory[s]...')
            cursor.execute(q)
            heaven_dirs = cursor.fetchall()
            if heaven_dirs:
                logger.debug("server returned data from query")
            else:
                logger.warning("server returned heaven dirs as an empty set")

        except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
            logger.error(f"err encountered while attempting to collect directory data from server: {c}.", exc_info=True)
            raise
        else:
            logger.debug('remote file scoping completed w.o exception')
            # if heaven_dirs:
            #     logger.info('server returned directories from server')
            # else:
            #     logger.warn('server returned directories as an empty set')

    return heaven_dirs

# COMPARING

def contrast(raw_heaven, raw_hell): # unfiform for all scripts
    """Accepts two lists of tupled pairs which each hold a files relative path and hash. It makes a list of the first item for every item in each list; 
    every file's relative path. It compares these lists to get the files that are remote-only and local-only and makes each one into a dictionary with 
    the same key for every item: 'frp'. Then, for the files that are in both locations, they ar emade into a new dictionary containing each file's 
    respective hash and relative path. Using their key values, each item in the local directory's hash is compared to the remote file's hash. If a 
    discrepancy is found, it is added to the same dictionary key values as the first two result sets: 'frp'. 'frp' is the substitution key for the 
    mysql queries these lists of dictionaries will be used for.
    """
    heaven_souls = {s[0] for s in raw_heaven}
    hell_souls = {d[0] for d in raw_hell}

    cherubs = [{'frp':cherub} for cherub in heaven_souls - hell_souls] # get - cherubs as a dict: 'frp'
    serpents = [{'frp':serpent} for serpent in hell_souls - heaven_souls]
    logger.debug(f"found {len(cherubs)} cherubs [server only] and {len(serpents)} serpents [local only] file[s]")

    people = heaven_souls & hell_souls # those in both - unaltered
    logger.debug(f"found {len(people)} people [found in both] file[s]")
    souls = []
    stags = []

    heaven = {lo: id for lo, id in raw_heaven if lo in people}
    hell = {lo: id for lo, id in raw_hell if lo in people}

    for key in hell:
        if hell[key] != heaven[key]:
            souls.append({'frp': key}) # altered [transient, like water buddy]
        else:
            stags.append(key) # unchanged [hash verified]
    
    logger.debug(f"found {len(souls)} souls [altered contents] and {len(stags)} stags [unchanged] file[s]")

    logger.debug('contrasted files and id\'d discrepancies')
    return cherubs, serpents, stags, souls # files in server but not present, files present not in server, files in both, files in both but with hash discrepancies


def compare(heaven_dirs, hell_dirs): # all
    """Makes a set of each list of directories and formats them each into a dictionary. It compares the differences and returns a list of remote-only and local-only directories."""
    heaven = set(heaven_dirs)
    hell = set(hell_dirs)

    gates = [{'drp':gate[0]} for gate in heaven - hell]
    caves = [{'drp':cave[0]} for cave in hell - heaven]

    ledeux = heaven & hell

    logger.debug(f"found {len(gates)} gates [server-only], {len(caves)} caves [local-only], and {len(ledeux)} ledeux's [found in both]")

    logger.debug('compared directories & id\'d discrepancies')
    return gates, caves, ledeux # dirs in heaven not found locally, dirs found locally not in heaven

# [atomically] edit local data; rosa_get mostly

@contextlib.contextmanager
def fat_boy(abs_path):
    """Context manager for temporary directory and backup."""
    tmp_ = None
    backup = None

    try:
        try:
            tmp_, backup = configure(abs_path)
            if tmp_ and backup:
                logger.debug(f"fat boy made {tmp_} and {backup}; yielding...")
                yield tmp_, backup # return these & freeze in place

        except KeyboardInterrupt as e:
            logger.warning('boss killed it; wrap it up')
            try:
                _lil_guy(abs_path, backup, tmp_)
            except:
                raise
            else:
                logger.warning('_lil guy recovered on err w.o exception [on the big guy\'s orders]')
                sys.exit(1)

        except (mysql.connector.Error, ConnectionError, Exception) as e:
            logger.error(f"{RED}err encountered while attempting atomic wr: {e}.", exc_info=True)
            try:
                _lil_guy(abs_path, backup, tmp_)
            except:
                raise
            else:
                logger.warning('_lil guy recovered on err w.o exception')
                sys.exit(1)

        else:
            try:
                apply_atomicy(tmp_, abs_path, backup)

            except KeyboardInterrupt as c:
                logger.warning('boss killed it; wrap it up')
                try:
                    _lil_guy(abs_path, backup, tmp_)
                except:
                    raise
                else:
                    logger.warning('_lil guy recovered on err w.o exception [on the big guy\'s orders]')
                    sys.exit(1)

            except (mysql.connector.Error, ConnectionError, Exception) as c:
                logger.error(f"{RED}err encountered while attempting to apply atomicy: {c}.", exc_info=True)
                try:
                    _lil_guy(abs_path, backup, tmp_)
                except:
                    raise
                else:
                    logger.warning('_lil guy recovered on err w.o exception')
                    sys.exit(1)

            else:
                logger.debug("applied 'atomicy' w.o exception")

    except (KeyboardInterrupt, Exception) as e:
        logger.critical(f"{RED}uncaught exception slipped through: {e}{RESET}", exc_info=True)
        logger.warning(f"{abs_path} is likely remains @{backup}, and {tmp_} was probably not deleted")
        sys.exit(1)
    else:
        logger.info('fat boy completed w.o exception')


def _lil_guy(abs_path, backup, tmp_):
    """Handles recovery on error for the context manager fat_boy."""
    try:
        if backup and backup.exists():
            if abs_path.exists():
                shutil.rmtree(tmp_)
                if tmp_.exists():
                    try:
                        logger.warning('5 sec pause to retry recursive delete; standby')
                        time.sleep(5)
                        shutil.rmtree(tmp_)
                    except Exception as e:
                        logger.error(f"directory failed to delete even on second round of brute force: {e}.", exc_info=True)
                        raise
                    else:
                        logger.warning('rm -rf flopped again, but we handled it')

                logger.warning('removed damaged attempt')
            try:
                backup.rename(abs_path)
            except:
                raise
            else:
                logger.warning('moved backup back to original location')

        if tmp_ and tmp_.exists():
            shutil.rmtree(tmp_)
            if tmp_.exists():
                try:
                    logger.warning('5 sec pause to retry recursive delete; standby')
                    time.sleep(5)
                    shutil.rmtree(tmp_)
                except Exception as e:
                    logger.warning(f"directory failed to delete even on second round of brute force: {e}.", exc_info=True)
                    raise
                else:
                    logger.warning('shutil flopped again, but we handled it')
            else:
                logger.warning('removed damaged temporary directory')

    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.error(f"{RED}replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
        raise
    else:
        logger.info("_lil_guy's cleanup complete")


def configure(abs_path): # raise err & say 'run get all or fix config's directory; there is no folder here'
    """Configure the temporary directory & move the original to a backup location. 
    Returns the _tmp directory's path.
    """
    try:
        if abs_path.exists():
            tmp_ = Path(tempfile.mkdtemp(dir=abs_path.parent))
            backup = Path( (abs_path.parent) / f"Backup_{time.time():2f}" )
            if tmp_ and backup:
                logger.debug(f"{tmp_} and {backup} configured by [configure]")

            abs_path.rename(backup)
            logger.debug('local directory moved to backup')
        else:
            logger.warning(f"{abs_path} doesn't exist; fix the config or run 'rosa get all'")
            sys.exit(1)
    
    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.error(f"err encountered while trying move {abs_path} to a backup location: {e}.", exc_info=True)
        raise
    else:
        logger.debug('temporary directory created & original directory moved to backup w.o exception')
        return tmp_, backup


def calc_batch(conn):
    """Get the average row size of the notes table to estimate optimal batch size for downloading. ASSESS2 is 1/100 the speed of ASSESS"""
    batch_size = 5 # default
    row_size = 10 # don't divide by 0

    with conn.cursor() as cursor:
        try:
            beg = time.perf_counter()

            cursor.execute(ASSESS2)
            row_size = cursor.fetchone()

            if row_size:
                logger.debug(f"ASSESS2 returned {row_size}")
            end = time.perf_counter()
            logger.info(f"ASSESS2 took {(end - beg):.4f} seconds")

        except (ConnectionError, TimeoutError, Exception) as c:
            logger.error(f"err encountered while attempting to find avg_row_size: {c}", exc_info=True)
            raise
        else:
            if row_size:
                if row_size[0] and row_size[0] != 0:
                    batch = int( (0.94*MAX_ALLOWED_PACKET) / row_size[0] )
                    batch_size = max(1, batch)
                    logger.info(f"batch size determined: {batch_size}")
                    logger.debug(f"check against row: {row_size[0]}")
                    logger.debug(f"check against max_pack: {row_size[0]*batch_size}")
                    logger.debug(f"check compared to max_packet: {0.94*MAX_ALLOWED_PACKET}")
                    return batch_size, row_size
                else:
                    logger.warning(f"couldn't use row_size; defaulting to batch size = {batch_size}")
                    return batch_size, row_size
            else:
                logger.warning(f"ASSESS2 returned nothing; defaulting to batch size = {batch_size}")
                return batch_size, row_size


def save_people(people, backup, tmp_):
    """Hard-links unchanged files present in the server and locally from the backup directory (original) 
    to the _tmp directory. Huge advantage over copying because the file doesn't need to move."""
    logger.debug('...saving people [hard-linking]...')
    with tqdm(people, unit="hard-links", leave=True) as pbar:
        for person in pbar:
            curr = Path( backup / person )
            tmpd = Path( tmp_ / person )
            try:
                # (tmpd.parent).mkdir(parents=True, exist_ok=True)
                os.link(curr, tmpd)
                logger.debug(f"linked {curr} to {tmp_}")

            except (PermissionError, FileNotFoundError, KeyboardInterrupt, Exception) as te:
                raise
            else:
                logger.debug('people saved w.o exception')


def download_batches(flist, conn, batch_size, tmp_): # get
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    paths = [item['frp'] for item in flist]
    params = ', '.join(['%s']*len(paths))

    logger.debug('downloading batches w.offset & limit')

    batch_size = batch_size
    offset = 0

    with conn.cursor() as cursor:
        try:
            while True:
                query = f"SELECT frp, content FROM notes WHERE frp IN ({params}) LIMIT {batch_size} OFFSET {offset};"

                try:
                    beg = time.perf_counter()
                    cursor.execute(query, paths)
                    batch = cursor.fetchall()
                    end = time.perf_counter()

                    dur = end - beg
                    logger.info(f"collected batch in {dur:.4f} seconds")

                except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
                    logger.warning(f"err while trying to download data: {c}.", exc_info=True)
                    raise
                else:
                    if batch:
                        stt = time.perf_counter()
                        wr_batches(batch, tmp_)
                        stp = time.perf_counter()

                        timez = stp - stt
                        logger.debug(f"wrote batch in {timez:.4f} seconds")

                    if len(batch) < batch_size:
                        break

                    offset += batch_size

        except: # tout de monde
            logger.critical(f"{RED}err while attempting batched atomic write{RESET}", exc_info=True)
            raise
        else:
            logger.debug('atomic wr w.batched download completed w.o exception')


def download_batches2(flist, conn, batch_size, tmp_): # get
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    paths = [item[0] for item in flist]
    params = ', '.join(['%s']* len(paths))

    logger.debug('downloading batches w.offset & limit')

    batch_size = batch_size
    offset = 0

    with conn.cursor() as cursor:
        try:
            while True:
                query = f"SELECT frp, content FROM notes WHERE frp IN ({params}) LIMIT {batch_size} OFFSET {offset};"

                try:
                    cursor.execute(query, paths)
                    batch = cursor.fetchall()

                except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
                    logger.warning(f"error while trying to download data: {c}.", exc_info=True)
                    raise
                else:
                    if batch:
                        wr_batches(batch, tmp_)

                    if len(batch) < batch_size:
                        break

                    offset += batch_size

        except: # tout de monde
            logger.critical(f"{RED}err while attempting batched atomic write{RESET}", exc_info=True)
            raise
        else:
            logger.debug('atomic wr w.batched download completed w.o exception')


def download_batches5(souls, conn, batch_size, row_size, tmp_): # get_all ( aggressive )
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    batch_count = int(len(souls) / batch_size)
    if len(souls) % batch_size:
        batch_count += 1

    kbb = False
    # curr_count = 0
    batched_list = list(batched(souls, batch_size))

    logger.debug(f"split list into {batch_count} batches")

    batch_mbytes = (batch_size * row_size[0]) / (1024*1024)

    # bar = "{l_bar}{bar}| {n:.3f}/{total:.3f} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
    bar = "{l_bar}{bar}| {n:.0f}/{total:.0f} [{rate_fmt}{postfix}]"

    try:
        with logging_redirect_tqdm(loggers=[logger]):
            with tqdm(batched_list,
            desc=f"Pulling {batch_count} batches", unit=" batches", unit_scale=True, 
            unit_divisor=1024, colour="white", bar_format = bar) as pbar:
                for bunch in pbar:
                    # batch = []
                    actual = 0
                    # cpr = 0

                    current_rate = pbar.format_dict['rate']
                    spd_str = "? mb/s"
                    # cpr_str = "?:1"

                    if current_rate:
                        actual = current_rate * batch_mbytes
                        spd_str = f"{actual:.2f}mb/s"

                    with conn.cursor() as cursor:
                        try:
                            inputs = ', '.join(['%s']*len(bunch))
                            query = f"SELECT frp, content FROM notes WHERE frp IN ({inputs});"

                            cursor.execute(query, bunch)
                            batch = cursor.fetchall()
                            logger.debug('got one batch of data')
                            
                            if batch:
                                logger.debug('...passing batch to wr_batches...')
                                wr_batches(batch, tmp_)
                                # uncpr = wr_batches(batch, tmp_)

                                pbar.set_postfix_str(f"{spd_str}")
                                # if cpr and uncpr:
                                #     current_rate = uncpr / cpr
                                #     cpr_str = f"{current_rate:.1f}:1"
                                    # wr_pace = current_rate * actual
                                    # pbar.set_postfix_str(f"{spd_str} | cmpr: {cpr_str}")
                                    # pbar.set_postfix_str(f"{spd_str} | cmpr: {cpr_str} | wr_rate: {wr_pace:.2f}mb/s")

                        except KeyboardInterrupt as c:
                            pbar.leave = False
                            try:
                                cursor.fetchall()
                                cursor.close()
                            except:
                                pass
                            tqdm.write(f"{"\033[91m"}boss killed it; deleting partial download")
                            logger.warning(f"{RED}boss killed it; deleting partial downlaod")
                            pbar.close()
                            raise
                        except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
                            logger.error(f"err while trying to downwrite data: {c}.", exc_info=True)
                            try:
                                cursor.fetchall()
                                cursor.close()
                            except:
                                pass
                            tqdm.write(f"{RED}err caught while downloading; removing tmp_ directory")
                            pbar.close()
                            raise

    except KeyboardInterrupt as c:
        raise
    else:
        logger.debug('atomic wr w.batched download completed w.o exception')


def wr_batches(data, tmp_):
    """Writes each batch to the _tmp directory as they are pulled. Each file has it and its parent directory flushed from memory for assurance of atomicy."""
    logger.debug('...writing batch to disk...')
    # dcmpr = zstd.ZstdDecompressor() # initiate outside of loop; duh

    try:
        for frp, content in data:
            t_path = Path ( tmp_ / frp ) #.resolve()
            (t_path.parent).mkdir(parents=True, exist_ok=True)

            # d_content = dcmpr.decompress(content)

            with open(t_path, 'wb') as t:
                t.write(content)

    except KeyboardInterrupt as ki:
        raise
    except (PermissionError, FileNotFoundError, Exception) as e:
        raise
    else:
        logger.debug('wrote batch w.o exception')


def apply_atomicy(tmp_, abs_path, backup):
    """If the download and write batches functions both complete entirely w.o error, this function moved the _tmp directory back to the original abs_path. 
    If this completes w.o error, the backup is deleted.
    """
    try:
        tmp_.rename(abs_path)

    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.critical(f"{RED}exception encountered while attempting atomic write: {e}.", exc_info=True)
        raise
    else:
        logger.debug('temporary directory renamed w.o exception')
        if backup.exists():
            try:
                shutil.rmtree(backup)
            except Exception as e:
                logger.warning('5 sec pause before retry recursive delete; standby')
                time.sleep(5)
                try:
                    shutil.rmtree(backup)
                except:
                    raise
                else:
                    logger.warning('removed backup after execution w.One exception')
            else:
                logger.debug('removed backup after execution w.o exception')


def mk_dir(gates, abs_path):
    """Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
    logger.debug('...writing directory tree to disk...')
    try:
        for gate in gates:
            path = gate['drp']
            fdpath = (abs_path / path ).resolve()
            fdpath.mkdir(parents=True, exist_ok=True)

    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.error(f"err when tried to make directories: {e}.", exc_info=True)
        raise
    else:
        logger.debug('created directory tree on disk w.o exception')


def mk_rdir(gates, abs_path):
    """Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
    logger.debug('...writing directory tree to disk...')
    try:
        with logging_redirect_tqdm(loggers=[logger]):
            with tqdm(gates, unit="dirs") as pbar:
                for gate in pbar:
                    fdpath = (abs_path / gate ).resolve()
                    fdpath.mkdir(parents=True, exist_ok=True)

    except (PermissionError, FileNotFoundError, Exception) as e:
        pbar.leave = False
        pbar.close()
        logger.error(f"err when tried to make directories: {e}.", exc_info=True)
        raise
        # raise
    else:
        logger.debug('created directory tree on disk w.o exception')


def mk_rrdir(gates, abs_path):
    """Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
    logger.debug('...writing directory tree to disk...')
    try:
        with logging_redirect_tqdm(loggers=[logger]):
            with tqdm(gates, desc=f"Writing {len(gates)} directories", unit="dirs") as pbar:
                for gate in pbar:
                    fdpath = Path(abs_path / gate[0] ).resolve()
                    fdpath.mkdir(parents=True, exist_ok=True)

    except (PermissionError, FileNotFoundError, Exception) as e:
        pbar.leave = False
        pbar.close()
        logger.error(f"error when tried to make directories: {e}.", exc_info=True)
        sys.exit(1)
        # raise
    else:
        logger.debug('created directory tree on disk w.o exception')

# EDIT SERVER - rosaGIVE

# deletes

def rm_remdir(conn, gates): # only give 3.0
    """Remove remote-only directories from the server. Paths [gates] passed as list of dictionaries for executemany(). This, and every other call to 
    make an edit on the database, is rolled back on errors. Its only for specific calls to change the information in them. The default with mysql - conn
    is to roll back on disconnect, so its an ok safety net, but its supposed to be all over this shit.
    """
    logger.debug('...deleting remote-only drectory[s] from server...')
    g = "DELETE FROM directories WHERE drp = %(drp)s;"

    with conn.cursor() as cursor:
        try:
            cursor.executemany(g, gates)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"err encountered when trying to delete directory[s] from server: {c}.", exc_info=True)
            raise
        else:
            logger.debug('removed remote-only directory[s] from server w.o exception')


def rm_remfile(conn, cherubs): # only give 3.0
    """Remove remote-only files from the server. Paths [cherubs] passed as a list of dictionaries for executemany()."""
    logger.debug('...deleting remote-only file[s] from server...')
    f = "DELETE FROM notes WHERE frp = %(frp)s;"

    with conn.cursor() as cursor:
        try:
            cursor.executemany(f, cherubs)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"err encountered when trying to delete file[s] from server: {c}", exc_info=True)
            raise
        else:
            logger.debug('removed remote-only file[s] from server w.o exception')

# uploads

def collect_info(dicts_, abs_path): # give - a
    """For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
    This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
    be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany().
    For batched uploads, the script reads the files to get their size so it can optimize queries-per-execution within the 
    limitation for packet sizes. Pretty inneficient because reading every file just for its size when we already have the content 
    in memory is a waste.
    """
    logger.debug('...collecting info on file[s] sizes to upload...')
    # cmpr = zstd.ZstdCompressor(level=3)
    curr_batch = 0

    batch_items = []
    all_batches = []
    
    for i in dicts_:
        size = 0
        item = ( abs_path / i ).resolve()

        size = os.path.getsize(item) 

        if size > MAX_ALLOWED_PACKET:
            logger.error(f"a single file is larger than the maximum packet size allowed: {item}")
            raise

        elif (curr_batch + size) > MAX_ALLOWED_PACKET:
            logger.debug("collected one batch's items")
            all_batches.append((batch_items,))

            batch_items = [i]
            curr_batch = size
        else:
            batch_items.append(i)
            curr_batch += size
    
    if batch_items:
        all_batches.append((batch_items,))
    
    logger.debug('all batches collected')
    return all_batches


def collect_info2(dicts_, abs_path):
    """For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
    This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
    be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany().
    For batched uploads, the script reads the files to get their size so it can optimize queries-per-execution within the 
    limitation for packet sizes. Pretty inneficient because reading every file just for its size when we already have the content 
    in memory is a waste.
    """
    logger.debug('...collecting info on file[s] sizes for batching...')
    # cmpr = zstd.ZstdCompressor(level=3)
    # hasher = hashlib.sha256()
    hasher = xxhash.xxh64()

    curr_batch = 0
    item_data = []
    
    for i in dicts_:
        hasher.reset()
        size = 0
        item = ( abs_path / i ).resolve()

        size = os.path.getsize(item)

        if size > MAX_ALLOWED_PACKET:
            logger.error(f"a single file is larger than the maximum packet size allowed: {item}")
            raise

        elif (curr_batch + size) > MAX_ALLOWED_PACKET:
            logger.debug('...yielding one batch of data...')
            yield item_data

            item_data = []
            curr_batch = 0

        content = item.read_bytes()

        hasher.update(content)
        hash_id = hasher.digest()

        # ccontent = cmpr.compress(content)

        item_data.append({
            'content': content,
            'hash_id': hash_id,
            'frp': i
        },)
        curr_batch += size

    if item_data:
        logger.debug('...yielding last batch of data')
        yield item_data


def collect_data(dicts_, abs_path, conn): # give - redundant
    """For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
    This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
    be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany(). Works with the 
    output of the collect_info function in terms of data type & format.
    """
    logger.debug('...collecting data on file[s] to upload...')
    item_data = []

    # cmpr = zstd.ZstdCompressor(level=3)
    # hasher = hashlib.sha256()
    hasher = xxhash.xxh64()

    for x in dicts_: # for tuple in list of tuples generated by collect_info
        for i in x: # for relative path in the list of relative paths from the tuple
            item = ( abs_path / i ).resolve()
            hasher.reset()

            content = item.read_bytes()

            # c_content = cmpr.compress(content)

            hasher.update(content)
            hash_id = hasher.digest() # why would I ever want to digest this hash into a hexidecimal str of 2x the original length?

            item_data.append((content, hash_id, i))

    logger.debug("collected one batch of data's information")
    return item_data


def upload_dirs(conn, caves): # give
    """Insert into the directories table any local-only directories found."""
    logger.debug('...uploading local-only directory[s] to server...')
    h = "INSERT INTO directories (drp) VALUES (%(drp)s);"
    with conn.cursor() as cursor:
        try:
            cursor.executemany(h, caves)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"err encountered while attempting to upload [new] directory[s] to server: {c}", exc_info=True)
            raise
        else:
            logger.debug('local-only directory[s] written to server w.o exception')


def upload_created(conn, serpent_data): # give
    """Insert into the notes table the new record for local-only files that do not exist in the server. *This function triggers no actions in the database*."""
    logger.debug('...writing new file[s] to server...')
    i = "INSERT INTO notes (content, hash_id, frp) VALUES (%s, %s, %s);"

    try:
        with conn.cursor(prepared=True) as cursor:
            cursor.executemany(i, serpent_data)

    except (mysql.connector.Error, ConnectionError, Exception) as c:
        logger.error(f"err encountered while attempting to upload [new] file[s] to server: {c}", exc_info=True)
        raise
    else:
        logger.debug('wrote new file[s] to server w.o exception')


def upload_created2(conn, serpent_data): # give
    """Insert into the notes table the new record for local-only files that do not exist in the server. *This function triggers no actions in the database*."""
    logger.debug('...writing new file[s] to server...')
    i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, %(content)s, %(hash_id)s);"

    try:
        with conn.cursor() as cursor:
            for data in serpent_data:
                cursor.executemany(i, serpent_data)

    except (mysql.connector.Error, ConnectionError, Exception) as c:
        logger.error(f"err encountered while attempting to upload [new] file[s] to server: {c}", exc_info=True)
        raise
    else:
        logger.debug('wrote new file[s] to server w.o exception')


def upload_edited(conn, soul_data): # only give 3.0
    """Update the notes table to show the current content for a note that was altered, or whose hash did not show identical contents. *This function 
    triggers the on_update_notes trigger which will record the previous version of the file's contents and the time of changing.
    """
    logger.debug('...writing altered file[s] to server...')
    j = "UPDATE notes SET content = %(content)s, hash_id = %(hash_id)s WHERE frp = %(frp)s;"

    try:
        with conn.cursor(prepared=True) as cursor:
            cursor.executemany(j, soul_data)

    except (mysql.connector.Error, ConnectionError, Exception) as c:
        logger.error(f"err encountered while attempting to upload altered file to server: {c}", exc_info=True)
        raise
    else:
        logger.debug("wrote altered file[s]'s contents & new hashes to server w,o exception")


def upload_edited2(conn, soul_data): # only give 3.0
    """Update the notes table to show the current content for a note that was altered, or whose hash did not show identical contents. *This function 
    triggers the on_update_notes trigger which will record the previous version of the file's contents and the time of changing.
    """
    logger.debug('...writing altered file[s] to server...')
    j = "UPDATE notes SET content = %(content)s, hash_id = %(hash_id)s WHERE frp = %(frp)s;"

    with conn.cursor() as cursor:
        for data in soul_data:
            try:
                cursor.executemany(j, soul_data)

            except (mysql.connector.Error, ConnectionError, Exception) as c:
                logger.error(f"err encountered while attempting to upload altered file to server: {c}", exc_info=True)
                raise
            else:
                logger.debug('uploaded altered file[s]\'s content & new hash')
        else:
            logger.debug("wrote altered file[s]'s contents & new hashes to server w,o exception")


# USER INPUT & HANDLING

def confirm(conn): # give
    """Double checks that user wants to commit any changes made to the server. Asks for y/n response and rolls-back on any error or [n] no."""
    confirm = input("commit changes to server? y/n: ").lower()
    if confirm in ('y', ' y', 'y ', ' y ', 'yes', 'yeah', 'i guess', 'i suppose'):
        try:
            conn.commit()

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"err encountered while attempting to commit changes to server: {c}", exc_info=True)
            raise
        else:
            logger.info('commited changes to server')

    elif confirm in ('n', ' n', 'n ', ' n ', 'no', 'nope', 'hell no', 'naw'):
        try:
            conn.rollback()

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"err encountered while attempting to rollback changes to server: {c}", exc_info=True)
            raise
        else:
            logger.info('changes rolled back')
    else:
        logger.error('unknown response; rolling server back')
        raise