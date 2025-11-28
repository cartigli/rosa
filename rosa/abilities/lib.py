import os
import sys
import time
import shutil
import logging
import tempfile
import datetime
# import hashlib
import contextlib
from pathlib import Path
from itertools import batched

# these three are the only external packages required
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import xxhash # this one is optional and can be replaced with hashlib which is more secure & in the native python library
import mysql.connector # to connect with the mysql server - helps prevent injection while building queries as well
import zstandard as zstd # compressor for files before uploading and decompressing after download

from rosa.abilities.queries import ASSESS2
from rosa.abilities.config import MAX_ALLOWED_PACKET # why am I not importing variables from the config directly into here? No point in having a middle-man - or is it better for tracing errors? Ig not seeing what is being passed could be sketch, but for the conn it does not change.


"""
Library of functions used in the management scripts. Many functions overlap and share uses, so moving them here is much easier for 
maintainability. There is also queries for longer MySQL queries. Some scripts, like get_moment, hold their own unique functions.
"""

# logging

logger = logging.getLogger(__name__)

# connection & management thereof

@contextlib.contextmanager
def phone_duty(db_user, db_pswd, db_name, db_addr):
    """Context manager for the mysql.connector connection object."""
    conn = None

    # try:
        # from mysql.connector import CMySQLConnection
        # import _mysql_connector

    # except ModuleNotFoundError as me:
    #     logger.critical(f"C Conn module not found: {me}.", exc_info=True)
    #     raise
    # else:
    try:
        conn = init_conn(db_user, db_pswd, db_name, db_addr)
        yield conn

    # except (ConnectionError, ConnectionRefusedError, TimeoutError, mysql.connector.Error, mysql.connector.errors.InternalError, Exception) as e:
    except (ConnectionError, ConnectionRefusedError, TimeoutError, Exception) as e:
        logger.error(f"Error encountered while connecting to the server: {e}.", exc_info=True)
        if conn:
            _safety(conn)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
        # try:
        # 	conn.ping(reconnect=True, attempts=3, delay=0.5)
        # except Exception as e:
        # 	logger.warning(f"Connection obj could not be re-established after error:{e}.", exc_info=True)
        # else:
        # 	yield conn
        # finally:
        # 	conn.close()
        # try:
        # 	_safety(conn)
        # finally:
        # 	raise

    # finally:
    # # 	if conn and conn.is_connected():
    # 	conn.close()
    # 		logger.info('Connection closed.')

        # except Exception as mce:
        # 	logger.warning(f"Error encountered while closing connection: {mce}.", exc_info=True)
            # try:
            # 	_safety(conn)
            # finally:
            # 	raise


def _safety(conn):
    """Handles rollback of the server on err from phone_duty."""
    try:
        # if conn and conn.is_connected():
        if conn:
            conn.rollback()
            logger.info('Server rolled back.')
        # else:
        #     try:
        #         conn.ping(reconnect=True, attempts=3, delay=1)
        #     except:
        #         logger.warning('Connection object lost and unable to reconnect to.', exc_info=True)
        #     else:
        #         conn.rollback()
        #         logger.warning('Server rolled back.')

    except (mysql.connector.Error, ConnectionRefusedError, ConnectionError, TimeoutError, Exception) as e:
        logger.critical(f"Error encountered while attempting rollback on connection error: {e}.", exc_info=True)
        raise
    else:
        logger.info('_safety completed without exception.')
    finally:
        if conn:
            conn.close()


def init_conn(db_user, db_pswd, db_name, db_addr): # used by all scripts
    """Initiate the connection to the server. If an error occurs, freak out."""
    try:
        config = {
            'unix_socket': '/tmp/mysql.sock',
            # 'host': db_addr,
            'user': db_user,
            'password': db_pswd,
            'database': db_name,
            'autocommit': False,
            # 'use_pure': False # 5.886, 5.902, 5.903 | p2 | 3.266, 
            'use_pure': True # 5.122, 5.117, 5.113 | p2 | 
            # 'use_unicode': False # 3.213 (after use_pure: 3.266)
            # 'pool_size': 5
            # 'raw': True
        }

        try:
            conn = mysql.connector.connect(**config)
            # conn = CMySQLConnection(**config)
            print(f"Connection object forced to use c backend: {type(conn)}.")

        except ImportError as ie:
            logger.warning(f"Error establishing C Extension-connection: {ie}.", exc_info=True)
            try:
                config['use_pure']=True # switch back to pure_python
                conn = mysql.connector.connect(**config)
    
            except (ConnectionRefusedError, ConnectionError) as ce:
                logger.error(f"Error establishing pure python connection: {ce}.", exc_info=True)
                raise
            else:
                logger.warning('Fellback to pure_python connection & established connection.')
                return conn
        else:
            logger.info('C Extension connection established w/o exception.')
            return conn

    except (ConnectionRefusedError, TimeoutError) as e:
        logger.error(f"Error encountered while attempting to establish connection: {e}", exc_info=True)
        raise

# collecting local info for comparison

def scope_loc(local_dir): # all
    """Collect the relative path and 256-bit hash for every file in the given directory. Ignore any files with paths that contain
    the '.DS_Store', '.git', or '.obsidian'. Record the hash in BINARY format. Hex for python ( and UNHEX for the sql queries ). 
    Every file that meets the criteria has its relative path and hash generated and recorded. Returns the pairs of paths/hashes 
    in a tuple to match data returned from the server.
    """
    blk_list = ['.DS_Store', '.git', '.obsidian'] 
    abs_path = Path(local_dir).resolve()
    hasher = xxhash.xxh64()

    cnt = 0
    tsz = 0
    raw_hell = []
    hell_dirs = []

    if abs_path.exists():
        for item in abs_path.rglob('*'):
            path_str = item.resolve().as_posix()
            if any(blocked in path_str for blocked in blk_list):
                continue # skip item if blkd item in path
            else:
                if item.is_file():
                    frp = item.relative_to(abs_path).as_posix()
                    # hasher = hashlib.sha256() # 512 technically would b faster - slower in the db though
                    # hasher = xxhash.xxh64()

                    with open(item, 'rb') as f:
                        while chunk := f.read(1024):
                            hasher.update(chunk) # stream

                    hash_id = hasher.digest() # - BINARY(32)

                    raw_hell.append((frp, hash_id)) # hash_id.hex()
                    # logger.info(f"Recorded path & hash for file: {item.name}.")

                    cnt += 1
                    tsz += os.path.getsize(item)

                elif item.is_dir():
                    drp = item.relative_to(abs_path).as_posix()

                    hell_dirs.append((drp,)) # keep the empty list of dirs
                    # logger.info(f"Recorded path for directory: {item.name}.")
                else:
                    continue
    else:
        logger.info('Local directory does not exist.')
        return raw_hell, hell_dirs, abs_path
    
    avg_sz = tsz / cnt

    logger.info('Collected local paths and hashes.')
    return raw_hell, hell_dirs, abs_path, avg_sz

# collecting server data for comparison

def scope_rem(conn): # all
    """Select and return every single relative path and hash from the notes table. Returned as a list of tuples (rel_path, hash_id)."""
    q = f"SELECT frp, hash_id FROM notes;"

    with conn.cursor() as cursor:
        try:
            cursor.execute(q)

        except ConnectionError as c:
            logger.error(f"Connection Error encountered while attempting to collect file data from server: {c}.", exc_info=True)
            sys.exit(1)
        else:
            raw_heaven = cursor.fetchall()
            logger.info('Collected remote paths & hashes.')
            
            return raw_heaven


def ping_cass(conn): # all
    """Ping the kid Cass because you just need a quick hand with the directories. If a directory is empty or contais only subdirectories and no files,
    Cass is the kid to clutch it. He returns a list of directories as tuples containing their relative paths.
    """
    q = "SELECT * FROM directories;" # drp's

    with conn.cursor() as cursor:
        try:
            cursor.execute(q)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Exception encountered while attempting to collect directory data from server: {c}.", exc_info=True)
            raise
        else:
            heaven_dirs = cursor.fetchall()
            logger.info('Collected directories from server.')

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

    people = heaven_souls & hell_souls # those in both - unaltered
    souls = []
    stags = []

    heaven = {lo: id for lo, id in raw_heaven if lo in people}
    hell = {lo: id for lo, id in raw_hell if lo in people}

    for key in hell:
        if hell[key] != heaven[key]:
            souls.append({'frp': key}) # get - souls as a dict: 'frp'
        else:
            stags.append(key)

    logger.info('Contrasted collections and id\'d discrepancies.')
    return cherubs, serpents, stags, souls # files in server but not present, files present not in server, files in both, files in both but with hash discrepancies


def compare(heaven_dirs, hell_dirs): # all
    """Makes a set of each list of directories and formats them each into a dictionary. It compares the differences and returns a list of remote-only and local-only directories."""
    heaven = set(heaven_dirs)
    hell = set(hell_dirs)

    gates = [{'drp':gate[0]} for gate in heaven - hell]
    caves = [{'drp':cave[0]} for cave in hell - heaven]

    logger.info('Compared directories & id\'d discrepancies.')
    return gates, caves # dirs in heaven not found locally, dirs found locally not in heaven

# [atomically] edit local data; rosa_get mostly

@contextlib.contextmanager
def fat_boy(abs_path):
    """Context manager for temporary directory and backup."""
    tmp_ = None
    backup = None

    try:
        tmp_, backup = configure(abs_path)
        yield tmp_, backup # return these & freeze in place

    # except ConnectionError | TimeoutError | mysql.connector.Error as e:
    except (ConnectionError, TimeoutError) as e:
        logger.critical(f"Error encountered while attempting atomic wr: {e}.", exc_info=True)
        try:
            _lil_guy(abs_path, backup, tmp_)
        finally:
            raise
    else:
        try:
            apply_atomicy(tmp_, abs_path, backup)
            logger.info('Atomic write complete.')

        # except mysql.connector.Error | ConnectionError as c:
        except (mysql.connector.Error, ConnectionError) as c:
            logger.critical(f"Error encountered while attempting to apply atomicy: {c}.", exc_info=True)
            try:
                _lil_guy(abs_path, backup, tmp_)
            finally:
                raise


def _lil_guy(abs_path, backup, tmp_):
    """Handles recovery on error for the context manager fat_boy."""
    try:
        if backup and backup.exists():
            if abs_path.exists():
                shutil.rmtree(tmp_)
                logger.info('Removed damaged attempt.')
            backup.rename(abs_path)
            logger.info('Moved backup back to original location.')
        if tmp_ and tmp_.exists():
            shutil.rmtree(tmp_)
            logger.info('Removed damaged temporary directory.')

    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.critical(f"Replacement of {abs_path} and cleanup encountered an error: {e}.", exc_info=True)
        raise
    else:
        logger.info('Cleanup complete.')


def calc_batch(conn):
    """Get the average row size of the notes table to estimate optimal batch size for downloading."""
    batch_size = 5
    row_size = 0

    with conn.cursor() as cursor:
        try:
            beg = time.perf_counter()

            cursor.execute(ASSESS2)
            row_size = cursor.fetchone()

            end = time.perf_counter()
            logging.info(f"ASSESS2 took {(end - beg):.4f} seconds to return a value: {row_size}.")

        except (ConnectionError, Exception) as c:
            logger.error('Exception encountered while attempting to find avg_row_size.', exc_info=True)
            raise
        else:
            if row_size:
                if row_size[0]:
                    batch = int( (0.92*MAX_ALLOWED_PACKET) / row_size[0] )
                    batch_size = max(1, batch)
                    logger.info(f"Batch size determined: {batch_size}.")
                    logger.info(f"Check against row: {row_size[0]}.")
                    logger.info(f"Check against max_pack: {row_size[0]*batch_size}.")
                    logger.info(f"Check compared to max_packet: {0.92*MAX_ALLOWED_PACKET}.")
                    return batch_size, row_size
                else:
                    logger.info('Using default batch size of 5.')
                    return batch_size, row_size
            else:
                logger.info('Using default batch size of 5.')
                return batch_size, row_size


def configure(abs_path):
    """Configure the temporary directory & move the original to a backup location. 
    Returns the _tmp directory's path.
    """
    try:
        if abs_path.exists():
            tmp_ = Path(tempfile.mkdtemp(dir=abs_path.parent))
            backup = Path( (abs_path.parent) / f"Backup_{datetime.datetime.now(datetime.UTC).timestamp()}" )

            abs_path.rename(backup)
            logger.info('Local directory renamed to backup.')
        else:
            abs_path.mkdir(parents=True, exist_ok=True)

            tmp_ = Path(tempfile.mkdtemp(dir=abs_path.parent))
            backup = Path( (abs_path.parent) / f"Backup_{datetime.datetime.now(datetime.UTC).timestamp()}" )

            abs_path.rename(backup) # return empty dir for consistency
            logger.info('Local directory not found so placeholder made for consistency.')
    
    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.critical(f"Exception encountered while trying move {abs_path} to a backup location: {e}.", exc_info=True)
        raise
    else:
        logger.info('Temporary directory created & original directory moved to backup without exception.')
        return tmp_, backup


def save_people(people, backup, tmp_):
    """Hard-links unchanged files present in the server and locally from the backup directory (original) 
    to the _tmp directory. Huge advantage over copying because the file doesn't need to move."""
    for person in people:
        curr = Path( backup / person )
        tmpd = Path( tmp_ / person )
        try:
            # (tmpd.parent).mkdir(parents=True, exist_ok=True)
            os.link(curr, tmpd)

        except FileNotFoundError as fne:
            logger.error('File Not Found error encountered while attempting to hard link unchanged files', exc_info=True)
            try:
                (tmpd.parent).mkdir(parents=True, exist_ok=True)
                os.link(curr, tmpd)

            except FileNotFoundError | Exception as e:
                logger.critical(f"Exception encountered while attempting handling of primary error: {e},", exc_info=True)
                raise
            else:
                logger.info('Error handled and parent directory created in tmp_ direcotry.')

        except PermissionError | Exception as te:
            logger.critical(f"Exception occured outside of handle-able scope: {te}.", exc_info=True)
            raise
        else:
            logger.info('Linked unchanged file without exception.')


def download_batches(flist, conn, batch_size, tmp_): # get
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    paths = [item['frp'] for item in flist]
    params = ', '.join(['%s']*len(paths))

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
                    logging.info(f"Collected batch in {dur:.4f} seconds.")
                    # logger.info('Got one batch of data.')

                except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
                    logger.critical(f"Error while trying to download data: {c}.", exc_info=True)
                    raise
                else:
                    if batch:
                        stt = time.perf_counter()
                        wr_batches(batch, tmp_)
                        stp = time.perf_counter()

                        timez = stp - stt
                        logger.info(f"Wrote batch in {timez:.4f} seconds.")

                    if len(batch) < batch_size:
                        break

                    offset += batch_size

        except: # tout de monde
            logger.critical('Error while attempting batched atomic write.', exc_info=True)
            raise
        else:
            logger.info('Atomic wr w batched download complete.')


def download_batches2(flist, conn, batch_size, tmp_): # get
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    paths = [item[0] for item in flist]
    params = ', '.join(['%s']* len(paths))

    batch_size = batch_size
    offset = 0

    with conn.cursor() as cursor:
        try:
            while True:
                query = f"SELECT frp, content FROM notes WHERE frp IN ({params}) LIMIT {batch_size} OFFSET {offset};"

                try:
                    cursor.execute(query, paths)
                    batch = cursor.fetchall()
                    # logger.info('Got one batch of data.')

                except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
                    logger.critical(f"Error while trying to download data: {c}.", exc_info=True)
                    raise
                else:
                    if batch:
                        # start = time.perf_counter()
                        wr_batches(batch, tmp_)
                        # stop = time.perf_counter()

                        # score = stop - start
                        # logger.info(f"Wrote batch in {score:.4f} seconds.")
                        # logger.info('Wrote batch to disk.')

                    if len(batch) < batch_size:
                        break

                    offset += batch_size

        except: # tout de monde
            logger.critical('Error while attempting batched atomic write.', exc_info=True)
            raise
        else:
            logger.info('Atomic wr w batched download complete.')


def download_batches4(flist, conn, batch_size, tmp_): # get
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    # paths = [item['frp'] for item in flist]
    params = ', '.join(['%(frp)s']*batch_size)

    # query = "SELECT frp, content FROM notes WHERE frp IN %(frp)s;"
    query = f"SELECT frp, content FROM notes WHERE frp IN ({params});"
    # batches = list(batched(paths, batch_size))
    batches = list(batched(flist, batch_size))

    with conn.cursor() as cursor:
        try:
            for b in batches:
                try:
                    beg = time.perf_counter()
                    # for frp in b:
                    # 	cursor.execute(query, (frp,))
                    # 	cbatch.append(cursor.fetchone())
                    cursor.executemany(query, (b,))
                    cbatch = cursor.fetchall()

                    if cbatch:
                        end = time.perf_counter()
                        logging.info(f"Got batch in {(end - beg):.4f} seconds.")

                except (mysql.connector.Error, ConnectionError, KeyboardInterrupt) as c:
                    logger.critical(f"Error while trying to download data: {c}.", exc_info=True)
                    raise
                else:
                    if cbatch:
                        # stt = time.perf_counter()
                        wr_batches(cbatch, tmp_)
                        stop = time.perf_counter()

                        timez = stop - end
                        logger.info(f"Wrote batch in {timez:.4f} seconds.")

                    # if len(batch) < batch_size:
                    # 	break
                    # offset += batch_size

        except: # tout de monde
            logger.critical('Error while attempting batched atomic write.', exc_info=True)
            raise
        else:
            logger.info('Atomic wr w batched download complete.')



def download_batches5(souls, conn, batch_size, row_size, tmp_): # get_all ( aggressive )
    """Executes the queries to find the content for the notes that do not exist locally, or whose contents do not exist locally. Takes the list of 
    dictionaries from contrast and makes them into queries for the given file[s]. *Executemany() cannot be used with SELECT; it is for DML quries only.
    This function passes the found data to the wr_data function, which writes the new data structure to the disk.
    """
    # query = "SELECT frp, content FROM notes WHERE frp = %s;"
    batch_count = int(len(souls) / batch_size)
    if len(souls) % batch_size:
        batch_count += 1

    kbb = False
    # curr_count = 0
    batched_list = list(batched(souls, batch_size))

    # batch_bytes = batch_size * row_size[0]
    # batch_kbytes = (batch_size * row_size[0]) / 1024
    batch_mbytes = (batch_size * row_size[0]) / (1024*1024)

    # bar = "{l_bar}{bar}| {n:.3f}/{total:.3f} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
    bar = "{l_bar}{bar}| {n:.0f}/{total:.0f} [{rate_fmt}{postfix}]"

    try:
        with logging_redirect_tqdm(loggers=[logger]):
            with tqdm(batched_list,
            desc=f"Pulling {batch_count} batches", unit=" batches", unit_scale=True, 
            unit_divisor=1024, colour="white", bar_format = bar) as pbar:
                for bunch in pbar:
                    # query = "SELECT frp, content FROM notes WHERE frp = %s;"
                    # curr_count += 1
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
                            # for item in bunch:
                            #     cursor.execute(query, (item,))
                            #     note = cursor.fetchone()
                            # #     cpr += (len(note[0]) + len(note[1]))
                            #     batch.append(note)
                            
                            if batch:
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
                            tqdm.write(f"{"\033[91m"}Boss killed it; deleting partial download.")
                            pbar.close()
                            kbb = True
                            break
                        except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
                            logger.error(f"Error while trying to downwrite data: {c}.", exc_info=True)
                            try:
                                cursor.fetchall()
                                cursor.close()
                            except:
                                pass
                            tqdm.write(f"{"\033[91m"}Error caught while downloading; removing tmp_ directory.")
                            pbar.close()
                            kbb = True
                            break

    except KeyboardInterrupt as c:
        kbb = True

    if kbb:
        try:
            shutil.rmtree(tmp_)
        except (FileNotFoundError, Exception) as fe:
            logger.warning(f"Err while removing dat on err-handling: {e}.")
        else:
            logger.warning('Removed tmp_ directory.')
            sys.exit(1)


def wr_batches(data, tmp_):
    """Writes each batch to the _tmp directory as they are pulled. Each file has it and its parent directory flushed from memory for assurance of atomicy."""
    dcmpr = zstd.ZstdDecompressor() # init outside of loop; duh
    b_size = 0

    for frp, content in data:
        try:
            t_path = Path ( tmp_ / frp ) #.resolve()
            (t_path.parent).mkdir(parents=True, exist_ok=True)

            d_content = dcmpr.decompress(content)

            with open(t_path, 'wb') as t:
                t.write(d_content)

            # b_size += len(d_content)
        except KeyboardInterrupt as ki:
            raise
        except (PermissionError, FileNotFoundError) as e:
            logger.critical(f"Exception encountered while attempting atomic wr: {e}.", exc_info=True)
            raise
    # return b_size


def apply_atomicy(tmp_, abs_path, backup):
    """If the download and write batches functions both complete entirely without error, this function moved the _tmp directory back to the original abs_path. 
    If this completes without error, the backup is deleted.
    """
    try:
        tmp_.rename(abs_path)

    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.critical(f"Exception encountered while attempting atomic write: {e}.", exc_info=True)
        raise
    else:
        logger.info('Temporary directory renamed without exception.')
        if backup.exists():
            shutil.rmtree(backup)
            logger.info('Removed backup after execution without exception.')


def mk_dir(gates, abs_path):
    """Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
    try:
        for gate in gates:
            path = gate['drp']
            fdpath = (abs_path / path ).resolve()
            fdpath.mkdir(parents=True, exist_ok=True)

    except (PermissionError, FileNotFoundError, Exception) as e:
        logger.error(F"Permission Error when tried to make directories: {e}.", exc_info=True)
        raise
    else:
        logger.info('Created directory structure on disk without exception.')
        # with logging_redirect_tqdm(loggers=[logger]):
        # 		with tqdm(batched_list, desc=f"Pulling {batch_count} batches", unit="batch", colour="white") as pbar:

def mk_rdir(gates, abs_path):
    """Takes the list of remote-only directories as dicts from contrast & writes them on the disk."""
    logger.info(f"{len(gates)} found; writing to disk.")
    try:
        with logging_redirect_tqdm(loggers=[logger]):
            with tqdm(gates, desc=f"Writing {len(gates)} directories", unit="dirs") as pbar:
                for gate in pbar:
                    fdpath = (abs_path / gate ).resolve()
                    fdpath.mkdir(parents=True, exist_ok=True)

    except (PermissionError, FileNotFoundError, Exception) as e:
        pbar.leave = False
        pbar.close()
        logger.error(F"Error when tried to make directories: {e}.", exc_info=True)
        sys.exit(1)
        # raise
    else:
        logger.info('Created directory structure on disk without exception.')

# EDIT SERVER - rosaGIVE

# deletes

def rm_remdir(conn, gates): # only give 3.0
    """Remove remote-only directories from the server. Paths [gates] passed as list of dictionaries for executemany(). This, and every other call to 
    make an edit on the database, is rolled back on errors. Its only for specific calls to change the information in them. The default with mysql - conn
    is to roll back on disconnect, so its an ok safety net, but its supposed to be all over this shit.
    """
    g = "DELETE FROM directories WHERE drp = %(drp)s;"

    with conn.cursor() as cursor:
        try:
            cursor.executemany(g, gates)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered when trying to delete directory[s] from server: {c}.", exc_info=True)
            raise
        else:
            logger.info('Removed remote-only directory[s] from server.')


def rm_remfile(conn, cherubs): # only give 3.0
    """Remove remote-only files from the server. Paths [cherubs] passed as a list of dictionaries for executemany()."""
    f = "DELETE FROM notes WHERE frp = %(frp)s;"

    with conn.cursor() as cursor:
        try:
            cursor.executemany(f, cherubs)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered when trying to delete file[s] from server: {c}.", exc_info=True)
            raise
        else:
            logger.info('Removed remote-only file[s] from server.')

# uploads

def collect_info(dicts_, abs_path): # give - a
    """For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
    This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
    be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany().
    For batched uploads, the script reads the files to get their size so it can optimize queries-per-execution within the 
    limitation for packet sizes. Pretty inneficient because reading every file just for its size when we already have the content 
    in memory is a waste.
    """
    # cmpr = zstd.ZstdCompressor(level=3)
    curr_batch = 0
    # total_size = 0
    # items = []
    batch_items = []
    all_batches = []
    
    for i in dicts_:
        size = 0
        item = ( abs_path / i ).resolve()

        # if item.is_file():
        size = os.path.getsize(item) 

        if size > MAX_ALLOWED_PACKET:
            logger.error(f"A single file is larger than the maximum packet size allowed: {item}.")
            raise

        elif (curr_batch + size) > MAX_ALLOWED_PACKET:
            all_batches.append((batch_items,))
            # collect_data(batch_items,)

            batch_items = [i]
            curr_batch = size

        else:
            batch_items.append(i)
            curr_batch += size
    
    if batch_items:
        all_batches.append((batch_items,))

    return all_batches #, total_size


def collect_info2(dicts_, abs_path): # give - a
    """For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
    This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
    be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany().
    For batched uploads, the script reads the files to get their size so it can optimize queries-per-execution within the 
    limitation for packet sizes. Pretty inneficient because reading every file just for its size when we already have the content 
    in memory is a waste.
    """
    # cmpr = zstd.ZstdCompressor(level=3)
    hasher = xxhash.xxh64()

    curr_batch = 0
    item_data = []
    
    for i in dicts_:
        hasher.reset()
        size = 0
        # item = ( abs_path / frp ).resolve()
        item = ( abs_path / i ).resolve()

        size = os.path.getsize(item) # x 1.1
        # content = item.read_bytes()
        # size = len(content)

        if size > MAX_ALLOWED_PACKET:
            logger.error(f"A single file is larger than the maximum packet size allowed: {item}.")
            raise

        elif (curr_batch + size) > MAX_ALLOWED_PACKET:
            # upload_created(conn, item_data)
            yield item_data

            item_data = []
            curr_batch = 0

        # content = item.read_bytes()

        hasher.update(content)
        hash_id = hasher.digest()

        # ccontent = cmpr.compress(content)

        item_data.append({
            'content': content,
            'hash_id': hash_id,
            'frp': i
        },)
        curr_batch += size
        # else:
        #     content = item.read_bytes()

        #     hasher.update(content)
        #     hash_id = hasher.digest()

        #     item_data.append({
        #         'content': content,
        #         'hash_id': hash_id,
        #         'frp': i
        #     })
        #     curr_batch += size
        #     continue
    if item_data:
        yield item_data
    
    # if batch_items:
    #     all_batches.append((bach_items,))

    # return all_batches #, total_size


def collect_data(dicts_, abs_path, conn): # give - redundant
    """For whatever lists of paths as dictionaries are passed to this fx, the output is given file's content, hash, and relative path.
    This is pased to the upload functions as required. Both functions use the same three variables for every file, so they can all
    be built with this function. Order is irrelevant for the dictionaries & %(variable)s method with executemany(). Works with the 
    output of the collect_info function in terms of data type & format.
    """
    import binascii
    # c_size = 0
    item_data = []

    # cmpr = zstd.ZstdCompressor(level=3)
    hasher = xxhash.xxh64()

    for x in dicts_: # for tuple in list of tuples generated by collect_info
        for i in x: # for relative path in the list of relative paths from the tuple
            item = ( abs_path / i ).resolve()
            # hasher = xxhash.xxh64()
            hasher.reset()

            content = item.read_bytes()

            # c_content = cmpr.compress(content)

            hasher.update(content)
            hash_id = hasher.digest() # why would I ever want to digest this hash into a hexidecimal str of 2x the original length?

            # if hash_id.startswith('0x'):
            #     hash_id = hash_id[2:]
            # if hash_id % 2 != 0:
            #     hash_id = hash_id + '0'

            # hash_idd = binascii.unhexlify(hash_id)
            # frp = i.encode('utf-8')

            item_data.append({
                'content': content,
                'hash_id': hash_id,
                'frp': i
            })
            # item_data.append((content, hash_id, frp))

    return item_data #, c_size


def upload_dirs(conn, caves): # give
    """Insert into the directories table any local-only directories found."""
    h = "INSERT INTO directories (drp) VALUES (%(drp)s);"
    with conn.cursor() as cursor:
        try:
            cursor.executemany(h, caves)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered while attempting to upload [new] directory[s] to server: {c}.", exc_info=True)
            raise
        else:
            logger.info('Uploaded local-only directory[s] to server.')


def upload_created(conn, serpent_data): # give
    """Insert into the notes table the new record for local-only files that do not exist in the server. *This function triggers no actions in the database*."""
    # i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, UNHEX(%(content)s), UNHEX(%(hash_id)s));" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));"
    # i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, %(content)s, %(hash_id)s);" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));" # don't need to hex/unhex for binary regardless
    # i = "INSERT INTO notes (frp, content, hash_id) VALUES (%s, %s, %s);" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));" # don't need to hex/unhex for binary regardless
    i = "INSERT INTO notes (content, hash_id, frp) VALUES (%s, %s, %s);" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));" # don't need to hex/unhex for binary regardless

    with conn.cursor(prepared=True) as cursor:
        try:
            # beg = time.perf_counter()
            cursor.executemany(i, serpent_data)
            # end = time.perf_counter()
            # logging.info(f"Excecutemany completed in: {(end - beg):.4f} seconds.")

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered while attempting to upload [new] file[s] to server: {c}.", exc_info=True)
            raise
        # else:
        #     logger.info('Uploaded local-only file[s] to server.')


def upload_created2(conn, serpent_data): # give
    """Insert into the notes table the new record for local-only files that do not exist in the server. *This function triggers no actions in the database*."""
    # i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, UNHEX(%(content)s), UNHEX(%(hash_id)s));" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));"
    i = "INSERT INTO notes (frp, content, hash_id) VALUES (%(frp)s, %(content)s, %(hash_id)s);" #, (SELECT id FROM directories WHERE dpath = %(dpath)s));" # don't need to hex/unhex for binary regardless

    with conn.cursor() as cursor:
        for data in serpent_data:
            try:
                cursor.executemany(i, serpent_data)

            except (mysql.connector.Error, ConnectionError, Exception) as c:
                logger.error(f"Connection Error encountered while attempting to upload [new] file[s] to server: {c}.", exc_info=True)
                raise
            else:
                logger.info('Uploaded local-only file[s] to server.')


def upload_edited(conn, soul_data): # only give 3.0
    """Update the notes table to show the current content for a note that was altered, or whose hash did not show identical contents. *This function 
    triggers the on_update_notes trigger which will record the previous version of the file's contents and the time of changing.
    """
    # j = "UPDATE notes SET content = UNHEX(%(content)s), hash_id = UNHEX(%(hash_id)s) WHERE frp = %(frp)s;"
    j = "UPDATE notes SET content = %(content)s, hash_id = %(hash_id)s WHERE frp = %(frp)s;"

    with conn.cursor(prepared=True) as cursor:
        try:
            cursor.executemany(j, soul_data)
            # for data in soul_data:
            #     cursor.execute(j, data)

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered while attempting to upload altered file to server: {c}.", exc_info=True)
            raise
        else:
            logger.info('Uploaded altered file[s]\'s content & new hash.')


def upload_edited2(conn, soul_data): # only give 3.0
    """Update the notes table to show the current content for a note that was altered, or whose hash did not show identical contents. *This function 
    triggers the on_update_notes trigger which will record the previous version of the file's contents and the time of changing.
    """
    # j = "UPDATE notes SET content = UNHEX(%(content)s), hash_id = UNHEX(%(hash_id)s) WHERE frp = %(frp)s;"
    j = "UPDATE notes SET content = %(content)s, hash_id = %(hash_id)s WHERE frp = %(frp)s;"

    with conn.cursor() as cursor:
        for data in soul_data:
            try:
                cursor.executemany(j, soul_data)

            except (mysql.connector.Error, ConnectionError, Exception) as c:
                logger.error(f"Connection Error encountered while attempting to upload altered file to server: {c}.", exc_info=True)
                raise
            else:
                logger.info('Uploaded altered file[s]\'s content & new hash.')


# USER INPUT & HANDLING

def confirm(conn): # give
    """Double checks that user wants to commit any changes made to the server. Asks for y/n response and rolls-back on any error or [n] no."""
    confirm = input("Commit changes to server? y/n: ")

    if confirm in ('y', 'Y', 'yes', 'Yes', 'YES', 'yeah', 'i guess', 'I guess', 'i suppose', 'I suppose'):
        try:
            conn.commit()

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered while attempting to commit changes to server: {c}.", exc_info=True)
            raise
        else:
            logger.info('Commited changes to server.')

    elif confirm in ('n', 'N', 'no', 'No', 'NO', 'nope', 'hell no', 'naw'):
        try:
            conn.rollback()

        except (mysql.connector.Error, ConnectionError, Exception) as c:
            logger.error(f"Connection Error encountered while attempting to rollback changes to server: {c}.", exc_info=True)
            raise
        else:
            logger.info('Changes rolled back.')
    else:
        logger.error('Unknown response; rolling server back.')
        raise