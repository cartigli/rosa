"""SQLite manager for storing records of files' metadata.

Configures a SQLite table and populates with each files' ctime & size.
Compares the record with the current state.
*Will not work on Windows. If need, change st_ctime to st_mtime.
"""
import os
import time
import sqlite3
from pathlib import Path

from rosa.confs import LOCAL_DIR


def _configure():
    """Configures paths and directory for the index.

    Makes the directory for the sqlite3 .db file.
    Sets the path & returns it for connecting to the database.

    Args:
        None
    
    Returns: ihome (Path): The full path of the indeces.db file.
    """
    curr = Path(__file__).resolve()
    home = curr.parent.parent / "index" # directory for index

    home.mkdir(parents=True, exist_ok=True)
    ihome = home / "indeces.db" # sqlite3.db file with indeces
    xhome = home / "history.db"

    return ihome, xhome

def _maker(home, xhome):
    """Makes the table for the index.

    Args:
        home (str): The path of the sqlite database's .db file.

    Returns:
        None
    """
    with sqlite3.connect(home) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER PRIMARY KEY,
                frp TEXT NOT NULL UNIQUE,
                bytes INTEGER NOT NULL,
                ctime INTEGER NOT NULL
            );
        """)
        conn.commit()

    with sqlite3.connect(xhome) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY,
                moment NOT NULL, 
                message TEXT
            );
        """)
        conn.commit()

def _erasor():
    home, xhome = _configure()
    if Path(home).exists():
        shutil_fx(home.parent)

def recursive(abs_path):
    """Recursive directory walking.

    Args:
        abs_path (str): A path to a directory.
    
    Yields:
        item (file): The file found in a given folder.
    """
    for item in os.scandir(abs_path):
        if item.is_dir():
            yield from recursive(item.path)
        else:
            yield item

def _sweeper(abs_path):
    """Collects a directory's contents' metadata for insertion into the database.

    Args:
        abs_path (str): A path to be scanned and collected.
    
    Returns:
        inventory (list): Tupled (relative paths, ctimes, and sizes) of the files in the directory.
    """
    inventory = []
    prefix = len(abs_path.as_posix()) + 1
    blk_list = ['.DS_Store', '.git', '.obsidian']

    for file in recursive(abs_path):
        tx = str(file)
        if any(blocked in tx for blocked in blk_list):
            pass
        else:
            path = file.path[prefix:]
            stats = file.stat()

            ctime = stats.st_ctime*(10**7)
            size = stats.st_size

            inventory.append((path, ctime, size))

    return inventory

def _sweeper2(abs_path, paths):
    """Collects a directory's contents' metadata for insertion into the database.
    Args:
        abs_path (str): A path to be scanned and collected.
    Returns:
        inventory (list): Tupled (relative paths, ctimes, and sizes) of the files in the directory.
    """
    inventory = []
    prefix = len(abs_path.as_posix()) + 1
    # for file in recursive(abs_path):
    for file in paths:
        fp = abs_path / file
        stats = fp.stat()
        ctime = stats.st_ctime*(10**7)
        size = stats.st_size
        inventory.append((file, ctime, size))
    return inventory

def _scanner(abs_path):
    """Same as _sweeper but as a dictionary.
    
    For comparison, not initiation.

    Args:
        abs_path (str): A path to be searched and collected.
    
    Returns:
        real_stats (list): Dictionaries with the relative paths as their keys and a tuple containing ctime and size.
    """
    real_stats = {}
    prefix = len(str(abs_path)) + 1
    blk_list = ['.DS_Store', '.git', '.obsidian']

    for file in recursive(abs_path):
        tx = str(file)
        if any(blocked in tx for blocked in blk_list):
            pass
        else:
            rp = file.path[prefix:]

            stats = file.stat()

            size = stats.st_size
            ctime = stats.st_ctime*(10**7)
            real_stats[rp] = (ctime, size)

    return real_stats

def populate(inventory, key=None, message=None):
    """Inserts the collected metadata into the index.

    Args:
        inventory (list): Tupled values for the sqlite query.
        home (str): The path of the sqlite database's .db file.
        key (str): Variable containing either UPDATE or INSERT to act accordingly.

    Returns:
        None
    """
    home, xhome = _configure()
    # home, xhome = landline()

    with sqlite3.connect(home) as conn:
        cursor = conn.cursor()
        if key == "UPDATE":
            for item in inventory:
                cursor.execute("UPDATE OR IGNORE notes SET ctime = ?, bytes = ? WHERE frp = ?;", (item[1], item[2], item[0]))
        elif key == "INSERT":
            for item in inventory:
                cursor.execute("INSERT OR IGNORE INTO notes (frp, ctime, bytes) VALUES (?, ?, ?);", item)
        elif key == "DELETE":
            for item in inventory:
                cursor.execute("DELETE FROM notes WHERE frp = %s;", item)
        else:
            print('no key, returning')
            return
        # cursor.executemany("INSERT OR IGNORE INTO notes (frp, ctime, bytes) VALUES (?, ?, ?);", inventory)
        conn.commit()

def historian(message=None, type=None):
    home, xhome = configure()
    moment = datetime.datetime.timestamp(datetime.UTC).as_timestamp()
    if message:
        values = (moment, message)
        query = "INSERT INTO history (moment, message) VALUES (?, ?);"
    else:
        query = "INSERT INTO history (moment) VALUES (?);"
        values = (moment,)

    with sqlite3.connect(xhome) as conn:
        cursor = conn.cursor()
        cursor.execute(query, values)

def adjust(deltas, key=None, message=None):
    # home, xhome = _configure()

    deletes = deltas[0]
    inserts = deltas[1]
    updates = deltas[2]

    populate(deletes, "DELETE")

    if key == "FULL":
        # populate(deletes, "DELETE")
        populate(inserts, "INSERT")
        populate(updates, "UPDATE")

    else:
        insert_data = _sweeper2(abs_path, inserts)
        update_data = _sweeper2(abs_path, updates)

        # populate(deletes, key="DELETE")
        populate(insert_data, key="INSERT")
        populate(update_data, key="UPDATE")

    historian(message, type="GET")

def get_record(home):
    """Selects every current record of the metadata from the index.

    Args:
        home (str): The path of the sqlite database's .db file.

    Returns:
        index_records (dictionary): Key values are the relative paths and the values are the ctime and size.
    """
    index_records = {}

    with sqlite3.connect(home) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT frp, ctime, bytes FROM notes;")
        indexed_all = cursor.fetchall() # every currently recorded stat

        for indexed in indexed_all:
            index_records[indexed[0]] = (indexed[1], indexed[2])
    
    return index_records

def quick_diff(index_records, real_stats):
    """Compares sets of the input records of the metadata.

    Args:
        index_records (dictionary): Key values are the relative paths and the values are the ctime and size.
        real_stats (list): Dictionaries with the relative paths as their keys and a tuple containing ctime and size.

    Returns:
        A 3-element tuple containing: 
            new (set): New files not seen in the index.
            deleted (set): Files in the index not found in the scan.
            diffs (list): Files with ctime or size discrepancies.
    """
    all_indexes = set(index_records.keys())
    all_files = set(real_stats.keys())

    deleted = all_indexes - all_files
    new = all_files - all_indexes

    remaining = all_files & all_indexes

    # time_diff = []
    # size_diff = []
    diffs = []

    for rp in remaining:
        if index_records[rp][0] == real_stats[rp][0]:
            if index_records[rp][1] != real_stats[rp][1]:
                diffs.append(rp) # size diff
        elif index_records[rp][0] != real_stats[rp][0]:
            diffs.append(rp) # time diff
    
    return new, deleted, diffs

def init_index():
    """Creates, configires, and populates a new database.

    Args:
        key (str): Variable for specifying action to update the database.

    Returns:
        None
    """
    start = time.perf_counter()

    # _erasor()

    home, xhome = _configure()
    _maker(home, xhome)
    abs_path = Path(LOCAL_DIR)

    inventory = _sweeper(abs_path)

    populate(inventory, key="INSERT")
    historian(message="INITIATION", type="INIT") # type does nothing atm

def query_index():
    """Compares the files and the index.

    Prints the results of the search.
    Prints 'no diff!' if all are 0.

    Args:
        None

    Returns:
        A 3-element tuple containing: 
            new (set): New files not seen in the index.
            deleted (set): Files in the index not found in the scan.
            diffs (list): Files with ctime or size discrepancies.
    """
    diff = False
    start = time.perf_counter()

    home, xhome = _configure()
    abs_path = Path(LOCAL_DIR)

    real_stats = _scanner(abs_path)

    index_records = get_record(home)
    new, deleted, diffs = quick_diff(index_records, real_stats)

    if len(new) + len(deleted) + len(diffs) == 0:
        diff = False
    else:
        print(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} diffs in the files.")
        diff = True
    
    return new, deleted, diffs, diff

def main(args=None):
    
    start = time.perf_counter()
    home, xhome = _configure()
    _maker(home, xhome)
    abs_path = Path(LOCAL_DIR)
    init_index() # initiates the local index - ignores blocked items/paths - 'rosa .'

#     # inventory = _sweeper(abs_path) # creating / updating
#     real_stats = _scanner(abs_path) # checking / comparing
#     end = time.perf_counter()
#     # populate(inventory, home, key="INSERT") # ran once; INSERT OR IGNORE would mitigate the problem but would be innefficient to run everytime
#     # refresh(inventory, home, key="UPDATE") # ran on 'rosa give' & 'rosa get'?
#     start1 = time.perf_counter()
#     index_records = get_record(home, abs_path)
#     new, deleted, time_diff, size_diff = quick_diff(index_records, real_stats)
#     end1 = time.perf_counter()
#     if len(new) + len(deleted) + len(time_diff) + len(size_diff) == 0:
#         print('no diff!')
#     else:
#         print(f"found {len(new)} new files, {len(deleted)} deleted files, {len(time_diff)} time-diff files, and {len(size_diff)} size-diff files.")
#     print(f"collection took {(end - start):.4f} seconds.")
#     print(f"assessment took {(end1 - start1):.4f} seconds.")
#     print(f"total took {(end1 - start):.4f} seconds.")

    # ran intially, and adds all the found files & statistics to the index
    # then user runs get diff a few days later. This needs to rescan the indexes, compare all the file paths, and find those that are missing.
    # These are new files.
    # Then, for the files present in both, it should compare their ctimes. If the filesystem has changed this since the recorded ctime, flag it.
    # Do the same for the sizes, although I don't see the point because how could a file have the same ctime recorded but have a different size?
    # with flahgged files, pull the remote hash and compare it to a freshly generated hash. If different, the file has changed.
    # If not different, the discrepancy is ignored and comparison continues.
    # Then, the user runs 'rosa give'. Now, the ctimes must be updated, as well as the file sizes. This is refresh.
    # If the user runs 'rosa get', do the same re_population.
# if __name__=="__main__":
#     main()