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

    return ihome

def _maker(home):
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
            )
        """)
        # cursor.execute("""
        #     CREATE INDEX IF NOT EXISTS relative_paths ON notes(frp);
        #     """) # has no benefit for speed because doing whole table search now
        conn.commit()

def recursive(abs_path):
    """Recursive directory walking.

    If a directory, it yields from itself the given directory.
    If not, yields the item.
    *Will not yield any directories.

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

    for file in recursive(abs_path):
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
    for file in recursive(abs_path):
        rp = file.path[prefix:]

        stats = file.stat()

        size = stats.st_size
        ctime = stats.st_ctime*(10**7)
        real_stats[rp] = (ctime, size)

    return real_stats

def populate(inventory, home, key=None):
    """Inserts the collected metadata into the index.

    Args:
        inventory (list): Tupled values for the sqlite query.
        home (str): The path of the sqlite database's .db file.
        key (str): Variable containing either UPDATE or INSERT to act accordingly.

    Returns:
        None
    """
    with sqlite3.connect(home) as conn:
        cursor = conn.cursor()
        if key == "UPDATE":
            for item in inventory:
                cursor.execute("UPDATE OR IGNORE notes SET ctime = ?, bytes = ? WHERE frp = ?;", (item[1], item[2], item[0]))
        elif key == "INSERT":
            for item in inventory:
                cursor.execute("INSERT OR IGNORE INTO notes (frp, ctime, bytes) VALUES (?, ?, ?);", item)
        else:
            print('no key, returning')
            return
        # cursor.executemany("INSERT OR IGNORE INTO notes (frp, ctime, bytes) VALUES (?, ?, ?);", inventory)
        conn.commit()

def adjust(home, deltas):
    deletes = deltas[0]
    inserts = deltas[1]
    updates = deltas[2]

    insert_data = sweeper2(abs_path, inserts)
    update_data = sweeper2(abs_path, updates)

    with sqlite2.connect(home) as conn:
        cursor = conn.cursor()
        for d in deletes:
            cursor.execute("DELETE FROM notes WHERE frp = ?;", d)
        for u in update_data:
            cursor.execute("UPDATE notes SET ctime = ?, bytes = ? WHERE frp = ?;", (u[1], u[2], u[0])
        for i in insert_data:
            cursor.execute("INSERT INTO notes (frp, ctime, bytes) VALUES (?, ?, ?);", i)
        

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

def init_index(key):
    """Creates, configires, and populates a new database.

    Args:
        key (str): Variable for specifying action to update the database.

    Returns:
        None
    """
    start = time.perf_counter()

    home = _configure()
    _maker(home)
    abs_path = Path(LOCAL_DIR)

    inventory = _sweeper(abs_path)
    populate(inventory, home, key)

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
    start = time.perf_counter()

    home = _configure()
    abs_path = Path(LOCAL_DIR)

    real_stats = _scanner(abs_path)

    index_records = get_record(home)
    new, deleted, diffs = quick_diff(index_records, real_stats)

    if len(new) + len(deleted) + len(diffs) == 0:
        print('no diff!')
    else:
        print(f"found {len(new)} new files, {len(deleted)} deleted files, and {len(diffs)} diffs in the files.")
        # print(f"found {len(new)} new files, {len(deleted)} deleted files, {len(time_diff)} time-diff files, and {len(size_diff)} size-diff files.")
    
    return new, deleted, diffs

# def main(args=None):
#     start = time.perf_counter()
#     home = _configure()
#     # _maker(home)
#     abs_path = Path(LOCAL_DIR)
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