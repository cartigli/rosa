import time
import sqlite3
from pathlib import Path

# pathname = Path("Volumes/HomeXx/compuir/texts20")
pathname = Path("/Volumes/HomeXx/compuir/texts20")

def main():
    files = []

    for file in pathname.rglob('*'):
        if file.is_file():
            stats = file.stat()
            ctime = stats.st_ctime*(10**7) # no decimals
            size = stats.st_size
            ino = stats.st_ino
            print(ino)
            frp = file.relative_to(pathname).as_posix() # relative path

            files.append((frp, size, ctime)) # relative path (indexed), size (in bytes), and ctime (as integers)

    with sqlite3.connect('file_ctimes.db') as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER AUTO INCREMENT,
                frp TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                ctime INTEGER NOT NULL
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS relative_paths ON notes(frp);
            """)

        for file in files:
            cursor.execute("INSERT INTO notes (frp, bytes, ctime) VALUES (?, ?, ?)", file)
        
    filecheck = []

    for file in pathname.rglob('*'):
        if file.is_file():
            stats = file.stat()
            ctime = stats.st_ctime*(10**7)
            frp = file.relative_to(pathname).as_posix()

            filecheck.append((frp, ctime))
    
    for file in filecheck:
        # for frp, ctime in file:
        with sqlite3.connect('file_ctimes.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ctime FROM notes WHERE frp = ?;", (file[0],))
            ftime = cursor.fetchall()
            ctime = file[1]
            if ctime != ftime[0][0]:
               print(f"{ftime} - {ctime}")

if __name__=="__main__":
    begin = time.perf_counter()
    main()
    end = time.perf_counter()
    print(f"Time to scan & insert every file & ctime: {(end - begin):.2f} seconds.")