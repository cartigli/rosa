import os
import datetime
from pathlib import Path

from config import *
from lib import phone_duty

def remote(conn, files):
    times = []
    ghosts = []
    remotez = [{'frp': str(file[2])} for file in files]
    q = "SELECT frp, COALESCE(tol_edit, torigin) FROM notes WHERE frp = %(frp)s;"
    with conn.cursor() as cursor:
        for remote in remotez:
            cursor.execute(q, remote)
            time = cursor.fetchall()
            if time:
                times.append(time)
            else:
                ghosts.append(remote)
    if times or ghosts:
        return times, ghosts

def main():
    curr_dir = Path(os.getcwd())
    files = []
    meta = []

    for file in curr_dir.glob('*'):
        if file.is_file():
            met_file = os.stat(file)
            rpath = file.relative_to(LOCAL_DIR)
            files.append((met_file, file, rpath))
        
    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        times, ghosts = remote(conn, files)
    
    for file in times:
        local_dir = Path(LOCAL_DIR)
        fpath = local_dir / file[0][0]
        forigin = file[0][1]
        # fedit = file[2]

        if fpath.exists() and fpath.is_file():
            local_stats = os.stat(fpath)
            local_torigin = local_stats.st_mtime
            local_tstr = datetime.datetime.fromtimestamp(local_torigin)

            #     print(f"{fpath} was created before the last upload") # catch 22
            if local_tstr > forigin:
                # print(f"{fpath} has been altered since the last upload")
                print(f"{YELLOW}{fpath}{RESET}")
            else:
                # print(f"{fpath} has not been altered since the last upload")
                print(f"{GREEN}{fpath}{RESET}")
    
    for filex in ghosts:
        local_dir = Path(LOCAL_DIR)
        path = Path(str(filex.values()))
        fpath = local_dir / path
        print(f"{RED}{fpath}{RESET}")


if __name__=="__main__":
    main()


