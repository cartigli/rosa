import os
import xxhash
import datetime
from pathlib import Path

from config import *
from lib import phones

def remote(conn, files):
    times = []
    ghosts = []
    remotez = [{'frp': str(file[2])} for file in files]
    q = "SELECT frp, MAX(torigin, tol_edit) FROM notes WHERE frp = %(frp)s;"
    # q = "SELECT frp, COALESCE(torigin, tol_edit) FROM notes WHERE frp = %(frp)s;"
    with conn.cursor() as cursor:
        for remote in remotez:
            cursor.execute(q, remote)
            time = cursor.fetchall()
            if time:
                times.append(time)
            else:
                # if it doesn't exist in the server
                ghosts.append(remote)
    if times or ghosts:
        return times, ghosts


def rh(conn, fq):
    q = "SELECT hash_id FROM notes WHERE frp = %(frp)s;"
    with conn.cursor() as cursor:
        cursor.execute(q, fq)
        time = cursor.fetchone()
        if time:
            return time


def main():
    hasher = xxhash.xxh64()
    curr_dir = Path(os.getcwd())
    tout = []
    files = []
    meta = []

    for file in curr_dir.glob('*'):
        if file.is_file():
            met_file = os.stat(file)
            rpath = file.relative_to(LOCAL_DIR)
            files.append((met_file, file, rpath))

    if files:
        # with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        with phones() as conn:
            times, ghosts = remote(conn, files)
        
        for file in times:
            local_dir = Path(LOCAL_DIR)
            rpath = file[0][0]
            fpath = local_dir / rpath
            forigin = file[0][1]
            # fedit = file[2]

            if fpath.exists() and fpath.is_file():
                local_stats = os.stat(fpath)
                local_torigin = local_stats.st_mtime
                local_tstr = datetime.datetime.fromtimestamp(local_torigin)

                #     print(f"{fpath} was created before the last upload") # catch 22
                if local_tstr > forigin:
                    with phones() as conn:
                        q = {'frp': rpath}
                        rhash_id = rh(conn, q)
                        cont = fpath.read_bytes()
                        hasher.update(cont)
                        fhash_id = hasher.digest()
                        if fhash_id != rhash_id:
                            # print(f"{fpath} has been altered since the last upload")
                            # print(f"{YELLOW}{fpath}{RESET}")
                            tout.append(f"{YELLOW}{rpath}{RESET} [hash discrepancy]") # - nothing really, would be hash discrepancies
                        else:
                            # print(f"{fpath} has not been altered since the last upload")
                            tout.append(f"{GREEN}{rpath}{RESET} - [uploaded to the server]")
                else:
                    # print(f"{fpath} has not been altered since the last upload")
                    tout.append(f"{GREEN}{rpath}{RESET} - [uploaded to the server]")
        
        for filex in ghosts:
            local_dir = Path(LOCAL_DIR)
            path = Path(str(filex.values()))
            fpath = local_dir / path
            tout.append(f"{RED}{rpath}{RESET} - [not present in the server]")
        
        for file in tout:
            print(f"{file}")



if __name__=="__main__":
    main()


