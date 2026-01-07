#!/usr/bin/env python3
"""Pulls the directory's state from a chosen version.

Downloads to a directory named 'rosa_v[version]'
next to the original, which remains untouched.
"""


import os
import shutil
import sqlite3
from datetime import datetime

import diff_match_patch as dmp_

from rosa.confs import VERSIONS
from rosa.lib import (
    phones, fat_boy, mk_rrdir, calc_batch, 
    mini_ps, finale, sfat_boy
)

NOMIC = "[get][version]"

def main(args=None):
    """Fetches all versions and downloads the user's choice."""
    xdiff = False

    logger, force, prints, start = mini_ps(args, NOMIC)

    with phones() as conn:
        with conn.cursor() as cursor:
            cursor.execute(VERSIONS)
            versions = cursor.fetchall()

    fvers = []
    vers = []

    for version, moment, message in versions:
        date_obj = datetime.utcfromtimestamp(moment)
        date = date_obj.strftime("%Y-%m-%d - %M:%H:%S")

        if message:
            fvers.append((version, date, message))
        else:
            fvers.append((version, date))

        vers.append(version)

    print('all the currently recorded commitments:')
    for v in fvers:
        print(v)

    version = input(f"Enter the version you would like to receive ({vers}): ")

    if version:
        logger.info(f"requested version recieved: v{version}")
        vers_ = {'vs': version}

        target = os.path.expanduser('~')
        tmpd = os.path.join(target, f"rosa_v{version}")

        dmp = dmp_.diff_match_patch()

        with sfat_boy(tmpd) as dirx:
            with phones() as conn:
                with conn.cursor() as cursor:
                    batch_size, r_sz = calc_batch(conn)

                    logger.info('downloading directories...')
                    VDIRECTORIES = "SELECT rp FROM directories WHERE version <= %s;"

                    cursor.execute(VDIRECTORIES, (version,))
                    drps = cursor.fetchall()

                    VD_DIRECTORIES = "SELECT rp FROM depr_directories WHERE oversion <= %(vs)s AND %(vs)s < xversion;"
                    cursor.execute(VD_DIRECTORIES, vers_)

                    ddrps = cursor.fetchall()
                    for d in ddrps:
                        drps.append(d)

                    logger.info('writing directory tree...')
                    mk_rrdir(drps, dirx)

                    logger.info('writing unaltered files...')

                    VFILES = "SELECT rp, content FROM files WHERE version <= %s;"
                    cursor.execute(VFILES, (version,))

                    while True:
                        fdata = cursor.fetchmany(batch_size)

                        if not fdata:
                            break

                        for rp, content in fdata:
                            # fp = dirx / rp
                            fp = os.path.join(dirx, rp)
                            # fp.touch()

                            with open(fp, 'wb') as f:
                                f.write(content)

                    logger.info('downloading and writing altered files...')
                    VM_FILES = "SELECT DISTINCT rp FROM deltas WHERE oversion <= %(vs)s AND %(vs)s < xversion;"

                    cursor.execute(VM_FILES, vers_)
                    rpsto_patch = cursor.fetchall()

                    for rp in rpsto_patch:
                        VMDC_FILES = """SELECT content FROM files WHERE rp = %s 
                                        UNION ALL
                                        SELECT content FROM deleted WHERE rp = %s;"""

                        cursor.execute(VMDC_FILES, (rp[0], rp[0]))
                        bcontent = cursor.fetchone()
                        enc = "utf-8"

                        try:
                            content = bcontent[0].decode('utf-8')
                        except UnicodeDecodeError as ude:
                            enc = "latin-1"
                            content = bcontent[0].decode('latin-1')

                        vals = (rp[0], version, version)

                        VMP_FILES = "SELECT patch FROM deltas WHERE rp = %s AND oversion <= %s AND %s < xversion ORDER BY xversion DESC;"
                        cursor.execute(VMP_FILES, vals)

                        while True:
                            cpatch = cursor.fetchmany(1)

                            if not cpatch:
                                # fp = dirx / rp[0]
                                fp = os.path.join(dirx, rp[0])

                                back_tobytes = content.encode(enc)

                                with open(fp, 'wb') as f:
                                    f.write(back_tobytes)
                                break

                            patch_astext = cpatch[0][0]
                            patch = dmp.patch_fromText(patch_astext)

                            original_ = dmp.patch_apply(patch, content)

                            previous = original_[0]
                            success = original_[1]

                            if all(success):
                                content = previous
                            else:
                                logger.error(success)
                                raise Exception ('error occured while applying patches')

                    logger.info('downloading & writing deleted files...')
                    VD_FILES = "SELECT content, rp FROM deleted WHERE oversion <= %(vs)s AND xversion > %(vs)s;"

                    cursor.execute(VD_FILES, vers_)

                    while True:
                        fdata = cursor.fetchmany(batch_size)

                        if not fdata:
                            break

                        for content, rp in fdata:
                            # fp = tmpd / rp
                            fp = os.path.join(tmpd, rp)

                            # if fp.exists() and fp.is_file():
                            if os.path.isfile(fp):
                                continue

                            # fp.touch()

                            with open(fp, 'wb') as f:
                                f.write(content)

                    logger.info(f"rosa got v{version}")

    else:
        logger.info('no version given')
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()