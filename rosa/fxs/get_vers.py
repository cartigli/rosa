#!/usr/bin/env python3
"""Pulls the directory's state from a chosen version.

Downloads to a directory named 'rosa_v[version]'
next to the original, which remains untouched.
"""

import os
import sys
import shutil
import sqlite3
from datetime import datetime, timezone

import diff_match_patch as dmp_

from rosa.confs import VERSIONS
from rosa.lib import (
    phones, mk_rrdir, calc_batch, 
    mini_ps, finale, sfat_boy, Heart
)

NOMIC: str = "[get][version]"

def main(args: argparse = None):
    """Fetches all versions and downloads the user's choice."""
    xdiff: bool = False

    logger, force: bool, prints: bool, start: float = mini_ps(args, NOMIC)

    with phones() as conn:
        with conn.cursor() as cursor:
            cursor.execute(VERSIONS)
            versions: list = cursor.fetchall()

    fvers: list = []
    vers: list = []

    for version: int, moment: float, message: str in versions:
        date_obj = datetime.fromtimestamp(moment, tz=timezone.utc)
        date = date_obj.strftime("%Y-%m-%d - %M:%H:%S")

        if message:
            fvers.append((version, date, message))
        else:
            fvers.append((version, date))

        vers.append(version)

    print('all the currently recorded commitments:')
    for v in fvers:
        print(v)

    version: str = input(f"Enter the version you would like to receive ({vers}): ")

    if version:
        logger.info(f"requested version recieved: v{version}")

        try:
            version: int = int(version)
        except:
            logger.error(f"version is not an int: {version}")
            sys.exit()

        vers_: dict = {'vs': version}

        home: str = os.path.expanduser('~')
        tmpd: str = os.path.join(home, f"rosa_v{version}")

        dmp = dmp_.diff_match_patch()

        local = Heart()

        with sfat_boy(tmpd) as dirx:
            with phones() as conn:
                with conn.cursor() as cursor:
                    batch_size: int, r_sz: tuple = calc_batch(conn)

                    logger.info('downloading directories...')
                    VDIRECTORIES: str = """
                    SELECT rp 
                    FROM directories 
                    WHERE version <= %s;
                    """
                    cursor.execute(VDIRECTORIES, (version,))
                    drps: list = cursor.fetchall()

                    VD_DIRECTORIES: str = """
                    SELECT rp 
                    FROM depr_directories 
                    WHERE from_version <= %(vs)s 
                    AND %(vs)s < to_version;
                    """
                    cursor.execute(VD_DIRECTORIES, vers_)
                    ddrps: list = cursor.fetchall()
                    for d in ddrps:
                        drps.append(d)

                    logger.info('writing directory tree...')
                    mk_rrdir(drps, dirx)

                    logger.info('writing un-altered files...')
                    VFILES: str = """
                    SELECT rp, content 
                    FROM files 
                    WHERE from_version <= %s;
                    """
                    cursor.execute(VFILES, (version,))
                    vcount: int = 0

                    while True:
                        fdata: list = cursor.fetchmany(batch_size)

                        if not fdata:
                            break

                        for rp, content in fdata:
                            vcount: int += 1
                            fp: str = os.path.join(dirx, rp)

                            with open(fp, 'wb') as f:
                                f.write(content)
                    
                    logger.info(f"wrote {vcount} un-altered files")

                    logger.info('downloading and writing altered files...')
                    JCVM_FILES: str = """
                    SELECT DISTINCT d.rp, COALESCE(del.track, f.track) AS track
                    FROM deltas d
                    LEFT JOIN files f ON d.rp = f.rp
                    LEFT JOIN deleted del ON d.rp = del.rp
                    WHERE d.from_version <= %(vs)s
                    AND %(vs)s < d.to_version;
                    """

                    cursor.execute(JCVM_FILES, vers_)
                    rpsto_patch: list = cursor.fetchall()
                    logger.info(f"found {len(rpsto_patch)} files needing patching for version {version} (type {type(version)}")

                    mcount: int = 0
                    vv_count: int = 0
                    vc_count: int = 0

                    for rp, track in rpsto_patch:
                        mcount: int += 1
                        fp: str = os.path.join(dirx, rp)

                        if track == "T":
                            vv_count: int += 1
                            VMDC_FILES: str = """
                            SELECT content 
                            FROM files
                                WHERE rp = %s
                                AND original_version <= %s
                            UNION ALL
                            SELECT content 
                            FROM deleted 
                                WHERE rp = %s
                                AND original_version <= %s
                                AND to_version > %s;
                            """

                            cursor.execute(VMDC_FILES, (rp, version, rp, version, version))
                            content: bytes = cursor.fetchone()[0]

                            content: str = content.decode('utf-8')

                            VMP_FILES: str = """
                            SELECT patch 
                            FROM deltas 
                            WHERE rp = %s
                            AND %s < to_version 
                            AND original_version <= %s
                            ORDER BY to_version DESC;
                            """
                            cursor.execute(VMP_FILES, (rp, version, version))

                            while True:
                                cpatch: tuple = cursor.fetchmany(1)

                                if not cpatch:
                                    with open(fp, 'w') as f:
                                        f.write(content)

                                    break

                                ptxt: bytes = cpatch[0][0]
                                patch: str = ptxt.decode("utf-8")
                                
                                patch = dmp.patch_fromText(patch)
                                original_: str = dmp.patch_apply(patch, content)

                                previous: str = original_[0]
                                success: bool = original_[1]
                                try:
                                    if all(success):
                                        content: str = previous
                                    else:
                                        logger.error(success)
                                        raise Exception (f"error occured while applying patches")
                                except Exception as e:
                                    logger.error(f"error occured while applying patches: {success}", exc_info=True)
                                    sys.exit(1)

                        elif track == "F":
                            vc_count: int += 1
                            VC_CONTENT: str = """
                            SELECT patch
                            FROM deltas
                            WHERE rp = %s
                            AND from_version <= %s
                            AND original_version <= %s
                            AND %s < to_version
                            ORDER BY from_version DESC
                            LIMIT 1;
                            """
                            cursor.execute(VC_CONTENT, (rp, version, version, version))
                            content: bytes = cursor.fetchone()[0]

                            with open(fp, 'wb') as f:
                                f.write(content)
                    
                    logger.info(f"wrote {mcount} altered files ({vc_count} not tracked and {vv_count} tracked)")

                    logger.info('downloading & writing deleted files...')
                    VD_FILES: str = """
                    SELECT content, rp 
                    FROM deleted 
                    WHERE original_version <= %(vs)s
                    AND to_version > %(vs)s;
                    """
                    cursor.execute(VD_FILES, vers_)
                    vdcount: int = 0

                    while True:
                        fdata: list = cursor.fetchmany(batch_size)

                        if not fdata:
                            break

                        for content, rp in fdata:
                            vdcount: int += 1
                            fp: str = os.path.join(dirx, rp)

                            # if os.path.isfile(fp):
                            #     continue

                            with open(fp, 'wb') as f:
                                f.write(content)
                    
                    logger.info(f"wrote {vdcount} deleted files")

        logger.info(f"rosa got v{version}")

    else:
        logger.info('no version given')
    
    finale(NOMIC, start, prints)

if __name__=="__main__":
    main()