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

    version = input(f"Enter the version you would like to receive ({vers}): ")

    if version:
        logger.info(f"requested version recieved: v{version}")

        try:
            version = int(version)
        except:
            logger.error(f"version is not an int: {version}")
            sys.exit()

        vers_ = {'vs': version}

        home = os.path.expanduser('~')
        tmpd = os.path.join(home, f"rosa_v{version}")

        dmp = dmp_.diff_match_patch()

        local = Heart()

        with sfat_boy(tmpd) as dirx:
            with phones() as conn:
                with conn.cursor() as cursor:
                    batch_size, r_sz = calc_batch(conn)

                    logger.info('downloading directories...')
                    VDIRECTORIES = """
                    SELECT rp 
                    FROM directories 
                    WHERE version <= %s;
                    """

                    cursor.execute(VDIRECTORIES, (version,))
                    drps = cursor.fetchall()

                    VD_DIRECTORIES = """
                    SELECT rp 
                    FROM depr_directories 
                    WHERE from_version <= %(vs)s 
                    AND %(vs)s < to_version;
                    """
                    cursor.execute(VD_DIRECTORIES, vers_)


                    ddrps = cursor.fetchall()
                    for d in ddrps:
                        drps.append(d)

                    logger.info('writing directory tree...')
                    mk_rrdir(drps, dirx)

                    logger.info('writing un-altered files...')
                    VFILES = """
                    SELECT rp, content 
                    FROM files 
                    WHERE from_version <= %s;
                    """
                    cursor.execute(VFILES, (version,))
                    vcount = 0

                    while True:
                        fdata = cursor.fetchmany(batch_size)

                        if not fdata:
                            break

                        for rp, content in fdata:
                            vcount += 1
                            # print("ORIGINAL", rp)
                            fp = os.path.join(dirx, rp)

                            with open(fp, 'wb') as f:
                                f.write(content)
                    
                    logger.info(f"wrote {vcount} un-altered files")

                    logger.info('downloading and writing altered files...')
                    VM_FILES = """
                    SELECT DISTINCT rp 
                    FROM deltas 
                    WHERE from_version <= %(vs)s 
                    AND %(vs)s < to_version;
                    """
                    JVM_FILES = """
                    SELECT DISTINCT d.rp, f.track
                    FROM deltas d
                    JOIN files f 
                        ON d.rp = f.rp 
                    WHERE d.from_version <= %(vs)s 
                    AND %(vs)s < to_version;
                    """
                    JCVM_FILES = """
                    SELECT DISTINCT d.rp, COALESCE(del.track, f.track) AS track
                    FROM deltas d
                    LEFT JOIN files f ON d.rp = f.rp
                    LEFT JOIN deleted del ON d.rp = del.rp
                    WHERE d.from_version <= %(vs)s
                    AND %(vs)s < d.to_version;
                    """

                    cursor.execute(JCVM_FILES, vers_)
                    rpsto_patch = cursor.fetchall()
                    logger.info(f"found {len(rpsto_patch)} files needing patching for version {version} (type {type(version)}")
                    mcount = 0
                    vv_count = 0
                    vc_count = 0

                    for rp, track in rpsto_patch:
                        mcount += 1
                        fp = os.path.join(dirx, rp)

                        if track == "T":
                            vv_count += 1
                            print("MODIFIED - T", rp)
                            VMDC_FILES = """
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
                            content = cursor.fetchone()[0]

                            content = content.decode('utf-8')

                            VMP_FILES = """
                            SELECT patch 
                            FROM deltas 
                            WHERE rp = %s
                            AND %s < to_version 
                            AND original_version <= %s
                            ORDER BY to_version DESC;
                            """
                            cursor.execute(VMP_FILES, (rp, version, version))

                            while True:
                                cpatch = cursor.fetchmany(1)

                                if not cpatch:
                                    with open(fp, 'w') as f:
                                        f.write(content)

                                    break

                                ptxt = cpatch[0][0]
                                patch = ptxt.decode("utf-8")
                                
                                patch = dmp.patch_fromText(patch)
                                original_ = dmp.patch_apply(patch, content)

                                previous = original_[0]
                                success = original_[1]
                                try:
                                    if all(success):
                                        logger.info(f"applied a patch for {rp}")
                                        content = previous
                                    else:
                                        logger.error(success)
                                        raise Exception (f"error occured while applying patches")
                                except Exception as e:
                                    logger.error(f"error occured while applying patches: {success}", exc_info=True)
                                    sys.exit(1)

                        elif track == "F":
                            print("MODIFIED - F", rp)
                            vc_count += 1
                            VC_CONTENT = """
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
                            content = cursor.fetchone()[0]

                            with open(fp, 'wb') as f:
                                f.write(content)
                    
                    logger.info(f"wrote {mcount} altered files ({vc_count} not tracked and {vv_count} tracked)")

                    logger.info('downloading & writing deleted files...')
                    VD_FILES = """
                    SELECT content, rp 
                    FROM deleted 
                    WHERE original_version <= %(vs)s
                    AND to_version > %(vs)s;
                    """
                    ORIGINAL_VD_FILES = """
                    SELECT content, rp 
                    FROM deleted 
                    WHERE from_version <= %(vs)s
                    AND original_version <= %(vs)s
                    AND to_version > %(vs)s;
                    """
                    VDT_FILES = """
                    SELECT content, rp 
                    FROM deleted 
                    WHERE to_version > %(vs)s;
                    AND original_version <= %(vs)s
                    """
                    cursor.execute(VD_FILES, vers_)
                    vdcount = 0

                    while True:
                        fdata = cursor.fetchmany(batch_size)

                        if not fdata:
                            break

                        for content, rp in fdata:
                            vdcount += 1
                            print("DELETED", rp)
                            fp = os.path.join(tmpd, rp)

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