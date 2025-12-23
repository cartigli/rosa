import os 

VDIRECTORIES = os.getenv("""VDIRECTORIES""","""
SELECT rp 
FROM directories 
WHERE version <= %s;
""")

VD_DIRECTORIES = os.getenv("""VD_DIRECTORIES""","""
SELECT rp 
FROM depr_directories 
WHERE oversion <= %(vs)s 
AND %(vs)s < xversion;
""")

VD_DIRECTORIES1 = os.getenv("""VD_DIRECTORIES""","""
SELECT rp 
FROM depr_directories 
WHERE %s 
BETWEEN oversion 
AND (xversion - 1);
""")

VFILES = os.getenv("""VFILES""","""
SELECT rp, content 
FROM files 
WHERE version <= %s;
""")

VM_FILES = os.getenv("""VM_FILES""","""
SELECT DISTINCT rp 
FROM deltas 
WHERE oversion <= %(vs)s 
AND %(vs)s < xversion;
""") # get all files who need to be repatched

VMC_FILES = os.getenv("""VMO_FILES""","""
SELECT content
FROM files
WHERE rp = %s;
""")

# originals of files to-be-patched
VMDC_FILES = os.getenv("""VMDC_FILES""","""
SELECT content 
FROM files 
WHERE rp = %s
UNION ALL
SELECT content 
FROM deleted 
WHERE rp = %s;
""") # get their original content

VMP_FILES = os.getenv("""VMP_FILES""","""
SELECT patch 
FROM deltas 
WHERE rp = %s 
AND oversion <= %s 
AND %s < xversion
ORDER BY xversion DESC;
""") # limit 1 and write/diff each file as it is recieved in the While loop
# but put that whole thing ina for loop

VD_FILES = os.getenv("""D_FILES""","""
SELECT content, rp 
FROM deleted
WHERE oversion <= %(vs)s
AND xversion > %(vs)s;
""") # this will end up pulling altered files' originals, which is sub-ideal
# maybe select all from deleted in the given range, find the ones with edits,
# deal accordingly, and then select all deleted where rp = a rp left from the
# full table search AND the versions are within te given range