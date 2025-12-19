import os 

VDIRECTORIES = os.getenv("""VDIRECTORIES""","""
SELECT rp FROM directories WHERE version <= %s;
""")

VD_DIRECTORIES = os.getenv("""VD_DIRECTORIES""","""
SELECT rp FROM depr_directories WHERE version <= %s;
""")