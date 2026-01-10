"""Holds queries for interacting with the MySQL database.
"""

import os

# MySQL tables

# files
# original_version = Version of initial upload
# version = Version of last edit (default/initial value is original_version)

# deltas 
# original_version = Version of initial upload
# from_version = Previous version at edit (from files table)
# to_version = Version at time of edit

# deleted 
# original_version = Version of initial upload
# from_version = Previous version at deletion (from files table)
# to_version = Version at time of edit


INIT2 = os.getenv("""INIT2""","""
CREATE TABLE IF NOT EXISTS interior (
     _id INT AUTO_INCREMENT NOT NULL,
     moment INTEGER NOT NULL UNIQUE,
     version INTEGER NOT NULL UNIQUE,
     message VARCHAR(256),
PRIMARY KEY (_id)
);

CREATE TABLE IF NOT EXISTS files (
     id INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL UNIQUE,
     content LONGBLOB NOT NULL,
     hash BINARY(8) NOT NULL,
     original_version INTEGER NOT NULL,
     from_version INTEGER NOT NULL,
     track ENUM ('T', 'F') NOT NULL,
PRIMARY KEY (id),
INDEX rps (rp)
);

CREATE TABLE IF NOT EXISTS directories (
     did INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL,
     version INT NOT NULL,
PRIMARY KEY (did),
INDEX dps (rp)
);

CREATE TABLE IF NOT EXISTS deltas (
     d_id INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL,
     patch LONGBLOB NOT NULL,
     original_version INTEGER NOT NULL,
     from_version INTEGER NOT NULL,
     to_version INTEGER NOT NULL,
PRIMARY KEY (d_id),
INDEX mrps (rp)
);

CREATE TABLE IF NOT EXISTS deleted (
     idd INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL,
     content LONGBLOB NOT NULL,
     original_version INTEGER NOT NULL,
     from_version INTEGER NOT NULL,
     to_version INTEGER NOT NULL,
     track ENUM ('T', 'F') NOT NULL,
PRIMARY KEY (idd),
INDEX drps (rp)
);

CREATE TABLE IF NOT EXISTS depr_directories (
     ddid INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL,
     from_version INT NOT NULL,
     to_version INT NOT NULL,
PRIMARY KEY (ddid),
INDEX ddps (rp)
);
""")

"""
modified ENUM('M') NULL DEFAULT NULL
or
original ENUM('O') NULL DEFAULT 'O'
(^could be state)
"""

# SQLite tables 

SINIT = os.getenv("""SINIT""","""
CREATE TABLE IF NOT EXISTS records (
     id INTEGER PRIMARY KEY,
     rp TEXT NOT NULL,
     original_version INTEGER NOT NULL,
     from_version INTEGER NOT NULL,
     ctime INTEGER NOT NULL,
     bytes INTEGER NOT NULL,
     track CHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS interior (
     id INTEGER PRIMARY KEY,
     moment TIMESTAMP NOT NULL,
     message TEXT,
     version INT NOT NULL
);

CREATE TABLE IF NOT EXISTS directories (
     did INT PRIMARY KEY,
     rp VARCHAR(256) NOT NULL,
     version INT NOT NULL
);

CREATE INDEX IF NOT EXISTS rps ON records(rp);

CREATE INDEX IF NOT EXISTS drps ON directories (rp);
""")

# additional queries

T_CHECK = os.getenv("""T_CHECK""","""
SHOW TABLES;
""")

ASSESS = os.getenv("""ASSESS""","""
     SELECT AVG_ROW_LENGTH
     FROM information_schema.tables
     WHERE table_schema = 'notation'
     AND table_name = 'files';
""")

ASSESS2 = os.getenv("""ASSESS2""",""" 
SELECT AVG_ROW_LENGTH
FROM information_schema.tables
WHERE table_schema = 'notation'
AND table_name = 'files';
""")

CVERSION = os.getenv("""VERSION""","""
     SELECT version, moment FROM interior
     ORDER BY moment DESC
     LIMIT 1;
""")

VERSIONS = os.getenv("""VERSION""","""
     SELECT version, moment, message
     FROM interior
     ORDER BY moment;
""")

TABLE_CHECK = os.getenv("""TABLE_CHECK""","""
     SHOW TABLES;
""")

_TRUNCATE = os.getenv("""_TRUNCATE""","""
     TRUNCATE TABLE index;
     TRUNCATE TABLE interior;
     TRUNCATE TABLE records;
""")

_DROP = os.getenv("""_DROP""","""
     DROP TABLE deltas;
     DROP TABLE interior;
     DROP TABLE files;
     DROP TABLE deleted;
     DROP TABLE directories;
     DROP TABLE depr_directories;
""")

TRUNCATE = os.getenv("""TRUNCATE""","""
     TRUNCATE TABLE files;
     TRUNCATE TABLE deltas;
     TRUNCATE TABLE records;
""")

DROP = os.getenv("""DROP""","""
     DROP TABLE records;
     DROP TABLE interior;
     DROP TABLE directories;
""")