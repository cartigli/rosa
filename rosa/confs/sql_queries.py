"""Holds queries for interacting with the MySQL database.
"""

import os

# MySQL tables

INIT = os.getenv("""ONE""","""
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
     version INTEGER NOT NULL,
PRIMARY KEY (id),
INDEX rps (rp)
);

CREATE TABLE IF NOT EXISTS directories (
     did INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL,
     version INT NOT NULL,
PRIMARY KEY (did)
);

CREATE TABLE IF NOT EXISTS deltas (
     d_id INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL,
     patch MEDIUMTEXT NOT NULL,
     oversion INTEGER NOT NULL,
     xversion INTEGER NOT NULL,
PRIMARY KEY (d_id)
);

CREATE TABLE IF NOT EXISTS deleted (
     idd INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL,
     content LONGBLOB NOT NULL,
     oversion INTEGER NOT NULL,
     xversion INTEGER NOT NULL,
PRIMARY KEY (idd)
);

CREATE TABLE IF NOT EXISTS depr_directories (
     ddid INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL,
     oversion INT NOT NULL,
     xversion INT NOT NULL,
PRIMARY KEY (ddid)
);
""")

# let's do this later; getting the desired version is more important
INIT2 = os.getenv("""ONE""","""
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
     hash BINARY(8) NOT NULL,
     version INTEGER NOT NULL,
PRIMARY KEY (id),
INDEX rps (rp)
);

CREATE TABLE IF NOT EXISTS guts (
     ig INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL UNIQUE,
     content LONGBLOB NOT NULL,
PRIMARY KEY (ig)
);

CREATE TABLE IF NOT EXISTS directories (
     did INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL,
     version INT NOT NULL,
PRIMARY KEY (did)
);

CREATE TABLE IF NOT EXISTS deltas (
     d_id INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL,
     patch MEDIUMTEXT NOT NULL,
     version INTEGER NOT NULL,
PRIMARY KEY (d_id)
);

CREATE TABLE IF NOT EXISTS deleted (
     idd INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(512) NOT NULL,
     version INTEGER NOT NULL,
PRIMARY KEY (idd)
);

CREATE TABLE IF NOT EXISTS deleted_guts (
     dig INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL UNIQUE,
     content LONGBLOB NOT NULL,
PRIMARY KEY (dig)
);

CREATE TABLE IF NOT EXISTS depr_directories (
     ddid INT AUTO_INCREMENT NOT NULL,
     rp VARCHAR(256) NOT NULL,
     version INT NOT NULL,
PRIMARY KEY (ddid)
);
""")

# SQLite tables 

RECORDS = os.getenv("""RECORDS""","""
CREATE TABLE IF NOT EXISTS records (
     id INTEGER PRIMARY KEY,
     rp TEXT NOT NULL,
     version INTEGER NOT NULL,
     ctime INTEGER NOT NULL,
     bytes INTEGER NOT NULL
);
""")

INTERIOR = os.getenv("""INTERIOR""","""
CREATE TABLE IF NOT EXISTS interior (
     id INTEGER PRIMARY KEY,
     moment TIMESTAMP NOT NULL,
     message TEXT,
     version INT NOT NULL
);
""")

DIRECTORIES = os.getenv("""DIRECTORIES""","""
CREATE TABLE IF NOT EXISTS directories (
     did INT PRIMARY KEY,
     rp VARCHAR(256) NOT NULL,
     version INT NOT NULL
);
""")

RECORDS_INDEX = os.getenv("""RECORDS_INDEX""","""
CREATE INDEX rps ON records(rp);
""")

DIRECTORIES_INDEX = os.getenv("""DIRECTORIES_INDEX""","""
CREATE INDEX drps ON directories (rp);
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
AND table_name = 'notes';
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