"""Environment file for getting queries as needed. Source of every query used for database configuration & management.

[queries] INITIATION, EDIT_TRIGGER, DELETE_TRIGGER, ASSESS, ASSESS2, T_CHECK, TRIG_CHECK, TRUNC, DROP, SNAP, SNAP2
"""

import os

# INITIATION/CONFIGURATION OF THE DATABASE

INITIATION1 = os.getenv("""INITIATION1""","""
CREATE TABLE IF NOT EXISTS records(
	_id INT AUTO_INCREMENT NOT NULL,
	moment TIMESTAMP DEFAULT TIMESTAMP,
	message VARCHAR(256),
	version VARCHAR(8) NOT NULL,
PRIMARY KEY (_id)
);

CREATE TABLE IF NOT EXISTS files(
	id INT AUTO_INCREMENT NOT NULL,
	rp VARCHAR(512) NOt NULL,
	content LONGBLOB NOT NULL,
	hash BINARY(8) NOT NULL,
	version VARCHAR(8) NOT NULL,
PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS deltas(
	d_id INT AUTO_INCREMENT NOT NULL,
	path TINETEXT NOT NULL,
	version VARCHAR(8) NOT NULL,
PRIMARY KEY (d_id)
);
""") # no primary keys cause mysql trips too hard; rely on python logic management
# this may be a terrible idea but it will make it faster

INITIATION = os.getenv("""INITIATION""","""
CREATE TABLE IF NOT EXISTS directories(
	drp VARCHAR(512) NOT NULL
);

CREATE TABLE IF NOT EXISTS notes(
	id INT AUTO_INCREMENT NOT NULL,
	frp VARCHAR(512) NOT NULL,
	content LONGBLOB NOT NULL,
	hash_id BINARY(8) NOT NULL,
	torigin TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	tol_edit TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id),
INDEX pth_hsh (frp, hash_id),
INDEX origins (torigin)
);

CREATE TABLE IF NOT EXISTS deltas(
	delta_id INT AUTO_INCREMENT NOT NULL,
	id INT NOT NULL,
	content LONGBLOB NOT NULL,
	hash_id BINARY(8) NOT NULL,
	tstart TIMESTAMP NOT NULL,
	tfinal TIMESTAMP NOT NULL,
PRIMARY KEY (delta_id),
FOREIGN KEY (id) REFERENCES notes(id) ON DELETE CASCADE,
INDEX origins (tstart, tfinal),
INDEX delt_ids (id)
);

-- GRAVEYARD --

CREATE TABLE IF NOT EXISTS deleted(
    rmid INT AUTO_INCREMENT NOT NULL,
	id INT NOT NULL,
	frp VARCHAR(512) NOT NULL,
	content LONGBLOB NOT NULL,
	hash_id BINARY(8) NOT NULL,
	torigin TIMESTAMP NOT NULL,
	tfinal TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (rmid),
INDEX finals (tfinal)
);

CREATE TABLE IF NOT EXISTS dead_deltas(
    iddelta INT AUTO_INCREMENT NOT NULL,
    rmid INT NOT NULL,
	id INT NOT NULL,
	content LONGBLOB NOT NULL,
	hash_id BINARY(8) NOT NULL,
	tstart TIMESTAMP NOT NULL,
	tfinal TIMESTAMP NOT NULL,
PRIMARY KEY (iddelta),
FOREIGN KEY (rmid) REFERENCES deleted(rmid) ON DELETE CASCADE,
INDEX dorigins (rmid, tstart, tfinal)
);
"""
)

EDIT_TRIGGER = os.getenv("""EDIT_TRIGGER""","""
CREATE TRIGGER on_notes_edit
AFTER UPDATE ON notes
FOR EACH ROW 
BEGIN
	INSERT INTO deltas (id, content, hash_id, tstart, tfinal) VALUES (OLD.id, OLD.content, OLD.hash_id, OLD.tol_edit, NEW.tol_edit);
END
"""
)

DELETE_TRIGGER = os.getenv("""DELETE_TRIGGER""","""
CREATE TRIGGER on_notes_delete
BEFORE DELETE ON notes
FOR EACH ROW
BEGIN
	INSERT INTO deleted (id, frp, content, hash_id, torigin) 
    VALUES (OLD.id, OLD.frp, OLD.content, OLD.hash_id, OLD.torigin);

    INSERT INTO dead_deltas (rmid, id, content, hash_id, tstart, tfinal) 
    SELECT LAST_INSERT_ID(), id, content, hash_id, tstart, tfinal 
    FROM deltas d 
    WHERE d.id = OLD.id;
END
"""
)

ASSESS = os.getenv("""ASSESS""","""
SELECT AVG(OCTET_LENGTH(content)) FROM notation.notes;
"""
)

# 1/100th the time, less accurate
ASSESS2 = os.getenv("""ASSESS2""",""" 
SELECT AVG_ROW_LENGTH
FROM information_schema.tables
WHERE table_schema = 'notation'
AND table_name = 'notes';
"""
)

T_CHECK = os.getenv("""T_CHECK""","""
SHOW TABLES;
"""
)

TRIG_CHECK = os.getenv("""TRIG_CHECK""","""
SHOW TRIGGERS;
"""
)

TRUNC = os.getenv("""TRUNC""","""
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE directories;
TRUNCATE TABLE deltas;
TRUNCATE TABLE notes;
TRUNCATE TABLE dead_deltas;
TRUNCATE TABLE deleted;
SET FOREIGN_KEY_CHECKS = 1;
"""
)

DROP = os.getenv("""DROP""","""
DROP TABLE directories;
DROP TABLE deltas;
DROP TABLE notes;
DROP TABLE dead_deltas;
DROP TABLE deleted;
"""
)

# GET MOMENT

SNAP = os.getenv("""SNAP""","""
SELECT n.frp,
	COALESCE(de.content, n.content)
FROM notes n 
LEFT OUTER JOIN deltas de
	ON de.id = n.id 
		AND de.tstart < %s
		AND de.tfinal > %s
WHERE n.torigin < %s
UNION ALL
SELECT d.frp,
	COALESCE(dd.content, d.content)
FROM deleted d 
LEFT OUTER JOIN dead_deltas dd 
	ON dd.rmid = d.rmid
		AND dd.tstart < %s
		AND dd.tfinal > %s
WHERE d.tfinal > %s;
"""
)

SNAP2 = os.getenv("""SNAP""","""
SELECT n.id,
FROM notes n 
LEFT OUTER JOIN deltas de
	ON de.id = n.id 
		AND de.tstart < %s
		AND de.tfinal > %s
WHERE n.torigin < %s
UNION ALL
SELECT d.id
FROM deleted d 
LEFT OUTER JOIN dead_deltas dd 
	ON dd.rmid = d.rmid
		AND dd.tstart < %s
		AND dd.tfinal > %s
WHERE d.tfinal > %s;
"""
)