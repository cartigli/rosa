# rosa
Version: 1.2.0
Self-hosted versioning. Track your data on your machine.

## Prerequisites
Before using [rosa], MySQL should be installed and running.
MacOS (via Homebrew):
```
brew install mysql
brew services start mysql
```
### MySQL Management:
- **Configure root:** `mysql -u root -p`
- **Start:** `mysql.server start`
- **Restart:** `mysql.server restart`
- **Shutdown:** `mysql.server stop`

### Setup Python Environment
It is recommended to run [rosa] in a virtual environment.
```bash
python -m venv venv_name
source venv_name/bin/activate
```

### Install [rosa]
Navigate to the project root and install the package.
*This should install the dependencies.*
```bash
pip install .
```
If manual installation is required:
```bash
pip install mysql-connector-python diff-match-patch xxhash
```

## Configuration
[rosa] requires configuration variables to authenticate with the server & manage preferences.
File location: ./rosa/confs/config.py
Adjust the following variables according to your preferences:
- [XCONFIG] authenticating with the server
    - [user] username ('root' if on host machine)
    - [pswd] password (set at initial [mysql -u root -p login])
    - [name] database name
    - [addr] server ip address ('localhost' if on host machine)
- [BLACKLIST] directories whose contents should not be tracked ('.index' should not be removed)
- [MAX_ALLOWED_PACKET] maximum packet size for the server
- [LOGGING_LEVEL] verbosity level of the logging output
- [TZ] time-zone

## Usage
```bash
rosa [command] [options]
```

### Initialization
Initiate the versioning base. Upload current state 
as v0, backup originals, and populate the index.
```bash
rosa .
```
*If already initialized, asks if you want to wipe the program.*

### Track changes
Identifies changes since last local commit.
Creates diff of deleted, created, and altered files (hash verified).
```bash
rosa diff
```

### Upload_changes
Uploads the difference found to the database.
Uploads created, backs up deleted, and updates altered files.
```bash
rosa give
```

### Rollback changes
Tracks changes and recovers from them.
Deletes new, recovers deleted, and reverts altered files to last local commitment.
```bash
rosa get
```

### Download history
Shows all currently stored versions; downloads given selection (i.e., v3, v6, etc.).
Rebuilds edits through reverse patches and ignores files uploaded after the given version.
```bash
rosa get version
```

## Examples
### Checking for changes
```bash
(tensor) compuir@minicartiglia ~ % rosa diff
not an indexed directory
(tensor) compuir@minicartiglia ~ % rosa diff --redirect /Volumes/HomeXx/compuir/texts
no diff!
rosa [diff] complete [0.0718 seconds]
(tensor) compuir@minicartiglia ~ % rosa diff -r ~/texts                      
no diff!
rosa [diff] complete [0.0721 seconds]
(tensor) compuir@minicartiglia ~ %
```

### Uploading changes found
```bash
(tensor) compuir@minicartiglia texts % rosa diff
no diff!
rosa [diff] complete [0.0729 seconds]
(tensor) compuir@minicartiglia texts % rosa give
no diff!
rosa [give] complete [0.0714 seconds]

...edit and delete files...

(tensor) compuir@minicartiglia texts % rosa diff
found 0 new files, 47 deleted files, and 169 altered files.
discrepancy[s] found between the server and local data:
found 47 file[s] that only exist in server. do you want details? y/n: n
heard
found 169 file[s] with hash discrepancies. do you want details? y/n: n
heard
rosa [diff] complete [4.1789 seconds]
(tensor) compuir@minicartiglia texts % rosa give   
found 0 new files, 47 deleted files, and 169 altered files.
versions: twinned
attach a message to this version (or enter for None): Test
updating records...
uploading altered files...
generating altered files's patches...
uploading altered files' patches...
removing deleted files...
updating local index...
backing up deleted files...
rosa [give] complete [9.2518 seconds]
(tensor) compuir@minicartiglia texts % 
```

### Rolling back edits
```bash
(tensor) compuir@minicartiglia texts % rosa diff
no diff!
rosa [diff] complete [0.0751 seconds]
(tensor) compuir@minicartiglia texts % rosa get
no diff!
rosa [get] complete [0.0645 seconds]
(tensor) compuir@minicartiglia texts % rosa rm

...edit and delete files...

(tensor) compuir@minicartiglia texts % rosa get
found 0 new files, 41 deleted files, and 163 altered files.
copying directory tree...
hard linking 4476 unchanged files...
replacing files with deltas
replacing index & originals
refreshing the index
rosa [get] complete [1.5632 seconds]
(tensor) compuir@minicartiglia texts % rosa diff
no diff!
rosa [diff] complete [0.0727 seconds]
(tensor) compuir@minicartiglia texts % 
```

### Getting a version
```bash
(tensor) compuir@minicartiglia texts % rosa get version
all the currently recorded commitments:
(0, '2026-01-07 - 29:05:58', 'INITIAL')
(1, '2026-01-07 - 30:05:14', 'upload v0')
(2, '2026-01-07 - 30:05:33')
(3, '2026-01-07 - 40:05:12')
(4, '2026-01-07 - 25:06:33', 'j')
(5, '2026-01-07 - 35:22:54')
(6, '2026-01-07 - 39:22:15')
(7, '2026-01-07 - 41:22:34', 'n')
(8, '2026-01-07 - 48:22:10', 'upload v7')
(9, '2026-01-07 - 38:23:51', 'Test')
Enter the version you would like to receive ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]): 8
requested version recieved: v8
downloading directories...
writing directory tree...
writing unaltered files...
downloading and writing altered files...
downloading & writing deleted files...
rosa got v8
rosa [get][version] complete [11.8313 seconds]
(tensor) compuir@minicartiglia texts %
```

## Flags
- [--force] [-f] skips confirmations for diff, skips message for give
- [--verbose] [-v] sets logging level to DEBUG (max output)
- [--silent] [-s] sets logging level to CRITICAL (min output)