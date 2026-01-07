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

## Flags
- [--force] [-f] skips confirmations for diff, skips message for give
- [--verbose] [-v] sets logging level to DEBUG (max output)
- [--silent] [-s] sets logging level to CRITICAL (min output)
- [--remote] [-r] diff will compare local and remote versions