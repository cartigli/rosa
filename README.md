This is the child of snql3 [check profile] built for more robust and demanding uploading, downloading, and versioning. Some significant changes between the two are batched uploads / downloads (snql3 is fundamentally limited by the size of the maximum network packet), atomic writes when downloading (all or nothing), and aggressive rollback on error when uploading. The hash generation was also delegated to xxhash instead of hashlib for speed, and checkpoints, like timing of uploads/downloads and the size before/after compression, were included (compression is also present before uploading, and decompression during downloading). *Compression is not currently being used as it decreased upload/download times.

The improvements made around speed allowede the pc hosting the MySQL server to download and write 3 GB worth of files in under 10 seconds and upload the same in under a minute [current test downloaded 22 GB in ~50 seconds and uploaded in about 3 minutes]. An older generation laptop running Arch Linux ran the same download of the remote server in under 2 minutes and uploaded the same in 2.1 minutes.

Other improvements/adjustments are the MAX_ALLOWED_PACKET variable in the config file; this determines or helps provide a base for the ideal packet size for uploading and downloading. The batch_sizes are calculated based off this variable and averages of the server's table. Determining the files' size based off of their counted content was replaced with os.path.getsize() which is multitudes faster than reading the file [duh].

This program is built to be modular and imported as needed. Previously, the code below was placed before every relative import so execution could be done manually from anywhere (i.e., from outside the root directory) but was removed. It is not a huge benefit, and can be run manually with [python -m path.to.file] so no big benefit is lost. It can be added back in simply; just insert the block below above all the relative imports of any script you want to manually run outside of the root directory.
```
if __name__=="__main__":
    cd = Path(__file__).resolve().parent.parent
    if str(cd) not in sys.path:
        sys.path.insert(0, str(cd))
```

As of right now, the script only compared hashes to find out if files have been modified. Comparing timestamps of file meta data is coming soon. Also, many junk functions and duplicate logics have been removed. Instead of one single library file, the logic has been split into 5 mini libraries where they have more specific functions. 
The lib now includes:
  - Analyst: handles the collection of files & their info, generates hashes, and identifies discrepancies
  - Technician: handles uploading to the server and commiting changes, includes uplaoding & batching logic
  - Contractor: handles the downloading and writing of data to the disk, including context managers & download logic
  - Dispatch: manages the connection to the server: initiates, yields, and handles errors for the connection object
  - Opps: handles wrap-ups and conclusions, mini functions, and helpers for the main scripts that don't fit in the other libraries

The scripts were moved into a seperate directory named 'fxs' and import the needed functions from the lib directory. The router.py script's imports were corrected and the following commands are the main functions of rosa [rosa...]:
  - [give] finds changes between the local and remote data, uploads new files/directories, removes old ones, and updates altered files
  - [give][all] does not consider changes and brute forces upload of the entire local directory to the server (faster and simpler than [give])

  - [get] finds the changes between the local and remote data, downloads new files/directories, removes old ones, and replaces outdated content
  - [get][all] does not consider the changes and brute forces download of the entire server to the disk (faster and also simpler than [get])

  - [diff] finds changes between the two sources and simply presents them to the user; makes no changes to either of the sources
  - [init] checks the server for tables and triggers, asks the user how to proceed. Used for initiation of the server & truncating/deleting 
        * [rosa][diff] *can also be called with* [rosa][get][diff] *for convienance*

All scripts accept certain flags with their behavior outlined below:
    - [force] (-f), (--force) if the scripts asks for confirmation or input normally, it will skip and proceed with the default
    - [prints] (-p), (--prints) if the script prints something (all scripts print 'All set.' by defualt), it will ignore the print statement
    - [verbose] (-v), (--verbose) sets the logging_level to DEBUG for maximum logging output
    - [silent] (-s), (--silent) sets the logging_level to CRITICAL for minimum logging output
        - *Neither the silent nor verbose flags affect the rosa.log file's contents; it logs at the DEBUG level regardless of flags used*

Check out the snql3 repository for the history and more information on this program and the MySQL server for this project.

Required packages:
  - xxhash (unless using hashlib; xxhash is mad fast but less secure. Just swap the code with xxhash for the commented out hashlib functions)
  - mysql-connector-python
  - ~~Zstandard~~