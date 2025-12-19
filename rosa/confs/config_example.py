""" Configuration file for mysql-connector connector variables as well as other system components.

[variables] LOGGING_LEVEL, XCONFIG, MAX_ALLOWED_PACKET, TZ, LOCAL_DIR, RED, GREEN, YELLOW, RESET
"""

import os

LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'info')

DB_USER = os.getenv('DB_USER','root')
DB_PSWD = os.getenv('DB_PSWD','password')
DB_NAME = os.getenv('DB_NAME','database_name')
DB_ADDR = os.getenv('DB_ADDR','localhost')

XCONFIG = os.getenv('XCONFIG',
            {
    'user': 'root',
    'pswd': 'password',
    'name': 'database_name',
    'addr': 'local_host' # config ex for host machine
    }
)

MAX_ALLOWED_PACKET = os.getenv('MAX_ALLOWED_PACKET', 16_000_000) # 16 mb
# MAX_ALLOWED_PACKET = os.getenv('MAX_ALLOWED_PACKET', 100000000) # genuine server max

TZ = os.getenv('TZ', 'America/New York')

LOCAL_DIR = os.getenv('LOCAL_DIR', '/Full/path/to/directory')

# BLACK_LIST = os.getenv('BLACK_LIST','.DS_Store,.git,.obsidian')

RED = "\x1b[31;1m"
RESET = "\x1b[0m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"