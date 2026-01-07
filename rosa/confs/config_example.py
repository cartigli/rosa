""" Configuration file for mysql-connector connector and other system components."""

import os

XCONFIG = os.getenv('XCONFIG',
            {
    'user': 'root',
    'pswd': 'password',
    'name': 'database_name',
    'addr': 'local_host' # config ex for host machine
    }
)

BLACKLIST = os.getenv('BLACKLIST', ['.index', '.git', '.obsidian', '.vscode', '.DS_Store'])

# MAX_ALLOWED_PACKET = os.getenv('MAX_ALLOWED_PACKET', 100000000) # genuine server max
MAX_ALLOWED_PACKET = os.getenv('MAX_ALLOWED_PACKET', 16_000_000) # 16 mb

LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'info')

TZ = os.getenv('TZ', 'America/New York')

RED = "\x1b[31;1m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
RESET = "\x1b[0m"