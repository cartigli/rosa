""" Configuration file for mysql-connector connector and other system components."""

import os

XCONFIG = {
    'user': os.getenv('DB_USER', 'root'),
    'pswd': os.getenv('DB_PASS', 'password'),
    'name': os.getenv('DB_NAME', 'database_name'),
    'addr': os.getenv('DB_ADDR', 'local_host')
} # EX for host machine

BLACKLIST = os.getenv('BLACKLIST', '.index,.git,.obsidian,.vscode,.DS_Store,.pyc,.db').split(',')

MAX_ALLOWED_PACKET = int(os.getenv('MAX_ALLOWED_PACKET', 16_000_000)) # 16 mb

LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'info')

TZ = os.getenv('TZ', 'America/New York')

RED = "\x1b[31;1m"
GREEN = "\x1b[32m"
YELLOW = "\x1b[33m"
RESET = "\x1b[0m"