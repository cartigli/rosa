import os

DB_USER = os.getenv('DB_USER','username')
DB_PSWD = os.getenv('DB_PSWD','mysql_password')
DB_NAME = os.getenv('DB_NAME','database_name')
DB_ADDR = os.getenv('DB_ADDR','ip_addr_ofServer')

MAX_ALLOWED_PACKET = os.getenv('MAX_ALLOWED_PACKET', 5_000_000) # 5 mb
# MAX_ALLOWED_PACKET = os.getenv('MAX_ALLOWED_PACKET', 100000000)

TZ = os.getenv('TZ', 'America/New York') # example Timezone

LOCAL_DIR = os.getenv('LOCAL_DIR','/Volumes/Full/path/to/your/folder') # full path of folder to 'track'