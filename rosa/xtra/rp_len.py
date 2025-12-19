import os
from pathlib import Path

dir_ = Path("/Volumes/HomeXx/compuir/texts")

def main():
     prefix = len(dir_.as_posix())
     rps = 0

     for file in dir_.rglob('*'):
          if file.is_file():
               rp = file.as_posix()[prefix:]
               if len(rp) > 70:
                    rps += 1

     print(rps)

if __name__=="__main__":
     main()