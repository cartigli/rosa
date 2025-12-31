import os
from pathlib import Path

dir_1 = Path("/Volumes/HomeXx/compuir/rosa/rosa/lib")
dir_2 = Path("/Volumes/HomeXx/compuir/rosa/rosa/fxs")
dir_3 = Path("/Volumes/HomeXx/compuir/rosa/rosa/confs")

def main(dirx):
     prefix = len(dirx.as_posix())
     tot = 0

     for file in dirx.rglob('*'):
          if file.is_file():
               lines = 0
               with open(file, 'rb') as f:
                    lines = sum(1 for line in f)
                    tot += lines
                    # [(lines += 1) for line in f]
               # rp = file.as_posix()[prefix:]
               # if len(rp) > 70:
               #      rps += 1

     return tot

if __name__=="__main__":
     rps = 0
     rps += main(dir_1)
     rps += main(dir_2)
     rps += main(dir_3)

     print(rps)
     