import os
from pathlib import Path


def main(args=None):
     path1 = Path.cwd()
     path2 = Path(__file__)

     print(path1)
     print(path2)

if __name__=="__main__":
     main()