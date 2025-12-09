import os
from pathlib import Path

def main():
    folder = Path("/Volumes/HomeXx/compuir/xxhash/rosa/lib")

    for file in folder.glob('*'):
        lines = 0
        if file.is_file():
            with open(file, 'r') as f:
                for line in f:
                    lines += 1
            
            print(f"{file} has {lines} lines.")

if __name__=="__main__":
    main()