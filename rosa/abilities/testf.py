import time
import subprocess
from pathlib import Path

local_dir = "/Volumes/HomeXx/compuir/texts8"

def main():
    # cmd = ["find", local_dir, "-depth"]
    # process = subprocess.run(cmd, capture_output=True, text=True)
    # files_backwards = process.stdout.splitlines()

    cmd = ["find", local_dir, "-delete"]
    process = subprocess.run(cmd)

    # print(f"Found {len(files_backwards)} items to process bottom-up with find.")

def slow():
    allp =[]
    path = Path(local_dir)
    for item in path.rglob('*'):
        allp.append(item)
    
    print(f"Found {len(allp)} items to process bottom-up with python.")


if __name__=="__main__":
    bg = time.perf_counter()
    main()
    nd = time.perf_counter()
    print(f"Find completed in {nd - bg:.4f} seconds.")