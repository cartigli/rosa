import os
import shutil
import random
from pathlib import Path

from rosa.abilities.config import LOCAL_DIR

local_dir = LOCAL_DIR

def lister():
    abs_path = Path(local_dir).resolve()

    raw_hell = []
    hell_dirs = []

    if abs_path.exists():
        # print('Found abs_path')
        for item in abs_path.rglob('*'):
            path_str = item.resolve().as_posix()
            # if item.is_file():
            #     continue
            if item.is_dir():
                # drp = item.relative_to(abs_path).as_posix()

                hell_dirs.append(item) # keep the empty list of dirs
                # print(f"Recorded path for directory: {item.name}.")
            # else:
            #     continue
    return hell_dirs

def rand(hell_dirs):
    mx = random.randint(1, len(hell_dirs))
    print(f"Random integer: {mx}.")
    return mx

def remover(mx, hell_dirs):
    i = 0
    for folder in hell_dirs:
        i += 1
        if i == mx:
            # os.rmdir(folder)
            shutil.rmtree(folder)
            print(f"Removed dir: {folder}.")

def main():
    hell_dirs = lister()
    # mx = rand(hell_dirs)

    for i in range(17):
        mx = rand(hell_dirs)
        remover(mx, hell_dirs)

if __name__=="__main__":
    main()