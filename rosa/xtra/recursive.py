import os
import time

def recursive(abs_path):
    for item in os.scandir(abs_path):
        if item.is_dir():
            yield from recursive(item.path)
        # if item.is_file():
        else:
            yield item

def scanner(abs_path):
    real_stats = {}
    prefix = len(str(abs_path))
    for file in recursive(abs_path):
        rp = file.path[prefix:]

        stats = file.stat()

        size = stats.st_size
        ctime = stats.st_ctime
        real_stats[rp] = (size, ctime)

    return real_stats

def main():
    start = time.perf_counter()
    abs_path = "/Volumes/HomeXx/compuir/texts20"
    real_stats = scanner(abs_path)
    end = time.perf_counter()
    print(f"os.scandir took {(end - start):.4f} seconds.")
    # print(real_statsj)

if __name__=="__main__":
    main()