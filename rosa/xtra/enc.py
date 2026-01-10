import os
import time
# from pathlib import Path
# import charset_normalizer
import cchardet

BLACKLIST = ['.index', '.git', '.obsidian', '.vscode', '.DS_Store', '.pyc', '.db']

def _r(dir_):
	for obj in os.scandir(dir_):
		if os.path.isdir(obj):
			yield from _r(obj.path)
		else:
			yield obj

def is_ignored(_str):
	return any(blckd in _str for blckd in BLACKLIST)

def check1(dir_):
     for obj in _r(dir_):
          if is_ignored(obj.path):
               continue
          elif obj.is_file():
               with open(obj, 'rb') as f:
                    raw = f.read()
               
               result = cchardet.detect(raw)
               # print(result['encoding'])
               enc = cchardet.detect(raw)

def encoding(obj):
     utf = False

     with open(obj, 'rb') as f:
          raw = f.read(1024*1024)
     
     try:
          raw.decode('utf-8')
     except UnicodeDecodeError:
          # print("binary")
          utf = "F"
     else:
          utf = "T"
     
     return utf

def check2(dir_):
     utf = False

     with open(obj, 'rb') as f:
          raw = f.read(1024*1024)
     
     try:
          raw.decode('utf-8')
     except UnicodeDecodeError:
          # print("binary")
          utf = False
     else:
          utf = True


def check(dir_):
     for obj in _r(dir_):
          if is_ignored(obj.path):
               continue
          elif obj.is_file():
               with open(obj, 'rb') as f:
                    raw = f.read()
               try:
                    raw.decode('utf-8')
               except UnicodeDecodeError as e:
                    start = max(0, e.start - 10)
                    end = min(len(raw), e.end + 10)
                    print(f"{obj.path}")
                    print(f"  Byte {e.start}: {hex(raw[e.start])} in context: {raw[start:end]}")
                    print()
                    print(obj.path, "NOT UNICODE")
                    result = charset_normalizer.from_bytes(raw).best()
                    if result is None:
                         print(f"Could not detect encoding for {obj.path}")
                         continue
                    
                    detected = result.encoding
                    conf = result.encoding_aliases

                    print(f"{obj.path} detected encoding: {detected}")

def main():
     start = time.perf_counter()
     check1(os.getcwd())
     end = time.perf_counter()

     one = end - start

     start = time.perf_counter()
     check2(os.getcwd())
     end = time.perf_counter()

     two = end - start

     print(f"cchardet took {one} seconds and os took {two} seconds")


if __name__ == "__main__":
     main()