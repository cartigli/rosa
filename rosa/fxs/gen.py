"""General 'helper' functions."""

import os
import sys
import xxhash
import random

from rosa.confs import BLACKLIST
from rosa.lib import mini_ps, _r

logger = logging.getLogger('rosa.log')

NOMIC: str = "[gen]"

def is_ignored(_str):
	return any(blckd in _str for blckd in BLACKLIST)

def contrast(dir1: str = "", dir2: str = ""):
	"""Compares contents and respective hashes for two directories.

	Args:
		dir1 (str): Path to the first directory (usually CWD).
		dir2 (str): Path to the alternative directory.
	
	Returns:
		corrupt (list): Files with hash discrepancies.
		o1 (list): Files only in dir1.
		ign1 (int): Count of files ignored in dir1.
		o2 (list): Files only in dir2.
		ign2 (int): Count of files ignored in dir2.
	"""
	corrupt: list = []

	i1: dict, ign1: list = inv(dir1)
	i2: dict, ign2: list = inv(dir2)

	s1 = set(i1.keys())
	s2 = set(i2.keys())

	ld: set = s1 & s2

	for l in ld:
		if i1[l] != i2[l]:
			corrupt.append(l)
	
	o1: set = s1 - s2
	o2: set = s2 - s1
	
	return corrupt, list(o1), ign1, list(o2), ign2

def inv(dirn: str = ""):
	"""Scans directory and collects paths and hashes.

	Args:
		dirn (str): Path to a directory.
	
	Returns:
		a (dict): Relative paths keyed to their hashes.
		ign (int): Count of ignored files.
	"""
	hasher = xxhash.xxh64()
	ign: int = 0
	a: dict = {}

	pfx: int = len(dirn) + 1

	for file in _r(dirn):
		if is_ignored(file):
			ign += 1
			continue
		hasher.reset()

		with open(file, 'rb') as f:
			content: bytes = f.read()

		hasher.update(content)
		hashx: bytes = hasher.digest()

		path: str = file[pfx:]

		a[path] = hashx
	
	return a, ign

def compare_contrast():
	"""Compares and contrasts two directories.

	Args:
		None

	Returns:
		None
	"""
	root: str = os.path.expanduser('~')

	dirb: str = input('enter the path of the alt directory: ')
	if not dirb:
		return

	dirfa: str = os.getcwd()
	cut: int = len(root) + 1
	dira: str = dirfa[cut:]
	dirfb: str = os.path.join(root, dirb)

	corrupt, ao, aign, bo, bign = contrast(dirfa, dirfb)
	x: str = "files"

	if corrupt:
		print(f"there are {len(corrupt)} {x} with hash discrepencies...")
	else:
		print(f"no corrupted files")

	if ao or bo:
		print(f"{len(ao)} {x} only in {dira}...")
		print(f"and {len(bo)} {x} only in {dirb}")

		print(f"ignored {aign} items in {dira}")
		print(f"ignored {bign} items in {dirb}")

		if ao:
			print(f"only in {dira}")
			fpr(ao)
		if bo:
			print(f"only in {dirb}")
			fpr(bo)

	elif not any((corrupt, ao, bo)):
		print('no diff!')

def fpr(files: list = []):
	"""Prints files (maxmimum = 15).

	Args:
		files (list): File paths to print.

	Returns:
		None
	"""
	width: int = min(15, len(files))

	if files:
		for f in range(width):
			print(files[f])

def length():
	"""Counts and prints the lines in the listed directories.

	Args:
		None
	
	Returns:
		None
	"""
	dir_1: str = "/Volumes/HomeXx/compuir/rosa/rosa/lib"
	dir_2: str = "/Volumes/HomeXx/compuir/rosa/rosa/fxs"
	dir_3: str = "/Volumes/HomeXx/compuir/rosa/rosa/confs"

	dirs: list = [dir_1, dir_2, dir_3]
	rps: int = 0

	for dirn in dirs:
		pfx: int = len(dirn) + 1

		for file in _r(dirn):
			lines: int = 0
			with open(file, 'rb') as f:
				lines = sum(1 for line in f)
				rps += lines

	print(f"{rps} lines")

def size(dirx: str = ""):
	"""Counts and prints the size in a given directories.

	Args:
		dirx (str): Path to a given directory.
	
	Returns:
		None
	"""
	size: int = 0

	for file in _r(dirx):
		size: int += os.stat(file).st_size

	mb: int = 1024*1024

	print(f"{size:.2f} bytes ({size/mb:.2f} mb)")

def rm():
	"""Randomly removes, alters, and touches files in the C.W.D..

	Args:
		None

	Returns:
		None
	"""
	logger.info(f"[rm] executed")
	hasher = xxhash.xxh64()

	abs_path: str = os.getcwd()
	pfx: int = len(os.path.dirname(abs_path)) + 1

	if "rosa" in abs_path:
		logger.critical('not here!')
		sys.exit(11)

	item_no: int = 0
	altd: int = 0
	deld: int = 0
	tchd: int = 0

	if os.path.exists(abs_path):
		for item in _r(abs_path):
			if not is_ignored(item):
				item_no += 1
				if random.random() < 0.05:
					ext: str = os.path.splitext(item)[1]

					if ext in ['.txt', '.md', '.py', '.log', '.json']:
						try:
							with open(item, 'a', encoding='utf-8') as f:
								f.write("\nhello, world")
							altd += 1
						except Exception as e:
							logger.warning(f"Could not edit {item}: {e}")
						else:
							continue

				if random.random() < 0.01:
					try:
						os.remove(item)
						deld += 1
					except Exception as e:
						logger.warning(f"Could not delete {item}: {e}")
					else:
						continue

				if random.random() < 0.02:
					try:
						os.utime(item, None)
						tchd += 1
					except Exception as e:
						logger.warning(f"Could not touch {item}: {e}")
					else:
						continue
			else:
				continue
	else:
		logger.error('Local directory does not exist; repair the config or run "rosa get all".')
		sys.exit(0)

	logger.debug(f"found {item_no} items; altered {altd}, touched {tchd}, and deleted {deld} of them")

def main(args: argparse = None):
	"""Completes a task."""
	logger, force: bool, prints: bool, start: float = mini_ps(args, NOMIC)

	if args and args.redirect:
		action: str = args.redirect
	else:
		action: str = input("compare directories [c], count files' length [l], count directory size [s],...? ").lower()

	if action in ("c", "compare", "contrast"):
		compare_contrast()
	elif action in ("l", "len", "length"):
		length()
	elif action in ("s", "size", "sz"):
		size(os.getcwd())
	elif action == "rm":
		rm()
	elif action == "txt":
		from rosa.xtra import txt2
		txt2.main()

if __name__ == "__main__":
	main()