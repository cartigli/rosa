import os
import sys
import xxhash
import random
from pathlib import Path

BLACKLIST = ['.index', '.git', '.obsidian', '.vscode', '.DS_Store', '.pyc', '.db']

def is_ignored(_str):
	return any(blckd in _str for blckd in BLACKLIST)

def r(dir_):
	"""Recursive function for directory's contents.

	Args:
		dir_ (str): Path to a directory.

	Yields:
		obj (str): Path to a object found.
	"""
	cnt = 0
	for obj in os.scandir(dir_):
		yield obj.path

		if os.path.isdir(obj):
			yield from r(obj.path)

def rf(dir_):
	"""Recursive function for directory's files.

	Args:
		dir_ (str): Path to a directory.

	Yields:
		obj (str): Path to a file found.
	"""
	for obj in os.scandir(dir_):
		if os.path.isdir(obj):
			yield from rf(obj.path)
		else:
			yield obj.path

def contrast(dir1, dir2):
	corrupt = []

	i1, ign1 = inv(dir1)
	i2, ign2 = inv(dir2)

	s1 = set(i1.keys())
	s2 = set(i2.keys())

	ld = s1 & s2

	for l in ld:
		if i1[l] != i2[l]:
			corrupt.append(l)
	
	o1 = s1 - s2
	o2 = s2 - s1
	
	return corrupt, list(o1), ign1, list(o2), ign2

def inv(dirx):
	hasher = xxhash.xxh64()
	ign = 0
	a = {}

	pfx = len(dirx) + 1
	for file in rf(dirx):
		if is_ignored(file):
			ign += 1
			continue
		hasher.reset()

		with open(file, 'rb') as f:
			content = f.read()

		hasher.update(content)
		hashx = hasher.digest()

		path = file[pfx:]

		a[path] = hashx
	
	return a, ign

def compare_contrast():
	root = "/Volumes/HomeXx/compuir"

	dirb = input('enter the path of the alt directory: ')
	if not dirb:
		return

	dirfa = os.getcwd()
	cut = len(root) + 1
	dira = dirfa[cut:]
	dirfb = os.path.join(root, dirb)

	corrupt, ao, aign, bo, bign = contrast(dirfa, dirfb)
	x = "files"

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

def fpr(files):
	if files:
		for f in range(15):
			print(files[f])

def length():
	dir_1 = "/Volumes/HomeXx/compuir/rosa/rosa/lib"
	dir_2 = "/Volumes/HomeXx/compuir/rosa/rosa/fxs"
	dir_3 = "/Volumes/HomeXx/compuir/rosa/rosa/confs"

	dirs = [dir_1, dir_2, dir_3]
	rps = 0

	for dirx in dirs:
		pfx = len(dirx) + 1

		for file in rf(dirx):
			lines = 0
			with open(file, 'rb') as f:
				lines = sum(1 for line in f)
				rps += lines

	print(f"{rps} lines")

def size(dirx):
	size = 0

	for file in rf(dirx):
		stats = os.stat(file).st_size
		size += stats

	mb = 1024*1024

	print(f"{size:.2f} bytes ({size/mb:.2f} mb)")

def rm():
	print(f"[rm] executed")
	hasher = xxhash.xxh64()

	local_dir = Path.cwd()
	abs_path = Path(local_dir).resolve()

	if abs_path.name == "rosa":
		print('not here!')
		sys.exit(11)

	dels = []
	alts = []

	item_no = 0
	altd = 0
	deld = 0
	tchd = 0

	if abs_path.exists():
		for item in abs_path.rglob('*'):
			path_str = item.resolve().as_posix()
			if not is_ignored(path_str):
				if any(blocked in item.as_posix() for blocked in BLACKLIST):
					continue

				elif item.is_file():
					item_no += 1
					if random.random() < 0.05: 
						if item.suffix.lower() in ['.txt', '.md', '.py', '.log', '.json']:
							try:
								with open(item, 'a', encoding='utf-8') as f:
									f.write("\nhello, world")
								altd += 1
								alts.append(path_str)
							except Exception as e:
								print(f"Could not edit {item.name}: {e}")

					if random.random() < 0.01:
						try:
							item.unlink()
							deld += 1
							dels.append(item.as_posix())
						except Exception as e:
							print(f"Could not delete {item.name}: {e}")

					elif random.random() < 0.02:
						item.touch()
						tchd += 1
				else:
					continue
	else:
		print('Local directory does not exist; repair the config or run "rosa get all".')
		sys.exit(0)

	print(f"found {item_no} items; altered {altd}, touched {tchd}, and deleted {deld} of them")
	print(f"altered the following files:")
	for a in alts:
			print(a)
	print(f"deleted the following files:")
	for d in dels:
		print(d)

def main(args=None):
	if args and args.redirect:
		action = args.redirect
	else:
		action = input("compare directories [c], count files' length [l], count directory size [s],...? ").lower()

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