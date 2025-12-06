#!/usr/bin/env python3
import sys
import time
import logging
import xxhash
from pathlib import Path

if __name__!="__main__":
	from rosa.abilities.config import *
	from rosa.abilities.lib import(mini_ps, diffr,
		scope_loc, scope_rem, ping_cass,
		contrast, compare, init_logger, 
		doit_urself, phones, counter
	)

"""
Compare local data to server, report back.
"""

NOMIC = "[diff]"

def scope_rem2(conn): # all
	"""Select and return every single relative path and hash from the notes table. Returned as a list of tuples (rel_path, hash_id)."""
	q = "SELECT frp FROM notes;"
	logger = logging.getLogger()

	with conn.cursor() as cursor:
		try:
			logger.debug('...scoping remote files...')
			cursor.execute(q)
			raw_heaven = cursor.fetchall()
			if raw_heaven:
				logger.debug("server returned data from query")
			else:
				logger.warning("server returned raw_heaven as an empty set")

		except (mysql.connector.Error, ConnectionError, TimeoutError, Exception) as c:
			logger.error(f"err while getting data from server:{RESET} {c}.", exc_info=True)
			raise
		else:
			logger.debug('remote file scoping completed w.o exception')

	return raw_heaven



def contrast2(raw_heaven, raw_hell, conn): # unfiform for all scripts
	"""Fast"""
	logger = logging.getLogger('rosa.log')
	logger.info('contrastor started')

	local_dir = Path(LOCAL_DIR).resolve()
	hasher = xxhash.xxh64()

	cherubs = []
	serpents = []
	local_frps = []
	remote_frps = []

	local_frps = {d[0] for d in raw_hell}

	remote_frps = {s[0] for s in raw_heaven}

	cherubs = remote_frps - local_frps
	serpents = local_frps - remote_frps
	# cherubs_or_serpents = remote_frps ^ local_frps # frps in remote OR frps in local; none that exist in both

	people = remote_frps & local_frps # those in both - unaltered
	# logger.debug(f"found {len(people)} people [found in both] file[s]")
	souls = []
	stags = []


	logger.info(f"found {len(cherubs)} cherubs, {len(serpents)} serpents, and {len(people)} people. comparing each persons' hash now")

	angelic_id = {}
	# angelic_id = {{frp: idh for frp, idh in raw_[0]} for raw_ in raw_heaven}
	# angelic_id = {{frp: hash_id for frp, hash_id in raw_} for raw_ in raw_heaven}
	# angelic_id = {angelic_id.append({pair[0]: pair[1]}) for pair in raw_heaven}
	for pair in raw_heaven:
		frp = pair[0]
		hash_id = pair[1]
		angelic_id[frp] = hash_id

	demon_id = hash_loc2(people, local_dir)

	for soul in people:
		if demon_id.get(soul):
			if angelic_id.get(soul):
				if angelic_id[soul] == demon_id[soul]:
					stags.append(soul)
				else:
					souls.append(soul)
	
	logger.debug(f"found {len(souls)} souls [altered contents] and {len(stags)} stags [unchanged] file[s]")

	logger.debug('contrasted files and id\'d discrepancies')

	return cherubs, serpents, stags, souls # files in server but not present, files present not in server, files in both, files in both but with hash discrepancies


def hash_rem(conn, frp):
	try:
		with conn.cursor() as cursor:
			q = "SELECT hash_id FROM notes WHERE frp = %s;"
			cursor.execute(q, (frp,))
			hash_id = cursor.fetchone()
		return hash_id[0]
	except Exception as e:
		raise
	else:
		logger.debug('collected remote hash w.o exception')


def check(diff):
	logger = logging.getLogger()

	title = diff["type"]
	count = len(diff["details"])
	descr = diff["message"]
	dict_key = diff["key"]

	if count > 0:
		decis0 = input(f"found {count} {title} ({descr}). do you want details? y/n: ").lower()
		formatted = []
		if decis0 in ('yes', 'y', 'ye', 'yeah','sure', ' y', 'y '):
			c = []
			if dict_key == "frp":
				[c.append(item["frp"]) for item in diff["details"]]
			else:
				[c.append(item["drp"]) for item in diff["details"]]
			[formatted.append(f"\n{item}") for item in c]
			print(f".../{title} ({descr}):\n{''.join(formatted)}")

		elif decis0 in ('n', ' n', 'n ', 'no', 'naw', 'hell naw'):
			pass
		else:
			print('ok freak')

def hash_loc2(raw_paths, abs_path):
	hasher = xxhash.xxh64()
	demon_id = {}

	logger.debug('...hashing2...')

	with tqdm(raw_paths, unit="hashes", leave=True) as pbar:
		for item in pbar:
			path = Path(abs_path / item).resolve()
			if path.is_file():
				hasher.reset()

				hasher.update(path.read_bytes())
				hash_id = hasher.digest()

				frp = path.relative_to(abs_path).as_posix()

				demon_id[frp] = hash_id

	return demon_id

def main(args):
	start = time.perf_counter()

	diff = False
	mini = mini_ps(args)

	logger = mini[0]
	force = mini[1]
	prints = mini[2]

	with phones() as conn:
		logger.info('conn is connected; pinging heaven...')
		try:
			logger.info('...pinging heaven...')
			raw_heaven = scope_rem(conn)
			logger.info('...pinging cass...')
			heaven_dirs = ping_cass(conn)

			if any(raw_heaven) or any(heaven_dirs):
				logger.info('confirmed data was returned from heaven; processing...')

				raw_paths, hell_dirs, abs_path = scope_loc(LOCAL_DIR)

				# raw_hell = [(raw_path.relative_to(abs_path).as_posix(), '0') for raw_path in raw_paths]

				raw_hell = []
				[raw_hell.append((file.relative_to(abs_path).as_posix(), "imagine_a_hash"),) for file in raw_paths]

				# if any(relraw_paths):
					# logger.info('...data returned from local directory; hashing file[s] found...')
				# raw_hell = hash_loc(raw_paths, abs_path)

				logger.info("found file[s]; proceeding to compare & contrast...")

				# cherubs, souls, stags, serpents = contrast(raw_heaven, raw_hell)
				cherubs, serpents, stags, souls = contrast2(raw_heaven, raw_hell, conn)
				logger.info('file[s] contrasted')
				f_delta = [cherubs, serpents, souls]

				gates, caves, ledeux = compare(heaven_dirs, hell_dirs)
				# dir_data = compare(heaven_dirs, hell_dirs)
				logger.info('directory[s] compared')
				# d_delta = [dir_data[0], dir_data[1]]
				d_delta = [gates, caves]
				data = [f_delta, d_delta]
				# check(gates)
				if any(cherubs) or any(serpents) or any(souls) or any(gates) or any(caves):
					diff = True
					end = time.perf_counter()
					logger.info(f"[contrast_two] took: {end - start:.4f} seconds (diff=True)")

				else:
					logger.info('no dif')
			else:
				logger.info('no heaven data; have you uploaded?')
				sys.exit(1)

		except (ConnectionError, KeyboardInterrupt, Exception) as e:
			logger.error(f"{RED}err occured while contrasting directories:{RESET} {e}.", exc_info=True)
			sys.exit(1)

	if diff is True:
		try:
			if force is True:
				logger.info('-force skipped ask-to-show')
				pass
			else:
				if prints is False:
					logger.info('-silent skipped ask-to-show')
					# pass
				else:
					fdiff = []
					ddiff = []

					fdiff.append( {
						"type": "cherubs",
						"details": cherubs,
						"message": "files not found locally or not found remotely",
						"key": "frp"
						}
					)
					fdiff.append( {
						"type": "serpents",
						"details": serpents,
						"message": "files not found remotely or not found locally",
						"key": "frp"
						}
					)
					fdiff.append( {
						"type": "souls",
						"details": souls,
						"message": "files whose contents have been altered [hash discrepancies]",
						"key": "frp"
						}
					)

					ddiff.append( {
						"type": "gates",
						"details": gates,
						"message": "directories not found locally [only exist in the server]", 
						"key": "drp"
						}
					)
					ddiff.append( {
						"type": "caves",
						"details": caves, 
						"message": "directories not found in the server [local only]", 
						"key": "drp"
						}
					)

					for item in fdiff:
						check(item)

					for item in ddiff:
						check(item)

			tot = 0
			altered = 0
			unchanged = 0
			
			altered += len(cherubs) +len(serpents) + len(souls)
			unchanged += len(stags)
			tot += unchanged + altered

			fratio = (((tot - altered) / tot)*100)
			# print(fratio)
			# if fratio <= 0:
			if fratio >= 1:
				logger.info(f"{(100 - fratio):.4f}% failed verification (did not match your last upload)")
			else:
				logger.info(f"less than 1% of files were altered: {fratio:.3f}")
			# else:
				# logger.debug('0% change')

		except KeyboardInterrupt as ko:
			logger.error(f"boss killed it; wrap it up")
			sys.exit(1)

	else:
		logger.info('no dif')
	
	# doit_urself()

	counter(start, NOMIC)

	# if start:
	# 	end = time.perf_counter()
	# 	proc_time = end - start
	# 	if proc_time > 60:
	# 		mins = proc_time / 60
	# 		logger.info(f"total processing time [in minutes] for rosa [contrast]: {mins:.3f}")
	# 	else:
	# 		logger.info(f"total processing time [in seconds] for rosa [contrast]: {proc_time:.3f}")

	# logger.info('[diff] completed')

	if prints is True:
		print('All set.')


if __name__=="__main__":
	from config import *
	from lib import(mini_ps,
		scope_loc, hash_loc, scope_rem, ping_cass,
		contrast, compare, phone_duty, init_logger,
		counter
	)

	main(args)