import argparse

def get(args):
	from rosa.fxs import get
	get.main(args)

def give(args):
	from rosa.fxs import give
	give.main(args)

def init(args):
	from rosa.fxs import init
	init.main(args)

def diff(args):
	from rosa.fxs import diff
	diff.main(args)

def get_curr(args):
	from rosa.fxs import get_curr
	get_curr.main(args)

def get_vers(args):
	from rosa.fxs import get_vers
	get_vers.main(args)

def test(args):
	print('Hello, world.')

def rm(args):
	from rosa.xtra import rm3
	rm3.main(args)

def index(args):
	from rosa.fxs import init
	init.main(args)

rosa = {
	'get': {
		'func': get, 
		'name': "get", 
		'root_cmds': {
			'current': { # rosa get current
				'func': get_curr,
				'name': "current"
			},
			'diff': { # rosa get diff
				'func': diff, 
				'name': "diff"
			}, 
			'version': { # rosa get version
				'func': get_vers, 
				'name': "version" # get version or get vers ? It should be like 10 things tbh. Bad setup?
			}, # it needs its own arguments so i'll add a more specific call
			'test': { # test function
				'func': test, 
				'name': "test"
			}
		}
	}, 
	'give': {
		'func': give, 
		'name': "give"
	},
	'.': { # rosa . [==init]
		'func': init,
		'name': "."
	},
	'init': { # rosa init
		'func': init, 
		'name': "init"
	}, 
	'diff': { # rosa diff
		'func': diff, 
		'name': "diff"
	},
	'version': {
		'func': get_vers,
		'name': "version"
	},
	'vers': {
		'func': get_vers,
		'name': "vers"
	},
	'test': {
		'func': test, 
		'name': "test"
	},
	'rm': {
		'func': rm,
		'name': "rm"
	} # dumb xtra/ fx for testing how it handles changes n whatnot
}

arguments = {
	'silent': {
		'flag': "--silent",
		'shorthand': "-s",
		'action': "store_true",
		'help': "runs with logging_level set to critical; disables print statements"
	},
	'force': {
		'flag': "--force",
		'shorthand': "-f",
		'action': "store_true",
		'help': "bypasses all user checks & confirmations [show & confirm]"
	},
	'verbose': {
		'flag': "--verbose",
		'shorthand': "-v",
		'action': "store_true",
		'help': "runs with logging_level set to debug; enables print statements"
	},
	'remote': {
		'flag': "--remote",
		'shorthand': "-r",
		'action': "store_true",
		'help': "diff checks also ping the server for version verification"
	}
	# 'redirect': {
	# 	'flag': "--redirect",
	# 	'shorthand': "-rd",
	# 	'type': "str",
	# 	'action': "store_true",
	# 	'help': "choose a path besides the one in the config file"
	# }
}

def main():
	prt = argparse.ArgumentParser(add_help=False)

	for arg in arguments.values():
		prt.add_argument(arg['shorthand'], arg['flag'], action=arg['action'], help=arg['help'])
	
	prt.add_argument("-rd", "--redirect", type=str, help="point to a different directory than in the config")

	ops = argparse.ArgumentParser()
	sp = ops.add_subparsers(dest='rosa', required=True) 

	for fx in rosa.values():
		ps = 'ps_' + fx['name']

		ps = sp.add_parser(fx['name'], parents=[prt])
		ps.set_defaults(func=fx['func']) # get, diff, etc.,
		
		if fx.get('root_cmds'):
			sbp = ps.add_subparsers() # for all, few, etc.,
			for sub_cmd, sb_data in fx['root_cmds'].items():
				sub_parser = sbp.add_parser(sb_data['name'], parents=[prt])
				sub_parser.set_defaults(func=sb_data['func'])
	
	args = ops.parse_args()
	args.func(args)


if __name__=="__main__":
	main()