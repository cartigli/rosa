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

def rm(args):
	from rosa.fxs import gen
	args.redirect = "rm"
	gen.main(args)

def gen(args):
	from rosa.fxs import gen
	gen.main(args)

def get_vers(args):
	from rosa.fxs import get_vers
	get_vers.main(args)

def get_curr(args):
	from rosa.fxs import get_curr
	get_curr.main(args)

rosa = {
	'get': { # rosa get
		'func': get, 
		'name': "get", 
		'root_cmds': {
			'current': { # rosa get current
				'func': get_curr,
				'name': "current"
			},
			'version': { # rosa get version
				'func': get_vers, 
				'name': "version"
			}
		}
	}, 
	'give': { # rosa give
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
	'version': { # rosa version
		'func': get_vers,
		'name': "version"
	},
	'rm': { # rosa rm
		'func': rm,
		'name': "rm"
	},
	'gen': { # rosa gen -r [options]
		'func': gen,
		'name': "gen"
	}
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
	}
}

def main():
	prt = argparse.ArgumentParser(add_help=False)

	for arg in arguments.values():
		prt.add_argument(arg['shorthand'], arg['flag'], action=arg['action'], help=arg['help'])

	ops = argparse.ArgumentParser()
	sp = ops.add_subparsers(dest='rosa', required=True) 

	for fx in rosa.values():
		ps = 'ps_' + fx['name']

		ps = sp.add_parser(fx['name'], parents=[prt])
		ps.set_defaults(func=fx['func']) # get, diff, etc.,

		if fx['name'] in("init", ".", "diff", "gen"):
			ps.add_argument("--redirect", "-r", type=str, help="give a path instead of C.W.D.")

		if fx['name'] == "diff":
			ps.add_argument("--extra", "-x", action="store_true", help="compare local and remote versions")

		if fx.get('root_cmds'):
			sbp = ps.add_subparsers() # for current, version

			for sub_cmd, sb_data in fx['root_cmds'].items():
				sub_parser = sbp.add_parser(sb_data['name'], parents=[prt])
				sub_parser.set_defaults(func=sb_data['func'])
	
	args = ops.parse_args()
	args.func(args)


if __name__=="__main__":
	main()