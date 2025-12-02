import argparse

def get(args):
	from rosa.abilities import get
	get.main(args)

def give(args):
	from rosa.abilities import give
	give.main(args)

def init(args):
	from rosa.abilities import init
	init.main(args)

def diff(args):
	from rosa.abilities import contrast
	contrast.main(args)

def get_all(args):
	from rosa.abilities import get_all
	get_all.main(args)

def few(args):
	from rosa.abilities import get_surg
	get_surg.main(args)

def moment(args):
	from rosa.abilities import rosa_get_moment
	rosa_get_moment.main(args)

def give_all(args):
	from rosa.abilities import give_all
	give_all.main(args)

def test(args):
	print('Hello, world.')

rosa = {
	'get': {
		'func': get, 
		'name': 'get', 
		'root_cmds': {
			'all': {
				'func': get_all, 
				'name': 'all'
			}, 
			'few': {
				'func': few, 
				'name': 'few'
			}, 
			'diff': {
				'func': diff, 
				'name': 'diff'
			}, 
			'moment': {
				'func': moment, 
				'name': 'moment'
			}, 
			'test': {
				'func': test, 
				'name': 'test'
			}
		}
	}, 
	'give': {
		'func': give, 
		'name': 'give',
		'root_cmds': {
			'all': {
				'func': give_all, 
				'name': 'all'
			}, 
			'structure': {
				'func': init, 
				'name': 'structure'
			}
		}
	}, 
	'init': {
		'func': init, 
		'name': 'init'
	}, 
	'diff': {
		'func': diff, 
		'name': 'diff'
	},
	'test': {
		'func': test, 
		'name': 'test'
	}
}

arguments = {
	'silent': {
		'flag': '--silent',
		'shorthand': '-s',
		'action': "store_true",
		'help': "runs with logging_level set to critical"
	},
	'force': {
		'flag': '--force',
		'shorthand': '-f',
		'action': "store_true",
		'help': "bypasses all user checks & confirmations; can be damaging if not careful"
	},
	'verbose': {
		'flag': '--verbose',
		'shorthand': '-v',
		'action': "store_true",
		'help': "runs with logging_level set to debug"
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
		
		if fx.get('root_cmds'):
			sbp = ps.add_subparsers() # for all, few, etc.,
			for sub_cmd, sb_data in fx['root_cmds'].items():
				sub_parser = sbp.add_parser(sb_data['name'], parents=[prt])
				sub_parser.set_defaults(func=sb_data['func'])
	
	args = ops.parse_args()
	args.func(args)





def ex_main():
	ps = argparse.ArgumentParser()
	sp = ps.add_subparsers(dest='rosa', required=True)


	# SHALLOWS
	ps_init = sp.add_parser('init') # [rosa][init] parser
	ps_init.set_defaults(func=init) # [rosa][init] default func

	ps_contrast = sp.add_parser('diff') # [rosa][diff] parser
	ps_contrast.set_defaults(func=diff) # [rosa][diff] default func
	ps_contrast.add_argument("-s", "--silent", action="store_true", help="runs silently; no ask/checks.")
	ps_contrast.add_argument("-fq", "--force-quiet", action="store_true", help="runs silenetly + no crit logging.")

	# GET PARSER & SUBPARCER
	ps_get = sp.add_parser('get') # [rosa get] parser
	ps_get.set_defaults(func=get) # [rosa get] default func

	get_sp = ps_get.add_subparsers() # sub cmds de [rosa get][...]

	ps_get_all = get_sp.add_parser('all') # [rosa get][all] parser
	ps_get_all.set_defaults(func=get_all) # [rosa get][all] default func

	ps_get_few = get_sp.add_parser('few') # [rosa get][few] parser
	ps_get_few.set_defaults(func=few) # [rosa get][few] default func

	ps_get_diff = get_sp.add_parser('diff') # [rosa get][diff] parser
	ps_get_diff.set_defaults(func=diff) # [rosa get][diff] default func
	ps_get_diff.add_argument("-s", "--silent", action="store_true", help="runs silently; no ask/checks.")
	ps_get_diff.add_argument("-fq", "--force-quiet", action="store_true", help="runs silenetly + no crit logging.")

	ps_get_test = get_sp.add_parser('test') # [rosa get][test] parser
	ps_get_test.set_defaults(func=get_test) # [rosa get][test] default func

	ps_get_moment = get_sp.add_parser('moment') # [rosa get][moment] parser
	ps_get_moment.set_defaults(func=moment) # [rosa get][moment] default func

	# GIVE PARSER & SUBPARCER
	ps_give = sp.add_parser('give') # [rosa][give] parser
	ps_give.set_defaults(func=give) # [rosa][give] default func

	give_sp = ps_give.add_subparsers() # sub cmds de [rosa give][...]

	ps_give_all = give_sp.add_parser('all') # [rosa give][all] parser
	ps_give_all.set_defaults(func=give_all) # [rosa give][all] default func

	ps_give_struc = give_sp.add_parser('structure') # [rosa give][structure] parser
	ps_give_struc.set_defaults(func=init) # [rosa give][structure] default func

	args = ps.parse_args()

	if hasattr(args, 'func'):
		args.func(args)

if __name__=="__main__":
	main()