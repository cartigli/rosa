import sys
import argparse
import logging
from rosa.abilities.config import LOGGING_LEVEL


def giver():
    from rosa.abilities import give
    give.main()

def getter():
    from rosa.abilities import get
    get.main()

def initir():
    from rosa.abilities import init
    init.main()

def contrastor():
    from rosa.abilities import contrast
    contrast.main()

def get_all():
    from rosa.abilities import get_all
    get_all.main()

def give_all():
    from rosa.abilities import give_all
    give_all.main()

def moment():
    from rosa.abilities import rosa_get_moment
    rosa_get_moment.main()

def get_test():
    import os
    import time
    import shutil
    from pathlib import Path
    from rosa.abilities import get_all
    from rosa.abilities.config import LOCAL_DIR

    tdir = Path(LOCAL_DIR).resolve()
    for i in range(7):
        if tdir.exists():
            if tdir.is_file():
                os.unlink(tdir)
            if tdir.is_dir():
                shutil.rmtree(tdir)
        b = time.perf_counter()
        get_all.main()
        e = time.perf_counter()
        print(f"{e - b} seconds for get_all (with counting compression or mb/s).")
        time.sleep(15)


def init_logger():
    f_handler = logging.FileHandler('rosa.log', mode='a')
    f_handler.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler()
    cons_handler.setLevel(LOGGING_LEVEL.upper())

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[f_handler, cons_handler]
    )

def main():
    init_logger()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='rosa', required=True)

    # parser_give = subparsers.add_parser('give') # rosa give
    # parser_give.set_defaults(func=giver)

    # give_subparsers = parser_give.add_subparsers()

    parser_get = subparsers.add_parser('get') # rosa get
    parser_get.set_defaults(func=getter)

    get_subparsers = parser_get.add_subparsers() # sub commands under 'rosa 'get''

    parser_get_all = get_subparsers.add_parser('all') # rosa get all
    parser_get.set_defaults(func=get_all)

    parser_get_diff = get_subparsers.add_parser('diff') # rosa get diff
    parser_get_diff.set_defaults(func=contrastor)

    parser_get_test = get_subparsers.add_parser('test') # rosa get diff
    parser_get_test.set_defaults(func=get_test)

    parser_get_moment = get_subparsers.add_parser('moment') # rosa get moment
    parser_get_moment.set_defaults(func=moment)

    parser_init = subparsers.add_parser('init') # rosa init
    parser_init.set_defaults(func=initir)

    parser_contrast = subparsers.add_parser('contrast') # rosa contrast ( == get diff )
    parser_contrast.set_defaults(func=contrastor)

    parser_get_all = subparsers.add_parser('get_all') # rosa get_all
    parser_get_all.set_defaults(func=get_all) # also, rosa get all

    parser_give = subparsers.add_parser('give') # rosa give
    parser_give.set_defaults(func=giver)

    give_subparsers = parser_give.add_subparsers() # rosa give subcommands (args)

    parser_give_all = give_subparsers.add_parser('all') # rosa give all
    parser_give_all.set_defaults(func=give_all)

    parser_give_all = subparsers.add_parser('give_all') # rosa give_all
    parser_give_all.set_defaults(func=give_all)

    parser_get_moment = subparsers.add_parser('get_moment') # rosa get_moment
    parser_get_moment.set_defaults(func=moment) # also, rosa get moment

    # parser_get_test = subparsers.add_parser('

    args = parser.parse_args()
    args.func() # if something needed to be passed to the scripts from the cmd, 
    # args.func(args) would go here

if __name__=="__main__":
    main()