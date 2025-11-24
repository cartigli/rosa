import sys
import argparse
import logging
from rosa.abilities.config import LOGGING_LEVEL

# from rosa.abilities import give, get
# from rosa.abilities import config

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

    parser_give = subparsers.add_parser('give') # rosa give
    parser_give.set_defaults(func=giver)

    parser_get = subparsers.add_parser('get') # rosa get
    parser_get.set_defaults(func=getter)

    parser_init = subparsers.add_parser('init') # rosa init
    parser_init.set_defaults(func=initir)

    parser_contrast = subparsers.add_parser('contrast') # rosa contrast
    parser_contrast.set_defaults(func=contrastor)

    parser_get_all = subparsers.add_parser('get_all') # rosa get all
    parser_get_all.set_defaults(func=get_all)

    args = parser.parse_args()

    args.func() # if something needed to be passed to the scripts from the cmd, 
    # args.func(args) would go here

if __name__=="__main__":
    main()