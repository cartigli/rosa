import sys
import logging
import datetime

from config import *
from rosa_lib import(
    scope_loc, scope_rem, ping_cass,
    contrast, compare,
    phone_duty #, init_conn
)

"""
Compare local data to server, report back.
"""


def ucheck(cherubs, serpents, stags, souls): # only contrast
    if cherubs:
        decis0 = input(f"Found {len(cherubs)} cherubs. Do you want details? y/n: ")
        if decis0 in ('yes', 'Yes', 'YES', 'y', 'Y', 'ye', 'Ye', 'yeah', 'Yeah', 'sure', 'Sure'):
            c = []
            [c.append(cherub['frp']) for cherub in cherubs]
            print(f"Cherubs ( In the server but not found locally ):\n{list(c)}")

        elif decis0 in ('n', 'N', 'no', 'No', 'NO', 'naw', 'Naw', 'NAW', 'hell naw', 'Hell naw', 'HELL NAW'):
            print('Heard.')
        else:
            logging.info('Unkown response, no details; proceding.')

    if serpents:
        decis1 = input(f"Found {len(serpents)} serpents. Do you want details? y/n: ")
        if decis1 in ('yes', 'Yes', 'YES', 'y', 'Y', 'ye', 'Ye', 'yeah', 'Yeah', 'sure', 'Sure'):
            s = []
            [s.append(serpent['frp']) for serpent in serpents]
            print(f"Serpents ( On the local disk but not in the server ):\n{list(s)}")

        elif decis1 in ('n', 'N', 'no', 'No', 'NO', 'naw', 'Naw', 'NAW', 'hell naw', 'Hell naw', 'HELL NAW'):
            print('Heard.')
        else:
            logging.info('Unkown response, no details; proceding.')
    
    if souls:
        decis2 = input(f"Found {len(souls)} souls. Do you want details? y/n: ")
        if decis2 in ('yes', 'Yes', 'YES', 'y', 'Y', 'ye', 'Ye', 'yeah', 'Yeah', 'sure', 'Sure'):
            ss = []
            [ss.append(soul['frp']) for soul in souls]
            print(f"Souls ( Files whose contents were altered. Human soul\'s natural state is transient, like water, buddy. ):\n{list(ss)}")            
        elif decis2 in ('n', 'N', 'no', 'No', 'NO', 'naw', 'Naw', 'NAW', 'hell naw', 'Hell naw', 'HELL NAW'):
            print('Heard.')
        else:
            logging.info('Unkown response, no details; proceding.')
    
    logging.info('Showing user file discrepancies completed.')
    

def udcheck(gates, caves):
    if caves:
        decis3 = input(f"Found {len(caves)} local-only directories. Do you want details? y/n: ")
        if decis3 in ('yes', 'Yes', 'YES', 'y', 'Y', 'ye', 'Ye', 'yeah', 'Yeah', 'sure', 'Sure'):
            print(f"Caves (directories on the disk but not seen in server): {caves}.")
        elif decis3 in ('n', 'N', 'no', 'No', 'NO', 'naw', 'Naw', 'NAW', 'hell naw', 'Hell naw', 'HELL NAW'):
            print('Bet.')
            pass
        else:
            print('Couldn\'nt catch that.')

    if gates:
        decis4 = input(f"Found {len(gates)} local-only directories. Do you want to see details? y/n: ")
        if decis4 in ('yes', 'Yes', 'YES', 'y', 'Y', 'ye', 'Ye', 'yeah', 'Yeah', 'sure', 'Sure'):
            print(f"Gates (directories on the disk but not seen in server): {gates}.")
        elif decis4 in ('n', 'N', 'no', 'No', 'NO', 'naw', 'Naw', 'NAW', 'hell naw', 'Hell naw', 'HELL NAW'):
            print('Bet.')
            pass
        else:
            print('Couldn\'nt catch that.')

    logging.info('Showing user directory discrepancies completed.')


# if __name__ == "__main__":
def main():
    logging.info('Rosa [contrast] executed.')
    start = datetime.datetime.now(datetime.UTC).timestamp()
    if start:
        logging.info('Timer started.')

    raw_hell, hell_dirs, abs_path = scope_loc(LOCAL_DIR)

    with phone_duty(DB_USER, DB_PSWD, DB_NAME, DB_ADDR) as conn:
        try:
            raw_heaven = scope_rem(conn)
            heaven_dirs = ping_cass(conn)

            relics = contrast(raw_heaven, raw_hell)
            hallowed = compare(heaven_dirs, hell_dirs)

            if any(relics) or any(hallowed):
                logging.info('Discrepancies found; showing to user.')

                if any(relics):
                    ucheck(*relics)

                if any(hallowed):
                    udcheck(*hallowed)
            else:
                print('No dif; All set.')

        except (ConnectionError, Exception) as e:
            logging.error(f"Exception occured while contrasting directories:{e}.", exc_info=True)
            raise

    if start:
        end = datetime.datetime.now(datetime.UTC).timestamp()
        proc_time = end - start

        logging.info(f"Processing time for rosa [contrast]: {proc_time}.")

    logging.info('[contrast] completed.')
    print('All set.')


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


if __name__=="__main__":
    init_logger()
    main()