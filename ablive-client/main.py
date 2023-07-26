import asyncio
import logging
import os
import time
from multiprocessing import Process
from threading import Thread

from ablive_client.rooms_worker import RoomsWorker
from ablive_client.packer import Packer
from configs import *

logging.basicConfig(
    format="[%(asctime)s][%(module)s] %(message)s"
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

_h = logging.FileHandler('error.log', encoding='utf-8')
_h.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
_h.setLevel(logging.ERROR)
logger.addHandler(_h)


def new_worker_process(trd: int, packer):
    if THRD_PER_PROC == 1:
        worker_thread(packer)
    else:
        th_list = []
        for _ in range(0, trd):
            th = Thread(target=worker_thread, args=(packer,))
            th_list.append(th)
            th.start()

        [th.join() for th in th_list]


def worker_thread(packer):
    rooms_worker = RoomsWorker(
        detail = f'{MACHINE_ID}-{os.getpid()}',
        api_key = SERVER_API_KEY,
        add_room_interval = ADD_ROOM_INTERVAL,
        server_url = ABLIVE_SERVER_URL,
    )

    rooms_worker.add_listener(packer)

    while True:
        logger.info("new rooms-worker started")
        try:
            asyncio.run(rooms_worker.run())
        except KeyboardInterrupt as e:
            break
        except Exception as e:
            logger.error(f'worker thread: {e}')
            time.sleep(5)


def main():
    packer = Packer(MY_DB_CONFIG)
    # store_dog = StoreDog(MY_DB_CONFIG)
    p_sd = Process(target=packer.run)
    p_sd.start()

    proc_needs = THRD_TOTAL
    while proc_needs > 0:
        trd_num: int
        if proc_needs >= THRD_PER_PROC:
            trd_num = THRD_PER_PROC
        else:
            trd_num = proc_needs

        proc_needs -= THRD_PER_PROC

        p = Process(target=new_worker_process, args=(trd_num, packer))
        p.start()

        time.sleep(90)

    p_sd.join()

    logger.error("over")


if __name__ == "__main__":
    main()
