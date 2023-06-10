import asyncio
import logging
import os
import time
from multiprocessing import Process
from threading import Thread

from ablive_client.rooms_worker import RoomsWorker
from ablive_client.store_dog import StoreDog
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


def new_worker_process(trd: int, buffer: dict):
    if THRD_PER_PROC == 1:
        worker_thread(buffer)
    else:
        th_list = []
        for _ in range(0, trd):
            th = Thread(target=worker_thread, args=(buffer,))
            th_list.append(th)
            th.start()

        [th.join() for th in th_list]


def worker_thread(buffer: dict):
    rooms_worker = RoomsWorker(
        detail = f'{MACHINE_ID}-{os.getpid()}',
        api_key = SERVER_API_KEY,
        add_room_interval = ADD_ROOM_INTERVAL,
        server_url = ABLIVE_SERVER_URL,
    )
    rooms_worker.init_packdog(buffer)
    while True:
        logger.info("new rooms-worker started")
        try:
            asyncio.run(rooms_worker.run())
        except Exception as e:
            logger.error(f'worker thread: {e}')
        time.sleep(5)


if __name__ == "__main__":
    store_dog = StoreDog(MY_DB_CONFIG)
    p_sd = Process(target=store_dog.run)
    p_sd.start()

    proc_needs = THRD_TOTAL
    while proc_needs > 0:
        trd_num: int
        if proc_needs >= THRD_PER_PROC:
            trd_num = THRD_PER_PROC
        else:
            trd_num = proc_needs

        proc_needs -= THRD_PER_PROC

        p = Process(target=new_worker_process, args=(trd_num, store_dog.buffer))
        p.start()

        time.sleep(90)

    p_sd.join()

    logger.error("over")
