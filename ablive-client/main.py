import asyncio
import logging
import os
import time
from multiprocessing import Process

from ablive_client.rooms_worker import RoomsWorker
from ablive_client.packer import Packer
from ablive_client.configs import Settings

settings = Settings() # type: ignore


def config_logging():
    _h1 = logging.FileHandler('error.log', encoding='utf-8')
    _h1.setLevel(logging.WARN)
    _h2 = logging.StreamHandler()
    _h2.setLevel(logging.INFO)
    logging.basicConfig(
        format="[%(asctime)s][%(module)s] %(message)s",
        level=logging.INFO,
        handlers=[_h1, _h2]
    )

config_logging()
logger = logging.getLogger(__name__)


def new_worker_process():
    asyncio.run(worker_thread())


async def worker_thread():
    rooms_worker = RoomsWorker(
        detail = f'{settings.machine_id}-{os.getpid()}',
        api_key = settings.server_api_key,
        add_room_interval = settings.add_room_interval,
        server_url = settings.server_url,
    )

    packer = Packer(settings.packer1_mysql_dsn)
    rooms_worker.add_packer(packer)
    await packer.run()

    while True:
        logger.info("new rooms-worker started")
        try:
            await rooms_worker.run()
        except KeyboardInterrupt as _:
            logger.warn("worker thread stoped")
            break
        except Exception as e:
            logger.error(f'worker thread: {e}')
            # time.sleep(5)
            await asyncio.sleep(5)


def main():
    p_list = []

    for _ in range(settings.workers_num):
        p = Process(target=new_worker_process)
        p.start()
        p_list.append(p)

        time.sleep(90)

    # p_sd.join()
    [p.join() for p in p_list]

    logger.error("over")


if __name__ == "__main__":
    main()
