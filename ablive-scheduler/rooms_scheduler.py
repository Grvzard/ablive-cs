import logging
from threading import Thread
from collections import deque
from contextlib import suppress
import time
from typing import Union

import pymongo

from configs import Config
from fill_rooms_pool import fill_rooms_pool

__all__ = ('RoomsScheduler',)


logger = logging.getLogger(__name__)
logger.setLevel("ERROR")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
logger.addHandler(handler)


class RoomsWorker:
    __slots__ = ('_id', 'rooms', 'length')

    def __init__(self, worker_doc: dict):
        self._id = worker_doc['_id']
        self.rooms = set(map(tuple, worker_doc['rooms']))
        self.length = len(self.rooms)

    def rooms_append(self, room: Union[list, tuple]):
        self.rooms.add(tuple(room))
        self.length += 1

    def rooms_remove(self, room: Union[list, tuple]):
        self.rooms.discard(tuple(room))
        self.length -= 1

    def to_dict(self):
        return {
            'checked': 0,
            'length': self.length,
            'rooms': list(self.rooms),
        }


class WorkerFeeder:
    """Round-robin scheduling"""

    def __init__(self, workers: list[RoomsWorker]) -> None:
        self.workers = deque(workers)

    @property
    def is_empty(self) -> bool:
        return len(self.workers) == 0

    def __next__(self) -> RoomsWorker:
        if self.is_empty:
            raise StopIteration()
        else:
            worker = self.workers[0]
            if worker.length >= Config.ROOMS_PER_WORKER:
                self.workers.popleft()
                return next(self)
            self.workers.rotate(-1)
            return worker


class RoomsScheduler(Thread):
    def __init__(self):
        super().__init__()
        self.db = pymongo.MongoClient(Config.MONGO_CONFIG['local'])['bili_liveroom']

    def do_schedule(self) -> None:
        logger.info("scheduler running")
        # _renew_workers
        workers = [RoomsWorker(_) for _ in self.db['workers'].find({})]
        if not workers:
            logger.info("no worker...")
            return

        capacity = Config.ROOMS_PER_WORKER * len(workers)
        rooms_pool = fill_rooms_pool()[:capacity]

        RoomsScheduler.AdjustWorkers(workers, set(rooms_pool))
        self._update_workers(workers)
        logger.info(f"updated {len(workers)} workers")

    @staticmethod
    def AdjustWorkers(workers: list[RoomsWorker], rooms_pool: set[tuple[int, int]]):
        for worker in workers:
            worker_rooms = worker.rooms.copy()
            for room_tup in worker_rooms:
                if room_tup not in rooms_pool:
                    worker.rooms_remove(room_tup)
                else:
                    rooms_pool.discard(room_tup)

        _workers = WorkerFeeder(workers)
        with suppress(StopIteration):
            for room_tup in rooms_pool:
                next(_workers).rooms_append(room_tup)

    def _update_workers(self, workers):
        self.db["workers"].bulk_write(
            [
                pymongo.UpdateOne({"_id": worker._id}, {"$set": worker.to_dict()})
                for worker in workers
            ],
            ordered=False,
        )
        roomid_recording = [
            room_tup[1] for worker in workers for room_tup in worker.rooms
        ]
        self.db["status"].update_one(
            {"name": "rooms_recording"},
            {"$set": {"value": roomid_recording}},
            upsert=True,
        )
        self._update_hb('rooms_scheduler')

    def _update_hb(self, module_name: str):
        tstamp = int(time.time())
        clock = time.strftime("%H:%M:%S", time.localtime())
        self.db['heartbeat'].update_one(
            {"module": module_name},
            {"$set": {"hb_ts": tstamp, "hb": clock}},
            upsert=True,
        )

    def run(self):
        try:
            self.do_schedule()
        except Exception as exc:
            logger.error(f"[error] {exc!r}")


if __name__ == "__main__":
    RoomsScheduler().do_schedule()
