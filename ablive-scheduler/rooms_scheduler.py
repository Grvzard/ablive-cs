import logging
from threading import Thread
from collections import deque
from contextlib import suppress
from typing import Union, TypeVar

import pymongo

from mongodb import get_client
from configs import Config
from fill_rooms_pool import fill_rooms_pool

__all__ = 'RoomsScheduler',


logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "[%(asctime)s] %(message)s"))
logger.addHandler(handler)


class RoomsWorker:
    __slots__ = '_id', 'rooms', 'length'

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
    """ Round-robin scheduling """
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
        Thread.__init__(self)
        self.coll = get_client('local')['bili_liveroom']['workers']
        self.workers: list[RoomsWorker] = None

    def do_schedule(self) -> None:
        logger.info("scheduler running")
        self._renew_workers()
        if not self.workers:
            logger.info("no worker...")
            return

        capacity = Config.ROOMS_PER_WORKER * self.workers_len
        rooms_pool = fill_rooms_pool()[:capacity]

        self._adjust_workers(set(rooms_pool))
        self._update_workers()
        logger.info(f"updated {self.workers_len} workers")

    def _renew_workers(self):
        _worker_docs = self.coll.find({})
        # self.workers = [RoomsWorker(worker_doc) for worker_doc in _worker_docs]
        self.workers = list(map(RoomsWorker, _worker_docs))
        self.workers_len = len(self.workers)

    def _adjust_workers(self, rooms_pool: set[tuple[int, int]]):
        for worker in self.workers:
            worker_rooms = worker.rooms.copy()
            for room_tup in worker_rooms:
                if room_tup not in rooms_pool:
                    worker.rooms_remove(room_tup)
                else:
                    rooms_pool.discard(room_tup)

        _workers = WorkerFeeder(self.workers)
        with suppress(StopIteration):
            for room_tup in rooms_pool:
                next(_workers).rooms_append(room_tup)

    def _update_workers(self):
        self.coll.bulk_write([
            pymongo.UpdateOne({
                    "_id": worker._id
                }, {
                    "$set": worker.to_dict()
                }
            )
            for worker in self.workers
            ],
            ordered=False
        )

    def run(self):
        self.do_schedule()
