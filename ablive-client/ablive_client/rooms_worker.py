import asyncio
import logging

import aiohttp
from tenacity import retry, stop_after_attempt, stop_after_delay

from .blrec import DanmakuClient, DanmakuListener

logger = logging.getLogger(__name__)


class RoomsWorker:
    def __init__(
        self,
        detail: str,
        api_key: str,
        server_url: str,
        add_room_interval: float = 0.05,
    ):
        self.worker_id = ''
        self.REQUEST_HEADER = {'x-api-key': api_key}
        self.REG_DETAIL = {'detail': detail}
        self.SERVER_URL = server_url
        self.ADD_ROOM_INTERVAL = add_room_interval
        self.async_sem = asyncio.Semaphore(200)
        self.pack_dogs = set()

    def _renew_states(self) -> None:
        self.rooms = set()
        self._tasks = set()
        self.dc_dict = {}
        self._adjusting = 0

    def add_packdog(self, packer: DanmakuListener) -> None:
        self.pack_dogs.add(packer)

    @retry(reraise=True, stop=stop_after_attempt(2))
    async def _worker_reg(self) -> None:
        self._renew_states()
        async with self.ablive_session.post(
            self.SERVER_URL + '/reg',
            headers=self.REQUEST_HEADER,
            params=self.REG_DETAIL,
        ) as r:
            if r.status == 401:
                raise Exception('[reg] auth failed')
            resp = await r.json()
            if 'ok' in resp and resp['ok']:
                self.worker_id = resp['worker_id']
            else:
                raise Exception('[reg] unknown server error')

    @retry(reraise=True, stop=stop_after_delay(60))
    async def _heartbeat(self):
        async with self.ablive_session.request(
            'post' if self._adjusting else 'get',
            self.SERVER_URL + f'/{self.worker_id}',
            headers=self.REQUEST_HEADER,
        ) as r:
            resp = await r.json()
            return resp

    def compare_rooms(self, rooms: set[tuple[int, int]]):
        rooms_diff = {
            'inc': rooms - self.rooms,
            'dec': self.rooms - rooms,
        }
        return rooms_diff

    async def _adjust_rooms(self, rooms: list[list[int]]):
        self._adjusting = 1
        rooms_ = {tuple(room) for room in rooms}
        rooms_diff = self.compare_rooms(rooms_)

        try:
            for room in rooms_diff['dec']:
                await self.remove_room(room)
            await asyncio.sleep(1)

            for room in rooms_diff['inc']:
                await self.add_room(room)
                await asyncio.sleep(self.ADD_ROOM_INTERVAL)

            logger.info(
                f"rooms + {len(rooms_diff['inc'])} - {len(rooms_diff['dec'])}"
            )

        except Exception as e:
            raise e

        finally:
            self._adjusting = 0

    def _add_async_task(self, coro):
        _task = asyncio.create_task(coro)
        _task.add_done_callback(self._async_task_callback)
        self._tasks.add(_task)

    def _async_task_callback(self, fut):
        if e := fut.exception():
            logger.error(f"worker async task error: {e}")
        self._tasks.discard(fut)

    async def run(self):
        self.bili_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0)
        )
        self.ablive_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
            connector=aiohttp.TCPConnector(ssl=False)
        )

        try:
            await self._worker_reg()

            while True:
                resp = await self._heartbeat()
                if resp['ok']:
                    if 'rooms' in resp:
                        self._add_async_task(
                            self._adjust_rooms(resp['rooms'])
                        )
                    await asyncio.sleep(resp['interval'])
                else:
                    raise Exception('heartbeat error')

        except Exception as e:
            raise e

        finally:
            await self._adjust_rooms([])
            await self.bili_session.close()
            await self.ablive_session.close()

    async def add_room(self, room: tuple[int, int]):
        liverid, room_id = room
        dc = DanmakuClient(
            self.bili_session,
            liverid=liverid,
            room_id=room_id,
        )
        self.rooms.add(room)
        for packer in self.pack_dogs:
            dc.add_listener(packer)
        self.dc_dict[liverid] = dc
        # await dc._do_start()
        async with self.async_sem:
            self._add_async_task(dc._do_start())

    async def remove_room(self, room: tuple[int, int]):
        liverid = room[0]
        self.rooms.discard(room)
        dc = self.dc_dict.pop(liverid)
        # await dc._do_stop()
        async with self.async_sem:
            self._add_async_task(dc._do_stop())
