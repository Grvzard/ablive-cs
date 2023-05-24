from abc import ABC
from typing import Any, Final

import aiohttp
from tenacity import retry, stop_after_delay, wait_exponential

__all__ = 'WebApi',


class BaseApi(ABC):
    _TIMEOUT = 10

    __slots__ = '_session',

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    @retry(
        reraise=True,
        stop=stop_after_delay(5),
        wait=wait_exponential(0.1),
    )
    async def _get(self, *args: Any, **kwds: Any):
        async with self._session.get(
            *args,
            **kwds,
            timeout=self._TIMEOUT,
        ) as res:
            json_res = await res.json()
            return json_res


class WebApi(BaseApi):
    # BASE_API_URL: Final[str] = 'https://api.bilibili.com'
    BASE_LIVE_API_URL: Final[str] = 'https://api.live.bilibili.com'

    GET_DANMU_INFO_URL: Final[str] = BASE_LIVE_API_URL + \
        '/xlive/web-room/v1/index/getDanmuInfo'
    GET_CHAT_CONF_URL: Final[str] = BASE_LIVE_API_URL + \
        '/room/v1/Danmu/getConf'

    __slots__ = ()

    async def get_danmu_info(self, room_id: int):
        params = {
            'id': room_id,
        }
        r = await self._get(self.GET_DANMU_INFO_URL, params=params)
        return r['data']

    async def get_chat_conf(self, room_id: int):
        params = {
            'room_id': room_id,
        }
        r = await self._get(self.GET_CHAT_CONF_URL, params=params)
        return r['data']
