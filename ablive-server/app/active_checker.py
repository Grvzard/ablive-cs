import asyncio
import logging

from app.crud.worker import Worker

logger = logging.getLogger(__name__)


async def active_checker():
    while True:
        await asyncio.sleep(60)
        del_cnt = await Worker.remove_expired(60)
        logger.debug(f'active checker removed {del_cnt} workers')
