# Used by packer.Packer only

import asyncio
import logging
import time
from typing import Any, Callable

import sqlalchemy as sa
import databases

logger = logging.getLogger(__name__)


def get_date():
    return time.strftime("%Y_%m_%d", time.localtime())


def date2ts(date_):
    return int(time.mktime(time.strptime(date_, "%Y_%m_%d")))


class Storer:
    def __init__(
        self,
        mysql_config: dict,
        table_gen: Callable[[str], sa.Table],
        buffer: asyncio.Queue,
        name: str,
    ):
        self.mysql_config = mysql_config
        self.buffer = buffer
        self._table_gen = table_gen
        self.schema_name = name
        self._engine: sa.engine.Engine
        self._table: sa.Table

    async def run(self):
        await self.init_db()
        try:
            while True:
                await asyncio.gather(
                    self._store(),
                    asyncio.sleep(5),
                )
        except Exception as e:
            raise e

    async def init_db(self):
        self._db_instance = databases.Database(
            "mysql+asyncmy://%(user)s:%(password)s@%(host)s:%(port)s/"
            % self.mysql_config
            + self.schema_name,
        )
        await self._db_instance.connect()
        await self._new_table()

    async def _new_table(self):
        table = self._table_gen(get_date())
        self._table = table

        query = str(
            sa.schema.CreateTable(table, if_not_exists=True).compile(
                dialect=sa.dialects.mysql.dialect()
            )
        )
        await self._db_instance.execute(query)

    async def _store(self) -> None:
        time_near = int(time.time()) % 86400
        if (time_near < 4) or (time_near > (86400 - 4)):
            logger.info('time is near 00:00, skip a round')
            return

        buffer = self.buffer
        rows_cnt = buffer.qsize()
        rows = [buffer.get_nowait() for _ in range(rows_cnt)]

        if get_date() == self._table.name:
            await self._insert(rows)
        else:
            min_ts = date2ts(self._table.name) + 86400
            prev_rows = []
            next_rows = []
            for row in rows:
                if row['ts'] < min_ts:
                    prev_rows.append(row)
                else:
                    next_rows.append(row)
            await self._insert(prev_rows)
            await self._new_table()
            await self._insert(next_rows)

        logger.info(f"{self.schema_name} + {rows_cnt}")

    async def _insert(self, rows: list[dict[str, Any]]):
        if not rows:
            return
        await self._db_instance.execute_many(self._table.insert(), rows)
