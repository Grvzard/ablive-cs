# Used by packer.Packer only

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
from typing import Any

import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

# from .models import *
from .table_schema import Schema, Table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_date():
    return time.strftime("%Y_%m_%d", time.localtime())


def date2ts(date_):
    return int(time.mktime(time.strptime(date_, "%Y_%m_%d")))


class Storer:
    def __init__(self, mysql_config: dict, schema: Schema, buffer: Queue, name):
        self.mysql_config = mysql_config
        self.buffer = buffer
        self.schema = schema
        self.schema_name = name
        self._engine: sa.engine.Engine
        self._table: Table

    def run(self):
        self.init_db()
        with ThreadPoolExecutor() as pool:
            while True:
                pool.submit(self._store)
                time.sleep(5)

    def init_db(self):
        self._engine = create_engine(
            "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/"
            % self.mysql_config
            + self.schema_name,
            pool_size=3,
            max_overflow=10,
            pool_recycle=1800,
        )
        self._new_table()

    def _new_table(self):
        engine = self._engine
        table = Table(
            name=get_date(),
            schema=self.schema,
            rows_type='sqlalchemy',
        )
        self._table = table
        with Session(engine) as session:
            session.execute(text(table.sql_create))
            session.commit()

    def _store(self) -> None:
        time_near = int(time.time()) % 86400
        if (time_near < 4) or (time_near > (86400 - 4)):
            logger.info('time is near 00:00, skip a round')
            return

        buffer = self.buffer
        rows_cnt = buffer.qsize()
        rows = [buffer.get() for _ in range(rows_cnt)]

        table = self._table
        if get_date() != table.name:
            min_ts = date2ts(table.name) + 86400
            prev_rows = []
            next_rows = []
            for row in rows:
                if row['ts'] < min_ts:
                    prev_rows.append(row)
                else:
                    next_rows.append(row)
            self._insert(prev_rows)
            self._new_table()
            self._insert(next_rows)
        else:
            if rows:
                self._insert(rows)

        logger.info(f"{self.schema_name} + {rows_cnt}")

    def _insert(self, rows: list[dict[str, Any]]):
        if not rows:
            return
        engine = self._engine
        table = self._table
        try:
            with Session(engine) as session:
                session.execute(text(table.sql_insert), rows)
                session.commit()
        except Exception as e:
            logger.error(e)
