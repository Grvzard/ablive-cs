import json
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


def read_json(filename: str):
    with open(filename, encoding='utf-8') as config:
        return json.load(config)


schemas_def = read_json('ablive_schema.json')


class StoreDog:
    def __init__(self, mysql_config: dict):
        self.mysql_config = mysql_config
        self.buffer: dict[str, Queue] = {}
        self.schema: dict[str, Schema] = {}
        self.engine: dict[str, sa.engine.Engine] = {}
        self.table: dict[str, Table] = {}
        for schema_name, schema_schema in schemas_def.items():
            self.buffer[schema_name] = Queue()
            self.schema[schema_name] = Schema(**schema_schema)

    def run(self):
        self.init_db()
        with ThreadPoolExecutor() as pool:
            while True:
                pool.map(self._store, self.schema.keys())
                time.sleep(5)

    def init_db(self):
        for schema in self.schema.keys():
            self.engine[schema] = create_engine(
                "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/"
                % self.mysql_config
                + schema,
                pool_size=3,
                max_overflow=10,
                pool_recycle=1800,
            )
            self._new_table(schema)

    def _new_table(self, schema: str):
        engine = self.engine[schema]
        table = Table(
            name=get_date(),
            schema=self.schema[schema],
            rows_type='sqlalchemy',
        )
        self.table[schema] = table
        with Session(engine) as session:
            session.execute(text(table.sql_create))
            session.commit()

    def _store(self, schema: str) -> None:
        time_near = int(time.time()) % 86400
        if (time_near < 4) or (time_near > (86400 - 4)):
            logger.info('time is near 00:00, skip a round')
            return

        buffer = self.buffer[schema]
        rows_cnt = buffer.qsize()
        rows = [buffer.get() for _ in range(rows_cnt)]

        table = self.table[schema]
        if get_date() != table.name:
            min_ts = date2ts(table.name) + 86400
            prev_rows = []
            next_rows = []
            for row in rows:
                if row['ts'] < min_ts:
                    prev_rows.append(row)
                else:
                    next_rows.append(row)
            self._insert(schema, prev_rows)
            self._new_table(schema)
            self._insert(schema, next_rows)
        else:
            if rows:
                self._insert(schema, rows)

        logger.info(f"{schema} + {rows_cnt}")

    def _insert(self, schema: str, rows: list[dict[str, Any]]):
        if not rows:
            return
        engine = self.engine[schema]
        table = self.table[schema]
        try:
            with Session(engine) as session:
                session.execute(text(table.sql_insert), rows)
                session.commit()
        except Exception as e:
            logger.error(e)
