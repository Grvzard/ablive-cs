import asyncio
from asyncio import Queue
import logging

import sqlalchemy as sa

from .blrec import Danmaku, DanmakuCommand, DanmakuListener
from .storer import Storer

logger = logging.getLogger(__name__)


class Packer(DanmakuListener):
    table_gen_map = {
        "ablive_en": lambda name: sa.Table(
            name,
            sa.MetaData(),
            sa.Column("ts", sa.Integer, nullable=False),
            sa.Column("uid", sa.BigInteger, nullable=False),
            sa.Column("uname", sa.String(64), nullable=False),
            sa.Column("liverid", sa.BigInteger, nullable=False),
            sa.Column("_id", sa.Integer, primary_key=True, autoincrement="auto"),
            mysql_engine='InnoDB',
            mysql_charset='utf8mb4',
        ),
        "ablive_dm": lambda name: sa.Table(
            name,
            sa.MetaData(),
            sa.Column("ts", sa.Integer, nullable=False),
            sa.Column("uid", sa.BigInteger, nullable=False),
            sa.Column("uname", sa.String(64), nullable=False),
            sa.Column("liverid", sa.BigInteger, nullable=False),
            sa.Column("text", sa.String(255), nullable=False),
            sa.Column("_id", sa.Integer, primary_key=True, autoincrement="auto"),
            mysql_engine='InnoDB',
            mysql_charset='utf8mb4',
        ),
        "ablive_gf": lambda name: sa.Table(
            name,
            sa.MetaData(),
            sa.Column("ts", sa.Integer, nullable=False),
            sa.Column("uid", sa.BigInteger, nullable=False),
            sa.Column("uname", sa.String(64), nullable=False),
            sa.Column("liverid", sa.BigInteger, nullable=False),
            sa.Column("gift_info", sa.String(127), nullable=False),
            sa.Column("gift_cost", sa.DECIMAL(7, 1), nullable=False),
            sa.Column("_id", sa.Integer, primary_key=True, autoincrement="auto"),
            mysql_engine='InnoDB',
            mysql_charset='utf8mb4',
        ),
        "ablive_sc": lambda name: sa.Table(
            name,
            sa.MetaData(),
            sa.Column("ts", sa.Integer, nullable=False),
            sa.Column("uid", sa.BigInteger, nullable=False),
            sa.Column("uname", sa.String(64), nullable=False),
            sa.Column("liverid", sa.BigInteger, nullable=False),
            sa.Column("text", sa.String(127), nullable=False),
            sa.Column("sc_price", sa.DECIMAL(7, 1), nullable=False),
            sa.Column("_id", sa.Integer, primary_key=True, autoincrement="auto"),
            mysql_engine='InnoDB',
            mysql_charset='utf8mb4',
        ),
    }

    def __init__(self, mysql_config):
        self.buffer: dict[str, Queue] = {}
        self.storers: list[Storer] = []
        self._tasks = []
        self._running = False

        for schema_name, table_gen in self.table_gen_map.items():
            buffer = Queue()
            self.buffer[schema_name] = buffer
            storer = Storer(mysql_config, table_gen, buffer, schema_name)
            self.storers.append(storer)

        self.buffer_dm = self.buffer['ablive_dm']
        self.buffer_en = self.buffer['ablive_en']
        self.buffer_gf = self.buffer['ablive_gf']
        self.buffer_sc = self.buffer['ablive_sc']

    async def run(self):
        if self._running:
            return
        self._running = True
        for s in self.storers:
            _task = s.run()
            self._tasks.append(_task)
            asyncio.create_task(_task)

    async def on_danmaku_received(self, danmu: Danmaku) -> None:
        cmd: str = danmu['cmd']

        try:
            if cmd.startswith(DanmakuCommand.DANMU_MSG.value):
                self.on_danmu_msg(danmu)
            elif cmd == DanmakuCommand.INTERACT_WORD.value:
                self.on_interact_word(danmu)
            # elif cmd == DanmakuCommand.ENTRY_EFFECT.value:
            #     self.on_entry_effect(danmu)
            elif cmd == DanmakuCommand.SEND_GIFT.value:
                self.on_send_gift(danmu)
            elif cmd == DanmakuCommand.USER_TOAST_MSG.value:
                self.on_user_toast_msg(danmu)
            elif cmd == DanmakuCommand.SUPER_CHAT_MESSAGE.value:
                self.on_super_chat_message(danmu)
            elif cmd == DanmakuCommand.USER_VIRTUAL_MVP.value:
                self.on_user_virtual_mvp(danmu)

        except Exception as e:
            logger.error(f'error in pack_dog: {e}')

    def on_interact_word(self, msg):
        liverid = msg['liverid']
        msg = msg['data']

        sql_data = {
            'ts': msg['timestamp'],
            'uid': msg['uid'],
            'uname': msg['uname'],
            'liverid': liverid,
        }

        self.buffer_en.put_nowait(sql_data)

    def on_entry_effect(self, msg):
        liverid = msg['liverid']
        msg = msg['data']

        ts = int(msg['trigger_time'] / 1000000000)
        uid = msg['uid']
        _uname = msg['copy_writing_v2'].split('%')[1]
        uname = f'[欢迎舰长]{_uname}'

        self.buffer_en.put_nowait(
            {
                'ts': ts,
                'uid': uid,
                'uname': uname,
                'liverid': liverid,
            }
        )

    def on_danmu_msg(self, msg):
        liverid = msg['liverid']
        msg = msg['info']

        text = msg[1]
        if isinstance(msg[0][13], dict):
            text = f'[{text}]'

        sql_data = {
            # 'ts': msg[0][4] // 1000,
            'ts': msg[9]['ts'],
            'uid': msg[2][0],
            'uname': msg[2][1],
            'text': text,
            'liverid': liverid,
        }

        self.buffer_dm.put_nowait(sql_data)

    def on_send_gift(self, msg):
        liverid = msg['liverid']
        msg = msg['data']
        # 不储存免费礼物
        if not msg['discount_price']:
            return

        sql_data = {
            'ts': msg['timestamp'],
            'uid': msg['uid'],
            'uname': msg['uname'],
            'liverid': liverid,
        }
        _gift_name = msg['giftName']
        _gift_num = msg['num']
        gift_info = f'{_gift_name}x{_gift_num}'
        gift_cost = (msg['discount_price'] * _gift_num) / 1000
        # 记录盲盒内容
        if msg['blind_gift']:
            # e.g. '[紫金宝盒]玫瑰x4'
            gift_info = '[%s]%s' % (msg['blind_gift']['original_gift_name'], gift_info)
            gift_cost = msg['total_coin'] / 1000

        self.buffer_gf.put_nowait(
            {
                **sql_data,
                'gift_info': gift_info,
                'gift_cost': gift_cost,
            }
        )

    def on_user_virtual_mvp(self, msg):
        liverid = msg['liverid']
        msg = msg['data']

        gift_info = f'[MVP]{msg["action"]}{msg["goods_name"]}x{msg["goods_num"]}'
        self.buffer_gf.put_nowait(
            {
                'ts': msg["timestamp"],
                'uid': msg["uid"],
                'uname': msg["uname"],
                'liverid': liverid,
                'gift_info': gift_info,
                'gift_cost': msg["goods_price"] / 1000,
            }
        )

    def on_super_chat_message(self, msg):
        liverid = msg['liverid']
        msg = msg['data']

        sql_data = {
            'ts': msg['ts'],
            'uid': msg['uid'],
            'uname': msg['user_info']['uname'],
            'liverid': liverid,
        }
        sc_price = msg['price']
        _sc_msg = msg['message']
        text = '[%ssc] %s' % (sc_price, _sc_msg)

        self.buffer_dm.put_nowait(
            {
                **sql_data,
                'text': text,
            }
        )

        self.buffer_gf.put_nowait(
            {
                **sql_data,
                'gift_info': '[SuperChat]',
                'gift_cost': sc_price,
            }
        )

        self.buffer_sc.put_nowait(
            {
                **sql_data,
                'text': msg['message'],
                'sc_price': sc_price,
            }
        )

    def on_user_toast_msg(self, msg):
        liverid = msg['liverid']
        msg = msg['data']

        _toast_msg = msg['toast_msg'].split('，')[-1]

        sql_data = {
            'ts': msg['start_time'],
            'uid': msg['uid'],
            'uname': msg['username'],
            'liverid': liverid,
            'gift_info': f'[大航海]{_toast_msg}',
            'gift_cost': msg['price'] / 1000,
        }

        self.buffer_gf.put_nowait(sql_data)
