import time

from mongodb import get_client
from configs import Config
from rooms_scheduler import RoomsScheduler


db = get_client('local')['bili_liveroom']


def update_hb(module_name):
    db['heartbeat'].update_one({
            "module": module_name
        }, {
            "$set": {"hb_ts": int(time.time())}
        },
        upsert=True
    )


if __name__ == '__main__':
    while True:
        rooms_scheduler = RoomsScheduler()
        rooms_scheduler.start()
        rooms_scheduler.join()
        update_hb('rooms_scheduler')
        time.sleep(Config.SLEEP_RATE)
