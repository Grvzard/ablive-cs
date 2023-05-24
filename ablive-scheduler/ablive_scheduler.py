import time

from configs import Config
from rooms_scheduler import RoomsScheduler


if __name__ == '__main__':
    while True:
        rooms_scheduler = RoomsScheduler()
        rooms_scheduler.start()
        # rooms_scheduler.join()
        time.sleep(Config.SLEEP_RATE)
