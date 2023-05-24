
TESTING = True


class Config:
    ROOMS_PER_WORKER = 50
    SLEEP_RATE = 900
    MONGO_CONFIG = {
        "remote_main": "mongodb://localhost:27017/",
    }
    if TESTING:
        MONGO_CONFIG["local"] = "mongodb://localhost:27017/"
    else:
        MONGO_CONFIG["local"] = MONGO_CONFIG["remote_main"]

    AREA_WEIGHT = {
        '虚拟主播': 1,
        '电台': 1,
        '娱乐': 1,
        '生活': 1,
        '单机游戏': 1,
        '手游': 1,
        '网游': 1,
        '赛事': 1,
        '学习': 1,  # 学习区已经变成知识区
        '知识': 1,
    }
