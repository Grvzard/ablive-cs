
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
