import pymongo

from configs import Config


def get_client(cli_name: str) -> pymongo.MongoClient:
    return pymongo.MongoClient(Config.MONGO_CONFIG[cli_name])
