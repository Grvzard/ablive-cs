import pymongo

from configs import Config


def get_client(cli_name):
    mongo_client = pymongo.MongoClient(Config.MONGO_CONFIG[cli_name])
    return mongo_client
