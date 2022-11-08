from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

from bson.objectid import ObjectId
from motor import motor_asyncio

from .config import settings

mongo_client = motor_asyncio.AsyncIOMotorClient(settings.MONGO_URI)
workers_coll = mongo_client['bili_liveroom']['workers']


def validate_workerid(func):
    @wraps(func)
    async def wrapper(worker_id: str, *args, **kwargs):
        if not ObjectId.is_valid(worker_id):
            raise Exception('invalid worker id')
        return await func(worker_id, *args, **kwargs)

    return wrapper


class Worker:
    class Status:
        CHECKED = {"checked": 1}

        @staticmethod
        def alive() -> dict[str, datetime]:
            return {"alive": datetime.utcnow()}

        @staticmethod
        def expired(seconds: int) -> dict[str, dict[str, datetime]]:
            return {
                "alive": {
                    "$lt": datetime.utcnow() - timedelta(seconds=seconds)
                }
            }

    @validate_workerid
    async def active(worker_id: str) -> bool:
        result = await workers_coll.update_one({
                "_id": ObjectId(worker_id)
            }, {
                "$set": Worker.Status.alive()
            },
            upsert=False
        )
        return result.matched_count == 1

    @validate_workerid
    async def is_checked(worker_id: str) -> bool:
        result = await workers_coll.update_one({
                "_id": ObjectId(worker_id)
            }, {
                "$set": Worker.Status.CHECKED
            }
        )
        return result.modified_count == 0

    @validate_workerid
    async def retrieve(worker_id: str) -> Optional[dict]:
        worker = await workers_coll.find_one({"_id": ObjectId(worker_id)})
        return worker

    async def add(worker_detail: str) -> ObjectId:
        result = await workers_coll.insert_one({
                "created": datetime.utcnow(),
                "detail": worker_detail,
                **Worker.Status.alive(),
                **Worker.Status.CHECKED,
                "length": 0,
                "rooms": [],
            },
        )
        return result.inserted_id

    async def remove_expired(seconds: int) -> int:
        result = await workers_coll.delete_many(Worker.Status.expired(seconds))
        return result.deleted_count
