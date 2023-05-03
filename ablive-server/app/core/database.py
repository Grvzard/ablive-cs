from motor import motor_asyncio

from .config import settings

mongo_client = motor_asyncio.AsyncIOMotorClient(settings.MONGO_URI)
