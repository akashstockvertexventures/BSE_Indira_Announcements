import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from config.settings import MONGO_URI
from core.logger import get_logger


class SharedResources:
    """Centralized shared MongoDB resource manager."""

    _mongo_client = None
    _async_mongo_client = None
    _logger = get_logger()

    # --- Sync Mongo Client ---
    @classmethod
    def get_mongo_client(cls):
        if cls._mongo_client is None:
            cls._logger.info(f"🧩 Initializing MongoDB client")
            cls._mongo_client = MongoClient(MONGO_URI)
        return cls._mongo_client

    # --- Async Mongo Client ---
    @classmethod
    def get_async_mongo_client(cls):
        if cls._async_mongo_client is None:
            cls._logger.info(f"⚙️ Initializing Async MongoDB client")
            cls._async_mongo_client = AsyncIOMotorClient(MONGO_URI)
        return cls._async_mongo_client
