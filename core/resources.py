import asyncio
from sentence_transformers import SentenceTransformer
from motor.motor_asyncio import AsyncIOMotorClient
from config.settings import MONGO_URI
from config.constants import DEVICE
from core.logger import get_logger

logger = get_logger()

class SharedResources:
    _mongo_client = None
    _embedding_model = None

    @classmethod
    def get_mongo_client(cls):
        if cls._mongo_client is None:
            cls._mongo_client = AsyncIOMotorClient(MONGO_URI)
            logger.info("Mongo client created")
        return cls._mongo_client

    @classmethod
    def get_embedding_model(cls):
        if cls._embedding_model is None:
            try:
                model = SentenceTransformer("BAAI/bge-large-en-v1.5", device=DEVICE)
                model.max_seq_length = 512
                # warmup
                model.encode(["warm up"], convert_to_tensor=False, normalize_embeddings=True)
                cls._embedding_model = model
                logger.info("Embedding model loaded and warmed up")
            except Exception as exc:
                logger.exception("Failed to load embedding model: %s", exc)
                raise
        return cls._embedding_model
