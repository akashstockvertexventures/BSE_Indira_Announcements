from config.settings import *
from config.constants import *
from core.resources import SharedResources
from core.logger import get_logger

class Base:
    def __init__(self, name = "news_pipeline"):
        self.logger = get_logger(name=name)
        self.mongo = SharedResources.get_mongo_client()
        self.db = self.mongo[DB_NAME]
        self.collection_master = self.mongo[ODIN_DB][COLLECTION_MASTER]
        self.collection_squack = self.db[COLLECTION_SQUACK]
        self.collection_bse = self.db[COLLECTION_BSE]
        self.collection_dashboard = self.db[COLLECTION_DASHBOARD]
        self.collection_metadata_updates = self.db[COLLECTION_METADATA_UPDATES]
        self.llm_usage_collection = self.db[COLLECTION_LLM_USAGE]
        self.collection_symbolmap_embeddings = self.db[COLLECTION_SYMBOLMAP_EMBEDDINGS]
        self.embedding_model = SharedResources.get_embedding_model()
        self.embedding_text_threshold = EMBEDDING_TEXT_THRESHOLD
        self.threshold = DASHBOARD_DEDUP_THRESHOLD
        self.no_of_days_check = NO_OF_DAYS_CHECK
