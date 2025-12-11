from config.settings import *
from config.constants import COMPANY_SYMBOL_MAP_QUERY
from core.resources import SharedResources

from core.logger import get_logger


class Base:
    """Hybrid Base class: sync for master, async for main DB."""

    def __init__(self, name="news_pipeline", save_time_logs=True):
        # === Logger ===
        self.logger = get_logger(name=name, save_time_logs=save_time_logs)

        # === Mongo connections (handled internally by SharedResources) ===
        self.mongo = SharedResources.get_mongo_client()
        self.async_mongo = SharedResources.get_async_mongo_client()

        # === Databases ===
        self.db_async = self.async_mongo[DB_NAME]   # async for main data
        self.odin_db = self.mongo[ODIN_DB]     # sync for master

        # === Collections ===
        # Sync
        self.collection_master_sync = self.mongo[ODIN_DB][COLLECTION_MASTER]

        # Async
        self.collection_all_ann = self.db_async[COLLECTION_ALL_ANN]
        self.collection_all_ip = self.db_async[COLLECTION_ALL_IP]
        self.collection_all_ar = self.db_async[COLLECTION_ALL_AR]
        self.collection_all_cr = self.db_async[COLLECTION_ALL_CR]
        self.collection_all_ectr = self.db_async[COLLECTION_ALL_ECTR]
        self.collection_metadata_updates = self.db_async[COLLECTION_METADATA_UPDATES]
        self.llm_usage_collection = self.db_async[COLLECTION_LLM_USAGE]

        self.logger.info(f"✅ Base initialized (Sync=ODIN_DB, Async=DB_NAME={DB_NAME})")

    def fetch_load_symbolmap(self):
        """Fetch valid companies from MongoDB."""
        projection = {"bsecode": 1, "nsesymbol": 1, "companyname": 1}
        docs = self.collection_master_sync.find(COMPANY_SYMBOL_MAP_QUERY, projection)
        company_dict = {}
        for d in docs:
            try:
                bse = str(int(d["bsecode"]))
                nse = d.get("nsesymbol")
                name = d.get("companyname", "").strip()
                company_dict[bse] = {
                    "NSE": nse,
                    "BSE": int(bse),
                    "Company_Name": name,
                    "SELECTED": nse if nse else int(bse)
                }
            except Exception:
                continue

        self.logger.info(f"✅ Company master loaded: {len(company_dict)} companies.")
        return company_dict
