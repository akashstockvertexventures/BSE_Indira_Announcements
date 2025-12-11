from config.settings import *
from config.constants import COMPANY_SYMBOL_MAP_QUERY
from core.resources import SharedResources
from core.logger import get_logger


class Base:
    """Hybrid Base class: sync for master, async for main DB."""

    def __init__(self, name="bse_indira_pipeline", save_time_logs=True):
        self.logger = get_logger(name=name, save_time_logs=save_time_logs)

        self.mongo = SharedResources.get_mongo_client()
        self.collection_master_sync = self.mongo[ODIN_DB][COLLECTION_MASTER]

        self.async_mongo = SharedResources.get_async_mongo_client()
        self.db_async = self.async_mongo[DB_NAME]  
        self.collection_all_ann = self.db_async[COLLECTION_ALL_ANN]
        self.collection_all_reports = self.db_async[COLLECTION_ALL_REPORTS]
        self.collection_metadata_updates = self.db_async[COLLECTION_METADATA_UPDATES]
        self.llm_usage_collection = self.db_async[COLLECTION_LLM_USAGE]

    def fetch_load_symbolmap(self):
        """Fetch valid companies from MongoDB."""
        projection = {"bsecode": 1, "nsesymbol": 1, "companyname": 1, "isin":1}
        docs = self.collection_master_sync.find(COMPANY_SYMBOL_MAP_QUERY, projection)
        company_dict = {}
        for d in docs:
            try:
                bse = str(int(d["bsecode"]))
                nse = d.get("nsesymbol")
                isin = d.get("isin")
                name = d.get("companyname", "").strip()
                selected = nse if nse else int(bse)
                company_dict[bse] = {
                    "company":isin,
                    "symbolmap":{
                    "NSE": nse,
                    "BSE": int(bse),
                    "Company_Name": name,
                    "SELECTED": selected
                }}
            except Exception:
                continue

        self.logger.info(f"✅ Company master loaded: {len(company_dict)} companies.")
        return company_dict
