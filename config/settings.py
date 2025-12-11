import os
import json
from dotenv import load_dotenv
load_dotenv()

# === Mongo / Database ===
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "BSECorpReports")
ODIN_DB = os.getenv("ODIN_DB", "OdinMasterData")

# === Collections (read dynamically from .env) ===
COLLECTION_ALL_ANN = os.getenv("COLLECTION_ALL_ANN", "AllAnnouncements")
COLLECTION_ALL_REPORTS = os.getenv("COLLECTION_ALL_REPORTS", "AllReports")
COLLECTION_MASTER = os.getenv("COLLECTION_MASTER", "CompanyMaster")
COLLECTION_LLM_USAGE = os.getenv("COLLECTION_LLM_USAGE", "LLMUsage")
COLLECTION_METADATA_UPDATES = os.getenv("COLLECTION_METADATA_UPDATES", "MetaDataLastUpdates")


BSE_INDIRA_API_URL = os.getenv("BSE_INDIRA_API_URL")
BSE_INDIRA_API_PARAMS_Live= json.loads(os.getenv("BSE_INDIRA_API_PARAMS_Live"))
BSE_INDIRA_API_PARAMS_Hist = json.loads(os.getenv("BSE_INDIRA_API_PARAMS_Hist"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")

