import os
import json
from pathlib import Path
from dotenv import load_dotenv

# === Load .env ===
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

# === Paths ===
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", 7))

# === Mongo / Database ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "BSECorpReports")
ODIN_DB = os.getenv("ODIN_DB", "OdinMasterData")

# === Collections (read dynamically from .env) ===
COLLECTION_ALL_ANN = os.getenv("COLLECTION_ALL_ANN", "AllAnnouncements")
COLLECTION_ALL_IP = os.getenv("COLLECTION_ALL_IP", "AllInvestorPresentation")
COLLECTION_ALL_AR = os.getenv("COLLECTION_ALL_AR", "AllAnnualReport")
COLLECTION_ALL_CR = os.getenv("COLLECTION_ALL_CR", "AllCreditRating")
COLLECTION_ALL_ECTR = os.getenv("COLLECTION_ALL_ECTR", "AllEarningsCallTranscript")
COLLECTION_MASTER = os.getenv("COLLECTION_MASTER", "CompanyMaster")
COLLECTION_LLM_USAGE = os.getenv("COLLECTION_LLM_USAGE", "LLMUsage")
COLLECTION_METADATA_UPDATES = os.getenv("COLLECTION_METADATA_UPDATES", "MetaDataLastUpdates")


BSE_INDIRA_API_URL = os.getenv("BSE_INDIRA_API_URL")
BSE_INDIRA_API_PARAMS_Live= json.loads(os.getenv("BSE_INDIRA_API_PARAMS_Live"))
BSE_INDIRA_API_PARAMS_Hist = json.loads(os.getenv("BSE_INDIRA_API_PARAMS_Hist"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")

