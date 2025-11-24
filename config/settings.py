import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Mongo / DB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
ODIN_DB = os.getenv("ODIN_DB")

SQUACK_URL = os.getenv("SQUACK_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")

# Collections
COLLECTION_MASTER = os.getenv("collection_master")
COLLECTION_SQUACK = os.getenv("collection_squack_news")
COLLECTION_BSE = os.getenv("collection_news")
COLLECTION_DASHBOARD = os.getenv("collection_dashboard")
COLLECTION_METADATA_UPDATES = os.getenv("collection_metadata_updates")
COLLECTION_LLM_USAGE = os.getenv("collection_llm_usage")
COLLECTION_SYMBOLMAP_EMBEDDINGS = os.getenv("collection_symbolmap_embed")
COLLECTION_DASHBOARD_ARCHIVE = os.getenv("collection_dashbaord_archive")


# Tuning
EMBEDDING_TEXT_THRESHOLD = float(os.getenv("EMBEDDING_TEXT_THRESHOLD", 0.70))
DASHBOARD_DEDUP_THRESHOLD = float(os.getenv("DASHBOARD_DEDUP_THRESHOLD", 0.80))
NO_OF_DAYS_CHECK = int(os.getenv("NO_OF_DAYS_CHECK", 2))
EMBED_CONCURRENCY = int(os.getenv("EMBED_CONCURRENCY", 2))

# Supervisor / heartbeat defaults (used by supervisor)
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 20))
FREEZE_TIMEOUT = int(os.getenv("FREEZE_TIMEOUT", 60))
RESTART_DELAY = int(os.getenv("RESTART_DELAY", 30))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", 7))

