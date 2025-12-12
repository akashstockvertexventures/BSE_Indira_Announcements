from datetime import datetime
from pathlib import Path


BSE_INDIRA_HIST_MIN_DATE = datetime(2023, 11, 1)  
BSE_INDIRA_HIST_MAX_DATE = datetime(2025, 10, 31) 
BSE_INDIRA_TIMEOUT_SEC = 50
BSE_INDIRA_RETRY_DELAY_SEC = 2
BSE_INDIRA_CONCURRENCY_LIMIT = 20
BSE_INDIRA_RETRY_COUNT = 3
BSE_INDIRA_LIVE_DATA_DAYS = 5
RECHECK_NO_OF_DAYS_ALLREPORTS = 5

COMPANY_SYMBOL_MAP_QUERY = {
            "bsecode": {"$ne": None},
            "mcap": {"$gt": 0},
            "isin": {"$not": {"$regex": "IN9"}},
            "companyname": {"$not": {"$regex": "(?i)partly\\s?paid"}},
        }

CATEGORY_MAP = {
    "Investor Presentation": {
        "short_name": "IP",
        "HeadLine": r"presentation",
        "NewsBody": None
    },
    "Annual Report": {
        "short_name": "AR",
        "HeadLine": r"annual report",
        "NewsBody": None
    },
    "Credit Rating": {
        "short_name": "CR",
        "HeadLine": r"credit rating",
        "NewsBody": None
    },
    "Earnings Call Transcript": {
        "short_name": "ECT",
        "HeadLine": r"earnings call|conference call|transcript",
        "NewsBody": None
    }
}


RUN_INTERVAL_TIME_MIN = 1 
LEN_PANDAS_MIN_DOCS = 10
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_LEVEL = "INFO"
LOG_RETENTION_DAYS = 7