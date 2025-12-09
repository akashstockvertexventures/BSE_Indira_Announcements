from datetime import datetime

BSE_INDIRA_HIST_MIN_DATE = datetime(2023, 11, 1)   # 1st Nov 2023
BSE_INDIRA_HIST_MAX_DATE = datetime(2025, 10, 31)  # 31st Oct 2025

COMPANY_SYMBOL_MAP_QUERY = {
            "bsecode": {"$ne": None},
            "mcap": {"$gt": 0},
            "isin": {"$not": {"$regex": "IN9"}},
            "companyname": {"$not": {"$regex": "(?i)partly\\s?paid"}},
        }

CATEGORY_MAP = {
            "Investor Presentation": {"HeadLine": r"presentation", "NewsBody": None},
            "Annual Report": {"HeadLine": r"annual report", "NewsBody": None},
            "Credit Rating": {"HeadLine": r"credit rating", "NewsBody": None},
            "Earnings Call Transcript": {"HeadLine": r"earnings call|conference call|transcript","NewsBody": r"concall"}
            }
LEN_PANDAS_MIN_DOCS = 10