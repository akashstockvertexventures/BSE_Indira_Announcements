from datetime import datetime, timedelta
from typing import Dict, Any, List
from core.base import Base
from config.constants import SOURCE_BSE, SOURCE_LIVESQUACK, SAFE_QUERY, CAT_NEWS_REMOVE_LIVESQUACK

class BackfillWatcher(Base):
    def __init__(self):
        super().__init__()

    def build_match(self, source: str, from_date: str) -> Dict[str, Any]:
        base = {
            "news_id": SAFE_QUERY,
            "company": SAFE_QUERY,
            "symbolmap": {"$ne": None},
            "dt_tm": {"$type": "string", "$gte": from_date},
            "sentiment": SAFE_QUERY,
            "impact": SAFE_QUERY,
        }
        if source == SOURCE_LIVESQUACK:
            return {
                **base,
                "category": {"$type": "string", "$nin": ["", *CAT_NEWS_REMOVE_LIVESQUACK]},
                "short summary": SAFE_QUERY,
                "impact score": {"$gt": 0},
            }
        return {
            **base,
            "category": SAFE_QUERY,
            "shortsummary": SAFE_QUERY,
            "summary": SAFE_QUERY,
            "pdf_link_live": SAFE_QUERY,
            "impactscore": {"$gt": 0},
        }

    async def backfill(self, source: str, days: int = 3) -> List[Dict[str, Any]]:
        if source == SOURCE_BSE:
            collection = self.collection_bse
        else:
            collection = self.collection_squack

        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        match = self.build_match(source, since)
        docs = await (collection.find(match).sort("dt_tm", 1)).to_list(length=None)
        self.logger.info("%s Backfill — fetched %s docs", source.upper(), len(docs))
        return docs
