from datetime import datetime, timedelta
from core.base import Base
import asyncio
from config.constants import NO_OF_ARCHIVE_DAYS

class NewsImpactArchiveUpdator(Base):
    def __init__(self):
        super().__init__(name="Archive_Updates", save_time_logs=False)

    async def fetch_archive_newsids(self):
        cutoff_date = datetime.now() - timedelta(days=NO_OF_ARCHIVE_DAYS)
        return await self.collection_archive_dashboard.distinct(
            "news_id",
            {
                "news_id": {"$exists": True, "$type": "string"},
                "dt_tm": {"$gte": cutoff_date}
            }
        )

    async def fetch_dashboard_news(self, archived_ids):
        cutoff_date = datetime.now() - timedelta(days=NO_OF_ARCHIVE_DAYS - 5)
        cursor = self.collection_dashboard.find(
            {
                "news_id": {"$exists": True, "$type": "string", "$nin": archived_ids},
                "dt_tm": {"$gte": cutoff_date}
            },
            {"_id": 0} 
        )
        return await cursor.to_list(None)

    async def archive_dashboard_news(self):
        archived_ids = await self.fetch_archive_newsids()
        unarchived_docs = await self.fetch_dashboard_news(archived_ids)
        if not unarchived_docs:
            return
        await self.collection_archive_dashboard.insert_many(unarchived_docs)

    async def run(self):
        await self.archive_dashboard_news()


if __name__ == "__main__":
    asyncio.run(NewsImpactArchiveUpdator().run())
