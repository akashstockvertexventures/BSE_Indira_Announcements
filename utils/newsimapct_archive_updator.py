from datetime import datetime, timedelta
from core.base import Base
import asyncio
from config.constants import NO_OF_ARCHIVE_DAYS

class NewsImpactArchiveUpdator(Base):
    def __init__(self):
        super().__init__(name="Archive_Updates", save_time_logs=False)
        self.logger.info("Initialized NewsImpactArchiveUpdator instance")

    async def fetch_archive_newsids(self):
        cutoff_date = datetime.now() - timedelta(days=NO_OF_ARCHIVE_DAYS)
        self.logger.info(f"Fetching archived news_ids newer than {cutoff_date}")
        archived_ids = await self.collection_archive_dashboard.distinct(
            "news_id",
            {
                "news_id": {"$exists": True, "$type": "string"},
                "dt_tm": {"$gte": cutoff_date}
            }
        )
        self.logger.info(f"Fetched {len(archived_ids)} archived news_ids")
        return archived_ids

    async def fetch_dashboard_news(self, archived_ids):
        cutoff_date = datetime.now() - timedelta(days=NO_OF_ARCHIVE_DAYS - 5)
        self.logger.info(
            f"Fetching unarchived dashboard news newer than {cutoff_date} "
            f"excluding {len(archived_ids)} already archived news_ids"
        )
        cursor = self.collection_dashboard.find(
            {
                "news_id": {"$exists": True, "$type": "string", "$nin": archived_ids},
                "dt_tm": {"$gte": cutoff_date}
            },
            {"_id": 0}
        )
        unarchived_docs = await cursor.to_list(None)
        self.logger.info(f"Fetched {len(unarchived_docs)} new dashboard news")
        return unarchived_docs

    async def archive_dashboard_news(self):
        self.logger.info("Starting dashboard news archival process")
        archived_ids = await self.fetch_archive_newsids()
        unarchived_docs = await self.fetch_dashboard_news(archived_ids)
        if not unarchived_docs:
            self.logger.info("No new dashboard news found for archiving")
            return
        await self.collection_archive_dashboard.insert_many(unarchived_docs)
        self.logger.info(f"Archived {len(unarchived_docs)} new dashboard news documents")

    async def run(self):
        self.logger.info("Running NewsImpactArchiveUpdator task")
        await self.archive_dashboard_news()
        self.logger.info("Archival update process completed successfully")


if __name__ == "__main__":
    asyncio.run(NewsImpactArchiveUpdator().run())
