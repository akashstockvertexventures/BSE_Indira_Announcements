import asyncio
from datetime import datetime, timedelta
from config.settings import COLLECTION_ALL_ANN, DB_NAME
from config.constants import RUN_INTERVAL_TIME_MIN
from core.resources import SharedResources
from core.logger import get_logger
from processes.bse_corp_ann_api import BSECorpAnnouncementClient
from utils.categorize_with_filter import FilterCategorize
from utils.reports_divider import ReportsDivider

# ====================== MAIN RUNNER ======================
class BSEAnnouncementPipeline:
    def __init__(self):
        self.logger = get_logger("bse_pipeline", save_time_logs=True)
        self.bse_client = BSECorpAnnouncementClient()
        self.categorizer = FilterCategorize()
        self.divider = ReportsDivider()

    async def fetch_and_process(self, fetch_type ="live", from_date=None, to_date=None, lastnews_dt_tm=None):
        try:
            if fetch_type == "hist":
                self.logger.info("📡 Step 1: Fetching Historical announcements...")
                announcements = await self.bse_client.fetch_hist_announcements(from_date, to_date)
            
            else:
                self.logger.info("📡 Step 1: Fetching live announcements...")
                announcements = await self.bse_client.fetch_live_announcements(lastnews_dt_tm)

            if not announcements:
                self.logger.warning("⚠️  No announcements fetched.")
                return

            self.logger.info(f"✅ Fetched {len(announcements)} announcements")
            self.logger.info("📊 Step 2: Categorizing announcements...")
            categorized_docs = await self.categorizer.run_formator(announcements)
            self.logger.info(f"✅ Categorized {len(categorized_docs)} announcements")

            if not categorized_docs:
                self.logger.info("No docs after filter using company master and assign category")
                return

            self.logger.info("📍 Step 3: Dividing by category and inserting to collections...")
            await self.divider.divide_and_insert_docs(categorized_docs)
            

        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
 

    async def run_pipeline(self, interval_minutes=None):

        await self.fetch_and_process(fetch_type="hist", from_date=datetime(2025, 9, 1), to_date=datetime(2025, 9, 10))

        if interval_minutes:
            self.logger.info(f"🚀 Starting BSE Live Announcements Pipeline Loop {interval_minutes}-minute interval")
            lastnews_dt_tm = None
            iteration = 0
            
            while True:
                iteration += 1
                self.logger.info("=" * 70)
                self.logger.info(f"⏱️  Iteration {iteration}")
                if lastnews_dt_tm:
                    self.logger.info(f"📅 Fetching announcements since: {lastnews_dt_tm}")
                else:
                    self.logger.info(f"📅 First run - using default window")

                run_start_time = datetime.now()
                await self.fetch_and_process(lastnews_dt_tm)

                lastnews_dt_tm = run_start_time
                self.logger.info(f"✅ Next fetch will use: {lastnews_dt_tm.strftime('%d/%m/%Y %H:%M:00')}")
                self.logger.info(f"💤 Sleeping for {interval_minutes} minutes...\n")
                self.logger.info("=" * 70)
                await asyncio.sleep(interval_minutes * 60)


# ====================== ENTRY POINT ======================
async def main():
    pipeline = BSEAnnouncementPipeline()
    try:
        await pipeline.run_pipeline(interval_minutes=0)
    except KeyboardInterrupt:
        pipeline.logger.info("\n✋ Pipeline interrupted by user")
    except Exception as e:
        pipeline.logger.error(f"❌ Unhandled error in main loop: {e}", exc_info=True)


if __name__ == "__main__":
    # ✅ LIVE ANNOUNCEMENTS FETCH (ENABLED)
    asyncio.run(main())
