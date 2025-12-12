import asyncio
from datetime import datetime, timedelta
import aiohttp, aiofiles, os, json
from tqdm.asyncio import tqdm_asyncio

from config.constants import (
    BSE_INDIRA_HIST_MIN_DATE,
    BSE_INDIRA_LIVE_DATA_DAYS,
    ALLREPORTS_CATEGORY_MAP
)
from core.logger import get_logger
from processes.bse_corp_ann_api import BSECorpAnnouncementClient
from utils.categorize_with_filter import FilterCategorize
from utils.reports_divider import ReportsDivider


class BSEAnnouncementPipeline:
    def __init__(self):
        self.logger = get_logger("bse_pipeline", save_time_logs=True)
        self.bse_client = BSECorpAnnouncementClient()
        self.categorizer = FilterCategorize()
        self.divider = ReportsDivider()
        self.maintain_json = False
        self.reports_cat = ALLREPORTS_CATEGORY_MAP.keys()

    # ------------------------ JSON Maintenance ------------------------
    async def maintain_json_file(self, new_data, data_type="normal", fetch_type="live"):
        mapping = {
            ("filter", "live"): "categorized_announcements_live.jsonl",
            ("filter", "hist"): "categorized_announcements_hist.jsonl",
            ("normal", "live"): "live_announcements.jsonl",
            ("normal", "hist"): "hist_announcements.jsonl",
        }
        filename = mapping.get((data_type, fetch_type), "unknown_data.jsonl")
        base_dir = "files"
        filepath, index_path = os.path.join(base_dir, filename), os.path.join(base_dir, filename + ".index")
        os.makedirs(base_dir, exist_ok=True)

        try:
            existing_ids = set()
            if os.path.exists(index_path):
                async with aiofiles.open(index_path, "r", encoding="utf-8") as idx:
                    content = await idx.read()
                    if content.strip():
                        existing_ids = set(content.splitlines())

            new_unique = [
                d for d in new_data
                if isinstance(d, dict) and (nid := d.get("news_id")) and nid not in existing_ids
            ]
            if not new_unique:
                return

            if len(new_unique) >= 1000:
                async def _to_json_line(doc):
                    return json.dumps(doc, ensure_ascii=False) + "\n"
                tasks = [_to_json_line(doc) for doc in new_unique]
                lines = [
                    await t for t in tqdm_asyncio.as_completed(
                        tasks, total=len(tasks), desc=f"Writing {filename}", unit="doc"
                    )
                ]
            else:
                lines = [json.dumps(doc, ensure_ascii=False) + "\n" for doc in new_unique]

            async with aiofiles.open(filepath, "a", encoding="utf-8") as f:
                await f.writelines(lines)
            async with aiofiles.open(index_path, "a", encoding="utf-8") as idx:
                await idx.writelines(f"{doc['news_id']}\n" for doc in new_unique if "news_id" in doc)

            self.logger.info(f"✅ Appended {len(new_unique)} new records → {filename}")
        except Exception as e:
            self.logger.error(f"⚠️ Failed to maintain {filename}: {e}")

    # ------------------------ Recheck Update Reports ------------------------
    async def temp_update_all_report_using_allannouncement(self, days_check=5, all_hist_days=False):
        if all_hist_days:
            tradedate_str = BSE_INDIRA_HIST_MIN_DATE.strftime("%Y-%m-%d 00:00:00")
            self.logger.info(f"🔁 Recheck: Processing all historical announcements since {tradedate_str}...")
        else:
            tradedate_str = (datetime.now() - timedelta(days=days_check)).strftime("%Y-%m-%d 00:00:00")
            self.logger.info(f"🔁 Recheck: Processing announcements from the last {days_check} days (since {tradedate_str})...")

        docs = await self.divider.collection_all_ann.find({"Tradedate": {"$gte": tradedate_str}, "category": {"$in":self.reports_cat}}).to_list(length=None)

        if not docs:
            self.logger.info(f"⚠️ No announcements found to recheck since {tradedate_str}.")
            return

        self.logger.info(f"📄 Found {len(docs)} announcements for recheck and report update.")
        await self.divider.all_reports_runner(docs=docs, tradedate=tradedate_str)
        self.logger.info("✅ Recheck update completed — all relevant reports refreshed.")

    # ------------------------ Main Fetching Logic ------------------------
    async def fetch_and_process(self, fetch_type="live", from_date=None, to_date=None, lastnews_dt_tm=None):
        try:
            if fetch_type == "hist" and from_date and to_date:
                self.logger.info("📡 Step 1: Fetching Historical announcements...")
                announcements = await self.bse_client.fetch_hist_announcements(from_date, to_date)
                if not from_date:
                    from_date = BSE_INDIRA_HIST_MIN_DATE
                tradedate_str = from_date.strftime("%Y-%m-%d 00:00:00")
            else:
                self.logger.info("📡 Step 1: Fetching Live announcements...")
                announcements = await self.bse_client.fetch_live_announcements(lastnews_dt_tm)
                if not lastnews_dt_tm:
                    lastnews_dt_tm = (datetime.now() - timedelta(days=(BSE_INDIRA_LIVE_DATA_DAYS-1)))
                tradedate_str = lastnews_dt_tm.strftime("%Y-%m-%d 00:00:00")
            
            if not announcements:
                self.logger.warning("⚠️ No announcements fetched.")
                return

            if self.maintain_json:
                await self.maintain_json_file(announcements, data_type="normal", fetch_type=fetch_type)

            self.logger.info(f"✅ Fetched {len(announcements)} announcements")
            self.logger.info("📊 Step 2: Categorizing announcements...")
            categorized_docs = await self.categorizer.run_formator(announcements, tradedate=tradedate_str)

            if not categorized_docs:
                self.logger.info("⚠️ No docs after filtering or categorization")
                return

            if self.maintain_json:
                await self.maintain_json_file(categorized_docs, data_type="filter", fetch_type=fetch_type)

            self.logger.info(f"✅ Categorized {len(categorized_docs)} announcements")
            self.logger.info("📍 Step 3: Dividing by category and inserting to collections...")
            await self.divider.divide_and_insert_docs(categorized_docs, tradedate=tradedate_str)

        except Exception as e:
            self.logger.error(f"❌ Pipeline failed during processing: {e}", exc_info=False)

   