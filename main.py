import asyncio
import aiohttp
import argparse
from datetime import datetime
from core.bse_pipeline import BSEAnnouncementPipeline
from config.constants import (
    RUN_INTERVAL_TIME_MIN,
    BSE_INDIRA_HIST_MIN_DATE,
    BSE_INDIRA_HIST_MAX_DATE

)

# ------------------------ Internet Check ------------------------
async def is_internet(logger) -> bool:
    test_url = "https://www.google.com/generate_204"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(test_url, timeout=5) as resp:
                    if resp.status == 204:
                        return True
                    else:
                        logger.warning("⚠ Captive portal detected or unexpected response.")
        except Exception:
            logger.warning("❌ No internet connection detected.")
        logger.info("💤 Retrying internet check in 15 min...\n")
        await asyncio.sleep(15 * 60)


# ------------------------ Pipeline Runner ------------------------
async def run_pipeline_loop(pipeline: BSEAnnouncementPipeline, hist=False):
    logger = pipeline.logger

    if hist:
        await is_internet(logger)
        await pipeline.fetch_and_process(
            fetch_type="hist",
            from_date=BSE_INDIRA_HIST_MIN_DATE,
            to_date=BSE_INDIRA_HIST_MAX_DATE,
        )
        logger.info("📚 Historical data fetch completed.")
        return  

    interval_minutes = RUN_INTERVAL_TIME_MIN or 1
    logger.info(f"🚀 Starting BSE Live Announcements Pipeline | Interval: {interval_minutes} min")

    lastnews_dt_tm = None
    iteration = 0

    while True:
        await is_internet(logger)
        iteration += 1
        logger.info("=" * 70)
        logger.info(f"⏱️ Iteration {iteration}")

        if lastnews_dt_tm:
            logger.info(f"📅 Fetching announcements since: {lastnews_dt_tm}")
        else:
            logger.info("📅 First run — using default window")

        start_time = datetime.now()
        run_start_time = start_time.replace(second=0, microsecond=0)

        await pipeline.fetch_and_process(lastnews_dt_tm=lastnews_dt_tm)
        duration = (datetime.now() - start_time).seconds
        logger.info(f"🕒 Cycle completed in {duration} seconds")

        lastnews_dt_tm = run_start_time
        logger.info(f"✅ Next fetch will use: {lastnews_dt_tm.strftime('%d/%m/%Y %H:%M:00')}")
        logger.info(f"💤 Sleeping for {interval_minutes} minutes...\n")
        await asyncio.sleep(interval_minutes * 60)


# ------------------------ Entry Point ------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run BSE Announcement Pipeline")
    parser.add_argument("--hist", action="store_true", help="Run historical data pipeline (one-time)")
    args = parser.parse_args()

    pipeline = BSEAnnouncementPipeline()
    logger = pipeline.logger

    try:
        asyncio.run(run_pipeline_loop(pipeline, hist=args.hist))
    except KeyboardInterrupt:
        logger.info("✋ Pipeline stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.error(f"❌ Unhandled error in main loop: {e}", exc_info=False)
    finally:
        logger.info("🧹 Exiting BSEAnnouncementPipeline cleanly.")
