import asyncio
from orchestrator.updater import NewsImpactDashboardUpdater
from ingestion.squack_llm_processor import LivesquackNewsLLM
from core.logger import get_logger
logger = get_logger()

async def main():
    news_imapct_dashbaord_updator = NewsImpactDashboardUpdater()
    livesquack_news_llm_runner = LivesquackNewsLLM()

    livesquack_llm_task  = asyncio.create_task(livesquack_news_llm_runner.run())
    backfill_task = asyncio.create_task(news_imapct_dashbaord_updator.backfill_missing_news(days=3))
    watcher_task = asyncio.create_task(news_imapct_dashbaord_updator.start_stream(batch_size=1))
    
    await asyncio.gather(backfill_task, livesquack_llm_task, watcher_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down on user interrupt")
