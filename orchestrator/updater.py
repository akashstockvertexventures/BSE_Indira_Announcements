import asyncio
from core.base import Base
from ingestion.watcher import NewsWatcher
from ingestion.backfill import BackfillWatcher
from processing.formatter import Formatter
from processing.embedder import EmbeddingGenerator
from processing.dedup_livesquack import LiveSquackNewsDuplicator
from processing.dedup_dashboard import ProcessNewsDuplicator


class NewsImpactDashboardUpdater(Base):
    def __init__(self):
        super().__init__()
        self.watcher = NewsWatcher()
        self.livesquack_updater = LiveSquackNewsDuplicator()
        self.backfiller = BackfillWatcher()
        self.formatter = Formatter()
        self.embedder = EmbeddingGenerator()

    async def trigger_dashboard_dedup(self, companies):
        try:
            self.logger.info("Running ProcessNewsDuplicator for companies: %s", companies)
            deduplicator = ProcessNewsDuplicator()
            await deduplicator.process_duplicates(companies)
            self.logger.info("Deduplication completed for companies: %s", companies)
        except Exception as exc:
            self.logger.error("Deduplication failed: %s", exc, exc_info=True)

    async def process_docs(self, source: str, new_docs: list = None):
        if new_docs is None:
            new_docs = []
        if not new_docs:
            self.logger.info("No new documents to process for source: %s", source)
            return

        self.logger.info("Processing %d new document(s) from source: %s", len(new_docs), source)
        try:
            formate_docs = self.formatter.prepare_dashboard_rows(source, new_docs)
            if not formate_docs:
                self.logger.info("Formatting returned no documents for source: %s", source)
                return

            final_docs = await self.embedder.embed_docs(formate_docs)
            if not final_docs:
                self.logger.info("Embedding returned no documents for source: %s", source)
                return

            self.logger.info("Successfully embedded %d document(s)", len(final_docs))

            if source == "Livesquack":
                before_count = len(final_docs)
                final_docs = await self.livesquack_updater.filter_unique_news(final_docs)
                if not final_docs:
                    self.logger.info("All %d Livesquack documents were duplicates – nothing inserted", before_count)
                    return
                self.logger.info("Livesquack deduplication: %d → %d unique document(s)", before_count, len(final_docs))

            res = await self.collection_dashboard.insert_many(final_docs)
            inserted = len(res.inserted_ids)
            self.logger.info("Inserted %d/%d document(s) into dashboard from source '%s'", inserted, len(final_docs), source)

            if source == "BSE":
                companies = {doc.get("company") for doc in final_docs if doc.get("company")}
                if companies:
                    self.logger.info("Triggering dashboard deduplication for %d BSE compan(ies): %s", len(companies), ", ".join(sorted(companies)))
                    asyncio.create_task(self.trigger_dashboard_dedup(list(companies)))
                else:
                    self.logger.info("No company data found in BSE documents – deduplication not triggered")

        except Exception as e:
            self.logger.error("Failed to process documents from source '%s': %s", source, e, exc_info=True)

    async def backfill_missing_news(self, days: int = 3):
        self.logger.info("Starting backfill for the last %d day(s)", days)

        # BSE Backfill
        bse_docs = await self.backfiller.backfill(source="BSE", days=days)
        if bse_docs:
            existing_ids = set(await self.collection_dashboard.distinct("news_id"))
            new_bse_docs = [d for d in bse_docs if d.get("news_id") not in existing_ids]
            self.logger.info("BSE backfill: %d fetched → %d new after deduplication", len(bse_docs), len(new_bse_docs))
            if new_bse_docs:
                await self.process_docs(source="BSE", new_docs=new_bse_docs)
        else:
            self.logger.info("No new BSE announcements found during backfill")

        # short delay
        await asyncio.sleep(1)

        # Livesquack Backfill
        ls_docs = await self.backfiller.backfill(source="Livesquack", days=days)
        if ls_docs:
            existing_ids = set(await self.collection_dashboard.distinct("news_id"))
            new_ls_docs = [d for d in ls_docs if d.get("news_id") not in existing_ids]
            self.logger.info("Livesquack backfill: %d fetched → %d new after deduplication", len(ls_docs), len(new_ls_docs))
            if new_ls_docs:
                await self.process_docs(source="Livesquack", new_docs=new_ls_docs)
        else:
            self.logger.info("No new Livesquack news found during backfill")

        self.logger.info("Backfill process finished successfully for all sources")

    async def start_stream(self, batch_size: int = 1):
        # This method will be started as a long-running task
        self.logger.info("Live stream updater started (batch_size=%d)", batch_size)
        async for batch in self.watcher.stream_changes(batch_size=batch_size):
            source, docs = batch["source"], batch["docs"]
            if not docs:
                self.logger.info("Received empty batch from %s – skipping", source)
                continue
            self.logger.info("Received batch: %d new document(s) from %s", len(docs), source)
            await self.process_docs(source=source, new_docs=docs)
