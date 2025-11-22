import asyncio
from core.base import Base
from config.constants import SOURCE_BSE, SOURCE_LIVESQUACK, SAFE_QUERY, CAT_NEWS_REMOVE_LIVESQUACK

class NewsWatcher(Base):
    def __init__(self):
        super().__init__()

    def build_pipeline(self, source: str):
        base = {
            "operationType": "insert",
            "fullDocument.news_id": SAFE_QUERY,
            "fullDocument.company": SAFE_QUERY,
            "fullDocument.symbolmap": {"$ne": None},
            "fullDocument.dt_tm": SAFE_QUERY,
            "fullDocument.sentiment": SAFE_QUERY,
            "fullDocument.impact": SAFE_QUERY,
        }
        if source == SOURCE_LIVESQUACK:
            match = {
                **base,
                "fullDocument.category": {"$type": "string", "$nin": ["", *CAT_NEWS_REMOVE_LIVESQUACK]},
                "fullDocument.short summary": SAFE_QUERY,
                "fullDocument.impact score": {"$gt": 0},
            }
        else:
            match = {
                **base,
                "fullDocument.category": SAFE_QUERY,
                "fullDocument.shortsummary": SAFE_QUERY,
                "fullDocument.summary": SAFE_QUERY,
                "fullDocument.pdf_link_live": SAFE_QUERY,
                "fullDocument.impactscore": {"$gt": 0},
            }
        return [{"$match": match}]

    async def monitor_collection(self, collection, queue_: asyncio.Queue, pipeline, batch_size: int = 5):
        batch = []
        if collection.name == self.collection_squack.name:
            source = SOURCE_LIVESQUACK
        else:
            source = SOURCE_BSE

        try:
            async with collection.watch(pipeline) as stream:
                async for change in stream:
                    doc = change.get("fullDocument")
                    if not doc:
                        continue
                    batch.append(doc)
                    if len(batch) >= batch_size:
                        await queue_.put({"source": source, "docs": batch})
                        self.logger.info("%s: queued %s docs", source, len(batch))
                        batch = []
        except Exception as exc:
            self.logger.exception("Error watching %s: %s", getattr(collection, "name", "collection"), exc)

    async def stream_changes(self, batch_size: int = 5):
        queue_ = asyncio.Queue()
        pipelines = {
            self.collection_bse: self.build_pipeline(SOURCE_BSE),
            self.collection_squack: self.build_pipeline(SOURCE_LIVESQUACK),
        }
        tasks = [asyncio.create_task(self.monitor_collection(coll, queue_, pl, batch_size)) for coll, pl in pipelines.items()]
        try:
            while True:
                yield await queue_.get()
        finally:
            for t in tasks:
                t.cancel()
