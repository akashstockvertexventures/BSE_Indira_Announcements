import asyncio
import multiprocessing
import concurrent.futures
from core.base import Base
from config.constants import DEVICE


_embedding_model_worker = None

def _init_embedding_worker():
    global _embedding_model_worker
    from sentence_transformers import SentenceTransformer
    _embedding_model_worker = SentenceTransformer("BAAI/bge-large-en-v1.5", device="cpu")
    _embedding_model_worker.max_seq_length = 512

def _embed_batch_worker(texts):
    global _embedding_model_worker
    embs = _embedding_model_worker.encode(
        texts,
        convert_to_tensor=False,
        normalize_embeddings=True
    )
    return embs.tolist() if hasattr(embs, "tolist") else list(embs)


class EmbeddingGenerator(Base):
    BATCH_SIZE_GPU = 64
    BATCH_SIZE_CPU = 128

    def __init__(self):

        self.use_gpu = DEVICE.type == "cuda"
        self.num_workers = multiprocessing.cpu_count()

        self.pool = None
        if not self.use_gpu:
            self.pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=self.num_workers,
                initializer=_init_embedding_worker
            )

    async def embed_docs(self, docs):
        if not docs:
            return docs

        summaries = [d.get("short summary") or "" for d in docs]

        # GPU path (direct call, no thread offload)
        if self.use_gpu:
            embeddings = []
            for i in range(0, len(summaries), self.BATCH_SIZE_GPU):
                batch = summaries[i:i+self.BATCH_SIZE_GPU]
                embs = self.embedding_model.encode(
                    batch,
                    convert_to_tensor=False,
                    normalize_embeddings=True,
                    device=str(DEVICE)
                )
                embeddings.extend(embs if isinstance(embs, list) else embs.tolist())

            for d, emb in zip(docs, embeddings):
                d["embedding_shortsummary"] = emb
            return docs

        # CPU multiprocess path
        batches = [
            summaries[i:i+self.BATCH_SIZE_CPU]
            for i in range(0, len(summaries), self.BATCH_SIZE_CPU)
        ]

        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(self.pool, _embed_batch_worker, batch)
            for batch in batches
        ]
        results = await asyncio.gather(*tasks)

        embeddings = [e for batch in results for e in batch]
        for d, emb in zip(docs, embeddings):
            d["embedding_shortsummary"] = emb

        return docs