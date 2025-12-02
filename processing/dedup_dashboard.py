import numpy as np
import faiss
import asyncio
from collections import defaultdict, Counter
from bson import ObjectId
from core.base import Base
from datetime import datetime, timedelta
from config.constants import DEVICE, CAT_NO_CHECK_DUPLICATE_DASHBOARD


class ProcessNewsDuplicator(Base):
    def __init__(self):
        super().__init__()

    async def load_dashboard_docs_for_companies(self, companies):
        days_ago = datetime.now() - timedelta(days=self.no_of_days_check)
        query = {
            "dt_tm": {"$gte": days_ago},
            "company": {"$in": companies},
            "source": "BSE",
            "category": {"$nin": CAT_NO_CHECK_DUPLICATE_DASHBOARD},
            "embedding_shortsummary": {"$exists": True, "$ne": None},
            "duplicate": False
        }
        projection = {"_id": 1, "company": 1, "dt_tm": 1, "news_id": 1, "embedding_shortsummary": 1}
        docs = await self.collection_dashboard.find(query, projection).sort("dt_tm", -1).to_list(length=None)
        if not docs:
            return {}
        grouped = defaultdict(list)
        for doc in docs:
            grouped[doc["company"]].append(doc)
        return grouped

    def _find_duplicate_ids_one_company(self, docs, threshold=0.95):
        n = len(docs)
        if n < 2:
            return {docs[0]["company"]: set()}

        company = docs[0]["company"]
        embeddings = []
        ids = [str(doc["_id"]) for doc in docs]
        dts = [doc["dt_tm"] for doc in docs]
        for doc in docs:
            emb = doc["embedding_shortsummary"]
            emb = np.array(emb, dtype=np.float32) if isinstance(emb, list) else emb.astype(np.float32)
            embeddings.append(emb)
        embeddings = np.vstack(embeddings)
        faiss.normalize_L2(embeddings)
        dim = embeddings.shape[1]

        if DEVICE.type == "cuda" and faiss.get_num_gpus() > 0:
            res = faiss.StandardGpuResources()
            index = faiss.index_cpu_to_gpu(res, 0, faiss.IndexFlatIP(dim))
        else:
            index = faiss.IndexFlatIP(dim)

        index.add(embeddings)
        k = min(50, n)
        scores, indices = index.search(embeddings, k)

        parent = list(range(n))
        def find(x): return x if parent[x] == x else find(parent[x])
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for i in range(n):
            for j in range(1, k):
                idx = indices[i][j]
                if idx == -1 or scores[i][j] <= threshold:
                    break
                union(i, idx)

        groups = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(i)

        duplicate_ids = set()
        for members in groups.values():
            if len(members) < 2:
                continue
            earliest_idx = min(members, key=lambda i: dts[i])
            for i in members:
                if i != earliest_idx:
                    duplicate_ids.add(ids[i])

        try:
            latest_idx = max(range(n), key=lambda i: dts[i])
            latest_id = ids[latest_idx]
            latest_emb = embeddings[latest_idx].reshape(1, -1)
            scores_latest, indices_latest = index.search(latest_emb, n)
            most_similar_idx = indices_latest[0][1]
            most_similar_score = float(scores_latest[0][1])
            is_duplicate = latest_id in duplicate_ids
            self.logger.info(
                f"[{company}] Latest doc {latest_id} (dt={dts[latest_idx]}) "
                f"is most similar to {ids[most_similar_idx]} (dt={dts[most_similar_idx]}) "
                f"with similarity={most_similar_score:.4f} | "
                f"{'DUPLICATE ✅' if is_duplicate else 'UNIQUE 🆕'}"
            )

        except Exception as e:
            self.logger.warning(
                f"[{company}] ⚠️ Failed to log similarity for latest doc: {e}"
            )

        return {company: duplicate_ids}

    async def process_duplicates(self, company_list=None):
        try:
            companies = company_list or []
            if not companies:
                return

            grouped_docs = await self.load_dashboard_docs_for_companies(companies)
            if not grouped_docs:
                self.logger.info("%s -ProcessNewsDuplicator No documents found for deduplication", self.__class__.__name__)
                return

            tasks = [asyncio.to_thread(self._find_duplicate_ids_one_company, doc_list, self.threshold) for doc_list in grouped_docs.values()]
            results = await asyncio.gather(*tasks)

            company_duplicate_counts = Counter()
            all_duplicate_ids = set()
            for result_dict in results:
                for company, dup_ids in result_dict.items():
                    count = len(dup_ids)
                    if count > 0:
                        company_duplicate_counts[company] += count
                        all_duplicate_ids.update(dup_ids)

            if not all_duplicate_ids:
                self.logger.info("%s -ProcessNewsDuplicator Deduplication complete — No duplicates found", self.__class__.__name__)
                return

            result = await self.collection_dashboard.update_many(
                {"_id": {"$in": [ObjectId(oid) for oid in all_duplicate_ids]}, "duplicate": False},
                {"$set": {"duplicate": True}}
            )
            self.logger.info("%s - ProcessNewsDuplicator Deduplication complete — Total: %d marked (modified: %d)", self.__class__.__name__, len(all_duplicate_ids), result.modified_count)
            for company, count in company_duplicate_counts.most_common():
                self.logger.info("%s - └→ %s: %d duplicates marked", self.__class__.__name__, company, count)
            
        except Exception as exc:
            self.logger.error("[ProcessNewsDuplicator] Error : %s", exc, exc_info=True)
            return []
