import torch
import asyncio
import torch.nn.functional as F
from core.base import Base
from datetime import datetime, timedelta
from config.constants import DEVICE, CAT_NO_CHECK_DUPLICATE_DASHBOARD


class LiveSquackNewsDuplicator(Base):
    def __init__(self):
        super().__init__()

    async def load_dashboard_docs_for_companies(self, companies):
        days_ago = datetime.now() - timedelta(days=self.no_of_days_check)
        q = {
            "dt_tm": {"$gte": days_ago},
            "company": {"$in": companies},
            "short summary": {"$type": "string", "$nin": ["", " "]},
            "source": "BSE",
            "category": {"$nin": CAT_NO_CHECK_DUPLICATE_DASHBOARD},
            "embedding_shortsummary": {"$exists": True, "$ne": None},
            "duplicate": False
        }
        projection = {
            "_id": 1, "news_id": 1, "company": 1, "short summary": 1, "embedding_shortsummary": 1
        }
        docs = await self.collection_dashboard.find(q, projection).sort("dt_tm", -1).to_list(None)
        return docs or []

    async def _process_company_batch(self, company, incoming_docs, dash_docs):
        if not dash_docs or not incoming_docs:
            return incoming_docs, []

        inc_embs = [d["embedding_shortsummary"] for d in incoming_docs]
        dash_embs = [d["embedding_shortsummary"] for d in dash_docs]

        A = torch.tensor(inc_embs, dtype=torch.float32, device=DEVICE)
        B = torch.tensor(dash_embs, dtype=torch.float32, device=DEVICE)
        A = F.normalize(A, dim=1)
        B = F.normalize(B, dim=1)
        sim = A @ B.T
        max_scores, max_indices = torch.max(sim, dim=1)

        unique_docs = []
        duplicates = []
        for doc, score, idx in zip(incoming_docs, max_scores.tolist(), max_indices.tolist()):
            if score < self.embedding_text_threshold:
                unique_docs.append(doc)
                self.logger.info(
                    f"[{company}] UNIQUE 🆕 | news_id={doc.get('news_id')} | "
                    f"similarity={score:.4f} < threshold={self.embedding_text_threshold}"
                )
            else:
                m = dash_docs[idx]
                duplicates.append({
                    "company": company,
                    "incoming_news_id": doc.get("news_id"),
                    "incoming_summary": doc.get("short summary", ""),
                    "matched_news_id": m.get("news_id"),
                    "matched_summary": m.get("short summary", ""),
                    "score": round(score, 5)
                })
                self.logger.info(
                    f"[{company}] DUPLICATE ✅ | incoming={doc.get('news_id')} "
                    f"matched_with={m.get('news_id')} | similarity={score:.4f} "
                    f"(≥ threshold={self.embedding_text_threshold})"
                )
        return unique_docs, duplicates

    async def filter_unique_news(self, docs):
        try:
            incoming_groups = {}
            for d in docs:
                if d.get("company") and d.get("embedding_shortsummary"):
                    incoming_groups.setdefault(d["company"], []).append(d)

            if not incoming_groups:
                return []

            companies = list(incoming_groups.keys())
            dashboard_docs = await self.load_dashboard_docs_for_companies(companies)

            dash_groups = {}
            for d in dashboard_docs:
                c = d.get("company")
                dash_groups.setdefault(c, []).append(d)

            tasks = [
                self._process_company_batch(company, inc_docs, dash_groups.get(company, []))
                for company, inc_docs in incoming_groups.items()
            ]
            results = await asyncio.gather(*tasks)

            unique = []
            duplicates = []
            for u, d in results:
                unique.extend(u)
                duplicates.extend(d)

            if duplicates:
                duplicates.sort(key=lambda x: x["score"], reverse=True)
                self.logger.info("[LiveSquackNewsDuplicator] DUPLICATES_FOUND total=%d", len(duplicates))
            else:
                self.logger.info("[LiveSquackNewsDuplicator] No duplicates found.")

            return unique

        except Exception as exc:
            self.logger.error("[LiveSquackNewsDuplicator] fatal_filter_error: %s", exc, exc_info=True)
            return []
