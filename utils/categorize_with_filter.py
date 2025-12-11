import re
import pandas as pd
from datetime import datetime
from core.base import Base
from config.constants import CATEGORY_MAP, LEN_PANDAS_MIN_DOCS


class FilterCategorize(Base):
    def __init__(self):
        super().__init__(name="categorize_with_filter", save_time_logs=True)

        self.company_dict = self.fetch_load_symbolmap()
        if not self.company_dict:
            self.logger.error("❌ Company symbol map not loaded.")

        self.category_map = CATEGORY_MAP
        self.min_len_doc_for_df = LEN_PANDAS_MIN_DOCS
        self.logger.info(
            f"✅ Initialized Formator | symbolmap: {len(self.company_dict) if self.company_dict else 0}"
        )

    async def fetch_existing_news_ids(self):
        existing_news_ids = await self.collection_all_ann.distinct("news_id")
        return existing_news_ids

    # =========================================================
    # ---------------- FOR LOOP HELPER ------------------------
    # =========================================================
    async def helper_forloop(self, docs, existing_news_ids=[]):
        filtered, existing_ids = [], set(existing_news_ids)

        for rec in docs:
            try:
                attach = str(rec.get("AttachmentName", "")).strip()
                if not attach.endswith(".pdf"):
                    continue

                news_id = attach.replace(".pdf", "")
                if news_id in existing_ids:
                    continue

                bse_cd = str(rec.get("SCRIP_CD", "")).strip()
                if bse_cd not in self.company_dict:
                    continue

                info = self.company_dict[bse_cd]
                rec["symbolmap"] = info.get("symbolmap")
                rec["company"] = info.get("company")
                rec["news_id"] = news_id

                for k, v in rec.items():
                    if isinstance(v, str):
                        rec[k] = v.strip()

                try:
                    rec["Tradedate"] = datetime.strptime(
                        rec["Tradedate"], "%d/%m/%Y %H:%M:%S"
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue

                desc = rec.get("Descriptor", "").strip()
                head = rec.get("HeadLine", "").lower()
                body = rec.get("NewsBody", "").lower()

                if desc in self.category_map:
                    rec["category"] = desc
                else:
                    rec["category"] = next(
                        (
                            cat
                            for cat, rule in self.category_map.items()
                            if (rule["HeadLine"] and re.search(rule["HeadLine"], head, re.I))
                            or (rule["NewsBody"] and re.search(rule["NewsBody"], body, re.I))
                        ),
                        "General",
                    )

                filtered.append(rec)

            except Exception as e:
                self.logger.error(f"⚠️ skip {rec.get('news_id', '?')}: {e}")

        return filtered


    # =========================================================
    # ---------------- PANDAS HELPER --------------------------
    # =========================================================
    async def helper_pandas(self, docs, existing_news_ids=[]):
        df = pd.DataFrame(docs)
        df["SCRIP_CD"] = df["SCRIP_CD"].astype(str).str.strip()
        df = df[df["SCRIP_CD"].isin(self.company_dict.keys())].copy()
        df["AttachmentName"] = df["AttachmentName"].astype(str).str.strip()
        df["news_id"] = df["AttachmentName"].str.replace(".pdf", "", regex=False)

        if existing_news_ids:
            df = df[~df["news_id"].isin(existing_news_ids)].copy()
            if df.empty:
                return []

        df = df.reset_index(drop=True)

        df[["company", "symbolmap"]] = df["SCRIP_CD"].astype(str).apply(
            lambda x: pd.Series(self.company_dict.get(x, {})).reindex(["company", "symbolmap"])
        )

        df["HeadLine"] = df["HeadLine"].astype(str).str.strip().str.lower()
        df["NewsBody"] = df["NewsBody"].fillna("").astype(str).str.strip().str.lower()
        df["Tradedate"] = (
            pd.to_datetime(df["Tradedate"], errors="coerce", format="%d/%m/%Y %H:%M:%S")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )

        df.loc[df["Descriptor"].isin(self.category_map.keys()), "category"] = df["Descriptor"]

        for cat, rule in self.category_map.items():
            mask = df["category"].isna() & df["HeadLine"].str.contains(rule["HeadLine"], case=False, na=False)
            if rule["NewsBody"]:
                mask |= df["category"].isna() & df["NewsBody"].str.contains(rule["NewsBody"], case=False, na=False)
            df.loc[mask, "category"] = cat

        df["category"] = df["category"].fillna("General")

        return df.to_dict("records")


    # =========================================================
    # ---------------- MASTER SWITCH --------------------------
    # =========================================================
    async def run_formator(self, docs):
        n = len(docs)
        existing_news_ids = await self.fetch_existing_news_ids()
        if n < self.min_len_doc_for_df:
            self.logger.info(f"🌀 Processing {n} records using FOR-LOOP helper")
            all_docs = await self.helper_forloop(docs, existing_news_ids)
        else:
            self.logger.info(f"🚀 Processing {n} records using PANDAS helper")
            all_docs = await self.helper_pandas(docs, existing_news_ids)
        return all_docs
