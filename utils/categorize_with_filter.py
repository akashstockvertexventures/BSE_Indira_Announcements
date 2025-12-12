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
        self.compile_category_rules()
        self.min_len_doc_for_df = LEN_PANDAS_MIN_DOCS
        self.logger.info(
            f"✅ Initialized Formator | symbolmap: {len(self.company_dict) if self.company_dict else 0}"
        )

    async def fetch_existing_news_ids(self, trade_date: str) -> list:
        cursor = self.collection_all_ann.find(
            {"Tradedate": {"$gte": trade_date}},
            {"_id": 0, "news_id": 1}
        ).sort("Tradedate", -1)
        news_ids = {doc["news_id"] async for doc in cursor if doc.get("news_id")}
        return list(news_ids)

    # ---------------- REGEX PRECOMPILATION -------------------
    def compile_category_rules(self):
        """Precompile regex patterns once for speed."""
        self.compiled_rules = {}
        for cat, rule in self.category_map.items():
            self.compiled_rules[cat] = {
                "HeadLine": re.compile(rule["HeadLine"], re.I) if rule.get("HeadLine") else None,
                "NewsBody": re.compile(rule["NewsBody"], re.I) if rule.get("NewsBody") else None,
            }
        self.logger.info(f"🧠 Precompiled {len(self.compiled_rules)} category regex rules.")

    # ---------------- FOR LOOP HELPER ------------------------
    async def helper_forloop(self, docs, existing_news_ids=None):
        if existing_news_ids is None:
            existing_news_ids = []

        filtered = []
        existing_ids = set(existing_news_ids)

        for rec in docs:
            try:
                attach = str(rec.get("AttachmentName", "")).strip()
                if not attach.endswith(".pdf"):
                    continue

                news_id = attach[:-4]
                if news_id in existing_ids:
                    continue

                bse_cd = str(rec.get("SCRIP_CD", "")).strip()
                info = self.company_dict.get(bse_cd)
                if not info:
                    continue

                rec["news_id"] = news_id
                rec["symbolmap"] = info.get("symbolmap")
                rec["company"] = info.get("company")

                for k, v in rec.items():
                    if isinstance(v, str):
                        rec[k] = v.strip()

                try:
                    rec["Tradedate"] = datetime.strptime(
                        rec["Tradedate"], "%d/%m/%Y %H:%M:%S"
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue

                desc = str(rec.get("Descriptor", "")).strip()
                head = str(rec.get("HeadLine", "")).lower()
                body = str(rec.get("NewsBody", "")).lower()

                if desc in self.category_map:
                    rec["category"] = desc
                else:
                    rec["category"] = next(
                        (
                            cat
                            for cat, rule in self.compiled_rules.items()
                            if (rule["HeadLine"] and rule["HeadLine"].search(head))
                            or (rule["NewsBody"] and rule["NewsBody"].search(body))
                        ),
                        "General",
                    )

                filtered.append(rec)
                existing_ids.add(news_id)

            except Exception as e:
                self.logger.error(f"⚠️ Error processing record {rec.get('news_id', '?')}: {e}")

        self.logger.info(f"✅ Processed {len(filtered)} new records (FOR-LOOP)")
        return filtered, list(existing_ids)

    # ---------------- PANDAS HELPER --------------------------
    async def helper_pandas(self, docs, existing_news_ids=None):
        if existing_news_ids is None:
            existing_news_ids = []
        if not docs:
            return []

        df = pd.json_normalize(docs)
        if df.empty:
            return []

        valid_scrips = set(self.company_dict.keys())
        df["AttachmentName"] = df["AttachmentName"].astype(str).str.strip()
        df.query('AttachmentName.str.endswith(".pdf")', inplace=True)

        df["news_id"] = df["AttachmentName"].str[:-4]
        df["SCRIP_CD"] = df["SCRIP_CD"].astype(str).str.strip()
        df.query("SCRIP_CD in @valid_scrips", inplace=True)
        df.drop_duplicates(subset="news_id", inplace=True)

        if existing_news_ids:
            existing_ids = set(existing_news_ids)
            df = df[~df["news_id"].isin(existing_ids)]
        if df.empty:
            return []

        company_df = (
            pd.DataFrame.from_dict(self.company_dict, orient="index")
            .reindex(columns=["company", "symbolmap"])
            .rename_axis("SCRIP_CD")
            .reset_index()
        )
        df = df.merge(company_df, on="SCRIP_CD", how="left")

        df["HeadLine"] = df["HeadLine"].astype(str).str.lower().str.strip()
        df["NewsBody"] = df["NewsBody"].fillna("").astype(str).str.lower().str.strip()
        df["Descriptor"] = df["Descriptor"].astype(str).str.strip()

        df["Tradedate"] = pd.to_datetime(df["Tradedate"], errors="coerce", dayfirst=True)
        df.dropna(subset=["Tradedate"], inplace=True)
        df["Tradedate"] = df["Tradedate"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df["category"] = None
        df.loc[df["Descriptor"].isin(self.category_map.keys()), "category"] = df["Descriptor"]

        # ✅ FIXED MASK LOGIC (no NoneType OR issue)
        for cat, rule in self.compiled_rules.items():
            mask = df["category"].isna()
            conds = []

            if rule["HeadLine"]:
                conds.append(df["HeadLine"].str.contains(rule["HeadLine"].pattern, case=False, na=False))
            if rule["NewsBody"]:
                conds.append(df["NewsBody"].str.contains(rule["NewsBody"].pattern, case=False, na=False))

            if not conds:
                continue

            combined_mask = conds[0]
            for c in conds[1:]:
                combined_mask |= c

            df.loc[mask & combined_mask, "category"] = cat

        # ✅ Future-proof: no inplace warning
        df["category"] = df["category"].fillna("General")

        self.logger.info(f"✅ Processed {len(df)} new records (PANDAS)")
        return df.to_dict("records")

    # ---------------- MASTER SWITCH --------------------------
    async def run_formator(self, docs, tradedate):
        n = len(docs)
        existing_news_ids = await self.fetch_existing_news_ids(tradedate)

        if n < self.min_len_doc_for_df:
            self.logger.info(f"🌀 Processing {n} records using FOR-LOOP helper")
            all_docs, _ = await self.helper_forloop(docs, existing_news_ids)
        else:
            self.logger.info(f"🚀 Processing {n} records using PANDAS helper")
            all_docs = await self.helper_pandas(docs, existing_news_ids)

        return all_docs
