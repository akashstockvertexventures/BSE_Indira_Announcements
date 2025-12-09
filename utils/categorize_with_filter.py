import re
import pandas as pd
from datetime import datetime
from core.base import Base
from config.constants import CATEGORY_MAP, LEN_PANDAS_MIN_DOCS


class FilterCategorize(Base):
    def __init__(self):
        super().__init__(name="bse_formator", save_time_logs=True)

        self.company_dict = self.fetch_load_symbolmap()
        if not self.company_dict:
            self.logger.error("❌ Company symbol map not loaded.")

        self.category_map = CATEGORY_MAP
        self.min_len_doc_for_df = LEN_PANDAS_MIN_DOCS
        self.logger.info(
            f"✅ Initialized Formator | symbolmap: {len(self.company_dict) if self.company_dict else 0}"
        )

    # =========================================================
    # ---------------- FOR LOOP HELPER ------------------------
    # =========================================================
    def helper_forloop(self, data):
        filtered = []
        for rec in data:
            try:
                bse_cd = str(rec.get("SCRIP_CD", "")).strip()
                if bse_cd not in self.company_dict:
                    continue
                rec["symbolmap"] = self.company_dict[bse_cd]

                for k, v in rec.items():
                    if isinstance(v, str):
                        rec[k] = v.strip()
                    if k == "AttachmentName" and isinstance(v, str):
                        rec["news_id"] = v.replace(".pdf", "")
                    if k == "Tradedate" and isinstance(v, str):
                        try:
                            rec[k] = datetime.strptime(
                                v, "%d/%m/%Y %H:%M:%S"
                            ).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            self.logger.info(
                                f"news_id skip — invalid Tradedate for {rec.get('news_id', 'unknown')}"
                            )
                            raise ValueError("Invalid Tradedate")

                desc = rec.get("Descriptor", "").strip()
                head = rec.get("HeadLine", "").lower()
                body = rec.get("NewsBody", "").lower()

                matched = False
                if desc in self.category_map:
                    rec["Category"] = desc
                    matched = True
                else:
                    for cat, rule in self.category_map.items():
                        if (rule["HeadLine"] and re.search(rule["HeadLine"], head, re.I)) or (
                            rule["NewsBody"] and re.search(rule["NewsBody"], body, re.I)
                        ):
                            rec["Category"] = cat
                            matched = True
                            break

                if not matched:
                    rec["Category"] = "General"

                filtered.append(rec)

            except Exception as e:
                self.logger.error(f"news_id skip — cleaning or categorization error: {e}")

        return filtered

    # =========================================================
    # ---------------- PANDAS HELPER --------------------------
    # =========================================================
    def helper_pandas(self, docs):
        df = pd.DataFrame(docs)
        df["SCRIP_CD"] = df["SCRIP_CD"].astype(str).str.strip()
        df = df[df["SCRIP_CD"].isin(self.company_dict.keys())].copy()
        df["symbolmap"] = df["SCRIP_CD"].map(self.company_dict)
        df["AttachmentName"] = df["AttachmentName"].astype(str).str.strip()
        df["news_id"] = df["AttachmentName"].str.replace(".pdf", "", regex=False)
        df["HeadLine"] = df["HeadLine"].astype(str).str.strip().str.lower()
        df["NewsBody"] = df["NewsBody"].fillna("").astype(str).str.strip().str.lower()
        df["Tradedate"] = (
            pd.to_datetime(df["Tradedate"], errors="coerce", format="%d/%m/%Y %H:%M:%S")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )

        df.loc[df["Descriptor"].isin(self.category_map.keys()), "Category"] = df["Descriptor"]

        for cat, rule in self.category_map.items():
            mask = df["Category"].isna() & df["HeadLine"].str.contains(
                rule["HeadLine"], case=False, na=False
            )
            if rule["NewsBody"]:
                mask |= df["Category"].isna() & df["NewsBody"].str.contains(
                    rule["NewsBody"], case=False, na=False
                )
            df.loc[mask, "Category"] = cat

        df["Category"] = df["Category"].fillna("General")

        return df.to_dict("records")

    # =========================================================
    # ---------------- MASTER SWITCH --------------------------
    # =========================================================
    def run_formator(self, docs):
        n = len(docs)
        if n < self.min_len_doc_for_df:
            self.logger.info(f"🌀 Processing {n} records using FOR-LOOP helper")
            all_docs = self.helper_forloop(docs)
        else:
            self.logger.info(f"🚀 Processing {n} records using PANDAS helper")
            all_docs = self.helper_pandas(docs)
        return all_docs
