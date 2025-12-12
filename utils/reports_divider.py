from datetime import datetime
from core.base import Base
import pandas as pd
import asyncio
from pymongo.errors import BulkWriteError
from config.constants import CATEGORY_MAP

class ReportsDivider(Base):
    def __init__(self):
        super().__init__(name="bse_reports_divider", save_time_logs=True)
        self.logger.info("✅ Initialized ReportsDivider")
        self.mongodb_insert_batch = 1000

    async def insert_in_batches(self, collection, docs, category=None):
        if not docs:
            self.logger.info(f"⚠️ No docs to insert for {category or collection.name}")
            return
        batch_size = self.mongodb_insert_batch
        total = len(docs)
        total_batches = (total + batch_size - 1) // batch_size
        inserted = 0
        duplicates = 0
        for i in range(0, total, batch_size):
            chunk = docs[i:i + batch_size]
            try:
                res = await collection.insert_many(chunk, ordered=False)
                inserted += len(res.inserted_ids)
            except BulkWriteError as e:
                write_errors = e.details.get("writeErrors", [])
                duplicates += sum(1 for err in write_errors if err.get("code") == 11000)
                inserted += e.details.get("nInserted", 0)
            except Exception:
                self.logger.warning(f"⚠️ Batch {i//batch_size + 1}/{total_batches} → {category or collection.name}: Exception")
                continue
            self.logger.info(f"✅ Batch {i//batch_size + 1}/{total_batches} → Inserted {inserted}/{total} (Skipped {duplicates} dups) → {category or collection.name}")
            await asyncio.sleep(0.5)
        self.logger.info(f"📦 Done → Inserted {inserted}/{total} (Skipped {duplicates} duplicates) → {category or collection.name}")

    async def structure_report_doc(self, doc: dict):
        try:
            category = doc.get("category")
            cat_info = CATEGORY_MAP.get(category, {})
            if not cat_info:
                return
            
            short_cat = cat_info.get("short_name", category[:2].upper())
            tradedate = doc.get("Tradedate")
            company = doc.get("company")
            news_id = doc.get("news_id")
            symbolmap = doc.get("symbolmap", {})
            attachment_url = doc.get("ATTACHMENTURL")
            newsbody = doc.get("NewsBody", "")
            dt_obj = datetime.strptime(tradedate, "%Y-%m-%d %H:%M:%S")
            datecode = dt_obj.strftime("%Y%m%d")
            month = dt_obj.month
            year = dt_obj.year           

            if 1 <= month <= 3:
                qtr = "Q3"
                fin_year = year - 1
            elif 4 <= month <= 6:
                qtr = "Q4"
                fin_year = year - 1
            elif 7 <= month <= 9:
                qtr = "Q1"
                fin_year = year
            else:
                qtr = "Q2"
                fin_year = year

            existing_report_ids = self.collection_all_reports.distinct('report_id', {"company":company, "Year": fin_year, "Qtr": qtr })
            company_short_cat_year_qtr_count = len(existing_report_ids) + 1

            report_id = f"{company}_{short_cat}_FY{year}{qtr}_{company_short_cat_year_qtr_count}"
            new_doc = {
                "company": company,
                "symbolmap": symbolmap,
                "news_id": news_id,
                "datecode": datecode,
                "Year": fin_year,
                "Qtr": qtr,
                "dt_tm": tradedate,
                "url": attachment_url,
                "report_id": report_id,
                "report_type": category,
                "report_line": newsbody,
                "count": company_short_cat_year_qtr_count,
                "document_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            return new_doc

        except Exception as e:
            self.logger.error(f"structure_report_doc error: {e}")
            return None
        
    def build_existing_counts_map(self, category_existing_report_ids: list) -> dict:
        counts_map = {}
        for rid in category_existing_report_ids:
            try:
                prefix = "_".join(rid.split("_")[:-1])
                counts_map[prefix] = counts_map.get(prefix, 0) + 1
            except Exception:
                continue
        return counts_map


    async def format_category_docs(self, df: pd.DataFrame, category: str, short_cat: str, existing_report_id_mapping: dict) -> list:
        try:
            df = df.sort_values(by="Tradedate", ascending=True)
            df["dt_obj"] = pd.to_datetime(df["Tradedate"], errors="coerce")
            df["month"] = df["dt_obj"].dt.month
            df["year"] = df["dt_obj"].dt.year
            df["Qtr"] = pd.cut(
                df["month"],
                bins=[0, 3, 6, 9, 12],
                labels=["Q3", "Q4", "Q1", "Q2"],
                include_lowest=True
            )
            df["FinYear"] = df.apply(lambda x: x["year"] - 1 if x["Qtr"] in ["Q3", "Q4"] else x["year"], axis=1)

            df["base_report_id"] = (df["company"] + "_" + short_cat + "_FY" + df["year"].astype(str) + df["Qtr"].astype(str))
            final_rows = []
            for base_id, group in df.groupby("base_report_id", sort=False):
                start_count = existing_report_id_mapping.get(base_id, 0)
                group = group.sort_values(by="dt_obj", ascending=True).reset_index(drop=True)
                group["count"] = range(start_count + 1, start_count + 1 + len(group))
                group["report_id"] = group["base_report_id"] + "_" + group["count"].astype(str)
                final_rows.append(group)

            df = pd.concat(final_rows, ignore_index=True)
            df["datecode"] = df["dt_obj"].dt.strftime("%Y%m%d")

            structured_df = pd.DataFrame({
                "company": df["company"],
                "symbolmap": df.get("symbolmap"),
                "news_id": df.get("news_id"),
                "datecode": df["datecode"],
                "Year": df["FinYear"],
                "Qtr": df["Qtr"],
                "dt_tm": df["Tradedate"],
                "url": df.get("ATTACHMENTURL"),
                "report_id": df["report_id"],
                "report_type": category,
                "report_line": df.get("NewsBody"),
                "count": df["count"],
                "document_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            return structured_df.to_dict(orient="records")

        except Exception as e:
            self.logger.error(f"❌ format_category_docs error for {category}: {e}")
            return []
        

    async def get_category_existing_report_ids(self, category: str, trade_date: str) -> list:
        if datetime.strptime(trade_date, "%Y-%m-%d %H:%M:%S").date() >= datetime.now().date():
            ids = await self.collection_all_reports.distinct(
                "report_id", {"report_type": category, "dt_tm": {"$gte": trade_date}}
            )
        else:
            ids = {
                d["report_id"] async for d in self.collection_all_reports.find(
                    {"dt_tm": {"$gte": trade_date}},
                    {"_id": 0, "report_id": 1}
                ).sort("dt_tm", -1)
                if d.get("report_id")
            }
        return list(ids)

    async def all_reports_runner(self, docs, tradedate, all_category_is_general=False):
        if not docs:
            return
        if not all_category_is_general:
            df = pd.DataFrame(docs)
            for category, cat_info in CATEGORY_MAP.items():
                category_existing_report_ids = await self.get_category_existing_report_ids(category, tradedate)
                df_filtered = df[
                    (df["category"] == category)
                    & (~df["news_id"].isin(category_existing_report_ids))
                ]
                if df_filtered.empty:
                    continue
                existing_report_id_mapping = self.build_existing_counts_map(category_existing_report_ids)
                short_cat = cat_info.get("short_name", category[:2].upper())
                structured_docs = await self.format_category_docs(
                    df_filtered, category, short_cat, existing_report_id_mapping
                )
                if structured_docs:
                    await self.insert_in_batches(
                        collection=self.collection_all_reports,
                        docs=structured_docs,
                        category=category,
                    )

    async def divide_and_insert_docs(self, docs, tradedate):
        try:
            if not docs:
                self.logger.info("No docs found for reports_divider")
                return
            
            df = pd.DataFrame(docs)
            all_category_is_general = False
            if "category" not in df.columns:
                df["category"] = "General"
                all_category_is_general = True
            
            annoucement_docs = df.to_dict(orient="records")
            await self.insert_in_batches(collection=self.collection_all_ann, docs=annoucement_docs)
            await self.all_reports_runner(docs=annoucement_docs, tradedate=tradedate, all_category_is_general=all_category_is_general)

        except Exception as e:
            self.logger.error(f"❌ process_and_distribute_reports_df failed: {e}")


