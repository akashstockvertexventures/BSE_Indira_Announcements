from datetime import datetime
import pandas as pd
from core.base import Base
from config.constants import SOURCE_BSE, SOURCE_LIVESQUACK


class Formatter(Base):
    def __init__(self):
        super().__init__()

    def _extract_stock(self, symbolmap):
        if isinstance(symbolmap, dict):
            return symbolmap.get("SELECTED")
        return None

    def prepare_dashboard_rows(self, source, docs):
        if not docs:
            return []

        rows = []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for d in docs:
            if source == SOURCE_LIVESQUACK:
                d["news link"] = d.get("news link", "")
                d["summary"] = d.get("summary", "")
                d["source"] = SOURCE_LIVESQUACK
            else:
                d["news link"] = d.get("pdf_link_live", "")
                d["short summary"] = d.get("shortsummary", "")
                d["impact score"] = d.get("impactscore", "")
                d["category"] = d.get("subcategory") if d.get("category") == "Financial Results" and not d.get("subcategory") == "Outcome of Board Meeting" else d.get("category")
                d["source"] = SOURCE_BSE

            symbolmap = d.get("symbolmap")
            stock = self._extract_stock(symbolmap)
            if not stock:
                self.logger.info("skip doc no stock | news_id=%s", d.get("news_id"))
                continue

            try:
                dt_tm = pd.to_datetime(d.get("dt_tm"))
                if pd.isna(dt_tm):
                    self.logger.info("skip doc invalid dt_tm | news_id=%s", d.get("news_id"))
                    continue
            except Exception:
                self.logger.info("skip doc parse dt_tm failed | news_id=%s", d.get("news_id"))
                continue

            d["stock"] = stock
            d["dt_tm"] = dt_tm
            d["duplicate"] = False
            d["document_date"] = now

            row = {
                "stock": d.get("stock"),
                "company": d.get("company"),
                "news_id": d.get("news_id"),
                "dt_tm": d.get("dt_tm"),
                "news link": d.get("news link", ""),
                "impact": d.get("impact"),
                "impact score": d.get("impact score"),
                "sentiment": d.get("sentiment"),
                "category": d.get("category"),
                "short summary": d.get("short summary"),
                "summary": d.get("summary"),
                "symbolmap": d.get("symbolmap"),
                "document_date": d.get("document_date"),
                "duplicate": d.get("duplicate"),
                "source": d.get("source")
            }
            rows.append(row)

        return rows
