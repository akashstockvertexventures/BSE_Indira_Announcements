import asyncio
import aiohttp
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from config.constants import BSE_INDIRA_HIST_MIN_DATE, BSE_INDIRA_HIST_MAX_DATE
from config.settings import BSE_INDIRA_API_URL, BSE_INDIRA_API_PARAMS_Live, BSE_INDIRA_API_PARAMS_Hist
from core.logger import get_logger


# ====================== UTILITIES ======================
def _normalize_datetime(dt_val):
    """Try to parse multiple datetime formats into datetime object."""
    if not dt_val:
        return None
    if isinstance(dt_val, datetime):
        return dt_val
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y%m%d",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(str(dt_val), fmt)
        except ValueError:
            continue
    return None

# ====================== MAIN CLIENT ======================
class BSECorpAnnouncementClient:
    def __init__(self):
        self.logger = get_logger("bse_corp_ann_async", save_time_logs=True)
        self.headers = {"Content-Type": "application/json"}
        self.TIMEOUT_SEC = 20
        self.RETRY_DELAY_SEC = 5
        self.CONCURRENCY_LIMIT = 5
        if not BSE_INDIRA_API_URL:
            self.logger.error("❌ Missing BSE_INDIRA_API_URL in settings.py")

        if not (BSE_INDIRA_HIST_MIN_DATE or BSE_INDIRA_HIST_MAX_DATE):
            self.bseapi_hist_mindate = datetime(2023, 11, 1) 
            self.bseapi_hist_maxdate = datetime(2025, 10, 31) 

        self.logger.info(f"✅ Initialized BseCorpAnnouncemnets | Hist: {self.bseapi_hist_mindate} → {self.bseapi_hist_maxdate}")


    # ------------------ PAYLOAD BUILDER ------------------
    def _ensure_payload_fields(self, payload: dict, date_time: datetime) -> dict:
        if not isinstance(date_time, datetime):
            raise TypeError(f"Expected datetime, got {type(date_time).__name__}")
        p = payload.copy()
        p["tradedt"] = date_time.strftime("%Y%m%d")
        p["hr"] = f"{date_time.hour:02d}"
        p["min"] = f"{date_time.minute:02d}"
        p["sec"] = f"{date_time.second:02d}"
        return p

    # ------------------ ASYNC FETCH ------------------
    async def _fetch_for_date(self, session: aiohttp.ClientSession, payload: dict, sem: asyncio.Semaphore) -> tuple[str, list]:
        tradedt = payload.get("tradedt", "")

        async with sem:
            async def _call_api():
                try:
                    async with session.post(
                        BSE_INDIRA_API_URL,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=self.TIMEOUT_SEC),
                    ) as resp:
                        if resp.status != 200:
                            self.logger.warning(f"HTTP {resp.status} for {tradedt}")
                            return []

                        data = await resp.json(content_type=None)

                        if isinstance(data, dict):
                            if data.get("Error_Msg") == "No Record found":
                                return []
                            self.logger.warning(f"Unexpected dict response for {tradedt}")
                            return []

                        if not isinstance(data, list):
                            self.logger.warning(f"Unexpected type: {type(data).__name__} for {tradedt}")
                            return []

                        return data

                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout for {tradedt}")
                except aiohttp.ClientError as e:
                    self.logger.warning(f"Request failed for {tradedt}: {e}")
                except Exception as e:
                    self.logger.error(f"Unexpected error for {tradedt}: {e}")
                return []

            result = await _call_api()
            if not result:
                self.logger.info(f"Retrying for {tradedt} after delay of {self.RETRY_DELAY_SEC}s")
                await asyncio.sleep(self.RETRY_DELAY_SEC)
                result = await _call_api()
            return tradedt, result

    # ------------------ HISTORICAL FETCH ------------------
    async def fetch_hist_announcements(self, from_date=None, to_date=None):
        from_dt = from_date or self.bseapi_hist_mindate
        to_dt = to_date or self.bseapi_hist_maxdate

        if not (from_dt or to_dt):
            self.logger.error("❌ no valid historical date range provided.")
            return []

        if from_dt > to_dt:
            from_dt, to_dt = to_dt, from_dt

        self.logger.info(f"Fetching historical range: {from_dt.date()} → {to_dt.date()}")

        all_data = []

        sem = asyncio.Semaphore(self.CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            curr_dt = from_dt
            tasks = []

            while curr_dt <= to_dt:
                payload = self._ensure_payload_fields(BSE_INDIRA_API_PARAMS_Hist.copy(), curr_dt)
                tasks.append(self._fetch_for_date(session, payload, sem))
                curr_dt += timedelta(days=1)

            for coro in tqdm.as_completed(tasks, desc="📅 Fetching Historical Data", total=len(tasks)):
                date_str, data = await coro
                self.logger.info(f"📆 {date_str} → {len(data)} records (filtered)")
                if data:
                    all_data.extend(data)
                await asyncio.sleep(0.05)

        self.logger.info(f"✅ Historical fetch complete: {len(all_data)} total filtered records.")
        return all_data

    # ------------------ LIVE FETCH ------------------
    async def fetch_live_announcements(self, lastnews_dt_tm=None):
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        window_start = today - timedelta(days=4)

        if not lastnews_dt_tm:
            last_dt = window_start
        else:
            last_dt = _normalize_datetime(lastnews_dt_tm)
            if last_dt < window_start:
                last_dt = window_start

        all_data = []
        sem = asyncio.Semaphore(self.CONCURRENCY_LIMIT)

        async with aiohttp.ClientSession(headers=self.headers) as session:
            curr_date = last_dt
            while curr_date <= today:
                payload = self._ensure_payload_fields(BSE_INDIRA_API_PARAMS_Live.copy(), curr_date)
                date_str, data = await self._fetch_for_date(session, payload, sem)
                self.logger.info(f"📆 {date_str} → {len(data)} records (filtered, live)")
                if data:
                    all_data.extend(data)
                curr_date += timedelta(days=1)
                await asyncio.sleep(0.2)

        self.logger.info(f"✅ Live fetch complete: {len(all_data)} total filtered records.")
        return all_data
