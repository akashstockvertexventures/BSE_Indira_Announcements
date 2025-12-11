import asyncio
import aiohttp
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm
from config.constants import (
    BSE_INDIRA_HIST_MIN_DATE,
    BSE_INDIRA_HIST_MAX_DATE,
    BSE_INDIRA_TIMEOUT_SEC,
    BSE_INDIRA_RETRY_DELAY_SEC,
    BSE_INDIRA_CONCURRENCY_LIMIT,
    BSE_INDIRA_RETRY_COUNT,
    BSE_INDIRA_LIVE_DATA_DAYS
)
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
        self.logger = get_logger("bse_corp_ann_api", save_time_logs=True)
        self.headers = {"Content-Type": "application/json"}
        self.timeout_sec = BSE_INDIRA_TIMEOUT_SEC
        self.retry_delay_sec = BSE_INDIRA_RETRY_DELAY_SEC
        self.semaphore_limit = BSE_INDIRA_CONCURRENCY_LIMIT
        self.retry_count = BSE_INDIRA_RETRY_COUNT
        self.no_of_live_days = BSE_INDIRA_LIVE_DATA_DAYS - 1

        if not BSE_INDIRA_API_URL:
            self.logger.error("❌ Missing BSE_INDIRA_API_URL in settings.py")

        self.bseapi_hist_mindate = _normalize_datetime(BSE_INDIRA_HIST_MIN_DATE) or datetime(2023, 11, 1)
        self.bseapi_hist_maxdate = _normalize_datetime(BSE_INDIRA_HIST_MAX_DATE) or datetime(2025, 10, 31)
        self.logger.info(f"✅ Initialized BseCorpAnnouncements | Hist Range: {self.bseapi_hist_mindate} → {self.bseapi_hist_maxdate}")

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

    # ------------------ ASYNC FETCH (retry + backoff) ------------------
    async def _fetch_for_date(self, session: aiohttp.ClientSession, payload: dict, sem: asyncio.Semaphore) -> tuple[str, list]:
        tradedt = payload.get("tradedt", "")

        async with sem:
            async def _call_api():
                try:
                    async with session.post(
                        BSE_INDIRA_API_URL,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
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

            # 🔁 Retry logic with exponential backoff
            for attempt in range(1, self.retry_count + 1):
                result = await _call_api()
                if result:
                    return tradedt, result
                if attempt < self.retry_count:
                    delay = self.retry_delay_sec * (2 ** (attempt - 1))
                    self.logger.warning(f"Retry {attempt}/{self.retry_count} failed for {tradedt}, retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)

            return tradedt, []

    # ------------------ HISTORICAL FETCH ------------------
    async def fetch_hist_announcements(self, from_date=None, to_date=None):
        from_dt = from_date or self.bseapi_hist_mindate
        to_dt = to_date or self.bseapi_hist_maxdate

        if from_date and (from_dt < self.bseapi_hist_mindate or from_dt > self.bseapi_hist_maxdate):
            self.logger.info(f"from_date below min or above max → reset to {self.bseapi_hist_mindate.date()}")
            from_dt = self.bseapi_hist_mindate

        if to_date and (to_dt > self.bseapi_hist_maxdate or to_dt < self.bseapi_hist_mindate):
            self.logger.info(f"to_date above max → clamped to {self.bseapi_hist_maxdate.date()}")
            to_dt = self.bseapi_hist_maxdate

        if from_dt > to_dt:
            self.logger.info(f"Swapped invalid range: {from_dt.date()} > {to_dt.date()}")
            from_dt, to_dt = to_dt, from_dt

        from_dt = max(from_dt, self.bseapi_hist_mindate)
        to_dt = min(to_dt, self.bseapi_hist_maxdate)

        self.logger.info(f"Fetching historical range: {from_dt.date()} → {to_dt.date()}")

        sem = asyncio.Semaphore(self.semaphore_limit)
        all_data = []

        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = []
            curr_dt = from_dt
            while curr_dt <= to_dt:
                payload = self._ensure_payload_fields(BSE_INDIRA_API_PARAMS_Hist.copy(), curr_dt)
                tasks.append(asyncio.create_task(self._fetch_for_date(session, payload, sem)))
                curr_dt += timedelta(days=1)

            results = []
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="📡 Fetching Hist Data"):
                results.append(await coro)
            # results = await asyncio.gather(*tasks)

        all_data = []
        for date_str, data in results:
            self.logger.info(f"📆 {date_str} → {len(data)} records")
            all_data.extend(data)

        self.logger.info(f"✅ Historical fetch complete: {len(all_data)} total records.")
        return all_data

    # ------------------ LIVE FETCH ------------------
    async def fetch_live_announcements(self, lastnews_dt_tm=None):

        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        window_start = today - timedelta(days=self.no_of_live_days)

        # --- Determine final start date ---
        if not lastnews_dt_tm:
            last_dt = window_start
            self.logger.info(f"No lastnews_dt_tm provided. Using {self.no_of_live_days}-day window from {window_start.date()}")
        else:
            last_dt = _normalize_datetime(lastnews_dt_tm)
            if not last_dt:
                self.logger.warning(f"Invalid lastnews_dt_tm '{lastnews_dt_tm}', defaulting to {window_start.date()}")
                last_dt = window_start
            elif last_dt < window_start:
                self.logger.info(f"lastnews_dt_tm before window → adjusted to {window_start.date()}")
                last_dt = window_start
            elif last_dt > today:
                self.logger.info(f"lastnews_dt_tm beyond today → adjusted to {today.date()}")
                last_dt = today

        # --- Decide mode: single day vs range ---
        if last_dt == today:
            self.logger.info(f"Fetching live data for {today.date()} (single-day mode)")
            async with aiohttp.ClientSession(headers=self.headers) as session:
                sem = asyncio.Semaphore(self.semaphore_limit)
                payload = self._ensure_payload_fields(BSE_INDIRA_API_PARAMS_Live.copy(), today)
                _, data = await self._fetch_for_date(session, payload, sem)
                self.logger.info(f"📆 {today.date()} → {len(data)} records (live)")
                return data or []

        # --- Multi-day mode ---
        self.logger.info(f"Fetching live range: {last_dt.date()} → {today.date()}")
        sem = asyncio.Semaphore(self.semaphore_limit)
        all_data = []

        async with aiohttp.ClientSession(headers=self.headers) as session:
            date_range = [
                self._ensure_payload_fields(BSE_INDIRA_API_PARAMS_Live.copy(), d)
                for d in (last_dt + timedelta(days=i) for i in range((today - last_dt).days + 1))
            ]

            # Run concurrent API calls
            tasks = [asyncio.create_task(self._fetch_for_date(session, p, sem)) for p in date_range]
            results = []
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="📡 Fetching Live Data"):
                results.append(await coro)
            # results = await asyncio.gather(*tasks)

        all_data = []
        for date_str, data in results:
            self.logger.info(f"📆 {date_str} → {len(data)} records")
            all_data.extend(data)

        self.logger.info(f"✅ Live fetch complete: {len(all_data)} total records.")
        return all_data


