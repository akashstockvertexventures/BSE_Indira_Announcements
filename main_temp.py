import asyncio
from datetime import datetime
from CorpReports.processes.bse_corp_ann_api import BSECorpAnnouncementClient  # adjust import path if needed

async def main():
    client = BSECorpAnnouncementClient()

    from_date = datetime(2025, 9, 1)
    to_date = datetime(2025, 9, 20)

    print(f"🚀 Fetching BSE historical announcements from {from_date.date()} to {to_date.date()}...\n")

    hist_data = await client.fetch_hist_announcements(from_date=from_date, to_date=to_date)

    print(f"\n✅ Done. Total records fetched: {len(hist_data)}")

    # Optional: save results to file
    import json
    with open("bse_hist_2025_09_01_20.json", "w", encoding="utf-8") as f:
        json.dump(hist_data, f, ensure_ascii=False, indent=2)

    print("📁 Saved to bse_hist_2025_09_01_20.json")

if __name__ == "__main__":
    asyncio.run(main())
