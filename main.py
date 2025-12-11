import asyncio
from datetime import datetime
from processes.bse_corp_ann_api import BSECorpAnnouncementClient
from utils.categorize_with_filter import FilterCategorize
from utils.reports_divider import ReportsDivider


async def main():
    from_date = datetime(2025, 9, 1)
    to_date = datetime(2025, 9, 2)

    print(f"🚀 Fetching BSE historical announcements from {from_date.date()} to {to_date.date()}...\n")

    client = BSECorpAnnouncementClient()
    filter_and_cat = FilterCategorize()
    report_divider = ReportsDivider()

    hist_data = await client.fetch_hist_announcements(from_date=from_date, to_date=to_date)
    if not hist_data:
        print("⚠️ No data received from BSE API.")
        return

    final_data = await filter_and_cat.run_formator(hist_data)
    if not final_data:
        print("⚠️ No valid formatted data after categorization.")
        return

    await report_divider.process_and_distribute_reports(final_data)

    print(f"\n✅ Done. Total records fetched: {len(final_data)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Process interrupted manually.")
    except Exception as e:
        print(f"\n❌ Fatal error in main(): {e}")
