
duplicate_news_ids = [
    "b7f75bfa-33ec-42b6-ae47-063f3ee45061",
    "95dfce52-f4fb-4fc8-a2bf-c6b32e82e44c",
    "a63fb8f5-f0e4-40ec-980a-3da3187a7836"
  ]

import asyncio, os
from collections import defaultdict
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
load_dotenv()

# === Mongo / Database ===
MONGO_URI = os.getenv("MONGO_URI")


async def remove_duplicate_news_ids():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client["BSECorpReports"]
    collection = db["AllReports"]

    print("🧹 Running async deduplication (single query mode)...")

    # 1️⃣ Single query: fetch all duplicate news_id docs
    cursor = collection.find(
        {"news_id": {"$in": duplicate_news_ids}},
        {"_id": 1, "news_id": 1}
    )

    # 2️⃣ Group all _ids by news_id
    grouped = defaultdict(list)
    async for doc in cursor:
        grouped[doc["news_id"]].append(doc["_id"])

    # 3️⃣ Collect all duplicate _ids to delete
    all_duplicate_ids = []
    for news_id, ids in grouped.items():
        if len(ids) > 1:
            keep_id = ids[0]
            dup_ids = ids[1:]
            all_duplicate_ids.extend(dup_ids)
            print(f"🧾 news_id={news_id} — keeping {keep_id}, marking {len(dup_ids)} for deletion")

    # 4️⃣ Bulk delete once
    if all_duplicate_ids:
        result = await collection.delete_many({"_id": {"$in": all_duplicate_ids}})
        print(f"✅ Bulk delete complete — removed {result.deleted_count} duplicates total.")
    else:
        print("🎯 No duplicates found to delete.")

    client.close()

if __name__ == "__main__":
    asyncio.run(remove_duplicate_news_ids())
