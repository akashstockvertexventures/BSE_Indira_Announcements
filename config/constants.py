import torch

SOURCE_LIVESQUACK = "Livesquack"
SOURCE_BSE = "BSE"

CAT_NEWS_REMOVE_LIVESQUACK = [
    "Financial Results", "Broker Report"
]

NO_OF_ARCHIVE_DAYS = 10

CAT_NO_CHECK_DUPLICATE_DASHBOARD = [
    "Investor Presentation", "Earnings Call Transcript", "Broker Report"
]

SAFE_QUERY = {"$type": "string", "$nin": ["", " "]}

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
