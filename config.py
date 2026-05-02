import os
import json

class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
    YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
    GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
    OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
    SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "60"))

    _raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    try:
        GOOGLE_SERVICE_ACCOUNT_INFO = json.loads(_raw_json)
    except json.JSONDecodeError:
        GOOGLE_SERVICE_ACCOUNT_INFO = {}

    SELLER_KEYWORDS = [
        "want to sell my house", "selling my property",
        "urgent sale", "distress sale", "relocating and selling",
        "property for sale", "land for sale", "house for sale",
        "sell my land", "duplex for sale", "flat for sale",
        "bungalow for sale", "plot for sale"
    ]

    BUYER_KEYWORDS = [
        "looking for apartment in", "need 2 bedroom in",
        "house for rent in", "looking to buy property",
        "need a flat in", "looking for house in",
        "short let needed", "3 bedroom needed",
        "self contain needed", "mini flat needed"
    ]

    TARGET_LOCATIONS = [
        "Lagos", "Abuja", "Ibadan", "Port Harcourt",
        "Benin City", "Enugu", "Owerri", "Abeokuta",
        "Lekki", "Ajah", "Yaba", "Surulere", "Ikeja",
        "Victoria Island", "Ikoyi", "Ogun"
]
