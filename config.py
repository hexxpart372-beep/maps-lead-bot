import os
import json
import re

class Config:
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
    YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
    GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
    OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
    SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "60"))

    @staticmethod
def get_service_account_info():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    raw = raw.strip()
    
    # Remove outer quotes if present
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1]
    
    # Fix all escape sequences
    raw = raw.replace("\\n", "\n")
    raw = raw.replace("\\r", "")
    raw = raw.replace("\\t", "")
    
    # Find and fix the private key specifically
    import re
    def fix_key(m):
        return m.group(0).replace("\n", "\\n")
    
    # Parse with strict=False to allow control characters
    try:
        return json.loads(raw, strict=False)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    SELLER_KEYWORDS = [
        "for sale", "selling", "sell", "distress", "urgent sale",
        "relocating", "owner selling", "landlord", "lease",
        "property for sale", "land for sale", "house for sale",
        "flat for sale", "duplex for sale", "bungalow for sale",
        "plot for sale", "sell my property", "selling my house",
        "selling my land", "building for sale", "shop for sale",
        "warehouse for sale", "office space for sale"
    ]

    BUYER_KEYWORDS = [
        "for rent", "looking for", "need", "want", "seeking",
        "searching", "require", "short let", "to let",
        "looking for apartment", "need 2 bedroom", "need 3 bedroom",
        "need a flat", "looking for house", "looking for land",
        "want to buy land", "looking to buy property",
        "need affordable apartment", "self contain", "mini flat",
        "room and parlour", "furnished apartment",
        "serviced apartment", "studio apartment", "boys quarter"
    ]

    JOB_KEYWORDS = [
        "hiring", "vacancy", "job", "recruit", "apply",
        "career", "employment", "staff needed", "worker needed",
        "we are hiring", "job vacancy", "job opening",
        "now hiring", "urgent vacancy", "talents needed",
        "job opportunity", "full time job", "part time job",
        "remote job Nigeria", "work from home Nigeria",
        "looking for job", "seeking employment", "need a job",
        "available for hire", "open to work"
    ]
