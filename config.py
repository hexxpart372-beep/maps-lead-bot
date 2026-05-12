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
    private_key = os.environ.get("GCP_PRIVATE_KEY", "")
    private_key = private_key.replace("\\n", "\n")
    return {
        "type": "service_account",
        "project_id": os.environ.get("GCP_PROJECT_ID", ""),
        "private_key_id": "",
        "private_key": private_key,
        "client_email": os.environ.get("GCP_CLIENT_EMAIL", ""),
        "client_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": ""
    }

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
