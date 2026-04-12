import os
import json
import time
import logging
import requests
import schedule
import threading
from datetime import datetime
from groq import Groq
import gspread
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.ext import (
    Updater, CommandHandler, CallbackContext
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Environment Variables ────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_USER_ID = int(os.environ["TELEGRAM_USER_ID"])
SCRAPINGDOG_API_KEY = os.environ["SCRAPINGDOG_API_KEY"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

groq_client = Groq(api_key=GROQ_API_KEY)

# ─── Settings ────────────────────────────────────────────
MIN_SCORE = 3
scheduled_scans = []
credits_used = 0

NIGERIA_CITIES = [
    "lagos", "abuja", "ibadan", "kano", "port harcourt",
    "benin", "enugu", "kaduna", "owerri", "warri",
    "calabar", "jos", "ilorin", "abeokuta", "onitsha",
    "uyo", "asaba", "maiduguri", "zaria", "sokoto"
]


# ─── Telegram Helper ─────────────────────────────────────
def send_telegram(bot, chat_id, text):
    try:
        bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        logger.error(f"Telegram error: {e}")


# ─── Google Sheets ───────────────────────────────────────
def get_sheets_client():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(
            creds_dict, scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Sheets error: {e}")
        return None


def log_to_sheet(data):
    try:
        client = get_sheets_client()
        if client:
            sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1
            sheet.append_row(data)
    except Exception as e:
        logger.error(f"Sheet log error: {e}")


# ─── ScrapingDog Maps Search ─────────────────────────────
def search_maps(niche, city, country_code="ng"):
    global credits_used
    try:
        url = "https://api.scrapingdog.com/google_local/"

        # Force city into query for better location accuracy
        if country_code == "ng":
            query = f"{niche}+in+{city}+Nigeria"
            location = f"{city}, Nigeria"
        else:
            query = f"{niche}+in+{city}"
            location = f"{city}"

        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "query": query,
            "country": country_code,
            "location": location,
            "language": "en"
        }

        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            credits_used += 1
            data = response.json()
            results = data.get("local_results", [])

            # Filter results to match city
            filtered = []
            for r in results:
                address = r.get("address", "").lower()
                if city.lower() in address or country_code == "us":
                    filtered.append(r)

            # If filter removes everything fall back to all results
            final = filtered if filtered else results
            logger.info(
                f"Found {len(final)} businesses for {niche} in {city}")
            return final
        else:
            logger.error(
                f"ScrapingDog error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        logger.error(f"Maps search error: {e}")
        return []


# ─── Weakness Scoring ────────────────────────────────────
def score_business(business):
    score = 0
    issues = []

    # Reviews
    reviews_raw = business.get("reviews", "0")
    try:
        reviews = int(
            str(reviews_raw)
            .replace(",", "")
            .replace("(", "")
            .replace(")", "")
            .strip()
        )
    except:
        reviews = 0

    if reviews == 0:
        score += 2
        issues.append("0 reviews on profile")
    elif reviews <= 15:
        score += 2
        issues.append(f"Only {reviews} reviews")
    elif reviews <= 30:
        score += 1
        issues.append(f"Low activity: {reviews} reviews")

    # Website
    website = business.get("website", "") or ""
    if not website:
        score += 2
        issues.append("No website linked")

    # Description
    description = business.get("description", "") or ""
    if not description or len(description) < 20:
        score += 1
        issues.append("No business description")

    # Photos — infer from thumbnail
    thumbnail = business.get("thumbnail", "") or ""
    if not thumbnail:
        score += 1
        issues.append("No photos on profile")

    return score, issues, reviews


# ─── Generate Pitch ──────────────────────────────────────
def generate_pitch(business_name, niche, city, issues):
    try:
        issues_text = "\n".join([f"- {i}" for i in issues])
        prompt = f"""Write a short WhatsApp message to a {niche} business owner.

Business name: {business_name}
City: {city}
Their issues:
{issues_text}

Use this exact structure:
- Start: "Hello, I found your {niche} on Google Maps and noticed you don't have a website."
- One sentence: mention you built a simple demo site for them
- End with: "Can I send you the link to see it?"

Under 60 words. Sound human. No marketing language.

Write ONLY the message. Nothing else."""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.6,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Groq pitch error: {e}")
        return (
            f"Hello, I found your {niche} on Google Maps and noticed "
            f"you don't have a website. I went ahead and designed a "
            f"simple one for you to show how customers could find you online. "
            f"Can I send you the link to see it?"
        )


# ─── Core Scan Function ──────────────────────────────────
def run_scan(bot, chat_id, niche, city, country_code="ng"):
    global MIN_SCORE
    send_telegram(
        bot, chat_id,
        f"Scanning {niche} in {city}...\nThis takes 1-2 minutes."
    )

    businesses = search_maps(niche, city, country_code)

    if not businesses:
        send_telegram(
            bot, chat_id,
            f"No results found for {niche} in {city}.\n\n"
            f"Try:\n"
            f"/scan salon lagos\n"
            f"/scan barber houston\n"
            f"/scan clinic abuja"
        )
        return

    weak_found = 0
    total = len(businesses)

    for business in businesses:
        try:
            name = business.get("title", "Unknown")
            address = business.get("address", "N/A")
            place_id = business.get("place_id", "")
            maps_link = (
                f"https://www.google.com/maps?cid={place_id}"
                if place_id else "N/A"
            )
            phone = business.get("phone", "") or ""

            score, issues, reviews = score_business(business)

            if score < MIN_SCORE:
                continue

            # Skip if address doesnt match city for Nigeria
            if country_code == "ng":
                address_lower = address.lower()
                if city.lower() not in address_lower and "nigeria" not in address_lower:
                    if address != "N/A":
                        continue

            weak_found += 1
            pitch = generate_pitch(name, niche, city, issues)
            issues_text = "\n".join([f"• {i}" for i in issues])

            # Message 1 — Lead info
            msg1 = (
                f"TARGET #{weak_found}\n\n"
                f"Name: {name}\n"
                f"Address: {address}\n"
                f"Maps: {maps_link}\n\n"
                f"Issues:\n{issues_text}"
            )
            send_telegram(bot, chat_id, msg1)

            time.sleep(1)

            # Message 2 — Phone number only easy to copy
            if phone:
                send_telegram(bot, chat_id, phone)
            else:
                send_telegram(bot, chat_id, "No phone listed — check Maps link")

            time.sleep(1)

            # Message 3 — Pitch only easy to copy
            send_telegram(bot, chat_id, pitch)

            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            log_to_sheet([
                now, name, niche, city,
                str(score), str(reviews),
                phone, address, maps_link, "Pending"
            ])

            time.sleep(5)

        except Exception as e:
            logger.error(f"Business processing error: {e}")
            continue

    summary = (
        f"SCAN COMPLETE\n"
        f"Niche: {niche} in {city}\n"
        f"Total scanned: {total}\n"
        f"Weak targets: {weak_found}\n"
        f"Credits used today: {credits_used}/1000"
    )
    send_telegram(bot, chat_id, summary)


# ─── Telegram Commands ───────────────────────────────────
def cmd_start(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    msg = (
        "MAPS LEAD BOT READY\n\n"
        "Commands:\n\n"
        "/scan [niche] [city]\n"
        "  Nigeria: /scan salon lagos\n"
        "  Abroad: /scan barber houston\n\n"
        "/setscore [number]\n"
        "  Min weakness score. Default: 3\n\n"
        "/schedule [niche] [city]\n"
        "  Auto scan every morning 8am\n\n"
        "/schedules\n"
        "  View scheduled scans\n\n"
        "/status\n"
        "  Credits and bot info\n\n"
        "/export\n"
        "  Check Google Sheet for leads"
    )
    update.message.reply_text(msg)


def cmd_scan(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /scan [niche] [city]\n"
            "Example: /scan salon lagos\n"
            "Example: /scan barber houston"
        )
        return

    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id
    country_code = "ng" if city.lower() in NIGERIA_CITIES else "us"

    thread = threading.Thread(
        target=run_scan,
        args=(bot, chat_id, niche, city, country_code)
    )
    thread.start()


def cmd_setscore(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    global MIN_SCORE
    args = context.args
    if not args:
        update.message.reply_text(
            f"Current minimum score: {MIN_SCORE}\n"
            f"Usage: /setscore 3\n"
            f"Lower = more results\n"
            f"Higher = only weakest targets"
        )
        return
    try:
        MIN_SCORE = int(args[0])
        update.message.reply_text(
            f"Minimum score updated to {MIN_SCORE}/10"
        )
    except:
        update.message.reply_text(
            "Enter a valid number. Example: /setscore 3"
        )


def cmd_schedule(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /schedule [niche] [city]\n"
            "Example: /schedule salon lagos"
        )
        return

    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id
    country_code = "ng" if city.lower() in NIGERIA_CITIES else "us"

    scheduled_scans.append({
        "niche": niche,
        "city": city,
        "country": country_code
    })
    schedule.every().day.at("08:00").do(
        run_scan, bot, chat_id, niche, city, country_code
    )

    update.message.reply_text(
        f"Scheduled daily scan set\n\n"
        f"Niche: {niche}\n"
        f"City: {city}\n"
        f"Time: Every morning 8:00 AM"
    )


def cmd_schedules(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    if not scheduled_scans:
        update.message.reply_text(
            "No scheduled scans yet.\n"
            "Use /schedule salon lagos to set one."
        )
        return
    msg = "SCHEDULED SCANS:\n\n"
    for i, s in enumerate(scheduled_scans, 1):
        msg += f"{i}. {s['niche']} in {s['city']} - Daily 8AM\n"
    update.message.reply_text(msg)


def cmd_status(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    msg = (
        f"BOT STATUS\n\n"
        f"Status: Running\n"
        f"Credits used today: {credits_used}/1000\n"
        f"Credits remaining: {1000 - credits_used}\n"
        f"Min weakness score: {MIN_SCORE}/10\n"
        f"Scheduled scans: {len(scheduled_scans)}"
    )
    update.message.reply_text(msg)


def cmd_export(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"All leads are in your Google Sheet.\n\n"
        f"Credits used today: {credits_used}/1000\n"
        f"Credits remaining: {1000 - credits_used}"
    )


# ─── Schedule Runner ─────────────────────────────────────
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)


# ─── Main ────────────────────────────────────────────────
def main():
    logger.info("Maps Lead Bot starting...")

    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", cmd_start))
    dp.add_handler(CommandHandler("scan", cmd_scan))
    dp.add_handler(CommandHandler("setscore", cmd_setscore))
    dp.add_handler(CommandHandler("schedule", cmd_schedule))
    dp.add_handler(CommandHandler("schedules", cmd_schedules))
    dp.add_handler(CommandHandler("status", cmd_status))
    dp.add_handler(CommandHandler("export", cmd_export))

    scheduler_thread = threading.Thread(
        target=run_scheduler, daemon=True
    )
    scheduler_thread.start()

    updater.start_polling()
    logger.info("Bot is running!")

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_USER_ID,
            "text": (
                "MAPS LEAD BOT IS LIVE\n\n"
                "Send /start to see commands\n\n"
                "Try:\n"
                "/scan salon lagos\n"
                "/scan barber houston\n"
                "/scan clinic abuja"
            )
        }
    )

    updater.idle()


if __name__ == "__main__":
    main()
