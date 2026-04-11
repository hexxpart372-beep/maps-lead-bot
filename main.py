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
MIN_SCORE = 7
scheduled_scans = []
credits_used = 0


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
def search_maps(niche, city, limit=20):
    global credits_used
    try:
        url = "https://api.scrapingdog.com/google_local/"
        params = {
            "api_key": SCRAPINGDOG_API_KEY,
            "query": f"{niche}+in+{city}",
        }
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            credits_used += 1
            data = response.json()
            results = data.get("local_results", [])
            logger.info(f"Found {len(results)} businesses")
            return results[:limit]
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
    weaknesses = []

    # No website
    website = business.get("website", "") or ""
    if not website:
        score += 2
        weaknesses.append("No website listed")

    # Low reviews
    reviews = business.get("reviews", 0)
    try:
        reviews = int(str(reviews).replace(",", ""))
    except:
        reviews = 0
    if reviews < 10:
        score += 3
        weaknesses.append(f"Only {reviews} reviews")
    elif reviews < 20:
        score += 2
        weaknesses.append(f"Only {reviews} reviews")

    # Low rating
    rating = business.get("rating", 0)
    try:
        rating = float(rating)
    except:
        rating = 0
    if rating > 0 and rating < 3.5:
        score += 2
        weaknesses.append(f"Low rating: {rating}")
    elif rating == 0:
        score += 1
        weaknesses.append("No rating yet")

    # No phone
    phone = business.get("phone", "") or ""
    if not phone:
        score += 1
        weaknesses.append("No phone listed")

    # Few photos
    photos = business.get("photos_count", 0)
    try:
        photos = int(photos)
    except:
        photos = 0
    if photos < 3:
        score += 2
        weaknesses.append(f"Only {photos} photos")

    return score, weaknesses


# ─── Groq AI Audit ───────────────────────────────────────
def generate_audit(business_name, niche, city, weaknesses):
    try:
        weakness_text = "\n".join([f"- {w}" for w in weaknesses])
        prompt = f"""You are a local business visibility expert.

Business: {business_name}
Type: {niche}
Location: {city}
Issues found:
{weakness_text}

Write a 2-3 sentence personalized audit that:
- Mentions their specific weaknesses
- Explains how it costs them customers
- Sounds urgent but not salesy
- Is specific not generic

Write ONLY the audit. Nothing else."""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Groq audit error: {e}")
        return f"{business_name} has visibility issues that are costing them customers daily."


def generate_pitch(business_name, niche, city, weaknesses):
    try:
        weakness_text = weaknesses[0] if weaknesses else "listing issues"
        prompt = f"""Write a short WhatsApp message to a {niche} business owner in {city}.

Their main issue: {weakness_text}
Business name: {business_name}

The message should:
- Be friendly and professional
- Point out their specific issue
- Offer to fix it
- End with a question
- Be under 50 words
- Sound human not robotic

Write ONLY the message. Nothing else."""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Groq pitch error: {e}")
        return f"Hi, I found {business_name} on Google Maps and noticed some issues with your listing that might be reducing your visibility. Can I show you what I mean?"


# ─── Core Scan Function ──────────────────────────────────
def run_scan(bot, chat_id, niche, city):
    global MIN_SCORE
    send_telegram(bot, chat_id,
                  f"Scanning {niche} in {city}...\nThis takes 1-2 minutes.")

    businesses = search_maps(niche, city)

    if not businesses:
        send_telegram(bot, chat_id,
                      f"No results found for {niche} in {city}.\nTry different keywords like:\n/scan restaurant lagos\n/scan salon abuja\n/scan hotel ibadan")
        return

    weak_found = 0
    total = len(businesses)

    for business in businesses:
        try:
            name = business.get("title", "Unknown")
            address = business.get("address", "N/A")
            phone = business.get("phone", "") or "N/A"
            rating = business.get("rating", "N/A")
            reviews = business.get("reviews", 0)
            website = business.get("website", "") or "None"
            maps_link = business.get(
                "maps_url", "") or business.get("link", "N/A")

            score, weaknesses = score_business(business)

            if score < MIN_SCORE:
                continue

            weak_found += 1

            audit = generate_audit(name, niche, city, weaknesses)
            pitch = generate_pitch(name, niche, city, weaknesses)

            weakness_text = "\n".join([f"• {w}" for w in weaknesses])

            # Message 1 - Full business details
            msg = (
                f"WEAK BUSINESS #{weak_found}\n"
                f"Score: {score}/10\n\n"
                f"Name: {name}\n"
                f"Type: {niche}\n"
                f"Rating: {rating} ({reviews} reviews)\n"
                f"Website: {website}\n"
                f"Phone: {phone}\n"
                f"Address: {address}\n\n"
                f"Weaknesses:\n{weakness_text}\n\n"
                f"AI AUDIT:\n{audit}\n\n"
                f"Maps: {maps_link}"
            )
            send_telegram(bot, chat_id, msg)

            time.sleep(2)

            # Message 2 - Pitch only, easy to copy
            pitch_msg = (
                f"WHATSAPP PITCH:\n\n"
                f"{pitch}"
            )
            send_telegram(bot, chat_id, pitch_msg)

            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            log_to_sheet([
                now, name, niche, city,
                str(score), phone, website,
                address, maps_link, "Pending"
            ])

            time.sleep(5)

        except Exception as e:
            logger.error(f"Business processing error: {e}")
            continue

    summary = (
        f"SCAN COMPLETE\n"
        f"Niche: {niche} in {city}\n"
        f"Total found: {total}\n"
        f"Weak businesses: {weak_found}\n"
        f"Credits used today: {credits_used}"
    )
    send_telegram(bot, chat_id, summary)


# ─── Telegram Commands ───────────────────────────────────
def cmd_start(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    msg = (
        "MAPS LEAD BOT READY\n\n"
        "Commands:\n"
        "/scan [niche] [city]\n"
        "  Example: /scan restaurants lagos\n\n"
        "/setscore [number]\n"
        "  Set minimum weakness score\n"
        "  Default: 7\n\n"
        "/schedule [niche] [city]\n"
        "  Auto scan every morning at 8am\n\n"
        "/schedules\n"
        "  View all scheduled scans\n\n"
        "/status\n"
        "  Credits used and bot status\n\n"
        "/export\n"
        "  Get summary of todays leads"
    )
    update.message.reply_text(msg)


def cmd_scan(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /scan [niche] [city]\n"
            "Example: /scan restaurants lagos"
        )
        return
    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id

    thread = threading.Thread(
        target=run_scan,
        args=(bot, chat_id, niche, city)
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
            f"Usage: /setscore 6"
        )
        return
    try:
        MIN_SCORE = int(args[0])
        update.message.reply_text(
            f"Minimum score set to {MIN_SCORE}/10")
    except:
        update.message.reply_text("Please enter a valid number")


def cmd_schedule(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /schedule [niche] [city]\n"
            "Example: /schedule restaurants lagos"
        )
        return
    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id

    scheduled_scans.append({"niche": niche, "city": city})
    schedule.every().day.at("08:00").do(
        run_scan, bot, chat_id, niche, city)

    update.message.reply_text(
        f"Scheduled daily scan:\n"
        f"Niche: {niche}\n"
        f"City: {city}\n"
        f"Time: Every morning at 8:00 AM"
    )


def cmd_schedules(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    if not scheduled_scans:
        update.message.reply_text("No scheduled scans yet.")
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
        f"Credits used today: {credits_used}/1000\n"
        f"Credits remaining: {1000 - credits_used}\n"
        f"Min weakness score: {MIN_SCORE}/10\n"
        f"Scheduled scans: {len(scheduled_scans)}\n"
        f"Status: Running"
    )
    update.message.reply_text(msg)


def cmd_export(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"Check your Google Sheet for all logged leads.\n"
        f"Total credits used today: {credits_used}"
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
        target=run_scheduler, daemon=True)
    scheduler_thread.start()

    updater.start_polling()
    logger.info("Bot is running!")

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_USER_ID,
            "text": (
                "MAPS LEAD BOT IS LIVE\n\n"
                "Send /start to see all commands\n"
                "Send /scan restaurants lagos to begin"
            )
        }
    )

    updater.idle()


if __name__ == "__main__":
    main()
