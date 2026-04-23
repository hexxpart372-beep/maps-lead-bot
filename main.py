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

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_USER_ID = int(os.environ["TELEGRAM_USER_ID"])
SCRAPINGDOG_API_KEY = os.environ["SCRAPINGDOG_API_KEY"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

groq_client = Groq(api_key=GROQ_API_KEY)

MIN_SCORE = 3
scheduled_scans = []
credits_used = 0

NIGERIA_CITIES = [
    "lagos", "abuja", "ibadan", "kano", "port harcourt",
    "benin", "enugu", "kaduna", "owerri", "warri",
    "calabar", "jos", "ilorin", "abeokuta", "onitsha",
    "uyo", "asaba", "maiduguri", "zaria", "sokoto"
]


def send_telegram(bot, chat_id, text):
    try:
        bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        logger.error(f"Telegram error: {e}")


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


def search_maps(niche, city, country_code="ng"):
    global credits_used
    try:
        url = "https://api.scrapingdog.com/google_local/"
        if country_code == "ng":
            query = f"{niche}+in+{city}+Nigeria"
            location = f"{city}, Nigeria"
        else:
            query = f"{niche}+in+{city}"
            location = city

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
            if country_code == "ng":
                filtered = [
                    r for r in results
                    if city.lower() in r.get("address", "").lower()
                    or "nigeria" in r.get("address", "").lower()
                ]
                return filtered if filtered else results
            return results
        else:
            logger.error(f"ScrapingDog error: {response.text}")
            return []
    except Exception as e:
        logger.error(f"Maps search error: {e}")
        return []


def score_business(business):
    score = 0
    issues = []

    reviews_raw = business.get("reviews", "0")
    try:
        reviews = int(
            str(reviews_raw)
            .replace(",", "").replace("(", "")
            .replace(")", "").strip()
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

    website = business.get("website", "") or ""
    if not website:
        score += 2
        issues.append("No website linked")

    description = business.get("description", "") or ""
    if not description or len(description) < 20:
        score += 1
        issues.append("No business description")

    thumbnail = business.get("thumbnail", "") or ""
    if not thumbnail:
        score += 1
        issues.append("No photos on profile")

    return score, issues, reviews


def format_wa_link(phone):
    if not phone:
        return ""
    clean = (
        phone.replace("+", "").replace(" ", "")
        .replace("-", "").replace("(", "")
        .replace(")", "").strip()
    )
    return f"https://wa.me/{clean}" if clean else ""


def generate_pitch(business_name, niche, city, issues):
    try:
        issues_text = "\n".join([f"- {i}" for i in issues])
        prompt = f"""Write a short WhatsApp message to a {niche} business owner.

Business: {business_name}, {city}
Issues: {issues_text}

Structure:
- Start: "Hello, I found your {niche} on Google Maps."
- Mention you built a demo website for them
- End: "Can I send you the link to see it?"

Under 60 words. Human tone. No marketing language.
Write ONLY the message."""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.6,
        )
        return response.choices[0].message.content.strip().strip('"')
    except Exception as e:
        logger.error(f"Groq pitch error: {e}")
        return (
            f"Hello, I found your {niche} on Google Maps. "
            f"I built a simple demo website for {business_name} "
            f"to show what it could look like online. "
            f"Can I send you the link to see it?"
        )


def generate_deepsite_prompt(business_name, niche, city, phone, issues):
    issues_text = ", ".join(issues) if issues else "incomplete profile"
    wa_number = phone.replace("+", "").replace(" ", "").replace("-", "") if phone else ""
    wa_link = f"https://wa.me/{wa_number}" if wa_number else "https://wa.me/"

    return (
        f"Create a professional one-page business website for:\n\n"
        f"Business Name: {business_name}\n"
        f"Type: {niche}\n"
        f"Location: {city}\n"
        f"WhatsApp: {wa_link}\n\n"
        f"Include these sections:\n"
        f"1. Hero section with business name and tagline\n"
        f"2. Services/What We Offer section\n"
        f"3. Location section mentioning {city}\n"
        f"4. Large WhatsApp contact button linking to {wa_link}\n"
        f"5. Opening hours placeholder\n\n"
        f"Style: Clean, modern, mobile-friendly, professional.\n"
        f"Color scheme: Warm and trustworthy.\n"
        f"Make it look like a real established business website."
    )


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
            f"No results for {niche} in {city}.\n\n"
            f"Try:\n/scan salon lagos\n"
            f"/scan barber houston\n/scan clinic abuja"
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
                if place_id else ""
            )
            phone = business.get("phone", "") or ""
            wa_link = format_wa_link(phone)

            score, issues, reviews = score_business(business)

            if score < MIN_SCORE:
                continue

            if not phone and not maps_link:
                continue

            weak_found += 1
            pitch = generate_pitch(name, niche, city, issues)
            deepsite_prompt = generate_deepsite_prompt(
                name, niche, city, phone, issues)
            issues_text = "\n".join([f"• {i}" for i in issues])

            # MSG 1 — Lead info
            send_telegram(bot, chat_id,
                f"TARGET #{weak_found}\n\n"
                f"Name: {name}\n"
                f"Type: {niche}\n"
                f"Address: {address}\n"
                f"Score: {score}/10\n\n"
                f"Issues:\n{issues_text}"
            )
            time.sleep(1)

            # MSG 2 — Phone only
            if phone:
                send_telegram(bot, chat_id, phone)
            else:
                send_telegram(bot, chat_id, "No phone — check Maps")
            time.sleep(1)

            # MSG 3 — WhatsApp link only
            if wa_link:
                send_telegram(bot, chat_id, wa_link)
            else:
                send_telegram(bot, chat_id, "No WhatsApp link available")
            time.sleep(1)

            # MSG 4 — Maps link only
            if maps_link:
                send_telegram(bot, chat_id, maps_link)
            else:
                send_telegram(bot, chat_id, "No Maps link available")
            time.sleep(1)

            # MSG 5 — Pitch only
            send_telegram(bot, chat_id, pitch)
            time.sleep(1)

            # MSG 6 — DeepSite prompt
            send_telegram(bot, chat_id,
                f"DEEPSITE PROMPT:\n\n{deepsite_prompt}"
            )

            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            log_to_sheet([
                now, name, niche, city, str(score),
                str(reviews), phone, address,
                maps_link, "Pending"
            ])

            time.sleep(5)

        except Exception as e:
            logger.error(f"Business error: {e}")
            continue

    send_telegram(bot, chat_id,
        f"SCAN COMPLETE\n"
        f"Niche: {niche} in {city}\n"
        f"Scanned: {total}\n"
        f"Weak targets: {weak_found}\n"
        f"Credits used: {credits_used}/1000"
    )


def cmd_start(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    msg = (
        "MAPS LEAD BOT READY\n\n"
        "Commands:\n\n"
        "/scan [niche] [city]\n"
        "  /scan salon lagos\n"
        "  /scan barber houston\n\n"
        "/setscore [number]\n"
        "  Default: 3\n\n"
        "/schedule [niche] [city]\n"
        "  Auto scan every 8am\n\n"
        "/schedules — view scheduled\n"
        "/status — credits info\n"
        "/export — check sheet\n\n"
        "Each lead sends 6 messages:\n"
        "1. Lead info\n"
        "2. Phone number\n"
        "3. WhatsApp link\n"
        "4. Maps link\n"
        "5. Pitch to copy\n"
        "6. DeepSite prompt"
    )
    update.message.reply_text(msg)


def cmd_scan(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /scan [niche] [city]\n"
            "Example: /scan salon lagos"
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
            f"Current score: {MIN_SCORE}\n"
            f"Usage: /setscore 3"
        )
        return
    try:
        MIN_SCORE = int(args[0])
        update.message.reply_text(f"Score updated to {MIN_SCORE}/10")
    except:
        update.message.reply_text("Enter a valid number.")


def cmd_schedule(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /schedule [niche] [city]"
        )
        return
    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id
    country_code = "ng" if city.lower() in NIGERIA_CITIES else "us"

    scheduled_scans.append({"niche": niche, "city": city})
    schedule.every().day.at("08:00").do(
        run_scan, bot, chat_id, niche, city, country_code
    )
    update.message.reply_text(
        f"Scheduled: {niche} in {city} — Daily 8AM"
    )


def cmd_schedules(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    if not scheduled_scans:
        update.message.reply_text("No scheduled scans yet.")
        return
    msg = "SCHEDULED SCANS:\n\n"
    for i, s in enumerate(scheduled_scans, 1):
        msg += f"{i}. {s['niche']} in {s['city']} — Daily 8AM\n"
    update.message.reply_text(msg)


def cmd_status(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"BOT STATUS\n\n"
        f"Running: Yes\n"
        f"Credits used: {credits_used}/1000\n"
        f"Credits left: {1000 - credits_used}\n"
        f"Min score: {MIN_SCORE}/10\n"
        f"Scheduled scans: {len(scheduled_scans)}"
    )


def cmd_export(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"Check your Google Sheet for all leads.\n"
        f"Credits used: {credits_used}/1000"
    )


def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)


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

    threading.Thread(target=run_scheduler, daemon=True).start()
    updater.start_polling()
    logger.info("Bot is running!")

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_USER_ID,
            "text": (
                "MAPS LEAD BOT LIVE\n\n"
                "Each lead = 6 separate messages:\n"
                "1. Lead info\n2. Phone\n3. WhatsApp link\n"
                "4. Maps link\n5. Pitch\n6. DeepSite prompt\n\n"
                "Try: /scan salon lagos"
            )
        }
    )
    updater.idle()


if __name__ == "__main__":
    main()
