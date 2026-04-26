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

MIN_SCORE = 3
scheduled_scans = []
credits_used = 0

NIGERIA_CITIES = [
    "lagos", "abuja", "ibadan", "kano", "port harcourt",
    "benin", "enugu", "kaduna", "owerri", "warri",
    "calabar", "jos", "ilorin", "abeokuta", "onitsha",
    "uyo", "asaba", "maiduguri", "zaria", "sokoto"
]


# ─── Telegram ────────────────────────────────────────────
def send_telegram(bot, chat_id, text):
    try:
        bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        logger.error(f"Telegram error: {e}")


def send_html_file(bot, chat_id, html_content, filename):
    try:
        import io
        file_bytes = html_content.encode("utf-8")
        file_obj = io.BytesIO(file_bytes)
        file_obj.name = filename
        bot.send_document(
            chat_id=chat_id,
            document=file_obj,
            filename=filename,
            caption=f"HTML file — drag to netlify.com/drop to get preview URL"
        )
    except Exception as e:
        logger.error(f"File send error: {e}")


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


# ─── Maps Search ─────────────────────────────────────────
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


# ─── Weakness Scoring ────────────────────────────────────
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


# ─── WhatsApp Link ───────────────────────────────────────
def format_wa_link(phone):
    if not phone:
        return ""
    clean = (
        phone.replace("+", "").replace(" ", "")
        .replace("-", "").replace("(", "")
        .replace(")", "").strip()
    )
    return f"https://wa.me/{clean}" if clean else ""


# ─── Pitch Generator ─────────────────────────────────────
def generate_pitch(business_name, niche, city, issues):
    try:
        issues_text = "\n".join([f"- {i}" for i in issues])
        prompt = f"""Write a short WhatsApp message to a {niche} business owner.

Business: {business_name}, {city}
Issues: {issues_text}

Structure:
- Start: "Hello, I found your {niche} on Google Maps."
- Mention you built a free preview website for them
- Include that they can view it before deciding anything
- End: "Can I send you the link to see it?"

Under 60 words. Human tone. No marketing language.
Write ONLY the message. No quotes around it."""

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
            f"I built a free preview website for {business_name} "
            f"so you can see what it looks like before deciding anything. "
            f"Can I send you the link to see it?"
        )


# ─── HTML Generator ──────────────────────────────────────
def generate_html(business_name, niche, city, phone,
                  description="", hours="", address=""):
    wa_number = (
        phone.replace("+", "").replace(" ", "")
        .replace("-", "").replace("(", "").replace(")", "")
        if phone else ""
    )
    wa_link = f"https://wa.me/{wa_number}" if wa_number else "https://wa.me/"

    desc = description if description else (
        f"Welcome to {business_name}, your trusted {niche} in {city}. "
        f"We are committed to providing excellent service to every customer."
    )

    hours_text = hours if hours else "Mon-Sat: 9:00 AM - 7:00 PM | Sun: 10:00 AM - 5:00 PM"
    address_text = address if address and address != "N/A" else city

    prompt = f"""Generate a complete professional HTML website for a business.
Output ONLY the HTML code. Nothing else. No explanation. No markdown.

Business: {business_name}
Type: {niche}
City: {city}
Phone: {phone if phone else 'Not listed'}
WhatsApp: {wa_link}
Description: {desc}
Address: {address_text}
Hours: {hours_text}

Requirements:
- Single complete HTML file with embedded CSS
- Sections: Hero, About, Services, Why Choose Us, Contact
- Hero has large business name, tagline, WhatsApp button
- Services: 5 relevant services for {niche}, NO prices
- Contact shows phone, address, hours, large WhatsApp button
- Dark elegant color scheme with gold accents
- Fully mobile responsive
- NO fake statistics, NO pricing, NO email, NO social media
- WhatsApp button color: #25D366 (green)
- All sections fully filled with content
- Professional and modern design"""

    try:
        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4000,
            temperature=0.4,
        )
        html = response.choices[0].message.content.strip()

        # Clean any markdown if present
        if "```html" in html:
            html = html.split("```html")[1].split("```")[0].strip()
        elif "```" in html:
            html = html.split("```")[1].split("```")[0].strip()

        return html
    except Exception as e:
        logger.error(f"HTML generation error: {e}")
        return generate_fallback_html(
            business_name, niche, city,
            phone, wa_link, desc,
            address_text, hours_text
        )


def generate_fallback_html(business_name, niche, city,
                            phone, wa_link, desc,
                            address, hours):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{business_name}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Segoe UI', sans-serif; background: #0a0a0a; color: #fff; }}
.hero {{ background: linear-gradient(135deg, #1a1a1a, #2d2d2d); padding: 80px 20px; text-align: center; }}
.hero h1 {{ font-size: 2.5em; color: #d4af37; margin-bottom: 10px; }}
.hero p {{ font-size: 1.1em; color: #ccc; margin-bottom: 30px; }}
.wa-btn {{ background: #25D366; color: white; padding: 15px 35px; border-radius: 50px; text-decoration: none; font-size: 1.1em; font-weight: bold; display: inline-block; }}
section {{ padding: 60px 20px; max-width: 800px; margin: 0 auto; }}
h2 {{ color: #d4af37; font-size: 1.8em; margin-bottom: 30px; text-align: center; }}
.services {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; }}
.service-card {{ background: #1a1a1a; padding: 25px; border-radius: 12px; border-left: 3px solid #d4af37; }}
.service-card h3 {{ color: #d4af37; margin-bottom: 8px; }}
.contact-info {{ background: #1a1a1a; padding: 30px; border-radius: 12px; text-align: center; }}
.contact-info p {{ margin: 10px 0; color: #ccc; font-size: 1em; }}
.contact-info .wa-btn {{ margin-top: 20px; font-size: 1.2em; padding: 18px 40px; }}
@keyframes fadeIn {{
    from {{ opacity: 0; transform: translateY(20px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes pulse {{
    0% {{ transform: scale(1); }}
    50% {{ transform: scale(1.05); }}
    100% {{ transform: scale(1); }}
}}
.hero {{ animation: fadeIn 1s ease forwards; }}
.service-card {{ animation: fadeIn 0.8s ease forwards; }}
.wa-btn {{ animation: pulse 2s infinite; }}
* {{ scroll-behavior: smooth; }}
</style>
</head>
<body>
<div class="hero">
<h1>{business_name}</h1>
<p>{niche.title()} in {city} — Professional & Reliable</p>
<p style="margin-bottom:20px;color:#aaa;">{desc}</p>
<a href="{wa_link}" class="wa-btn">📱 Book on WhatsApp</a>
</div>
<section>
<h2>Our Services</h2>
<div class="services">
<div class="service-card"><h3>Professional Service</h3><p>Expert care tailored to your needs</p></div>
<div class="service-card"><h3>Quality Guaranteed</h3><p>We deliver the best results every time</p></div>
<div class="service-card"><h3>Quick Turnaround</h3><p>Fast and efficient service delivery</p></div>
<div class="service-card"><h3>Affordable Rates</h3><p>Great value for premium quality</p></div>
<div class="service-card"><h3>Customer First</h3><p>Your satisfaction is our priority</p></div>
</div>
</section>
<section>
<h2>Contact Us</h2>
<div class="contact-info">
<p>📞 {phone if phone else 'Contact via WhatsApp'}</p>
<p>📍 {address}</p>
<p>🕐 {hours}</p>
<a href="{wa_link}" class="wa-btn">💬 Chat on WhatsApp</a>
</div>
</section>
</body>
</html>"""


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
            description = business.get("description", "") or ""
            hours = business.get("hours", "") or ""
            wa_link = format_wa_link(phone)

            score, issues, reviews = score_business(business)

            if score < MIN_SCORE:
                continue

            if not phone and not maps_link:
                continue

            weak_found += 1
            pitch = generate_pitch(name, niche, city, issues)
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

            # MSG 2 — Phone
            send_telegram(bot, chat_id,
                phone if phone else "No phone — check Maps"
            )
            time.sleep(1)

            # MSG 3 — WhatsApp link
            send_telegram(bot, chat_id,
                wa_link if wa_link else "No WhatsApp link"
            )
            time.sleep(1)

            # MSG 4 — Maps link
            send_telegram(bot, chat_id,
                maps_link if maps_link else "No Maps link"
            )
            time.sleep(1)

            # MSG 5 — Pitch
            send_telegram(bot, chat_id, pitch)
            time.sleep(2)

            # MSG 6 — HTML file
            send_telegram(bot, chat_id,
                "Generating website HTML... please wait"
            )
            html_content = generate_html(
                name, niche, city, phone,
                description=description,
                hours=hours,
                address=address
            )
            safe_name = (
                name.lower()
                .replace(" ", "-")
                .replace("&", "and")
                .replace("'", "")[:30]
            )
            filename = f"{safe_name}.html"
            send_html_file(bot, chat_id, html_content, filename)

            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            log_to_sheet([
                now, name, niche, city, str(score),
                str(reviews), phone, address,
                maps_link, "Pending"
            ])

            time.sleep(8)

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


# ─── Commands ────────────────────────────────────────────
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
        "Each lead = 6 messages:\n"
        "1. Lead info + issues\n"
        "2. Phone number\n"
        "3. WhatsApp link\n"
        "4. Maps link\n"
        "5. Pitch to copy\n"
        "6. HTML file — drag to netlify.com/drop"
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
        update.message.reply_text(
            f"Min score updated to {MIN_SCORE}/10"
        )
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
        f"Scheduled: {niche} in {city}\nRuns daily at 8:00 AM"
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
        f"Scheduled: {len(scheduled_scans)}"
    )


def cmd_export(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"Check your Google Sheet.\n"
        f"Credits used: {credits_used}/1000"
    )


# ─── Scheduler ───────────────────────────────────────────
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

    threading.Thread(
        target=run_scheduler, daemon=True).start()

    updater.start_polling()
    logger.info("Bot is running!")

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_USER_ID,
            "text": (
                "MAPS LEAD BOT LIVE\n\n"
                "Each lead = 6 messages:\n"
                "1. Lead info\n"
                "2. Phone\n"
                "3. WhatsApp link\n"
                "4. Maps link\n"
                "5. Pitch\n"
                "6. HTML file\n\n"
                "Drag HTML to netlify.com/drop\n"
                "Get preview URL in 10 seconds\n\n"
                "Try: /scan salon lagos"
            )
        }
    )
    updater.idle()


if __name__ == "__main__":
    main()
