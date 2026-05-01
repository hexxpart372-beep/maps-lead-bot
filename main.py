import os
import json
import time
import logging
import random
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
NETLIFY_TOKEN = os.environ["NETLIFY_TOKEN"]

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
US_CITIES = [
    "new york", "los angeles", "chicago", "houston", "phoenix",
    "philadelphia", "san antonio", "san diego", "dallas", "san jose",
    "austin", "miami", "atlanta", "boston", "seattle", "denver",
    "nashville", "las vegas", "malibu", "beverly hills", "brooklyn"
]
CANADA_CITIES = [
    "toronto", "vancouver", "montreal", "calgary", "edmonton",
    "ottawa", "winnipeg", "quebec", "hamilton", "victoria"
]
UK_CITIES = [
    "london", "birmingham", "manchester", "glasgow", "leeds",
    "liverpool", "edinburgh", "bristol", "sheffield"
]


def detect_country(city):
    c = city.lower()
    if c in NIGERIA_CITIES:
        return "ng"
    if c in CANADA_CITIES:
        return "ca"
    if c in UK_CITIES:
        return "gb"
    return "us"


# ─── Telegram ────────────────────────────────────────────
def send_telegram(bot, chat_id, text):
    try:
        bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        logger.error(f"Telegram error: {e}")


# ─── Netlify Auto Deploy ──────────────────────────────────
def deploy_to_netlify(html_content, site_name):
    try:
        import base64
        import zipfile
        import io

        # Clean site name for Netlify
        clean_name = (
            site_name.lower()
            .replace(" ", "-")
            .replace("&", "and")
            .replace("'", "")
            .replace(",", "")
            .replace(".", "")
            [:35]
        )

        # Create zip with index.html
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("index.html", html_content)
        zip_buffer.seek(0)
        zip_bytes = zip_buffer.read()

        headers = {
            "Authorization": f"Bearer {NETLIFY_TOKEN}",
            "Content-Type": "application/zip"
        }

        # Create new site
        create_resp = requests.post(
            "https://api.netlify.com/api/v1/sites",
            headers={"Authorization": f"Bearer {NETLIFY_TOKEN}",
                     "Content-Type": "application/json"},
            json={"name": clean_name}
        )

        if create_resp.status_code not in [200, 201]:
            # Name taken — add random suffix
            import random as r
            clean_name = f"{clean_name}-{r.randint(100,999)}"
            create_resp = requests.post(
                "https://api.netlify.com/api/v1/sites",
                headers={"Authorization": f"Bearer {NETLIFY_TOKEN}",
                         "Content-Type": "application/json"},
                json={"name": clean_name}
            )

        site_data = create_resp.json()
        site_id = site_data.get("id")

        if not site_id:
            logger.error(f"Site creation failed: {site_data}")
            return None

        # Deploy zip to site
        deploy_resp = requests.post(
            f"https://api.netlify.com/api/v1/sites/{site_id}/deploys",
            headers=headers,
            data=zip_bytes
        )

        if deploy_resp.status_code in [200, 201]:
            deploy_data = deploy_resp.json()
            url = deploy_data.get("ssl_url") or deploy_data.get("url")
            if url:
                return url
            # Fallback to site URL
            return f"https://{clean_name}.netlify.app"
        else:
            logger.error(f"Deploy failed: {deploy_resp.text}")
            return None

    except Exception as e:
        logger.error(f"Netlify deploy error: {e}")
        return None


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
def search_maps(niche, city, country_code="us"):
    global credits_used
    try:
        url = "https://api.scrapingdog.com/google_local/"
        locations = {
            "ng": (f"{niche}+in+{city}+Nigeria", f"{city}, Nigeria"),
            "ca": (f"{niche}+in+{city}+Canada", f"{city}, Canada"),
            "gb": (f"{niche}+in+{city}+UK", f"{city}, United Kingdom"),
            "us": (f"{niche}+in+{city}", f"{city}, USA")
        }
        query, location = locations.get(
            country_code, locations["us"])

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
        issues.append("0 reviews")
    elif reviews <= 15:
        score += 2
        issues.append(f"Only {reviews} reviews")
    elif reviews <= 50:
        score += 1
        issues.append(f"Low activity: {reviews} reviews")

    website = business.get("website", "") or ""
    if not website:
        score += 3
        issues.append("No website")

    description = business.get("description", "") or ""
    if not description or len(description) < 20:
        score += 1
        issues.append("No description")

    thumbnail = business.get("thumbnail", "") or ""
    if not thumbnail:
        score += 1
        issues.append("No photos")

    return score, issues, reviews, website


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


# ─── Pitch (hardcoded — no Groq hallucinations) ──────────
def generate_pitch(business_name, niche, city):
    templates = [
        f"Hi, I found {business_name} on Google Maps while searching for a {niche} in {city}. I noticed you don't have a website so I built a free preview for you. Can I send you the link?",
        f"Hi, I came across {business_name} on Google Maps. You don't have a website listed so I went ahead and created a preview — completely free. Want me to send it?",
        f"Hi, found your {niche} on Google Maps in {city}. I built a free website preview for {business_name} — no commitment needed. Want to see it?",
    ]
    return random.choice(templates)


# ─── Content Generator ───────────────────────────────────
def generate_content(business_name, niche, city):
    try:
        prompt = f"""You are generating website content. Return ONLY valid JSON. No explanation, no markdown, no extra text. Start with {{ and end with }}.

{{
  "tagline": "write a real compelling tagline for a {niche} business",
  "about": "write 2 real professional sentences about {business_name}, a {niche} in {city}",
  "s1_name": "write a real {niche} service name",
  "s1_desc": "one line description",
  "s2_name": "write a real {niche} service name",
  "s2_desc": "one line description",
  "s3_name": "write a real {niche} service name",
  "s3_desc": "one line description",
  "s4_name": "write a real {niche} service name",
  "s4_desc": "one line description",
  "s5_name": "write a real {niche} service name",
  "s5_desc": "one line description",
  "w1_title": "write a real strength of a {niche}",
  "w1_desc": "one line",
  "w2_title": "write a real strength of a {niche}",
  "w2_desc": "one line",
  "w3_title": "write a real strength of a {niche}",
  "w3_desc": "one line"
}}"""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.4,
        )
        raw = response.choices[0].message.content.strip()

        # Aggressive JSON extraction
        if "```" in raw:
            parts = raw.split("```")
            for part in parts:
                part = part.replace("json", "").strip()
                if part.startswith("{"):
                    raw = part
                    break

        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start != -1 and end > start:
            raw = raw[start:end]

        data = json.loads(raw)

        # Validate — reject if any value contains
        # placeholder-style text
        bad_phrases = [
            "write a", "your ", "insert", "example",
            "placeholder", "one line description",
            "one line"
        ]
        for key, val in data.items():
            for bad in bad_phrases:
                if bad.lower() in str(val).lower():
                    raise ValueError(f"Placeholder detected: {val}")

        return data

    except Exception as e:
        logger.error(f"Content generation error: {e}")
        # Niche-specific fallbacks
        fallbacks = {
            "dentist": {
                "tagline": "Expert dental care for a healthier, brighter smile",
                "about": f"{business_name} provides comprehensive dental services in {city}. Our experienced team is dedicated to your oral health and comfort.",
                "s1_name": "General Checkup", "s1_desc": "Full dental examination and cleaning",
                "s2_name": "Teeth Whitening", "s2_desc": "Professional whitening for a brighter smile",
                "s3_name": "Dental Fillings", "s3_desc": "Painless cavity treatment and restoration",
                "s4_name": "Root Canal", "s4_desc": "Expert endodontic treatment",
                "s5_name": "Orthodontics", "s5_desc": "Braces and alignment solutions",
                "w1_title": "Experienced Dentists", "w1_desc": "Qualified professionals with years of practice",
                "w2_title": "Pain-Free Treatment", "w2_desc": "Modern techniques for comfortable care",
                "w3_title": "Flexible Appointments", "w3_desc": "Book at a time that suits you"
            },
            "salon": {
                "tagline": "Where every visit leaves you looking and feeling your best",
                "about": f"{business_name} is a premium hair and beauty salon in {city}. We combine skill and creativity to deliver stunning results every time.",
                "s1_name": "Haircut & Styling", "s1_desc": "Precision cuts and expert styling",
                "s2_name": "Hair Coloring", "s2_desc": "Vibrant color treatments and highlights",
                "s3_name": "Hair Treatment", "s3_desc": "Deep conditioning and repair treatments",
                "s4_name": "Braiding & Weaves", "s4_desc": "All protective styles and weave installations",
                "s5_name": "Facial & Skincare", "s5_desc": "Rejuvenating skin treatments",
                "w1_title": "Skilled Stylists", "w1_desc": "Experienced professionals who love their craft",
                "w2_title": "Premium Products", "w2_desc": "Only the best products used on your hair",
                "w3_title": "Relaxing Atmosphere", "w3_desc": "A calm and welcoming space for every client"
            },
            "barbershop": {
                "tagline": "Clean cuts and sharp fades — every single time",
                "about": f"{business_name} is the go-to barbershop in {city} for clean cuts and fresh styles. We take pride in precision and attention to detail.",
                "s1_name": "Haircut", "s1_desc": "Precision cuts tailored to your style",
                "s2_name": "Fade & Taper", "s2_desc": "Clean fades from skin to scissor",
                "s3_name": "Beard Trim", "s3_desc": "Shape and define your beard",
                "s4_name": "Hot Towel Shave", "s4_desc": "Classic straight razor shave",
                "s5_name": "Kids Cut", "s5_desc": "Patient and fun cuts for children",
                "w1_title": "Master Barbers", "w1_desc": "Skilled barbers with years of experience",
                "w2_title": "No Wait Policy", "w2_desc": "Appointments available to save your time",
                "w3_title": "Clean & Hygienic", "w3_desc": "Fully sanitized tools for every client"
            }
        }

        niche_lower = niche.lower()
        for key in fallbacks:
            if key in niche_lower:
                return fallbacks[key]

        return {
            "tagline": f"Professional {niche} services you can trust in {city}",
            "about": f"{business_name} delivers exceptional {niche} services in {city}. We are committed to quality, reliability and complete customer satisfaction.",
            "s1_name": "Core Service", "s1_desc": "Our signature professional service",
            "s2_name": "Premium Package", "s2_desc": "Enhanced service for best results",
            "s3_name": "Consultation", "s3_desc": "Expert advice tailored to your needs",
            "s4_name": "Express Service", "s4_desc": "Quick and efficient same-day service",
            "s5_name": "Aftercare Support", "s5_desc": "Ongoing support after every service",
            "w1_title": "Experienced Team", "w1_desc": "Qualified professionals dedicated to excellence",
            "w2_title": "Quality Guaranteed", "w2_desc": "We stand behind every service we deliver",
            "w3_title": "Customer First", "w3_desc": "Your satisfaction is our top priority"
        }


# ─── HTML Builder ─────────────────────────────────────────
def build_html(business_name, niche, city, phone,
               description="", hours="", address=""):
    wa_number = (
        phone.replace("+", "").replace(" ", "")
        .replace("-", "").replace("(", "").replace(")", "")
        if phone else ""
    )
    wa_link = f"https://wa.me/{wa_number}" if wa_number else "#"
    hours_text = hours if hours else "Mon–Sat: 9AM–7PM  |  Sun: 10AM–5PM"
    address_text = address if address and address != "N/A" else city

    c = generate_content(business_name, niche, city)

    icons = {
        "dentist": "🦷", "dental": "🦷", "salon": "✂️",
        "spa": "💆", "barber": "💈", "barbershop": "💈",
        "restaurant": "🍽️", "cafe": "☕", "hotel": "🏨",
        "clinic": "🏥", "doctor": "👨‍⚕️", "gym": "💪",
        "fitness": "🏋️", "pharmacy": "💊", "real estate": "🏠",
        "realtor": "🏠", "lawyer": "⚖️", "plumber": "🔧",
        "electrician": "⚡", "contractor": "🔨"
    }
    niche_icon = icons.get(niche.lower(), "⭐")
    s_icons = ["✂️", "💆", "✨", "💅", "🌟"]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{business_name}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;scroll-behavior:smooth}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#f8f8f6;color:#1a1a1a;overflow-x:hidden}}

@keyframes fadeUp{{from{{opacity:0;transform:translateY(30px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes fadeIn{{from{{opacity:0}}to{{opacity:1}}}}
@keyframes pulse{{0%,100%{{transform:scale(1);box-shadow:0 6px 25px rgba(37,211,102,0.35)}}50%{{transform:scale(1.04);box-shadow:0 10px 35px rgba(37,211,102,0.5)}}}}
@keyframes float{{0%,100%{{transform:translateY(0)}}50%{{transform:translateY(-7px)}}}}

.reveal{{opacity:0;transform:translateY(25px);transition:all 0.65s ease}}
.reveal.visible{{opacity:1;transform:translateY(0)}}
.reveal-left{{opacity:0;transform:translateX(-25px);transition:all 0.65s ease}}
.reveal-left.visible{{opacity:1;transform:translateX(0)}}
.reveal-scale{{opacity:0;transform:scale(0.95);transition:all 0.55s ease}}
.reveal-scale.visible{{opacity:1;transform:scale(1)}}

nav{{position:fixed;top:0;width:100%;background:rgba(255,255,255,0.97);backdrop-filter:blur(12px);padding:14px 24px;z-index:1000;border-bottom:1px solid #eee;box-shadow:0 2px 12px rgba(0,0,0,0.06);animation:fadeIn 0.5s ease}}
.nav-inner{{display:flex;justify-content:space-between;align-items:center;max-width:960px;margin:0 auto}}
.logo{{font-weight:800;font-size:1.05em;color:#1a1a1a}}
.nav-cta{{background:#25D366;color:#fff;padding:9px 20px;border-radius:50px;text-decoration:none;font-size:0.85em;font-weight:700}}

.hero{{min-height:100vh;background:linear-gradient(150deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center;padding:110px 24px 80px;position:relative;overflow:hidden}}
.hero::before{{content:'';position:absolute;top:0;left:0;right:0;bottom:0;background:radial-gradient(ellipse at 60% 30%,rgba(255,255,255,0.04) 0%,transparent 65%)}}
.hero-badge{{background:rgba(255,255,255,0.1);border:1px solid rgba(255,255,255,0.2);color:#fff;padding:7px 18px;border-radius:50px;font-size:0.75em;letter-spacing:2px;text-transform:uppercase;margin-bottom:24px;display:inline-block;animation:fadeIn 0.7s ease 0.1s both}}
.hero-icon{{font-size:3.2em;margin-bottom:18px;display:block;animation:float 3s ease-in-out infinite}}
.hero h1{{font-size:clamp(2em,6.5vw,3.8em);font-weight:900;color:#fff;line-height:1.08;margin-bottom:16px;animation:fadeUp 0.8s ease 0.2s both}}
.hero-sub{{font-size:1.1em;color:rgba(255,255,255,0.7);max-width:460px;line-height:1.7;margin-bottom:40px;animation:fadeUp 0.8s ease 0.4s both}}
.hero-btns{{display:flex;flex-direction:column;gap:12px;align-items:center;animation:fadeUp 0.8s ease 0.6s both}}
.btn-primary{{background:#25D366;color:#fff;padding:16px 38px;border-radius:50px;text-decoration:none;font-size:1.02em;font-weight:700;animation:pulse 2.5s ease-in-out infinite;display:inline-flex;align-items:center;gap:8px}}
.btn-secondary{{color:rgba(255,255,255,0.8);border:1px solid rgba(255,255,255,0.25);padding:13px 30px;border-radius:50px;text-decoration:none;font-size:0.92em;transition:all 0.3s}}
.btn-secondary:hover{{background:rgba(255,255,255,0.08)}}

.about{{background:#fff;padding:80px 24px}}
.about-inner{{max-width:960px;margin:0 auto;display:grid;grid-template-columns:1.2fr 1fr;gap:48px;align-items:center}}
.section-tag{{color:#0f3460;font-size:0.72em;letter-spacing:3px;text-transform:uppercase;margin-bottom:10px;display:block;font-weight:700}}
h2{{font-size:clamp(1.6em,3.5vw,2.2em);font-weight:800;margin-bottom:18px;line-height:1.2;color:#1a1a1a}}
.about p{{color:#666;line-height:1.85;font-size:1em}}
.badges{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
.badge{{background:#f8f8f6;border:1px solid #eee;border-radius:14px;padding:20px;text-align:center;transition:all 0.3s}}
.badge:hover{{border-color:#0f3460;transform:translateY(-3px)}}
.badge-icon{{font-size:1.5em;margin-bottom:6px;display:block}}
.badge-text{{color:#888;font-size:0.8em;font-weight:500}}
@media(max-width:640px){{.about-inner{{grid-template-columns:1fr}}}}

.services{{background:#f8f8f6;padding:80px 24px}}
.services-inner{{max-width:960px;margin:0 auto}}
.services-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:18px;margin-top:8px}}
.service-card{{background:#fff;border:1px solid #eee;border-radius:16px;padding:28px;transition:all 0.3s;position:relative;overflow:hidden}}
.service-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,#0f3460,#533483)}}
.service-card:hover{{border-color:#ddd;transform:translateY(-5px);box-shadow:0 16px 40px rgba(0,0,0,0.08)}}
.service-icon{{width:46px;height:46px;background:#f0f4ff;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.3em;margin-bottom:16px}}
.service-card h3{{font-size:1.02em;font-weight:700;margin-bottom:8px;color:#1a1a1a}}
.service-card p{{color:#888;font-size:0.88em;line-height:1.6}}

.why{{background:#fff;padding:80px 24px}}
.why-inner{{max-width:960px;margin:0 auto}}
.why-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:22px}}
.why-card{{background:#f8f8f6;border-radius:16px;padding:32px 22px;text-align:center;border:1px solid #eee;transition:all 0.3s}}
.why-card:hover{{border-color:#0f3460;transform:translateY(-4px);box-shadow:0 12px 30px rgba(0,0,0,0.07)}}
.why-icon{{font-size:2.2em;margin-bottom:14px;display:block;animation:float 3s ease-in-out infinite}}
.why-card:nth-child(2) .why-icon{{animation-delay:0.6s}}
.why-card:nth-child(3) .why-icon{{animation-delay:1.2s}}
.why-card h3{{color:#0f3460;margin-bottom:8px;font-size:1em;font-weight:700}}
.why-card p{{color:#888;font-size:0.88em;line-height:1.5}}

.contact{{background:#f8f8f6;padding:80px 24px}}
.contact-inner{{max-width:700px;margin:0 auto}}
.contact-card{{background:linear-gradient(135deg,#1a1a2e,#0f3460);border-radius:24px;padding:52px 32px;text-align:center;color:#fff}}
.contact-card h2{{color:#fff;margin-bottom:8px}}
.contact-card .section-tag{{color:rgba(255,255,255,0.6)}}
.contact-items{{display:flex;flex-direction:column;gap:14px;margin:28px 0;align-items:center}}
.contact-item{{display:flex;align-items:center;gap:12px;color:rgba(255,255,255,0.85);font-size:0.97em}}
.c-icon{{background:rgba(255,255,255,0.1);border-radius:10px;width:38px;height:38px;display:flex;align-items:center;justify-content:center;font-size:1em;flex-shrink:0}}
.btn-wa-big{{background:#25D366;color:#fff;padding:18px 44px;border-radius:50px;text-decoration:none;font-size:1.1em;font-weight:800;display:inline-flex;align-items:center;gap:10px;animation:pulse 2.5s ease-in-out infinite;margin-top:8px}}

footer{{background:#1a1a1a;padding:24px;text-align:center;color:#666;font-size:0.82em}}
footer span{{color:#aaa}}
</style>
</head>
<body>

<nav>
  <div class="nav-inner">
    <span class="logo">{niche_icon} {business_name}</span>
    <a href="{wa_link}" class="nav-cta">💬 WhatsApp</a>
  </div>
</nav>

<section class="hero">
  <span class="hero-icon">{niche_icon}</span>
  <span class="hero-badge">{niche.title()} · {city}</span>
  <h1>{business_name}</h1>
  <p class="hero-sub">{c.get('tagline','Professional services you can trust')}</p>
  <div class="hero-btns">
    <a href="{wa_link}" class="btn-primary">💬 Book on WhatsApp</a>
    <a href="#services" class="btn-secondary">Our Services ↓</a>
  </div>
</section>

<div class="about">
  <div class="about-inner">
    <div class="reveal-left">
      <span class="section-tag">About Us</span>
      <h2>Who We Are</h2>
      <p>{c.get('about','We are committed to delivering excellent service.')}</p>
    </div>
    <div class="badges reveal">
      <div class="badge"><span class="badge-icon">⭐</span><div class="badge-text">Top Rated</div></div>
      <div class="badge"><span class="badge-icon">📍</span><div class="badge-text">{city}</div></div>
      <div class="badge"><span class="badge-icon">✅</span><div class="badge-text">Trusted</div></div>
      <div class="badge"><span class="badge-icon">💬</span><div class="badge-text">Fast Reply</div></div>
    </div>
  </div>
</div>

<section class="services" id="services">
  <div class="services-inner">
    <div class="reveal">
      <span class="section-tag">What We Offer</span>
      <h2>Our Services</h2>
    </div>
    <div class="services-grid">
      <div class="service-card reveal"><div class="service-icon">{s_icons[0]}</div><h3>{c.get('s1_name','Service')}</h3><p>{c.get('s1_desc','Professional care')}</p></div>
      <div class="service-card reveal"><div class="service-icon">{s_icons[1]}</div><h3>{c.get('s2_name','Service')}</h3><p>{c.get('s2_desc','Expert results')}</p></div>
      <div class="service-card reveal"><div class="service-icon">{s_icons[2]}</div><h3>{c.get('s3_name','Service')}</h3><p>{c.get('s3_desc','Quality treatment')}</p></div>
      <div class="service-card reveal"><div class="service-icon">{s_icons[3]}</div><h3>{c.get('s4_name','Service')}</h3><p>{c.get('s4_desc','Premium experience')}</p></div>
      <div class="service-card reveal"><div class="service-icon">{s_icons[4]}</div><h3>{c.get('s5_name','Service')}</h3><p>{c.get('s5_desc','Trusted service')}</p></div>
    </div>
  </div>
</section>

<div class="why">
  <div class="why-inner">
    <div class="reveal">
      <span class="section-tag">Why Choose Us</span>
      <h2>The {business_name} Difference</h2>
    </div>
    <div class="why-grid">
      <div class="why-card reveal-scale"><span class="why-icon">🏆</span><h3>{c.get('w1_title','Excellence')}</h3><p>{c.get('w1_desc','We deliver the best')}</p></div>
      <div class="why-card reveal-scale"><span class="why-icon">❤️</span><h3>{c.get('w2_title','Care')}</h3><p>{c.get('w2_desc','We genuinely care')}</p></div>
      <div class="why-card reveal-scale"><span class="why-icon">⚡</span><h3>{c.get('w3_title','Speed')}</h3><p>{c.get('w3_desc','Fast and efficient')}</p></div>
    </div>
  </div>
</div>

<section class="contact" id="contact">
  <div class="contact-inner">
    <div class="contact-card reveal-scale">
      <span class="section-tag">Get In Touch</span>
      <h2>Contact Us</h2>
      <div class="contact-items">
        <div class="contact-item"><span class="c-icon">📞</span><span>{phone if phone else 'Contact via WhatsApp'}</span></div>
        <div class="contact-item"><span class="c-icon">📍</span><span>{address_text}</span></div>
        <div class="contact-item"><span class="c-icon">🕐</span><span>{hours_text}</span></div>
      </div>
      <a href="{wa_link}" class="btn-wa-big">💬 Chat on WhatsApp</a>
    </div>
  </div>
</section>

<footer>
  <p>© 2025 <span>{business_name}</span> · {city}</p>
</footer>

<script>
const observer = new IntersectionObserver((entries) => {{
  entries.forEach(e => {{
    if(e.isIntersecting) e.target.classList.add('visible');
  }});
}}, {{threshold: 0.12}});
document.querySelectorAll('.reveal,.reveal-left,.reveal-scale')
  .forEach(el => observer.observe(el));
document.querySelectorAll('a[href^="#"]').forEach(a => {{
  a.addEventListener('click', function(e) {{
    e.preventDefault();
    const t = document.querySelector(this.getAttribute('href'));
    if(t) t.scrollIntoView({{behavior:'smooth'}});
  }});
}});
</script>
</body>
</html>"""


# ─── Core Scan ───────────────────────────────────────────
def run_scan(bot, chat_id, niche, city, country_code="us"):
    global MIN_SCORE
    send_telegram(bot, chat_id,
                  f"Scanning {niche} in {city}...")

    businesses = search_maps(niche, city, country_code)

    if not businesses:
        send_telegram(bot, chat_id,
                      f"No results for {niche} in {city}.\n"
                      f"Try: /scan dentist houston")
        return

    weak_found = 0
    total = len(businesses)

    for business in businesses:
        try:
            name = business.get("title", "Unknown")
            address = business.get("address", "N/A")
            place_id = business.get("place_id", "")
            gps = business.get("gps_coordinates", {}) or {}
            lat = gps.get("latitude", "")
            lng = gps.get("longitude", "")

            if place_id:
                maps_link = f"https://www.google.com/maps?cid={place_id}"
            elif lat and lng:
                maps_link = f"https://www.google.com/maps?q={lat},{lng}"
            else:
                maps_link = f"https://www.google.com/maps/search/{name.replace(' ', '+')}+{city.replace(' ', '+')}"

            phone = business.get("phone", "") or ""
            description = business.get("description", "") or ""
            hours = business.get("hours", "") or ""
            wa_link = format_wa_link(phone)

            score, issues, reviews, website = score_business(business)

            if not phone:
                continue
            if website:
                continue
            if score < MIN_SCORE:
                continue

            weak_found += 1
            pitch = generate_pitch(name, niche, city)
            issues_text = "\n".join([f"• {i}" for i in issues])

            # MSG 1 — Lead info
            send_telegram(bot, chat_id,
                f"TARGET #{weak_found}\n\n"
                f"Name: {name}\n"
                f"Type: {niche.title()}\n"
                f"Address: {address}\n"
                f"Score: {score}/10\n\n"
                f"Issues:\n{issues_text}"
            )
            time.sleep(1)

            # MSG 2 — Phone
            send_telegram(bot, chat_id, phone)
            time.sleep(1)

            # MSG 3 — WhatsApp link
            if wa_link:
                send_telegram(bot, chat_id, wa_link)
            time.sleep(1)

            # MSG 4 — Maps link
            send_telegram(bot, chat_id, maps_link)
            time.sleep(1)

            # MSG 5 — Pitch
            send_telegram(bot, chat_id, pitch)
            time.sleep(2)

            # MSG 6 — Build + auto deploy
            send_telegram(bot, chat_id, "Building and deploying site...")
            html = build_html(
                name, niche, city, phone,
                description=description,
                hours=hours,
                address=address
            )
            site_url = deploy_to_netlify(html, name)

            if site_url:
                send_telegram(bot, chat_id,
                    f"SITE LIVE\n\n"
                    f"{site_url}\n\n"
                    f"Send this link with your pitch."
                )
            else:
                send_telegram(bot, chat_id,
                    "Deploy failed — retry with /redeploy"
                )

            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            log_to_sheet([
                now, name, niche, city, str(score),
                str(reviews), phone, address,
                maps_link, site_url or "Deploy failed", "Pending"
            ])

            time.sleep(8)

        except Exception as e:
            logger.error(f"Business error: {e}")
            continue

    send_telegram(bot, chat_id,
        f"SCAN COMPLETE\n"
        f"{niche} in {city}\n"
        f"Scanned: {total}\n"
        f"Qualified: {weak_found}\n"
        f"Credits used: {credits_used}/1000"
    )


# ─── Commands ────────────────────────────────────────────
def cmd_start(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        "MAPS LEAD BOT\n\n"
        "Commands:\n\n"
        "/scan [niche] [city]\n"
        "  /scan salon lagos\n"
        "  /scan dentist houston\n"
        "  /scan clinic toronto\n\n"
        "/setscore [number] — default 3\n"
        "/schedule [niche] [city]\n"
        "/schedules\n"
        "/status\n"
        "/export\n\n"
        "Each lead = 6 messages:\n"
        "1. Lead info\n"
        "2. Phone\n"
        "3. WhatsApp link\n"
        "4. Maps link\n"
        "5. Pitch\n"
        "6. Live site URL (auto deployed)"
    )


def cmd_scan(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    args = context.args
    if len(args) < 2:
        update.message.reply_text(
            "Usage: /scan [niche] [city]\n"
            "Example: /scan dentist houston"
        )
        return
    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id
    country_code = detect_country(city)
    threading.Thread(
        target=run_scan,
        args=(bot, chat_id, niche, city, country_code)
    ).start()


def cmd_setscore(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    global MIN_SCORE
    args = context.args
    if not args:
        update.message.reply_text(f"Current: {MIN_SCORE}")
        return
    try:
        MIN_SCORE = int(args[0])
        update.message.reply_text(f"Score set to {MIN_SCORE}/10")
    except:
        update.message.reply_text("Enter a number.")


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
    country_code = detect_country(city)
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
        update.message.reply_text("No scheduled scans.")
        return
    msg = "SCHEDULED:\n\n"
    for i, s in enumerate(scheduled_scans, 1):
        msg += f"{i}. {s['niche']} in {s['city']}\n"
    update.message.reply_text(msg)


def cmd_status(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"STATUS\n\n"
        f"Running: Yes\n"
        f"Credits: {credits_used}/1000\n"
        f"Min score: {MIN_SCORE}/10\n"
        f"Scheduled: {len(scheduled_scans)}"
    )


def cmd_export(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"Check Google Sheet.\n"
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
                "Auto deploys to Netlify\n"
                "You get live URL per lead\n\n"
                "Try: /scan dentist houston"
            )
        }
    )
    updater.idle()


if __name__ == "__main__":
    main()
