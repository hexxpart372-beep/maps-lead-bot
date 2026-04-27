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

# ─── City/Country Detection ──────────────────────────────
NIGERIA_CITIES = [
    "lagos", "abuja", "ibadan", "kano", "port harcourt",
    "benin", "enugu", "kaduna", "owerri", "warri",
    "calabar", "jos", "ilorin", "abeokuta", "onitsha",
    "uyo", "asaba", "maiduguri", "zaria", "sokoto"
]

US_CITIES = [
    "new york", "los angeles", "chicago", "houston", "phoenix",
    "philadelphia", "san antonio", "san diego", "dallas", "san jose",
    "austin", "jacksonville", "miami", "atlanta", "boston",
    "seattle", "denver", "nashville", "portland", "las vegas",
    "memphis", "louisville", "baltimore", "milwaukee", "albuquerque",
    "tucson", "fresno", "sacramento", "mesa", "kansas city",
    "malibu", "beverly hills", "santa monica", "brooklyn", "manhattan"
]

CANADA_CITIES = [
    "toronto", "vancouver", "montreal", "calgary", "edmonton",
    "ottawa", "winnipeg", "quebec", "hamilton", "kitchener",
    "london", "victoria", "halifax", "saskatoon", "regina"
]

UK_CITIES = [
    "london", "birmingham", "manchester", "glasgow", "leeds",
    "liverpool", "edinburgh", "bristol", "sheffield", "leicester"
]

# ─── High Value Niches ───────────────────────────────────
HIGH_VALUE_NICHES = [
    "dentist", "dental", "clinic", "doctor", "medical",
    "salon", "spa", "barbershop", "barber",
    "restaurant", "cafe", "hotel",
    "real estate", "realtor", "lawyer", "attorney",
    "gym", "fitness", "physiotherapy",
    "pharmacy", "optician", "veterinary",
    "plumber", "electrician", "contractor",
    "accountant", "insurance", "mortgage"
]


def detect_country(city):
    city_lower = city.lower()
    if city_lower in NIGERIA_CITIES:
        return "ng"
    if city_lower in CANADA_CITIES:
        return "ca"
    if city_lower in UK_CITIES:
        return "gb"
    if city_lower in US_CITIES:
        return "us"
    return "us"


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
            caption=(
                "WEBSITE FILE\n\n"
                "1. Save this file\n"
                "2. Go to netlify.com/drop\n"
                "3. Upload file → get live URL\n"
                "4. Rename to business name\n"
                "5. Send URL in your pitch"
            )
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
def search_maps(niche, city, country_code="us"):
    global credits_used
    try:
        url = "https://api.scrapingdog.com/google_local/"
        if country_code == "ng":
            query = f"{niche}+in+{city}+Nigeria"
            location = f"{city}, Nigeria"
        elif country_code == "ca":
            query = f"{niche}+in+{city}+Canada"
            location = f"{city}, Canada"
        elif country_code == "gb":
            query = f"{niche}+in+{city}+UK"
            location = f"{city}, United Kingdom"
        else:
            query = f"{niche}+in+{city}"
            location = f"{city}, USA"

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
        issues.append("No business description")

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


# ─── Pitch Generator ─────────────────────────────────────
def generate_pitch(business_name, niche, city):
    try:
        prompt = f"""Write a short outreach message to a {niche} business owner in {city}.

Rules:
- Under 50 words total
- Mention you already built a website preview for them
- Say they can see it before deciding anything
- End with one simple question
- Sound human, not salesy
- No emojis, no exclamation marks

Write ONLY the message."""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80,
            temperature=0.6,
        )
        return response.choices[0].message.content.strip().strip('"')
    except Exception as e:
        logger.error(f"Groq pitch error: {e}")
        return (
            f"Hi, I found {business_name} on Google Maps and "
            f"noticed you don't have a website. I already built "
            f"a free preview for you. Want me to send the link?"
        )


# ─── Content Generator ───────────────────────────────────
def generate_content(business_name, niche, city):
    try:
        prompt = f"""Return ONLY a valid JSON object for a {niche} business. No extra text.

{{
  "tagline": "compelling one-line tagline for {business_name}",
  "about": "2 professional sentences about this {niche} in {city}",
  "s1_name": "service name", "s1_desc": "one line",
  "s2_name": "service name", "s2_desc": "one line",
  "s3_name": "service name", "s3_desc": "one line",
  "s4_name": "service name", "s4_desc": "one line",
  "s5_name": "service name", "s5_desc": "one line",
  "w1_title": "strength", "w1_desc": "one line",
  "w2_title": "strength", "w2_desc": "one line",
  "w3_title": "strength", "w3_desc": "one line"
}}"""

        response = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.5,
        )
        raw = response.choices[0].message.content.strip()
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start != -1 and end > start:
            raw = raw[start:end]
        return json.loads(raw)
    except Exception as e:
        logger.error(f"Content generation error: {e}")
        return {
            "tagline": f"Premium {niche} services in {city}",
            "about": f"{business_name} delivers exceptional {niche} services in {city}. We are committed to quality and customer satisfaction.",
            "s1_name": "Expert Service", "s1_desc": "Professional care tailored to you",
            "s2_name": "Quality Results", "s2_desc": "Consistent excellence every visit",
            "s3_name": "Skilled Team", "s3_desc": "Experienced specialists at your service",
            "s4_name": "Premium Experience", "s4_desc": "Comfortable and welcoming environment",
            "s5_name": "Fast & Reliable", "s5_desc": "We value your time",
            "w1_title": "Trusted Expertise", "w1_desc": "Years of experience in {niche}",
            "w2_title": "Client First", "w2_desc": "Your satisfaction is everything",
            "w3_title": "Always Available", "w3_desc": "Easy to reach, quick to respond"
        }

# ─── HTML Builder ─────────────────────────────────────────
def build_html(business_name, niche, city, phone,
               description="", hours="", address=""):
    wa_number = (
        phone.replace("+", "").replace(" ", "")
        .replace("-", "").replace("(", "").replace(")", "")
        if phone else ""
    )
    wa_link = f"https://wa.me/{wa_number}" if wa_number else "https://wa.me/"
    hours_text = hours if hours else "Mon–Sat: 9AM–7PM  |  Sun: 10AM–5PM"
    address_text = (
        address if address and address != "N/A" else city
    )

    c = generate_content(business_name, niche, city)

    icons = {
        "dentist": "🦷", "dental": "🦷", "salon": "✂️", "spa": "💆",
        "barber": "💈", "barbershop": "💈", "restaurant": "🍽️",
        "cafe": "☕", "hotel": "🏨", "clinic": "🏥", "doctor": "👨‍⚕️",
        "gym": "💪", "fitness": "🏋️", "pharmacy": "💊",
        "real estate": "🏠", "realtor": "🏠", "lawyer": "⚖️",
        "plumber": "🔧", "electrician": "⚡", "contractor": "🔨"
    }
    niche_icon = icons.get(niche.lower(), "⭐")

    service_icons = ["✂️", "💆", "✨", "💅", "🌟"]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{business_name}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box;scroll-behavior:smooth}}
body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#0c0c0c;color:#fff;overflow-x:hidden}}

/* ── ANIMATIONS ── */
@keyframes fadeUp{{
  from{{opacity:0;transform:translateY(40px)}}
  to{{opacity:1;transform:translateY(0)}}
}}
@keyframes fadeIn{{
  from{{opacity:0}}to{{opacity:1}}
}}
@keyframes slideRight{{
  from{{opacity:0;transform:translateX(-30px)}}
  to{{opacity:1;transform:translateX(0)}}
}}
@keyframes pulse{{
  0%,100%{{transform:scale(1);box-shadow:0 8px 30px rgba(37,211,102,0.3)}}
  50%{{transform:scale(1.04);box-shadow:0 12px 40px rgba(37,211,102,0.5)}}
}}
@keyframes float{{
  0%,100%{{transform:translateY(0)}}
  50%{{transform:translateY(-8px)}}
}}
@keyframes gradientShift{{
  0%{{background-position:0% 50%}}
  50%{{background-position:100% 50%}}
  100%{{background-position:0% 50%}}
}}
@keyframes shimmer{{
  0%{{opacity:0.5}}50%{{opacity:1}}100%{{opacity:0.5}}
}}
@keyframes scaleIn{{
  from{{opacity:0;transform:scale(0.9)}}
  to{{opacity:1;transform:scale(1)}}
}}

/* ── SCROLL ANIMATIONS ── */
.reveal{{opacity:0;transform:translateY(30px);transition:all 0.7s ease}}
.reveal.visible{{opacity:1;transform:translateY(0)}}
.reveal-left{{opacity:0;transform:translateX(-30px);transition:all 0.7s ease}}
.reveal-left.visible{{opacity:1;transform:translateX(0)}}
.reveal-scale{{opacity:0;transform:scale(0.95);transition:all 0.6s ease}}
.reveal-scale.visible{{opacity:1;transform:scale(1)}}

/* ── NAV ── */
nav{{
  position:fixed;top:0;width:100%;
  background:rgba(12,12,12,0.92);
  backdrop-filter:blur(20px);
  padding:14px 24px;z-index:1000;
  border-bottom:1px solid rgba(212,175,55,0.15);
  animation:fadeIn 0.5s ease forwards
}}
.nav-inner{{
  display:flex;justify-content:space-between;
  align-items:center;max-width:960px;margin:0 auto
}}
.logo{{
  color:#d4af37;font-weight:800;font-size:1.05em;
  letter-spacing:0.5px
}}
.nav-cta{{
  background:linear-gradient(135deg,#25D366,#128C7E);
  color:#fff;padding:9px 20px;border-radius:50px;
  text-decoration:none;font-size:0.85em;font-weight:700;
  transition:transform 0.2s ease
}}
.nav-cta:hover{{transform:scale(1.05)}}

/* ── HERO ── */
.hero{{
  min-height:100vh;
  background:linear-gradient(135deg,#0c0c0c 0%,#141414 40%,#0c0c0c 100%);
  display:flex;flex-direction:column;
  justify-content:center;align-items:center;
  text-align:center;padding:110px 24px 80px;
  position:relative;overflow:hidden
}}
.hero::before{{
  content:'';position:absolute;
  top:-50%;left:-50%;width:200%;height:200%;
  background:radial-gradient(ellipse at 60% 40%,rgba(212,175,55,0.06) 0%,transparent 60%);
  animation:gradientShift 8s ease infinite;
  background-size:300% 300%
}}
.hero::after{{
  content:'';position:absolute;
  bottom:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(212,175,55,0.3),transparent)
}}
.hero-badge{{
  background:rgba(212,175,55,0.12);
  border:1px solid rgba(212,175,55,0.35);
  color:#d4af37;padding:7px 18px;
  border-radius:50px;font-size:0.75em;
  letter-spacing:3px;text-transform:uppercase;
  margin-bottom:28px;
  animation:fadeIn 0.8s ease 0.1s both;
  display:inline-block
}}
.hero-icon{{
  font-size:3.5em;margin-bottom:20px;
  animation:float 3s ease-in-out infinite;
  display:block
}}
.hero h1{{
  font-size:clamp(2.2em,7vw,4em);
  font-weight:900;line-height:1.05;
  margin-bottom:18px;
  background:linear-gradient(135deg,#ffffff 0%,#f0e0a0 50%,#d4af37 100%);
  -webkit-background-clip:text;
  -webkit-text-fill-color:transparent;
  background-clip:text;
  animation:fadeUp 0.8s ease 0.2s both
}}
.hero-sub{{
  font-size:1.15em;color:#999;
  max-width:480px;line-height:1.7;
  margin-bottom:44px;
  animation:fadeUp 0.8s ease 0.4s both
}}
.hero-btns{{
  display:flex;flex-direction:column;
  gap:14px;align-items:center;
  animation:fadeUp 0.8s ease 0.6s both
}}
.btn-primary{{
  background:linear-gradient(135deg,#25D366,#128C7E);
  color:#fff;padding:17px 40px;
  border-radius:50px;text-decoration:none;
  font-size:1.05em;font-weight:700;
  animation:pulse 2.5s ease-in-out infinite;
  display:inline-flex;align-items:center;gap:10px
}}
.btn-secondary{{
  color:#d4af37;
  border:1px solid rgba(212,175,55,0.35);
  padding:14px 32px;border-radius:50px;
  text-decoration:none;font-size:0.95em;
  transition:all 0.3s ease
}}
.btn-secondary:hover{{
  background:rgba(212,175,55,0.08);
  border-color:rgba(212,175,55,0.6)
}}

/* ── ABOUT ── */
.about{{
  background:#111;padding:80px 24px;
  border-top:1px solid rgba(255,255,255,0.04)
}}
.about-inner{{
  max-width:960px;margin:0 auto;
  display:grid;grid-template-columns:1fr 1fr;
  gap:48px;align-items:center
}}
.section-tag{{
  color:#d4af37;font-size:0.75em;
  letter-spacing:3px;text-transform:uppercase;
  margin-bottom:12px;display:block
}}
h2{{
  font-size:clamp(1.7em,4vw,2.4em);
  font-weight:800;margin-bottom:20px;
  line-height:1.2
}}
.about-text p{{
  color:#aaa;line-height:1.85;font-size:1.02em
}}
.badges{{
  display:grid;grid-template-columns:1fr 1fr;gap:16px;
  margin-top:20px
}}
.badge{{
  background:#1a1a1a;border:1px solid rgba(212,175,55,0.15);
  border-radius:12px;padding:20px;text-align:center
}}
.badge-icon{{font-size:1.6em;margin-bottom:8px;display:block}}
.badge-text{{color:#aaa;font-size:0.82em}}
@media(max-width:640px){{
  .about-inner{{grid-template-columns:1fr}}
}}

/* ── SERVICES ── */
.services{{padding:80px 24px;max-width:960px;margin:0 auto}}
.services-grid{{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(260px,1fr));
  gap:20px;margin-top:8px
}}
.service-card{{
  background:#111;
  border:1px solid rgba(212,175,55,0.12);
  border-radius:18px;padding:30px;
  position:relative;overflow:hidden;
  transition:all 0.35s ease;cursor:default
}}
.service-card::before{{
  content:'';position:absolute;
  top:0;left:0;width:100%;height:2px;
  background:linear-gradient(90deg,#d4af37,transparent)
}}
.service-card:hover{{
  border-color:rgba(212,175,55,0.35);
  transform:translateY(-6px);
  box-shadow:0 20px 40px rgba(0,0,0,0.4)
}}
.service-icon{{
  width:48px;height:48px;
  background:rgba(212,175,55,0.1);
  border-radius:12px;display:flex;
  align-items:center;justify-content:center;
  font-size:1.4em;margin-bottom:18px
}}
.service-card h3{{
  font-size:1.05em;font-weight:700;
  margin-bottom:10px;color:#fff
}}
.service-card p{{color:#888;font-size:0.9em;line-height:1.6}}

/* ── WHY ── */
.why{{
  background:#111;padding:80px 24px;
  border-top:1px solid rgba(255,255,255,0.04)
}}
.why-inner{{max-width:960px;margin:0 auto}}
.why-grid{{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
  gap:24px
}}
.why-card{{
  background:#161616;border-radius:18px;
  padding:36px 24px;text-align:center;
  border:1px solid rgba(255,255,255,0.05);
  transition:all 0.35s ease
}}
.why-card:hover{{
  border-color:rgba(212,175,55,0.2);
  transform:translateY(-4px)
}}
.why-icon{{
  font-size:2.4em;margin-bottom:18px;
  display:block;animation:float 3s ease-in-out infinite
}}
.why-card:nth-child(2) .why-icon{{animation-delay:0.5s}}
.why-card:nth-child(3) .why-icon{{animation-delay:1s}}
.why-card h3{{
  color:#d4af37;margin-bottom:10px;
  font-size:1.05em;font-weight:700
}}
.why-card p{{color:#888;font-size:0.9em;line-height:1.5}}

/* ── CONTACT ── */
.contact{{padding:80px 24px;max-width:960px;margin:0 auto}}
.contact-card{{
  background:linear-gradient(135deg,#111 0%,#161616 100%);
  border:1px solid rgba(212,175,55,0.2);
  border-radius:24px;padding:52px 32px;
  text-align:center;position:relative;overflow:hidden
}}
.contact-card::before{{
  content:'';position:absolute;
  top:-50%;left:-50%;width:200%;height:200%;
  background:radial-gradient(ellipse at center,rgba(212,175,55,0.04) 0%,transparent 60%)
}}
.contact-items{{
  display:flex;flex-direction:column;
  gap:16px;margin:32px 0;align-items:center
}}
.contact-item{{
  display:flex;align-items:center;
  gap:14px;color:#bbb;font-size:1em
}}
.c-icon{{
  background:rgba(212,175,55,0.1);
  border-radius:10px;width:40px;height:40px;
  display:flex;align-items:center;
  justify-content:center;font-size:1.1em;
  flex-shrink:0
}}
.btn-wa-big{{
  background:linear-gradient(135deg,#25D366,#128C7E);
  color:#fff;padding:20px 48px;
  border-radius:50px;text-decoration:none;
  font-size:1.15em;font-weight:800;
  display:inline-flex;align-items:center;gap:12px;
  animation:pulse 2.5s ease-in-out infinite;
  margin-top:8px
}}

/* ── FOOTER ── */
footer{{
  background:#080808;
  border-top:1px solid rgba(255,255,255,0.04);
  padding:28px 24px;text-align:center;
  color:#444;font-size:0.82em
}}
footer span{{color:#d4af37}}
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
  <p class="hero-sub">{c.get('tagline', f'Professional {niche} services in {city}')}</p>
  <div class="hero-btns">
    <a href="{wa_link}" class="btn-primary">💬 Book on WhatsApp</a>
    <a href="#services" class="btn-secondary">Explore Services ↓</a>
  </div>
</section>

<div class="about">
  <div class="about-inner">
    <div class="reveal-left">
      <span class="section-tag">About Us</span>
      <h2>Who We Are</h2>
      <p>{c.get('about', f'{business_name} provides top-quality {niche} services in {city}.')}</p>
    </div>
    <div class="badges reveal">
      <div class="badge"><span class="badge-icon">⭐</span><div class="badge-text">Top Rated</div></div>
      <div class="badge"><span class="badge-icon">📍</span><div class="badge-text">{city}</div></div>
      <div class="badge"><span class="badge-icon">✅</span><div class="badge-text">Verified</div></div>
      <div class="badge"><span class="badge-icon">💬</span><div class="badge-text">Fast Reply</div></div>
    </div>
  </div>
</div>

<section class="services" id="services">
  <div class="reveal">
    <span class="section-tag">What We Offer</span>
    <h2>Our Services</h2>
  </div>
  <div class="services-grid">
    <div class="service-card reveal"><div class="service-icon">{service_icons[0]}</div><h3>{c.get('s1_name','Service 1')}</h3><p>{c.get('s1_desc','Professional service')}</p></div>
    <div class="service-card reveal"><div class="service-icon">{service_icons[1]}</div><h3>{c.get('s2_name','Service 2')}</h3><p>{c.get('s2_desc','Expert care')}</p></div>
    <div class="service-card reveal"><div class="service-icon">{service_icons[2]}</div><h3>{c.get('s3_name','Service 3')}</h3><p>{c.get('s3_desc','Quality results')}</p></div>
    <div class="service-card reveal"><div class="service-icon">{service_icons[3]}</div><h3>{c.get('s4_name','Service 4')}</h3><p>{c.get('s4_desc','Premium treatment')}</p></div>
    <div class="service-card reveal"><div class="service-icon">{service_icons[4]}</div><h3>{c.get('s5_name','Service 5')}</h3><p>{c.get('s5_desc','Trusted quality')}</p></div>
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
  <div class="reveal">
    <span class="section-tag">Get In Touch</span>
    <h2>Contact Us</h2>
  </div>
  <div class="contact-card reveal-scale">
    <div class="contact-items">
      <div class="contact-item"><span class="c-icon">📞</span><span>{phone if phone else 'Contact via WhatsApp'}</span></div>
      <div class="contact-item"><span class="c-icon">📍</span><span>{address_text}</span></div>
      <div class="contact-item"><span class="c-icon">🕐</span><span>{hours_text}</span></div>
    </div>
    <a href="{wa_link}" class="btn-wa-big">💬 Chat on WhatsApp Now</a>
  </div>
</section>

<footer>
  <p>© 2025 <span>{business_name}</span> · {city} · All rights reserved</p>
</footer>

<script>
// Scroll reveal animation
const observer = new IntersectionObserver((entries) => {{
  entries.forEach(entry => {{
    if (entry.isIntersecting) {{
      entry.target.classList.add('visible');
    }}
  }});
}}, {{ threshold: 0.1 }});

document.querySelectorAll('.reveal, .reveal-left, .reveal-scale')
  .forEach(el => observer.observe(el));

// Smooth scroll for anchor links
document.querySelectorAll('a[href^="#"]').forEach(anchor => {{
  anchor.addEventListener('click', function(e) {{
    e.preventDefault();
    const target = document.querySelector(this.getAttribute('href'));
    if (target) target.scrollIntoView({{ behavior: 'smooth' }});
  }});
}});
</script>

</body>
</html>"""

# ─── Core Scan Function ──────────────────────────────────
def run_scan(bot, chat_id, niche, city, country_code="us"):
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
            f"/scan dentist houston\n"
            f"/scan clinic toronto"
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

            score, issues, reviews, website = score_business(business)

            # Quality filter — must have phone and no website
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
            if maps_link:
                send_telegram(bot, chat_id, maps_link)
            time.sleep(1)

            # MSG 5 — Pitch
            send_telegram(bot, chat_id, pitch)
            time.sleep(2)

            # MSG 6 — HTML file
            send_telegram(bot, chat_id,
                          "Building website... 30 seconds")
            html = build_html(
                name, niche, city, phone,
                description=description,
                hours=hours,
                address=address
            )
            safe = (
                name.lower()
                .replace(" ", "-")
                .replace("&", "and")
                .replace("'", "")
                .replace(",", "")[:35]
            )
            send_html_file(bot, chat_id, html, f"{safe}.html")

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
        f"Qualified targets: {weak_found}\n"
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
        "  /scan dentist houston\n"
        "  /scan clinic toronto\n"
        "  /scan barber london\n\n"
        "/setscore [number]\n"
        "  Default: 3\n\n"
        "/schedule [niche] [city]\n"
        "  Auto scan every 8am\n\n"
        "/schedules\n"
        "/status\n"
        "/export\n\n"
        "Each lead = 6 messages:\n"
        "1. Lead info\n"
        "2. Phone\n"
        "3. WhatsApp link\n"
        "4. Maps link\n"
        "5. Pitch\n"
        "6. HTML website file"
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
            "Example: /scan dentist houston"
        )
        return
    niche = args[0]
    city = " ".join(args[1:])
    bot = context.bot
    chat_id = update.effective_chat.id
    country_code = detect_country(city)

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
            f"Current: {MIN_SCORE}\nUsage: /setscore 3"
        )
        return
    try:
        MIN_SCORE = int(args[0])
        update.message.reply_text(f"Score set to {MIN_SCORE}/10")
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
    country_code = detect_country(city)

    scheduled_scans.append({"niche": niche, "city": city})
    schedule.every().day.at("08:00").do(
        run_scan, bot, chat_id, niche, city, country_code
    )
    update.message.reply_text(
        f"Scheduled: {niche} in {city}\nDaily at 8:00 AM"
    )


def cmd_schedules(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    if not scheduled_scans:
        update.message.reply_text("No scheduled scans yet.")
        return
    msg = "SCHEDULED:\n\n"
    for i, s in enumerate(scheduled_scans, 1):
        msg += f"{i}. {s['niche']} in {s['city']} — 8AM\n"
    update.message.reply_text(msg)


def cmd_status(update: Update, context: CallbackContext):
    if update.effective_user.id != TELEGRAM_USER_ID:
        return
    update.message.reply_text(
        f"STATUS\n\n"
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
        f"Check Google Sheet for all leads.\n"
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
                "Premium HTML sites generated per lead\n"
                "All animated, mobile-first, WhatsApp CTA\n\n"
                "Try:\n"
                "/scan salon lagos\n"
                "/scan dentist houston\n"
                "/scan clinic toronto"
            )
        }
    )
    updater.idle()


if __name__ == "__main__":
    main()
