import requests
import feedparser
import logging
from bs4 import BeautifulSoup
from config import Config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# WIDE KEYWORD LIST
# ─────────────────────────────────────────
REAL_ESTATE_KEYWORDS = [
    # Selling
    "property for sale", "land for sale", "house for sale",
    "flat for sale", "duplex for sale", "bungalow for sale",
    "plot for sale", "sell my property", "urgent sale",
    "distress sale", "relocating selling", "selling my house",
    "selling my land", "selling my property", "want to sell",
    "building for sale", "estate for sale", "mansion for sale",
    "commercial property for sale", "office space for sale",
    "warehouse for sale", "shop for sale",

    # Buying
    "property for rent", "house for rent", "flat for rent",
    "apartment for rent", "looking for apartment",
    "need 2 bedroom", "need 3 bedroom", "need a flat",
    "looking for house", "looking for land", "want to buy land",
    "looking to buy property", "need affordable apartment",
    "self contain", "mini flat", "room and parlour",
    "short let", "short let needed", "furnished apartment",
    "serviced apartment", "studio apartment", "penthouse",
    "boys quarter", "face me i face you",

    # Nigeria specific
    "Lagos property", "Abuja property", "Ibadan property",
    "Port Harcourt property", "Enugu property", "Owerri property",
    "Benin property", "Abeokuta property", "Lekki property",
    "Ajah property", "Yaba property", "Surulere property",
    "Ikeja property", "Victoria Island property", "Ikoyi property",
    "Magodo property", "Gbagada property", "Ojodu property",
    "Berger property", "Maitama property", "Wuse property",
    "Gwarinpa property", "Asokoro property", "Garki property",
    "Jabi property", "Lokogoma property", "Lugbe property",
    "real estate Nigeria", "Nigerian property market",
    "buy property Nigeria", "rent property Nigeria",
]

JOB_KEYWORDS = [
    # Hiring
    "we are hiring", "job vacancy", "job opening",
    "recruitment", "apply now", "now hiring",
    "urgent vacancy", "vacancy exists", "talents needed",
    "seeking candidates", "job opportunity", "career opportunity",
    "full time job", "part time job", "remote job Nigeria",
    "work from home Nigeria", "freelance Nigeria",

    # Job seeking
    "looking for job", "seeking employment", "need a job",
    "available for hire", "open to work", "job seeker",
    "fresh graduate", "experienced professional",
    "years experience", "CV available", "resume available",

    # Roles
    "sales rep needed", "marketer needed", "accountant needed",
    "software developer needed", "engineer needed",
    "manager needed", "secretary needed", "driver needed",
    "cleaner needed", "security needed", "nurse needed",
    "teacher needed", "lecturer needed",

    # Nigeria specific
    "Lagos jobs", "Abuja jobs", "Nigeria jobs",
    "Port Harcourt jobs", "Ibadan jobs",
    "entry level Nigeria", "graduate trainee Nigeria",
]

ALL_KEYWORDS = REAL_ESTATE_KEYWORDS + JOB_KEYWORDS

# ─────────────────────────────────────────
# GOOGLE NEWS RSS — GUARANTEED TO WORK
# ─────────────────────────────────────────
GOOGLE_NEWS_QUERIES = [
    # Real estate
    "property for sale Lagos Nigeria",
    "house for rent Abuja Nigeria",
    "land for sale Nigeria",
    "apartment for rent Lagos",
    "real estate Nigeria 2025",
    "short let Lagos Nigeria",
    "property for sale Ibadan",
    "buy land Abuja Nigeria",
    "duplex for sale Lagos",
    "mini flat for rent Lagos",
    "self contain for rent Nigeria",
    "distress sale property Nigeria",
    "urgent property sale Lagos",
    "cheap land for sale Nigeria",
    "estate housing Nigeria",

    # Jobs
    "job vacancy Lagos Nigeria",
    "hiring now Nigeria 2025",
    "recruitment Nigeria",
    "job opening Abuja Nigeria",
    "work from home Nigeria",
    "graduate trainee Nigeria",
    "urgent vacancy Nigeria",
    "sales job Lagos Nigeria",
    "remote job Nigeria",
]

# ─────────────────────────────────────────
# YOUTUBE SEARCH QUERIES
# ─────────────────────────────────────────
YOUTUBE_QUERIES = [
    "property for sale Lagos Nigeria",
    "buy land Nigeria 2025",
    "real estate investment Nigeria",
    "house for rent Abuja",
    "Nigeria property market",
    "short let apartment Lagos",
    "jobs in Nigeria 2025",
    "how to get job in Nigeria",
    "recruitment Nigeria 2025",
]

# ─────────────────────────────────────────
# DIRECT SCRAPE SITES
# ─────────────────────────────────────────
JIJI_URLS = [
    "https://jiji.ng/real-estate",
    "https://jiji.ng/houses-apartments-for-rent",
    "https://jiji.ng/houses-apartments-for-sale",
    "https://jiji.ng/land-plots-for-sale",
    "https://jiji.ng/commercial-property",
    "https://jiji.ng/jobs",
    "https://jiji.ng/jobs/accounting",
    "https://jiji.ng/jobs/sales",
    "https://jiji.ng/jobs/engineering",
]

NAIJAHOUSES_URLS = [
    "https://www.naijahouses.com/properties/for-sale",
    "https://www.naijahouses.com/properties/for-rent",
]

NIGERIAPROPERTYCENTRE_URLS = [
    "https://nigeriapropertycentre.com/for-sale",
    "https://nigeriapropertycentre.com/for-rent",
    "https://nigeriapropertycentre.com/land",
]

TONATON_URLS = [
    "https://tonaton.com.ng/real_estate.html",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


# ─────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────

def scrape_google_news():
    posts = []
    for query in GOOGLE_NEWS_QUERIES:
        try:
            encoded = query.replace(" ", "+")
            url = f"https://news.google.com/rss/search?q={encoded}&hl=en-NG&gl=NG&ceid=NG:en"
            feed = feedparser.parse(url)
            for entry in feed.entries[:10]:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                text = f"{title} {summary}".strip()
                if text:
                    posts.append({
                        "text": text[:600],
                        "source": "Google News"
                    })
        except Exception as e:
            logger.error(f"Google News error [{query}]: {e}")
    logger.info(f"Google News: {len(posts)} posts")
    return posts


def scrape_youtube():
    posts = []
    for query in YOUTUBE_QUERIES:
        try:
            search_res = requests.get(
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "part": "snippet",
                    "q": query,
                    "type": "video",
                    "maxResults": 5,
                    "key": Config.YOUTUBE_API_KEY,
                    "regionCode": "NG"
                },
                timeout=10
            )
            data = search_res.json()
            if "error" in data:
                logger.error(f"YouTube API error: {data['error']['message']}")
                continue

            items = data.get("items", [])
            for item in items:
                snippet = item.get("snippet", {})
                title = snippet.get("title", "")
                description = snippet.get("description", "")
                text = f"{title} {description}".strip()
                if text:
                    posts.append({
                        "text": text[:600],
                        "source": "YouTube"
                    })

                vid_id = item["id"].get("videoId")
                if not vid_id:
                    continue
                try:
                    cres = requests.get(
                        "https://www.googleapis.com/youtube/v3/commentThreads",
                        params={
                            "part": "snippet",
                            "videoId": vid_id,
                            "maxResults": 30,
                            "key": Config.YOUTUBE_API_KEY
                        },
                        timeout=10
                    )
                    cdata = cres.json()
                    if "error" not in cdata:
                        for c in cdata.get("items", []):
                            comment = c["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                            posts.append({
                                "text": comment[:600],
                                "source": "YouTube"
                            })
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"YouTube error [{query}]: {e}")
    logger.info(f"YouTube: {len(posts)} posts")
    return posts


def scrape_jiji():
    posts = []
    for url in JIJI_URLS:
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            logger.info(f"Jiji {url}: {res.status_code}")
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.text, "html.parser")

            # Grab listing titles and descriptions
            titles = soup.find_all(["h2", "h3", "h4"])
            for t in titles:
                text = t.get_text(separator=" ", strip=True)
                if text and len(text) > 10:
                    posts.append({"text": text[:600], "source": "Jiji"})

            # Grab listing cards
            cards = soup.find_all("div", class_=lambda c: c and "item" in c.lower())
            for card in cards:
                text = card.get_text(separator=" ", strip=True)
                if text and len(text) > 20:
                    posts.append({"text": text[:600], "source": "Jiji"})

        except Exception as e:
            logger.error(f"Jiji error [{url}]: {e}")
    logger.info(f"Jiji: {len(posts)} posts")
    return posts


def scrape_naijahouses():
    posts = []
    for url in NAIJAHOUSES_URLS:
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            logger.info(f"Naijahouses {url}: {res.status_code}")
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            for tag in soup.find_all(["h1", "h2", "h3", "p"]):
                text = tag.get_text(separator=" ", strip=True)
                if text and len(text) > 15:
                    posts.append({"text": text[:600], "source": "NaijaHouses"})
        except Exception as e:
            logger.error(f"NaijaHouses error [{url}]: {e}")
    logger.info(f"NaijaHouses: {len(posts)} posts")
    return posts


def scrape_nigeriapropertycentre():
    posts = []
    for url in NIGERIAPROPERTYCENTRE_URLS:
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            logger.info(f"NPC {url}: {res.status_code}")
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            for tag in soup.find_all(["h1", "h2", "h3", "p", "span"]):
                text = tag.get_text(separator=" ", strip=True)
                if text and len(text) > 15:
                    posts.append({"text": text[:600], "source": "NigeriaPropertyCentre"})
        except Exception as e:
            logger.error(f"NPC error [{url}]: {e}")
    logger.info(f"NigeriaPropertyCentre: {len(posts)} posts")
    return posts


def scrape_tonaton():
    posts = []
    for url in TONATON_URLS:
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            logger.info(f"Tonaton {url}: {res.status_code}")
            if res.status_code != 200:
                continue
            soup = BeautifulSoup(res.text, "html.parser")
            for tag in soup.find_all(["h1", "h2", "h3", "p"]):
                text = tag.get_text(separator=" ", strip=True)
                if text and len(text) > 15:
                    posts.append({"text": text[:600], "source": "Tonaton"})
        except Exception as e:
            logger.error(f"Tonaton error [{url}]: {e}")
    logger.info(f"Tonaton: {len(posts)} posts")
    return posts


# ─────────────────────────────────────────
# MASTER RUNNER
# ─────────────────────────────────────────

def run_all_scrapers():
    all_posts = []

    logger.info("=== SCRAPE CYCLE STARTED ===")

    logger.info("--- Google News ---")
    all_posts += scrape_google_news()

    logger.info("--- YouTube ---")
    all_posts += scrape_youtube()

    logger.info("--- Jiji ---")
    all_posts += scrape_jiji()

    logger.info("--- NaijaHouses ---")
    all_posts += scrape_naijahouses()

    logger.info("--- NigeriaPropertyCentre ---")
    all_posts += scrape_nigeriapropertycentre()

    logger.info("--- Tonaton ---")
    all_posts += scrape_tonaton()

    logger.info(f"=== TOTAL RAW POSTS: {len(all_posts)} ===")
    return all_posts
