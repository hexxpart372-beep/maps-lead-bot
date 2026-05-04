import requests
import feedparser
import logging
from bs4 import BeautifulSoup
from config import Config

logger = logging.getLogger(__name__)

ALL_KEYWORDS = Config.SELLER_KEYWORDS + Config.BUYER_KEYWORDS

SEARCH_QUERIES = [
    "Nigeria property for sale",
    "Lagos apartment for rent",
    "Abuja land for sale",
    "cheap apartment Lagos",
    "short let Lagos",
    "buy land Nigeria",
]

RSS_FEEDS = [
    "https://www.propertypro.ng/feed",
    "https://www.nigeriapropertycentre.com/feed",
    "https://www.privateproperty.com.ng/feed",
]

NAIRALAND_URLS = [
    "https://www.nairaland.com/properties",
    "https://www.nairaland.com/properties/1",
]

def is_nigeria_related(text: str) -> bool:
    nigeria_terms = [
        "nigeria", "lagos", "abuja", "ibadan", "naira",
        "port harcourt", "enugu", "lekki", "ajah", "yaba",
        "ikeja", "surulere", "ogun", "abeokuta"
    ]
    return any(term in text.lower() for term in nigeria_terms)

def scrape_youtube():
    posts = []
    try:
        query = "Nigeria property for sale"
        res = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": 3,
                "key": Config.YOUTUBE_API_KEY,
                "regionCode": "NG"
            },
            timeout=10
        )
        data = res.json()
        logger.info(f"YouTube search response keys: {list(data.keys())}")

        if "error" in data:
            logger.error(f"YouTube API error: {data['error']['message']}")
            return posts

        items = data.get("items", [])
        logger.info(f"YouTube videos found: {len(items)}")

        for item in items:
            vid_id = item["id"].get("videoId")
            if not vid_id:
                continue
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
            if "error" in cdata:
                logger.error(f"YouTube comments error: {cdata['error']['message']}")
                continue
            comments = cdata.get("items", [])
            logger.info(f"Comments for {vid_id}: {len(comments)}")
            for c in comments:
                text = c["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                posts.append({"text": text[:500], "source": "YouTube"})
    except Exception as e:
        logger.error(f"YouTube exception: {e}")
    return posts

def scrape_nairaland():
    posts = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for url in NAIRALAND_URLS:
        try:
            res = requests.get(url, headers=headers, timeout=15)
            logger.info(f"Nairaland {url} status: {res.status_code}")
            soup = BeautifulSoup(res.text, "html.parser")

            titles = soup.find_all("td", class_="subject")
            bodies = soup.find_all("td", class_="l")
            logger.info(f"Nairaland titles: {len(titles)} bodies: {len(bodies)}")

            for t in titles:
                text = t.get_text(separator=" ", strip=True)
                if text and len(text) > 5:
                    posts.append({"text": text[:500], "source": "Nairaland"})
            for b in bodies:
                text = b.get_text(separator=" ", strip=True)
                if text and len(text) > 10:
                    posts.append({"text": text[:500], "source": "Nairaland"})
        except Exception as e:
            logger.error(f"Nairaland exception: {e}")
    return posts

def scrape_rss():
    posts = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            logger.info(f"RSS {feed_url} entries: {len(feed.entries)}")
            for entry in feed.entries[:50]:
                text = f"{entry.get('title','')} {entry.get('summary','')}"
                if text.strip():
                    posts.append({"text": text[:500], "source": "RSS"})
        except Exception as e:
            logger.error(f"RSS exception: {e}")
    return posts

def run_all_scrapers():
    all_posts = []

    logger.info("=== Starting YouTube ===")
    yt = scrape_youtube()
    logger.info(f"YouTube total: {len(yt)}")
    all_posts += yt

    logger.info("=== Starting Nairaland ===")
    nl = scrape_nairaland()
    logger.info(f"Nairaland total: {len(nl)}")
    all_posts += nl

    logger.info("=== Starting RSS ===")
    rss = scrape_rss()
    logger.info(f"RSS total: {len(rss)}")
    all_posts += rss

    logger.info(f"=== GRAND TOTAL: {len(all_posts)} ===")
    return all_posts
