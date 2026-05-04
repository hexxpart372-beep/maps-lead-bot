import requests
import feedparser
import logging
from bs4 import BeautifulSoup
from config import Config

logger = logging.getLogger(__name__)

ALL_KEYWORDS = Config.SELLER_KEYWORDS + Config.BUYER_KEYWORDS

# Broader search queries to catch more content
SEARCH_QUERIES = [
    "Nigeria property for sale",
    "Lagos apartment for rent",
    "Abuja land for sale",
    "Ibadan house for sale",
    "Port Harcourt property",
    "Nigeria real estate",
    "Lagos house rent",
    "Abuja apartment",
    "Nigeria land sale",
    "cheap apartment Lagos",
    "self contain Lagos",
    "mini flat Abuja",
    "3 bedroom Lagos",
    "duplex for sale Nigeria",
    "short let Lagos",
    "short let Abuja",
    "room and parlour Lagos",
    "buy land Nigeria",
    "sell property Nigeria",
    "urgent property sale Nigeria"
]

RSS_FEEDS = [
    "https://www.propertypro.ng/feed",
    "https://www.nigeriapropertycentre.com/feed",
    "https://www.privateproperty.com.ng/feed",
]

NAIRALAND_URLS = [
    "https://www.nairaland.com/properties",
    "https://www.nairaland.com/properties/1",
    "https://www.nairaland.com/properties/2",
    "https://www.nairaland.com/properties/3",
    "https://www.nairaland.com/properties/4",
]

def keyword_match(text: str) -> bool:
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in ALL_KEYWORDS)

def is_nigeria_related(text: str) -> bool:
    nigeria_terms = [
        "nigeria", "lagos", "abuja", "ibadan", "naira", "₦",
        "ph", "port harcourt", "enugu", "benin", "owerri",
        "lekki", "ajah", "yaba", "ikeja", "surulere", "ogun",
        "abeokuta", "ilorin", "kaduna", "kano"
    ]
    text_lower = text.lower()
    return any(term in text_lower for term in nigeria_terms)

def scrape_youtube():
    posts = []
    for query in SEARCH_QUERIES:
        try:
            search_url = "https://www.googleapis.com/youtube/v3/search"
            search_params = {
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": 5,
                "key": Config.YOUTUBE_API_KEY,
                "regionCode": "NG"
            }
            res = requests.get(search_url, params=search_params, timeout=10)
            items = res.json().get("items", [])
            video_ids = [item["id"]["videoId"] for item in items]

            for vid_id in video_ids:
                comment_url = "https://www.googleapis.com/youtube/v3/commentThreads"
                comment_params = {
                    "part": "snippet",
                    "videoId": vid_id,
                    "maxResults": 50,
                    "key": Config.YOUTUBE_API_KEY
                }
                cres = requests.get(comment_url, params=comment_params, timeout=10)
                comments = cres.json().get("items", [])
                for c in comments:
                    text = c["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                    if keyword_match(text) or is_nigeria_related(text):
                        posts.append({
                            "text": text[:500],
                            "source": f"YouTube"
                        })
        except Exception as e:
            logger.error(f"YouTube error: {e}")
    return posts

def scrape_nairaland():
    posts = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    for url in NAIRALAND_URLS:
        try:
            res = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")

            # Grab thread titles and post bodies
            titles = soup.find_all("td", class_="subject")
            bodies = soup.find_all("td", class_="l")

            for title in titles:
                text = title.get_text(separator=" ", strip=True)
                if text and len(text) > 10:
                    posts.append({
                        "text": text[:500],
                        "source": "Nairaland"
                    })

            for post in bodies:
                text = post.get_text(separator=" ", strip=True)
                if text and len(text) > 20:
                    posts.append({
                        "text": text[:500],
                        "source": "Nairaland"
                    })
        except Exception as e:
            logger.error(f"Nairaland error: {e}")
    return posts

def scrape_rss():
    posts = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:50]:
                title = entry.get("title", "")
                summary = entry.get("summary", "")
                text = f"{title} {summary}"
                if text.strip():
                    posts.append({
                        "text": text[:500],
                        "source": "RSS Feed"
                    })
        except Exception as e:
            logger.error(f"RSS error: {e}")
    return posts

def run_all_scrapers():
    all_posts = []
    logger.info("Starting YouTube scrape...")
    yt = scrape_youtube()
    logger.info(f"YouTube: {len(yt)} posts")
    all_posts += yt

    logger.info("Starting Nairaland scrape...")
    nl = scrape_nairaland()
    logger.info(f"Nairaland: {len(nl)} posts")
    all_posts += nl

    logger.info("Starting RSS scrape...")
    rss = scrape_rss()
    logger.info(f"RSS: {len(rss)} posts")
    all_posts += rss

    logger.info(f"Total raw posts collected: {len(all_posts)}")
    return all_posts
