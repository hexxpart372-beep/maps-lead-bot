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
    "Ibadan house for sale",
    "Port Harcourt property",
    "Nigeria real estate"
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
]

def keyword_match(text: str) -> bool:
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in ALL_KEYWORDS)

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
                    "maxResults": 30,
                    "key": Config.YOUTUBE_API_KEY
                }
                cres = requests.get(comment_url, params=comment_params, timeout=10)
                comments = cres.json().get("items", [])
                for c in comments:
                    text = c["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                    if keyword_match(text):
                        posts.append({"text": text[:500], "source": f"YouTube - {query}"})
        except Exception as e:
            logger.error(f"YouTube error: {e}")
    return posts

def scrape_nairaland():
    posts = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for url in NAIRALAND_URLS:
        try:
            res = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
            for post in soup.find_all("td", class_="l"):
                text = post.get_text(separator=" ", strip=True)
                if keyword_match(text):
                    posts.append({"text": text[:500], "source": f"Nairaland"})
        except Exception as e:
            logger.error(f"Nairaland error: {e}")
    return posts

def scrape_rss():
    posts = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:20]:
                text = f"{entry.get('title','')} {entry.get('summary','')}"
                if keyword_match(text):
                    posts.append({"text": text[:500], "source": f"RSS Feed"})
        except Exception as e:
            logger.error(f"RSS error: {e}")
    return posts

def run_all_scrapers():
    all_posts = []
    all_posts += scrape_youtube()
    all_posts += scrape_nairaland()
    all_posts += scrape_rss()
    return all_posts
