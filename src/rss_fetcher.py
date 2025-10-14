# import os
# import json
# import feedparser
# from datetime import datetime, timezone
# from kafka import KafkaProducer
# from dotenv import load_dotenv
# import time

# load_dotenv()

# # -------------------- CONFIG --------------------
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# OUT_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")

# # RSS feeds from major tech news sources (NO API KEY NEEDED!)
# RSS_FEEDS = [
#     # Major Tech News
#     "https://techcrunch.com/feed/",
#     "https://www.theverge.com/rss/index.xml",
#     "https://www.wired.com/feed/rss",
#     "https://arstechnica.com/feed/",
#     "https://www.engadget.com/rss.xml",
#     "https://thenextweb.com/feed/",
    
#     # Specialized Tech
#     "https://www.bleepingcomputer.com/feed/",  # Cybersecurity
#     "https://venturebeat.com/feed/",  # AI/ML
#     "https://techradar.com/rss",
#     "https://www.zdnet.com/news/rss.xml",
#     "https://www.computerworld.com/index.rss",
    
#     # Developer News
#     "https://news.ycombinator.com/rss",  # Hacker News
#     "https://dev.to/feed",
#     "https://feeds.feedburner.com/TheRegister/headlines",
    
#     # Business Tech
#     "https://www.businesswire.com/portal/site/home/news/rss/?rss=technology",
#     "https://www.techmeme.com/feed.xml",
    
#     # AI/ML Specific
#     "https://www.artificialintelligence-news.com/feed/",
#     "https://machinelearningmastery.com/feed/",
# ]

# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         retries=5,
#     )

# def fetch_from_rss(feed_url):
#     """
#     Fetch articles from a single RSS feed.
#     """
#     try:
#         print(f"[rss] Fetching from {feed_url}...")
#         feed = feedparser.parse(feed_url)
        
#         articles = []
#         for entry in feed.entries:
#             article = {
#                 "id": entry.get("id", ""),
#                 "title": entry.get("title", "").strip(),
#                 "url": entry.get("link", "").strip(),
#                 "summary": entry.get("summary", entry.get("description", "")).strip(),
#                 "source": feed.feed.get("title", "Unknown"),
#                 "published_at": entry.get("published", iso_now()),
#                 "author": entry.get("author", "Unknown"),
#                 "fetched_at": iso_now(),
#             }
            
#             # Only add if title and URL exist
#             if article["title"] and article["url"]:
#                 articles.append(article)
        
#         print(f"[rss] Got {len(articles)} articles from {feed.feed.get('title', feed_url)}")
#         return articles
        
#     except Exception as e:
#         print(f"[rss] Error fetching {feed_url}: {e}")
#         return []

# def main():
#     print(f"[rss-fetcher] Starting fetch from {len(RSS_FEEDS)} RSS feeds...")
#     producer = make_producer()
    
#     all_articles = []
    
#     # Fetch from all RSS feeds
#     for feed_url in RSS_FEEDS:
#         articles = fetch_from_rss(feed_url)
#         all_articles.extend(articles)
    
#     print(f"[rss-fetcher] Total articles fetched: {len(all_articles)}")
    
#     # Deduplicate by title (case-insensitive)
#     seen_titles = set()
#     unique_articles = []
    
#     for article in all_articles:
#         title_normalized = article["title"].lower().strip()
        
#         # Skip if empty or duplicate
#         if not title_normalized or title_normalized in seen_titles:
#             continue
        
#         seen_titles.add(title_normalized)
#         unique_articles.append(article)
    
#     print(f"[rss-fetcher] Unique articles: {len(unique_articles)}")
#     print(f"[rss-fetcher] Duplicates removed: {len(all_articles) - len(unique_articles)}")
    
#     # Send to Kafka
#     sent_count = 0
#     for article in unique_articles:
#         producer.send(OUT_TOPIC, value=article)
#         sent_count += 1
        
#         if sent_count % 50 == 0:
#             print(f"[rss-fetcher] Sent {sent_count}/{len(unique_articles)} to {OUT_TOPIC}")
    
#     producer.flush()
#     print(f"[rss-fetcher] Done! Sent {sent_count} unique articles")

# if __name__ == "__main__":
#     main()
    
















import os
import json
import hashlib
import feedparser
from urllib.parse import urlparse
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
OUT_TOPIC    = os.getenv("CLEAN_TOPIC", "news.cleaned")

RSS_FEEDS = [
    "https://techcrunch.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://www.wired.com/feed/rss",
    "https://arstechnica.com/feed/",
    "https://www.engadget.com/rss.xml",
    "https://thenextweb.com/feed/",
    "https://www.bleepingcomputer.com/feed/",
    "https://venturebeat.com/feed/",
    "https://techradar.com/rss",
    "https://www.zdnet.com/news/rss.xml",
    "https://www.computerworld.com/index.rss",
    "https://news.ycombinator.com/rss",
    "https://dev.to/feed",
    "https://feeds.feedburner.com/TheRegister/headlines",
    "https://www.techmeme.com/feed.xml",
    "https://www.artificialintelligence-news.com/feed/",
    "https://machinelearningmastery.com/feed/",
]

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        acks="all",
        retries=5,
    )

def stable_id(link: str, guid: str | None, published: str | None) -> str:
    if guid:
        return guid.strip()
    h = hashlib.sha1()
    h.update((link or "").encode("utf-8"))
    h.update((published or "").encode("utf-8"))
    return h.hexdigest()

def domain_from_url(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host or ""
    except Exception:
        return ""

def fetch_from_rss(feed_url: str):
    try:
        print(f"[rss] Fetching from {feed_url}...")
        feed = feedparser.parse(feed_url)
        out = []
        for entry in feed.entries:
            link = (entry.get("link") or "").strip()
            title = (entry.get("title") or "").strip()
            if not link or not title:
                continue
            published = entry.get("published") or entry.get("updated") or ""
            guid = entry.get("id") or entry.get("guid") or None
            src_title = feed.feed.get("title", "Unknown")

            _id = stable_id(link, guid, published)
            fallback_watch_key = domain_from_url(link)

            out.append({
                "id": _id,
                "title": title,
                "url": link,
                "summary": (entry.get("summary") or entry.get("description") or "").strip(),
                "source": src_title,
                "published_at": published or iso_now(),
                "author": (entry.get("author") or "Unknown"),
                "fetched_at": iso_now(),
                "watch_key": fallback_watch_key,  # will be replaced by watchlist_filter later
            })
        print(f"[rss] Got {len(out)} from {feed.feed.get('title', feed_url)}")
        return out
    except Exception as e:
        print(f"[rss] Error fetching {feed_url}: {e}")
        return []

def main():
    print(f"[rss-fetcher] Start")
    producer = make_producer()

    all_articles = []
    for feed_url in RSS_FEEDS:
        all_articles.extend(fetch_from_rss(feed_url))

    print(f"[rss-fetcher] Total fetched: {len(all_articles)}")

    seen = set()
    unique_articles = []
    for a in all_articles:
        key = a["title"].lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique_articles.append(a)

    print(f"[rss-fetcher] Unique: {len(unique_articles)}")

    for i, a in enumerate(unique_articles, 1):
        # temporary key = domain; final key will be the matched watchlist term later
        producer.send(OUT_TOPIC, key=a["watch_key"], value=a)
        if i <= 3:
            print(f"[rss-fetcher][debug] key={a['watch_key']} id={a['id']} url={a['url']} title={a['title'][:60]!r}")

    producer.flush()
    print(f"[rss-fetcher] Done. Sent {len(unique_articles)} to {OUT_TOPIC}")

if __name__ == "__main__":
    main()









# import os
# import json
# import feedparser
# from datetime import datetime, timezone
# from kafka import KafkaProducer
# from dotenv import load_dotenv
# from concurrent.futures import ThreadPoolExecutor, as_completed

# load_dotenv()

# # -------------------- CONFIG --------------------
# KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# OUT_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
# MAX_WORKERS = 5  # Parallel fetching

# # RSS feeds - REDUCED for speed (only fast, reliable sources)
# RSS_FEEDS = [
#     "https://techcrunch.com/feed/",
#     "https://www.theverge.com/rss/index.xml",
#     "https://arstechnica.com/feed/",
#     "https://news.ycombinator.com/rss",
#     "https://venturebeat.com/feed/",
# ]

# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         retries=5,
#     )

# def fetch_from_rss(feed_url):
#     """
#     Fetch articles from a single RSS feed with timeout.
#     """
#     try:
#         print(f"[rss] Fetching {feed_url}...")
#         # Add timeout to prevent hanging
#         feed = feedparser.parse(feed_url, timeout=5)
        
#         articles = []
#         for entry in feed.entries[:25]:  # Increased to 25 per feed
#             # Clean summary/description - REMOVE ALL HTML
#             summary = entry.get("summary", entry.get("description", "")).strip()
#             from html import unescape
#             import re
#             from html.parser import HTMLParser
            
#             # Unescape HTML entities first
#             summary = unescape(summary)
            
#             # Remove ALL HTML tags aggressively
#             summary = re.sub(r'<[^>]+>', '', summary)
#             summary = re.sub(r'&[a-zA-Z]+;', '', summary)  # Remove HTML entities
#             summary = re.sub(r'\s+', ' ', summary)  # Remove extra whitespace
#             summary = summary.strip()[:500]  # Limit length
            
#             article = {
#                 "id": entry.get("link", ""),
#                 "title": entry.get("title", "").strip(),
#                 "url": entry.get("link", "").strip(),
#                 "summary": summary,
#                 "source": feed.feed.get("title", "Unknown"),
#                 "published_at": entry.get("published", iso_now()),
#                 "author": entry.get("author", "Unknown"),
#                 "fetched_at": iso_now(),
#             }
            
#             # Only add if title and URL exist
#             if article["title"] and article["url"]:
#                 articles.append(article)
        
#         print(f"[rss] ✓ Got {len(articles)} from {feed.feed.get('title', feed_url)}")
#         return articles
        
#     except Exception as e:
#         print(f"[rss] ✗ Error: {feed_url}: {e}")
#         return []

# def main():
#     print(f"[rss-fetcher] Fast mode: {len(RSS_FEEDS)} feeds in parallel")
#     print(f"[rss-fetcher] Target: ~100 unique articles")
#     producer = make_producer()
    
#     all_articles = []
    
#     # Fetch from all RSS feeds IN PARALLEL (much faster!)
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         future_to_url = {executor.submit(fetch_from_rss, url): url for url in RSS_FEEDS}
        
#         for future in as_completed(future_to_url):
#             try:
#                 articles = future.result()
#                 all_articles.extend(articles)
#             except Exception as e:
#                 print(f"[rss-fetcher] Feed failed: {e}")
    
#     print(f"\n[rss-fetcher] Total fetched: {len(all_articles)}")
    
#     # Deduplicate by title (case-insensitive)
#     seen_titles = set()
#     unique_articles = []
    
#     for article in all_articles:
#         title_normalized = article["title"].lower().strip()
        
#         # Skip if empty or duplicate
#         if not title_normalized or title_normalized in seen_titles:
#             continue
        
#         seen_titles.add(title_normalized)
#         unique_articles.append(article)
        
#         # LIMIT TO 100 ARTICLES
#         if len(unique_articles) >= 100:
#             break
    
#     print(f"[rss-fetcher] Unique: {len(unique_articles)} (removed {len(all_articles) - len(unique_articles)} dupes)")
#     print(f"[rss-fetcher] Limited to 100 articles to avoid rate limits")
    
#     # Send to Kafka
#     for article in unique_articles:
#         producer.send(OUT_TOPIC, value=article)
    
#     producer.flush()
#     print(f"\n[rss-fetcher] ✓ DONE! Sent {len(unique_articles)} articles to {OUT_TOPIC}")

# if __name__ == "__main__":
#     main()
