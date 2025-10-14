# import os, json, re, sqlite3
# from contextlib import closing
# from typing import Set

# from kafka import KafkaConsumer, KafkaProducer

# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC  = os.getenv("ENRICHED_TOPIC", "news.enriched")
# OUT_TOPIC = os.getenv("WATCH_TOPIC", "watchlist.events")
# WATCHLIST_PATH = os.getenv("WATCHLIST_PATH", "data/watchlist.txt")

# STATE_DIR = os.getenv("STATE_DIR", "state")
# DB_PATH   = os.path.join(STATE_DIR, "watchlist_first_mentions.db")
# os.makedirs(STATE_DIR, exist_ok=True)

# def init_db():
#     with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
#         cur.execute("""
#         CREATE TABLE IF NOT EXISTS first_seen (
#             key TEXT PRIMARY KEY,
#             ts  TEXT
#         )
#         """)
#         conn.commit()

# def is_first_mention(key: str) -> bool:
#     with closing(sqlite3.connect(DB_PATH)) as conn, closing(conn.cursor()) as cur:
#         cur.execute("SELECT ts FROM first_seen WHERE key = ? LIMIT 1", (key,))
#         row = cur.fetchone()
#         return row is None

# def mark_seen(key: str, ts: str):
#     with closing(sqlite3.connect(DB_PATH)) as conn, conn, closing(conn.cursor()) as cur:
#         cur.execute("INSERT OR IGNORE INTO first_seen(key, ts) VALUES(?, ?)", (key, ts))
#         conn.commit()

# def load_watchlist(path: str) -> Set[str]:
#     items = set()
#     try:
#         with open(path, "r", encoding="utf-8") as f:
#             for line in f:
#                 t = line.strip()
#                 if t:
#                     items.add(t.upper())
#     except FileNotFoundError:
#         print(f"[watchlist] WARNING: {path} not found. Using empty watchlist.")
#     return items

# def tokenize(text: str):
#     return re.findall(r"[A-Z0-9][A-Z0-9\-&\.]+", (text or "").upper())

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="watchlist-filter",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#         max_poll_records=200,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# def find_match(watchlist: Set[str], doc: dict):
#     title   = (doc.get("title") or "")
#     summary = (doc.get("summary") or "")
#     entities = doc.get("entities") or {}
#     tickers  = doc.get("tickers") or []

#     cands = set(tokenize(title)) | set(tokenize(summary)) | {t.upper() for t in tickers}
#     for label in ("ORG", "PERSON", "GPE"):
#         for val in entities.get(label, []):
#             v = val.strip().upper()
#             if v:
#                 cands.add(v)

#     for c in cands:
#         if c in watchlist:
#             return c
#     return None

# def main():
#     init_db()
#     watchlist = load_watchlist(WATCHLIST_PATH)
#     print(f"[watchlist] loaded {len(watchlist)} items from {WATCHLIST_PATH}")

#     consumer = make_consumer()
#     producer = make_producer()

#     kept = 0
#     for msg in consumer:
#         doc = msg.value
#         key = find_match(watchlist, doc)
#         if not key:
#             continue

#         first = is_first_mention(key)
#         if first:
#             mark_seen(key, doc.get("published_at") or "")

#         out = {
#             "watch_key": key,
#             "first_mention": bool(first),
#             "id": doc.get("id"),
#             "title": doc.get("title"),
#             "summary": doc.get("summary"),
#             "url": doc.get("url"),
#             "source": doc.get("source"),
#             "published_at": doc.get("published_at"),
#             "category": doc.get("category"),
#             "sentiment": doc.get("sentiment"),
#             "entities": doc.get("entities"),
#             "tickers": doc.get("tickers", []),
#         }
#         producer.send(OUT_TOPIC, value=out)
#         kept += 1
#         if kept % 10 == 0:
#             print(f"[watchlist] forwarded {kept} → {OUT_TOPIC}")

#     producer.flush()
#     print(f"[watchlist] done. total forwarded={kept}")

# if __name__ == "__main__":
#     main()
















# import os
# import json
# from kafka import KafkaConsumer, KafkaProducer
# from dotenv import load_dotenv

# load_dotenv()

# # -------------------- CONFIG --------------------
# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC = os.getenv("ENRICHED_TOPIC", "news.enriched")
# OUT_TOPIC = os.getenv("FILTERED_TOPIC", "news.filtered")
# WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "data/watchlist.txt")

# def load_watchlist(path: str) -> set:
#     """Load watchlist keywords (case-insensitive)"""
#     try:
#         with open(path, "r", encoding="utf-8") as f:
#             keywords = {line.strip().lower() for line in f if line.strip()}
#         print(f"[watchlist] loaded {len(keywords)} items from {path}")
#         return keywords
#     except FileNotFoundError:
#         print(f"[watchlist] File not found: {path}, using empty watchlist")
#         return set()

# def matches_watchlist(doc: dict, watchlist: set) -> bool:
#     """Check if article matches any watchlist keyword"""
#     if not watchlist:
#         return True  # If no watchlist, pass everything
    
#     # Search in title, summary, and entities
#     text_to_check = " ".join([
#         doc.get("title", ""),
#         doc.get("summary", ""),
#         " ".join(doc.get("entities", {}).get("ORG", [])),
#         " ".join(doc.get("entities", {}).get("PERSON", [])),
#     ]).lower()
    
#     # Check if any keyword is in the text
#     for keyword in watchlist:
#         if keyword in text_to_check:
#             return True
    
#     return False

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="watchlist-filter",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,  # 30 second timeout
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#     )

# def main():
#     print(f"[watchlist] broker={BROKER} {IN_TOPIC} → {OUT_TOPIC}")
    
#     watchlist = load_watchlist(WATCHLIST_FILE)
#     if not watchlist:
#         print("[watchlist] WARNING: Empty watchlist, will pass ALL articles")
    
#     consumer = make_consumer()
#     producer = make_producer()
    
#     total = 0
#     forwarded = 0
    
#     print(f"[watchlist] Waiting for messages from {IN_TOPIC}...")
    
#     for msg in consumer:
#         doc = msg.value
#         total += 1
        
#         if matches_watchlist(doc, watchlist):
#             producer.send(OUT_TOPIC, value=doc)
#             forwarded += 1
            
#             if forwarded % 10 == 0:
#                 print(f"[watchlist] forwarded {forwarded}/{total} → {OUT_TOPIC}")
        
#         if total % 50 == 0:
#             print(f"[watchlist] processed {total} (forwarded {forwarded})")
    
#     producer.flush()
#     print(f"[watchlist] done. total={total}, forwarded={forwarded}")

# if __name__ == "__main__":
#     main()














import os, json, re
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC       = os.getenv("ENRICHED_TOPIC", "news.enriched")
OUT_TOPIC      = os.getenv("FILTERED_TOPIC", "news.filtered")
WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "data/watchlist.txt")

def load_watchlist_ordered(path: str) -> list[str]:
    items = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s: items.append(s)
        print(f"[watchlist] loaded {len(items)} terms from {path}")
    except FileNotFoundError:
        print(f"[watchlist] ERROR: {path} not found")
    return items

def term_regex(term: str) -> re.Pattern:
    # word-boundary match, allow spaces in multi-word terms
    tokens = re.findall(r"\w+", term, flags=re.U) or [term]
    pattern = r"\b" + r"\s+".join(map(re.escape, tokens)) + r"\b"
    return re.compile(pattern, flags=re.I)

def build_matchers(terms: list[str]) -> list[tuple[str, re.Pattern]]:
    return [(t.upper(), term_regex(t)) for t in terms]

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id="watchlist-filter",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
        consumer_timeout_ms=30000,
    )

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        acks="all",
    )

def haystack(doc: dict) -> str:
    return " ".join([
        doc.get("title", ""),
        doc.get("summary", ""),
        " ".join(doc.get("entities", {}).get("ORG", [])),
        " ".join(doc.get("entities", {}).get("PERSON", [])),
    ])

def first_match(doc: dict, matchers: list[tuple[str, re.Pattern]]) -> str | None:
    text = haystack(doc)
    for term_upper, rx in matchers:
        if rx.search(text): return term_upper
    return None

def main():
    print(f"[watchlist] {IN_TOPIC} → {OUT_TOPIC}")
    terms = load_watchlist_ordered(WATCHLIST_FILE)
    matchers = build_matchers(terms)

    consumer, producer = make_consumer(), make_producer()
    total = forwarded = 0

    for msg in consumer:
        total += 1
        doc = msg.value

        match = first_match(doc, matchers)
        if match:
            doc["watchname"] = match             # ← what user wants to see
            doc["watch_key"] = match             # compatibility
            producer.send(OUT_TOPIC, key=match, value=doc)
            forwarded += 1
            if forwarded <= 3:
                print(f"[watchlist][debug] match={match} title={doc.get('title','')[:80]!r}")

        if total % 50 == 0:
            print(f"[watchlist] processed={total} forwarded={forwarded}")

    producer.flush()
    print(f"[watchlist] done. total={total}, forwarded={forwarded}")

if __name__ == "__main__":
    main()
