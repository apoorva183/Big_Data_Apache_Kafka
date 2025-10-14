# import os
# import json
# import csv
# import re
# from kafka import KafkaConsumer
# from dotenv import load_dotenv
# from datetime import datetime

# load_dotenv()

# # -------------------- CONFIG --------------------
# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC = os.getenv("FILTERED_TOPIC", "news.filtered")
# CSV_FILE = os.getenv("CSV_OUTPUT", "data/tech_news.csv")

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="csv-writer",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#     )

# def flatten_entities(entities: dict) -> str:
#     """Convert entities dict to readable string"""
#     parts = []
#     for key, values in entities.items():
#         if values:
#             parts.append(f"{key}: {', '.join(values)}")
#     return " | ".join(parts) if parts else "None"

# def main():
#     print(f"[csv-writer] broker={BROKER} {IN_TOPIC} → {CSV_FILE}")
    
#     consumer = make_consumer()
    
#     # Ensure data directory exists
#     os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    
#     # Open CSV file in write mode
#     with open(CSV_FILE, "w", newline="", encoding="utf-8") as csvfile:
#         fieldnames = [
#             "watch_key",
#             "first_mention",
#             "id",
#             "title",
#             "summary",
#             "url",
#             "source",
#             "published_at",
#             "category",
#             "sentiment",
#         ]
        
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
        
#         count = 0
#         seen_titles = set()  # Deduplicate in CSV too
        
#         print(f"[csv-writer] Waiting for messages from {IN_TOPIC}...")
        
#         for msg in consumer:
#             doc = msg.value

#             # Skip duplicates
#             title = doc.get("title", "").strip().lower()
#             if title in seen_titles:
#                 continue
#             seen_titles.add(title)
            
#             # Clean summary - remove any remaining HTML
#             summary = doc.get("summary", "")
#             summary = re.sub(r'<[^>]+>', '', summary)
#             summary = re.sub(r'&[a-zA-Z]+;', '', summary)
#             summary = re.sub(r'\s+', ' ', summary).strip()

#             # Write row
#             row = {
#                 "watch_key": doc.get("watch_key", ""),
#                 "first_mention": "YES" if doc.get("first_mention") else "NO",
#                 "id": doc.get("id", ""),
#                 "title": doc.get("title", ""),
#                 "summary": summary,
#                 "url": doc.get("url", ""),
#                 "source": doc.get("source", "Unknown"),
#                 "published_at": doc.get("published_at", ""),
#                 "category": doc.get("category", "General Technology"),
#                 "sentiment": doc.get("sentiment", "Neutral"),
#             }
            
#             writer.writerow(row)
#             count += 1
            
#             if count % 10 == 0:
#                 print(f"[csv-writer] wrote {count} rows to {CSV_FILE}")
#                 csvfile.flush()  # Force write to disk
        
#         print(f"[csv-writer] done. total rows={count}")
#         print(f"[csv-writer] Output saved to: {CSV_FILE}")

# if __name__ == "__main__":
#     main()























# import os
# import json
# import csv
# import re
# from urllib.parse import urlparse
# from kafka import KafkaConsumer
# from dotenv import load_dotenv

# load_dotenv()

# # -------------------- CONFIG --------------------
# BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC  = os.getenv("ENRICHED_TOPIC", "news.enriched")
# CSV_FILE  = os.getenv("CSV_OUTPUT", "data/tech_news.csv")
# GROUP_ID  = f"csv-writer-{os.getpid()}"  # fresh offsets per run
# SUMMARY_WORD_LIMIT = int(os.getenv("SUMMARY_WORD_LIMIT", "100"))

# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id=GROUP_ID,
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,  # msg.key -> str
#         consumer_timeout_ms=30000,
#     )

# def clean_summary(text: str) -> str:
#     text = text or ""
#     text = re.sub(r'<[^>]+>', '', text)
#     text = re.sub(r'&[a-zA-Z]+;', '', text)
#     text = re.sub(r'\s+', ' ', text).strip()
#     return text

# def truncate_words(text: str, max_words: int) -> str:
#     words = (text or "").split()
#     return " ".join(words[:max_words]) if len(words) > max_words else (text or "")

# def derive_watch_key_from_url(url: str) -> str:
#     try:
#         host = (urlparse(url).hostname or "").lower()
#         return host or ""
#     except Exception:
#         return ""

# def ensure_parent_dir(path: str):
#     parent = os.path.dirname(path)
#     if parent:
#         os.makedirs(parent, exist_ok=True)

# def main():
#     print(f"[csv-writer] {IN_TOPIC} → {CSV_FILE}")
#     consumer = make_consumer()
#     ensure_parent_dir(CSV_FILE)

#     with open(CSV_FILE, "w", newline="", encoding="utf-8") as csvfile:
#         fieldnames = [
#             "watch_key",
#             "first_mention",
#             "id",
#             "title",
#             "summary",
#             "url",
#             "source",
#             "published_at",
#             "category",
#             "sentiment",
#         ]
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()

#         count = 0
#         seen_titles = set()

#         print(f"[csv-writer] Waiting for messages from {IN_TOPIC}…")
#         for msg in consumer:
#             doc = msg.value

#             # Prefer Kafka record key; else payload; else derive from URL
#             watch_key = msg.key or doc.get("watch_key") or derive_watch_key_from_url(doc.get("url", "")) or ""

#             title = (doc.get("title") or "").strip()
#             tnorm = title.lower()
#             if not tnorm or tnorm in seen_titles:
#                 continue
#             seen_titles.add(tnorm)

#             # Clean and HARD cap summary to 100 words (defensive)
#             summary = truncate_words(clean_summary(doc.get("summary", "")), SUMMARY_WORD_LIMIT)

#             row = {
#                 "watch_key": watch_key,
#                 "first_mention": "YES" if doc.get("first_mention") else "NO",
#                 "id": doc.get("id", ""),
#                 "title": doc.get("title", ""),
#                 "summary": summary,
#                 "url": doc.get("url", ""),
#                 "source": doc.get("source", "Unknown"),
#                 "published_at": doc.get("published_at", ""),
#                 "category": doc.get("category", "General Technology"),
#                 "sentiment": doc.get("sentiment", "Neutral"),
#             }

#             writer.writerow(row)
#             count += 1
#             if count % 10 == 0:
#                 print(f"[csv-writer] wrote {count} rows to {CSV_FILE}")
#                 csvfile.flush()

#         print(f"[csv-writer] done. total rows={count}")
#         print(f"[csv-writer] Output saved to: {CSV_FILE}")

# if __name__ == "__main__":
#     main()














import os, json, csv, re
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC  = os.getenv("FILTERED_TOPIC", "news.filtered")  # IMPORTANT: filtered
CSV_FILE  = os.getenv("CSV_OUTPUT", "data/tech_news.csv")
GROUP_ID  = f"csv-writer-{os.getpid()}"
SUMMARY_WORD_LIMIT = int(os.getenv("SUMMARY_WORD_LIMIT", "100"))

def make_consumer():
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b is not None else None,
        consumer_timeout_ms=30000,
    )

def clean_summary(text: str) -> str:
    text = text or ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&[a-zA-Z]+;', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def truncate_words(text: str, max_words: int) -> str:
    words = (text or "").split()
    return " ".join(words[:max_words]) if len(words) > max_words else (text or "")

def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent: os.makedirs(parent, exist_ok=True)

def main():
    print(f"[csv-writer] {IN_TOPIC} → {CSV_FILE}")
    consumer = make_consumer()
    ensure_parent_dir(CSV_FILE)

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "watchname",
            "id","title","summary","url","source","published_at","category","sentiment"
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader()

        seen = set(); count = 0
        for msg in consumer:
            doc = msg.value

            # Watchname from payload (set by filter); fallback to Kafka key
            watchname = (doc.get("watchname") or msg.key or "").strip()
            if not watchname:
                # If this triggers, your writer is not reading news.filtered
                continue

            title = (doc.get("title") or "").strip()
            tkey = title.lower()
            if not tkey or tkey in seen: continue
            seen.add(tkey)

            summary = truncate_words(clean_summary(doc.get("summary", "")), SUMMARY_WORD_LIMIT)

            w.writerow({
                "watchname": watchname,
                "id": doc.get("id",""),
                "title": doc.get("title",""),
                "summary": summary,
                "url": doc.get("url",""),
                "source": doc.get("source","Unknown"),
                "published_at": doc.get("published_at",""),
                "category": doc.get("category","General Technology"),
                "sentiment": doc.get("sentiment","Negative"),
            })
            count += 1
            if count % 10 == 0:
                print(f"[csv-writer] wrote {count} rows to {CSV_FILE}")
                f.flush()

        print(f"[csv-writer] done. total rows={count}")
        print(f"[csv-writer] Output: {CSV_FILE}")

if __name__ == "__main__":
    main()
