# import os, json
# from datetime import datetime, timezone
# from typing import Dict, List, Union

# from kafka import KafkaConsumer, KafkaProducer
# from transformers import pipeline
# import spacy
# import nltk
# import requests
# from nltk.sentiment import SentimentIntensityAnalyzer

# # -------------------- CONFIG --------------------
# BROKER           = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC         = os.getenv("CLEAN_TOPIC", "news.cleaned")
# OUT_TOPIC        = os.getenv("ENRICHED_TOPIC", "news.enriched")
# SENTIMENT_MODEL  = os.getenv("SENTIMENT_MODEL", "sst2")  # "sst2" or "finbert"
# DEVICE           = -1  # CPU

# # -------------------- UTILS ---------------------
# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
#     if isinstance(preds, list) and preds and isinstance(preds[0], list):
#         return preds[0]
#     if isinstance(preds, list):
#         return preds
#     return [preds]

# # --- NEW TECHNOLOGY SUBCATEGORIES ---
# def category_from_text(text: str) -> str:
#     """
#     Rule-based technology subcategories.
#     """
#     t = text.lower()

#     if any(k in t for k in ["ai", "artificial intelligence", "machine learning", "deep learning", "neural network", "chatgpt", "openai", "llm"]):
#         return "AI / Machine Learning"
#     if any(k in t for k in ["cyber", "hack", "phishing", "data breach", "malware", "ransomware", "security flaw", "zero-day"]):
#         return "Cybersecurity"
#     if any(k in t for k in ["semiconductor", "chip", "nvidia", "intel", "amd", "gpu", "processor", "tpu"]):
#         return "Hardware / Semiconductors"
#     if any(k in t for k in ["startup", "funding", "acquisition", "ipo", "valuation", "venture capital"]):
#         return "Tech Business / Startups"
#     if any(k in t for k in ["social media", "facebook", "instagram", "twitter", "tiktok", "reddit", "youtube"]):
#         return "Social Media / Platforms"
#     if any(k in t for k in ["cloud", "aws", "azure", "gcp", "serverless", "infrastructure", "saas", "kubernetes"]):
#         return "Cloud / Infrastructure"
#     if any(k in t for k in ["robot", "autonomous", "self-driving", "tesla", "drone", "automation"]):
#         return "Robotics / Automation"
#     if any(k in t for k in ["quantum", "superconduct", "qubit"]):
#         return "Quantum Computing"
#     return "General Technology"

# def sentiment_to_score_sst2(scores: List[Dict]) -> float:
#     if not scores:
#         return 0.0
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     s = float(best.get("score") or 0.0)
#     return s if lab == "POSITIVE" else -s

# def sentiment_to_score_finbert(scores: List[Dict]) -> float:
#     if not scores:
#         return 0.0
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     s = float(best.get("score") or 0.0)
#     if lab == "POSITIVE":
#         return s
#     if lab == "NEGATIVE":
#         return -s
#     return 0.0

# def normalize_entities(hf_entities: List[Dict]) -> Dict[str, List[str]]:
#     out = {"ORG": [], "PERSON": [], "GPE": []}
#     for ent in hf_entities or []:
#         label = ent.get("entity_group", "")
#         text  = (ent.get("word") or "").strip()
#         if not text:
#             continue
#         if label in ("ORG",):
#             bucket = "ORG"
#         elif label in ("PER", "PERSON"):
#             bucket = "PERSON"
#         elif label in ("LOC",):
#             bucket = "GPE"
#         else:
#             continue
#         if text not in out[bucket]:
#             out[bucket].append(text)
#     return out

# def sentiment_label_from_scores_sst2(scores: List[Dict]) -> str:
#     if not scores:
#         return "Neutral"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     return "Positive" if lab == "POSITIVE" else "Negative"

# def sentiment_label_from_scores_finbert(scores: List[Dict]) -> str:
#     if not scores:
#         return "Neutral"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     if lab == "POSITIVE":
#         return "Positive"
#     if lab == "NEGATIVE":
#         return "Negative"
#     return "Neutral"

# # -------------------- LOADERS -------------------
# def load_sentiment_pipe():
#     if SENTIMENT_MODEL.lower() == "finbert":
#         return pipeline(
#             "text-classification",
#             model="ProsusAI/finbert",
#             device=DEVICE,
#             top_k=None
#         )
#     else:
#         return pipeline(
#             "text-classification",
#             model="distilbert-base-uncased-finetuned-sst-2-english",
#             device=DEVICE,
#             top_k=None
#         )

# def load_ner_pipe():
#     return pipeline(
#         "token-classification",
#         model="dslim/bert-base-NER",
#         aggregation_strategy="simple",
#         device=DEVICE
#     )

# def load_spacy():
#     try:
#         return spacy.load("en_core_web_sm")
#     except Exception:
#         from spacy.cli import download
#         download("en_core_web_sm")
#         return spacy.load("en_core_web_sm")

# def load_vader():
#     try:
#         nltk.data.find("sentiment/vader_lexicon.zip")
#     except LookupError:
#         nltk.download("vader_lexicon")
#     return SentimentIntensityAnalyzer()

# # -------------------- GROQ SUMMARIZER --------------------
# GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# def summarize_with_groq(title: str, summary: str) -> str:
#     if not GROQ_API_KEY:
#         return summary
#     prompt = (
#         "Summarize this tech news in one concise sentence (under 25 words). "
#         "Avoid hype and keep only factual key details.\n\n"
#         f"Title: {title}\nSummary: {summary}\n"
#     )
#     try:
#         response = requests.post(
#             "https://api.groq.com/openai/v1/chat/completions",
#             headers={
#                 "Authorization": f"Bearer {GROQ_API_KEY}",
#                 "Content-Type": "application/json",
#             },
#             json={
#                 "model": "llama-3.1-8b-instant",
#                 "messages": [{"role": "user", "content": prompt}],
#                 "temperature": 0.3,
#             },
#             timeout=15,
#         )
#         response.raise_for_status()
#         data = response.json()
#         return (data["choices"][0]["message"]["content"] or "").strip()
#     except Exception as e:
#         print(f"[groq-summary] error: {e}")
#         return summary

# # -------------------- KAFKA ---------------------
# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="enricher-service",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#         max_poll_records=100,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# # -------------------- MAIN ----------------------
# def main():
#     print(f"[enricher] broker={BROKER} {IN_TOPIC} → {OUT_TOPIC} (sentiment={SENTIMENT_MODEL})")

#     consumer = make_consumer()
#     producer = make_producer()

#     sent_pipe = load_sentiment_pipe()
#     ner_pipe  = load_ner_pipe()
#     _nlp      = load_spacy()
#     _vader    = load_vader()

#     count = 0
#     for msg in consumer:
#         doc = msg.value
#         title   = (doc.get("title") or "").strip()
#         summary = (doc.get("summary") or "").strip()

#         # summary = summarize_with_groq(title, summary)
#         # text = (f"{title}. {summary}").strip()[:4000]


#         summary = summarize_with_groq(title, summary)
#         text = (f"{title}. {summary}").strip()

#         # ensure input is within model token/character limit (~512 tokens ≈ 1000–1200 chars)
#         MAX_LEN = 1000
#         if len(text) > MAX_LEN:
#             text = text[:MAX_LEN]

#         # NEW TECH SUBCATEGORY CLASSIFICATION
#         category = category_from_text(text)

#         s_raw    = sent_pipe(text)
#         s_scores = unwrap_batch(s_raw)
#         if SENTIMENT_MODEL.lower() == "finbert":
#             sentiment_label = sentiment_label_from_scores_finbert(s_scores)
#         else:
#             sentiment_label = sentiment_label_from_scores_sst2(s_scores)

#         ner_raw   = ner_pipe(text)
#         ner_flat  = unwrap_batch(ner_raw)
#         entities  = normalize_entities(ner_flat)

#         out = {
#             **doc,
#             "category": category,
#             "sentiment": sentiment_label,
#             "entities": entities,
#             "enriched_at": iso_now(),
#         }

#         producer.send(OUT_TOPIC, value=out)
#         count += 1
#         if count % 20 == 0:
#             print(f"[enricher] sent {count} → {OUT_TOPIC}")

#     producer.flush()
#     print(f"[enricher] done. total sent={count}")

# if __name__ == "__main__":
#     main()












# import os, json, requests, nltk, spacy
# from bs4 import BeautifulSoup
# from datetime import datetime, timezone
# from typing import Dict, List, Union
# from kafka import KafkaConsumer, KafkaProducer
# from transformers import pipeline
# from nltk.sentiment import SentimentIntensityAnalyzer
# from dotenv import load_dotenv

# # -------------------- LOAD ENV --------------------
# load_dotenv()

# # -------------------- CONFIG --------------------
# BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC = os.getenv("CLEAN_TOPIC", "news.cleaned")
# OUT_TOPIC = os.getenv("ENRICHED_TOPIC", "news.enriched")
# SENTIMENT_MODEL = os.getenv("SENTIMENT_MODEL", "sst2")
# DEVICE = -1
# GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# # -------------------- UTILITIES --------------------
# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
#     if isinstance(preds, list) and preds and isinstance(preds[0], list):
#         return preds[0]
#     if isinstance(preds, list):
#         return preds
#     return [preds]

# # -------------------- FETCH ARTICLE TEXT --------------------
# def fetch_article_text(url: str) -> str:
#     """Fetch full article content using BeautifulSoup."""
#     try:
#         headers = {"User-Agent": "Mozilla/5.0"}
#         resp = requests.get(url, timeout=10, headers=headers)
#         resp.raise_for_status()
#         soup = BeautifulSoup(resp.text, "html.parser")
#         for tag in soup(["script", "style", "noscript"]):
#             tag.decompose()
#         text = " ".join(p.get_text(strip=True) for p in soup.find_all("p"))
#         print(f"[fetch-html] fetched {len(text)} chars from {url}")
#         return text.strip()[:7000]
#     except Exception as e:
#         print(f"[fetch-html] failed for {url}: {e}")
#         return ""

# # -------------------- CATEGORY DETECTION --------------------
# def category_from_text(text: str) -> str:
#     t = text.lower()
#     if any(k in t for k in ["ai", "machine learning", "neural network", "chatgpt", "openai", "llm"]):
#         return "AI / Machine Learning"
#     if any(k in t for k in ["cyber", "ransomware", "security", "breach", "malware"]):
#         return "Cybersecurity"
#     if any(k in t for k in ["semiconductor", "chip", "nvidia", "intel", "gpu", "processor"]):
#         return "Hardware / Semiconductors"
#     if any(k in t for k in ["startup", "funding", "acquisition", "ipo", "venture capital"]):
#         return "Tech Business / Startups"
#     if any(k in t for k in ["social media", "facebook", "instagram", "twitter", "tiktok", "youtube"]):
#         return "Social Media / Platforms"
#     if any(k in t for k in ["cloud", "aws", "azure", "gcp", "kubernetes", "infrastructure"]):
#         return "Cloud / Infrastructure"
#     if any(k in t for k in ["robot", "autonomous", "self-driving", "drone", "automation"]):
#         return "Robotics / Automation"
#     if any(k in t for k in ["quantum", "superconduct", "qubit"]):
#         return "Quantum Computing"
#     return "General Technology"

# # -------------------- SENTIMENT --------------------
# def sentiment_label_from_scores(scores: List[Dict]) -> str:
#     if not scores:
#         return "Neutral"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     label = (best.get("label") or "").upper()
#     if label == "POSITIVE":
#         return "Positive"
#     if label == "NEGATIVE":
#         return "Negative"
#     return "Neutral"

# # -------------------- LOAD MODELS --------------------
# def load_sentiment_pipe():
#     if SENTIMENT_MODEL.lower() == "finbert":
#         return pipeline("text-classification", model="ProsusAI/finbert", device=DEVICE, top_k=None)
#     else:
#         return pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english", device=DEVICE, top_k=None)

# def load_ner_pipe():
#     return pipeline("token-classification", model="dslim/bert-base-NER", aggregation_strategy="simple", device=DEVICE)

# def load_spacy():
#     try:
#         return spacy.load("en_core_web_sm")
#     except Exception:
#         from spacy.cli import download
#         download("en_core_web_sm")
#         return spacy.load("en_core_web_sm")

# def load_vader():
#     try:
#         nltk.data.find("sentiment/vader_lexicon.zip")
#     except LookupError:
#         nltk.download("vader_lexicon")
#     return SentimentIntensityAnalyzer()

# # -------------------- SUMMARIZATION --------------------
# summarizer_local = pipeline("summarization", model="facebook/bart-large-cnn")

# def summarize_with_groq(title: str, text: str) -> str:
#     """Summarize using Groq API, fallback to local BERT if Groq fails."""
#     if not text or len(text.split()) < 50:
#         return title
#     prompt = (
#         "Summarize this technology news article in 3–5 factual, concise sentences. "
#         "Do not mention that you cannot access or see the article.\n\n"
#         f"Title: {title}\nContent: {text[:6000]}\n"
#     )
#     try:
#         response = requests.post(
#             "https://api.groq.com/openai/v1/chat/completions",
#             headers={
#                 "Authorization": f"Bearer {GROQ_API_KEY}",
#                 "Content-Type": "application/json",
#             },
#             json={
#                 "model": "llama-3.1-8b-instant",
#                 "messages": [{"role": "user", "content": prompt}],
#                 "temperature": 0.3,
#             },
#             timeout=30,
#         )
#         response.raise_for_status()
#         data = response.json()
#         summary = (data["choices"][0]["message"]["content"] or "").strip()
#         # Avoid useless replies
#         if "unable to find" in summary.lower() or "provide the article" in summary.lower():
#             raise ValueError("Generic summary detected, falling back")
#         return summary.replace("\n", " ").strip()
#     except Exception as e:
#         print(f"[groq-summary] Groq failed, fallback: {e}")
#         local = summarizer_local(text[:4000], max_length=250, min_length=80, do_sample=False)
#         return local[0]["summary_text"].replace("\n", " ").strip()

# # -------------------- KAFKA --------------------
# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="enricher-service",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# # -------------------- MAIN --------------------
# def main():
#     print(f"[enricher] broker={BROKER} {IN_TOPIC} → {OUT_TOPIC}")
#     consumer = make_consumer()
#     producer = make_producer()
#     sent_pipe = load_sentiment_pipe()
#     ner_pipe = load_ner_pipe()
#     _nlp = load_spacy()
#     _vader = load_vader()

#     seen_ids = set()
#     count = 0

#     for msg in consumer:
#         doc = msg.value
#         uid = doc.get("id") or doc.get("url")
#         if uid in seen_ids:
#             continue
#         seen_ids.add(uid)

#         title = (doc.get("title") or "").strip()
#         url = (doc.get("url") or "").strip()
#         summary = (doc.get("summary") or "").strip()

#         article_text = fetch_article_text(url)
#         if not article_text or len(article_text) < 300:
#             print(f"[skip] Empty or blocked article: {url}")
#             continue

#         final_summary = summarize_with_groq(title, article_text)
#         category = category_from_text(article_text)
#         s_raw = sent_pipe(article_text)
#         s_scores = unwrap_batch(s_raw)
#         sentiment_label = sentiment_label_from_scores(s_scores)

#         out = {
#             **doc,
#             "summary": final_summary,
#             "category": category,
#             "sentiment": sentiment_label,
#             "enriched_at": iso_now(),
#         }

#         producer.send(OUT_TOPIC, value=out)
#         count += 1
#         if count % 20 == 0:
#             print(f"[enricher] sent {count} → {OUT_TOPIC}")

#     producer.flush()
#     print(f"[enricher] done. total sent={count}")

# if __name__ == "__main__":
#     main()












# import os, json
# from datetime import datetime, timezone
# from typing import Dict, List, Union

# from kafka import KafkaConsumer, KafkaProducer
# from transformers import pipeline
# import spacy
# import nltk
# import requests
# from nltk.sentiment import SentimentIntensityAnalyzer
# from dotenv import load_dotenv

# # -------------------- LOAD ENV --------------------
# load_dotenv()  # Load environment variables from .env file

# # -------------------- CONFIG --------------------
# BROKER           = os.getenv("KAFKA_BROKER", "localhost:9092")
# IN_TOPIC         = os.getenv("CLEAN_TOPIC", "news.cleaned")
# OUT_TOPIC        = os.getenv("ENRICHED_TOPIC", "news.enriched")
# SENTIMENT_MODEL  = os.getenv("SENTIMENT_MODEL", "sst2")
# DEVICE           = -1  # CPU

# # -------------------- UTILS ---------------------
# def iso_now() -> str:
#     return datetime.now(timezone.utc).isoformat()

# def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
#     if isinstance(preds, list) and preds and isinstance(preds[0], list):
#         return preds[0]
#     if isinstance(preds, list):
#         return preds
#     return [preds]

# # --- NEW TECHNOLOGY SUBCATEGORIES ---
# def category_from_text(text: str) -> str:
#     """
#     Rule-based technology subcategories.
#     """
#     t = text.lower()

#     if any(k in t for k in ["ai", "artificial intelligence", "machine learning", "deep learning", "neural network", "chatgpt", "openai", "llm"]):
#         return "AI / Machine Learning"
#     if any(k in t for k in ["cyber", "hack", "phishing", "data breach", "malware", "ransomware", "security flaw", "zero-day"]):
#         return "Cybersecurity"
#     if any(k in t for k in ["semiconductor", "chip", "nvidia", "intel", "amd", "gpu", "processor", "tpu"]):
#         return "Hardware / Semiconductors"
#     if any(k in t for k in ["startup", "funding", "acquisition", "ipo", "valuation", "venture capital"]):
#         return "Tech Business / Startups"
#     if any(k in t for k in ["social media", "facebook", "instagram", "twitter", "tiktok", "reddit", "youtube"]):
#         return "Social Media / Platforms"
#     if any(k in t for k in ["cloud", "aws", "azure", "gcp", "serverless", "infrastructure", "saas", "kubernetes"]):
#         return "Cloud / Infrastructure"
#     if any(k in t for k in ["robot", "autonomous", "self-driving", "tesla", "drone", "automation"]):
#         return "Robotics / Automation"
#     if any(k in t for k in ["quantum", "superconduct", "qubit"]):
#         return "Quantum Computing"
#     return "General Technology"

# def sentiment_to_score_sst2(scores: List[Dict]) -> float:
#     if not scores:
#         return 0.0
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     s = float(best.get("score") or 0.0)
#     return s if lab == "POSITIVE" else -s

# def sentiment_to_score_finbert(scores: List[Dict]) -> float:
#     if not scores:
#         return 0.0
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     s = float(best.get("score") or 0.0)
#     if lab == "POSITIVE":
#         return s
#     if lab == "NEGATIVE":
#         return -s
#     return 0.0

# def normalize_entities(hf_entities: List[Dict]) -> Dict[str, List[str]]:
#     out = {"ORG": [], "PERSON": [], "GPE": []}
#     for ent in hf_entities or []:
#         label = ent.get("entity_group", "")
#         text  = (ent.get("word") or "").strip()
#         if not text:
#             continue
#         if label in ("ORG",):
#             bucket = "ORG"
#         elif label in ("PER", "PERSON"):
#             bucket = "PERSON"
#         elif label in ("LOC",):
#             bucket = "GPE"
#         else:
#             continue
#         if text not in out[bucket]:
#             out[bucket].append(text)
#     return out

# def sentiment_label_from_scores_sst2(scores: List[Dict]) -> str:
#     if not scores:
#         return "Neutral"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     return "Positive" if lab == "POSITIVE" else "Negative"

# def sentiment_label_from_scores_finbert(scores: List[Dict]) -> str:
#     if not scores:
#         return "Neutral"
#     best = max(scores, key=lambda x: x.get("score", 0.0))
#     lab = (best.get("label") or "").upper()
#     if lab == "POSITIVE":
#         return "Positive"
#     if lab == "NEGATIVE":
#         return "Negative"
#     return "Neutral"

# # -------------------- LOADERS -------------------
# def load_sentiment_pipe():
#     if SENTIMENT_MODEL.lower() == "finbert":
#         return pipeline(
#             "text-classification",
#             model="ProsusAI/finbert",
#             device=DEVICE,
#             top_k=None
#         )
#     else:
#         return pipeline(
#             "text-classification",
#             model="distilbert-base-uncased-finetuned-sst-2-english",
#             device=DEVICE,
#             top_k=None
#         )

# def load_ner_pipe():
#     return pipeline(
#         "token-classification",
#         model="dslim/bert-base-NER",
#         aggregation_strategy="simple",
#         device=DEVICE
#     )

# def load_spacy():
#     try:
#         return spacy.load("en_core_web_sm")
#     except Exception:
#         from spacy.cli import download
#         download("en_core_web_sm")
#         return spacy.load("en_core_web_sm")

# def load_vader():
#     try:
#         nltk.data.find("sentiment/vader_lexicon.zip")
#     except LookupError:
#         nltk.download("vader_lexicon")
#     return SentimentIntensityAnalyzer()

# # -------------------- GROQ SUMMARIZER --------------------
# GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# # Debug: Print API key status on startup
# if GROQ_API_KEY:
#     print(f"[groq-summary] API key loaded: {GROQ_API_KEY[:10]}...")
# else:
#     print("[groq-summary] WARNING: No GROQ_API_KEY found in environment!")

# def summarize_with_groq(title: str, original_summary: str) -> str:
#     """
#     Generate a NEW concise summary different from the title using Groq API.
#     """
#     if not GROQ_API_KEY:
#         print("[groq-summary] No API key, returning original summary")
#         return original_summary
    
#     # Create a better prompt that ensures different output
#     prompt = (
#         "You are a tech news summarizer. Create a concise 2-3 sentence summary "
#         "that explains what happened in this news story. The summary MUST be different "
#         "from the title and provide additional context or details.\n\n"
#         f"Title: {title}\n"
#         f"Original Summary: {original_summary}\n\n"
#         "Write a NEW summary (2-3 sentences, under 60 words) that adds value beyond the title:"
#     )
    
#     try:
#         response = requests.post(
#             "https://api.groq.com/openai/v1/chat/completions",
#             headers={
#                 "Authorization": f"Bearer {GROQ_API_KEY}",
#                 "Content-Type": "application/json",
#             },
#             json={
#                 "model": "llama-3.1-8b-instant",
#                 "messages": [{"role": "user", "content": prompt}],
#                 "temperature": 0.5,  # Increased for more variation
#                 "max_tokens": 150,
#             },
#             timeout=15,
#         )
#         response.raise_for_status()
#         data = response.json()
#         new_summary = (data["choices"][0]["message"]["content"] or "").strip()
        
#         # Verify the summary is different from title
#         if new_summary and new_summary.lower() != title.lower():
#             print(f"[groq-summary] Generated new summary: {new_summary[:50]}...")
#             return new_summary
#         else:
#             print(f"[groq-summary] Summary too similar, using original")
#             return original_summary
            
#     except Exception as e:
#         print(f"[groq-summary] error: {e}")
#         return original_summary

# # -------------------- KAFKA ---------------------
# def make_consumer():
#     return KafkaConsumer(
#         IN_TOPIC,
#         bootstrap_servers=[BROKER],
#         group_id="enricher-service",
#         auto_offset_reset="earliest",
#         enable_auto_commit=True,
#         value_deserializer=lambda b: json.loads(b.decode("utf-8")),
#         consumer_timeout_ms=30000,
#         max_poll_records=100,
#     )

# def make_producer():
#     return KafkaProducer(
#         bootstrap_servers=[BROKER],
#         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         acks="all",
#         linger_ms=50,
#         retries=5,
#     )

# # -------------------- MAIN ----------------------
# def main():
#     print(f"[enricher] broker={BROKER} {IN_TOPIC} → {OUT_TOPIC} (sentiment={SENTIMENT_MODEL})")
#     print(f"[enricher] Waiting for messages from {IN_TOPIC}...")

#     consumer = make_consumer()
#     producer = make_producer()

#     sent_pipe = load_sentiment_pipe()
#     ner_pipe  = load_ner_pipe()
#     _nlp      = load_spacy()
#     _vader    = load_vader()

#     # Track seen articles to prevent duplicates (using title as key)
#     seen_titles = set()
#     count = 0
#     skipped = 0
    
#     for msg in consumer:
#         doc = msg.value
        
#         # Use title as the primary deduplication key (case-insensitive)
#         title = (doc.get("title") or "").strip()
#         title_normalized = title.lower().strip()
        
#         # Skip if we've already seen this exact title
#         if title_normalized in seen_titles:
#             skipped += 1
#             if skipped % 10 == 0:
#                 print(f"[enricher] skipped {skipped} duplicate titles")
#             continue
        
#         seen_titles.add(title_normalized)
        
#         title   = (doc.get("title") or "").strip()
#         summary = (doc.get("summary") or "").strip()

#         # Generate NEW summary using Groq (not commented out anymore!)
#         enhanced_summary = summarize_with_groq(title, summary)
        
#         # Use enhanced summary + title for analysis
#         text = (f"{title}. {enhanced_summary}").strip()

#         # Ensure input is within model token/character limit
#         MAX_LEN = 1000
#         if len(text) > MAX_LEN:
#             text = text[:MAX_LEN]

#         # NEW TECH SUBCATEGORY CLASSIFICATION
#         category = category_from_text(text)

#         s_raw    = sent_pipe(text)
#         s_scores = unwrap_batch(s_raw)
#         if SENTIMENT_MODEL.lower() == "finbert":
#             sentiment_label = sentiment_label_from_scores_finbert(s_scores)
#         else:
#             sentiment_label = sentiment_label_from_scores_sst2(s_scores)

#         ner_raw   = ner_pipe(text)
#         ner_flat  = unwrap_batch(ner_raw)
#         entities  = normalize_entities(ner_flat)

#         out = {
#             **doc,
#             "summary": enhanced_summary,  # Use the NEW summary
#             "category": category,
#             "sentiment": sentiment_label,
#             "entities": entities,
#             "enriched_at": iso_now(),
#         }

#         producer.send(OUT_TOPIC, key=msg.key, value=out)
#         count += 1
#         if count % 20 == 0:
#             print(f"[enricher] sent {count} → {OUT_TOPIC} (skipped {skipped} dupes)")

#     producer.flush()
#     print(f"[enricher] done. total sent={count}, duplicates skipped={skipped}")

# if __name__ == "__main__":
#     main()
























import os, json
from datetime import datetime, timezone
from typing import Dict, List, Union

from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
import spacy
import nltk
import requests
from nltk.sentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()

BROKER             = os.getenv("KAFKA_BROKER", "localhost:9092")
IN_TOPIC           = os.getenv("CLEAN_TOPIC", "news.cleaned")
OUT_TOPIC          = os.getenv("ENRICHED_TOPIC", "news.enriched")
SENTIMENT_MODEL    = os.getenv("SENTIMENT_MODEL", "sst2")
DEVICE             = -1
SUMMARY_WORD_LIMIT = int(os.getenv("SUMMARY_WORD_LIMIT", "100"))
GROUP_ID           = f"enricher-{int(datetime.now().timestamp())}"

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def unwrap_batch(preds: Union[List, Dict]) -> List[Dict]:
    if isinstance(preds, list) and preds and isinstance(preds[0], list): return preds[0]
    if isinstance(preds, list): return preds
    return [preds]

def truncate_words(text: str, max_words: int) -> str:
    words = (text or "").split()
    return " ".join(words[:max_words]) if len(words) > max_words else (text or "")

def category_from_text(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["ai","artificial intelligence","machine learning","deep learning","neural network","chatgpt","openai","llm"]): return "AI / Machine Learning"
    if any(k in t for k in ["cyber","hack","phishing","data breach","malware","ransomware","security flaw","zero-day"]): return "Cybersecurity"
    if any(k in t for k in ["semiconductor","chip","nvidia","intel","amd","gpu","processor","tpu"]): return "Hardware / Semiconductors"
    if any(k in t for k in ["startup","funding","acquisition","ipo","valuation","venture capital"]): return "Tech Business / Startups"
    if any(k in t for k in ["social media","facebook","instagram","twitter","tiktok","reddit","youtube"]): return "Social Media / Platforms"
    if any(k in t for k in ["cloud","aws","azure","gcp","serverless","infrastructure","saas","kubernetes"]): return "Cloud / Infrastructure"
    if any(k in t for k in ["robot","autonomous","self-driving","tesla","drone","automation"]): return "Robotics / Automation"
    if any(k in t for k in ["quantum","superconduct","qubit"]): return "Quantum Computing"
    return "General Technology"

def normalize_entities(hf_entities: List[Dict]) -> Dict[str, List[str]]:
    out = {"ORG": [], "PERSON": [], "GPE": []}
    for ent in hf_entities or []:
        label = ent.get("entity_group", "")
        text  = (ent.get("word") or "").strip()
        if not text: continue
        if label in ("ORG",): bucket = "ORG"
        elif label in ("PER","PERSON"): bucket = "PERSON"
        elif label in ("LOC",): bucket = "GPE"
        else: continue
        if text not in out[bucket]: out[bucket].append(text)
    return out

def sentiment_to_posneg_label(scores: List[Dict]) -> str:
    if not scores: return "Negative"
    best = max(scores, key=lambda x: x.get("score", 0.0))
    lab = (best.get("label") or "").upper()
    if "POS" in lab or lab.endswith("1") or lab == "LABEL_1" or "YES" in lab: return "Positive"
    return "Negative"

def load_sentiment_pipe():
    if SENTIMENT_MODEL.lower() == "finbert":
        return pipeline("text-classification", model="ProsusAI/finbert", device=DEVICE, top_k=None)
    else:
        return pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english", device=DEVICE, top_k=None)

def load_ner_pipe():
    return pipeline("token-classification", model="dslim/bert-base-NER", aggregation_strategy="simple", device=DEVICE)

def load_spacy():
    try: return spacy.load("en_core_web_sm")
    except Exception:
        from spacy.cli import download
        download("en_core_web_sm")
        return spacy.load("en_core_web_sm")

def load_vader():
    try: nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError: nltk.download("vader_lexicon")
    return SentimentIntensityAnalyzer()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
def summarize_with_groq(title: str, original_summary: str, max_words: int) -> str:
    base = (original_summary or "").strip()
    if not GROQ_API_KEY: return truncate_words(base, max_words)
    prompt = (
        f"Create a concise summary under {max_words} words, different from the title, adding helpful context.\n\n"
        f"Title: {title}\nOriginal Summary: {original_summary}\n\nNEW summary (<={max_words} words):"
    )
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.1-8b-instant", "messages": [{"role":"user","content":prompt}], "temperature":0.5, "max_tokens":220},
            timeout=15,
        )
        resp.raise_for_status()
        txt = (resp.json()["choices"][0]["message"]["content"] or "").strip()
        return truncate_words(txt, max_words) or truncate_words(base, max_words)
    except Exception:
        return truncate_words(base, max_words)

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
        max_poll_records=100,
    )

def make_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        acks="all",
        linger_ms=50,
        retries=5,
    )

def main():
    print(f"[enricher] {IN_TOPIC} → {OUT_TOPIC}")
    consumer, producer = make_consumer(), make_producer()
    sent_pipe, ner_pipe = load_sentiment_pipe(), load_ner_pipe()
    _ = load_spacy(); _ = load_vader()

    seen = set(); count = 0
    for msg in consumer:
        doc, in_key = msg.value, msg.key
        title = (doc.get("title") or "").strip()
        if not title or title.lower() in seen: continue
        seen.add(title.lower())

        enhanced_summary = summarize_with_groq(title, (doc.get("summary") or ""), SUMMARY_WORD_LIMIT)
        text = (f"{title}. {enhanced_summary}")[:2000]
        category = category_from_text(text)

        s_scores = unwrap_batch(sent_pipe(text))
        sentiment_label = sentiment_to_posneg_label(s_scores)

        ner_flat  = unwrap_batch(ner_pipe(text))
        entities  = normalize_entities(ner_flat)

        out_key = in_key or (doc.get("watch_key") or "") or "unknown-source"
        out = {**doc,
            "summary": enhanced_summary,
            "category": category,
            "sentiment": sentiment_label,
            "entities": entities,
            "enriched_at": iso_now(),
            "watch_key": out_key,
        }
        producer.send(OUT_TOPIC, key=out_key, value=out)
        count += 1
        if count % 20 == 0: print(f"[enricher] sent {count}")
    producer.flush(); print(f"[enricher] done. total sent={count}")

if __name__ == "__main__":
    main()
