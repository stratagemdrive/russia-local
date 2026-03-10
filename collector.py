# -*- coding: utf-8 -*-
"""
Russian Media Tracker — Collector + Translator + Topic Splitter (48h rolling)

Targets:
- Meduza
- RT
- Russia Beyond
- TASS
- The Moscow Times

Outputs:
- data/articles_latest.json
- data/articles_diplomacy.json
- data/articles_military.json
- data/articles_energy.json
- data/articles_economy.json
- data/articles_local_events.json

Behavior:
- Keeps only last 48 hours
- Translates all text fields into English
- Classifies articles into topic buckets
- Keeps ONLY articles that explicitly mention Russia/Russian
  in English or Russian roots
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import requests
import feedparser
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

# ================= CONFIG =================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.7",
    "Connection": "keep-alive",
}

TIMEOUT = 20
MAX_RETRIES = 3
RETRY_SLEEP = 2.0

KEEP_HOURS = int(os.getenv("KEEP_HOURS", "48"))
OUT_JSON = Path(os.getenv("OUT_JSON", "data/articles_latest.json"))

SOURCES: Dict[str, Dict] = {
    "TASS (EN)": {
        "feeds": ["https://tass.com/rss/v2.xml"],
    },
    "TASS (RU)": {
        "feeds": ["https://tass.ru/rss/v2.xml"],
    },
    "RT": {
        "feeds": ["https://www.rt.com/rss/news/"],
    },
    "Meduza (EN)": {
        "feeds": [
            "https://meduza.io/rss/en/all",
            "https://meduza.io/rss/en/news",
            "https://meduza.io/rss/all",
            "https://meduza.io/rss/news",
        ],
    },
    "Russia Beyond": {
        "feeds": ["https://www.rbth.com/rss"],
    },
    "The Moscow Times": {
        "feeds": [
            "https://www.themoscowtimes.com/rss/news",
            "https://www.themoscowtimes.com/rss",
            "https://www.themoscowtimes.com/page/rss",
        ],
    },
}

TOPIC_KEYWORDS = {
    "diplomacy": [
        "foreign ministry", "ministry of foreign affairs", "mfa",
        "diplomacy", "diplomatic", "talks", "negotiations", "meeting",
        "summit", "delegation", "envoy", "embassy", "ambassador",
        "bilateral", "multilateral", "agreement", "treaty",
        "strategic partnership", "joint statement", "consultations",
        "мид", "дипломат", "переговор", "встреч", "саммит", "делегац",
        "посол", "соглашен", "договор",
    ],
    "military": [
        "defense ministry", "ministry of defense", "military",
        "armed forces", "troops", "exercise", "drills", "deployment",
        "missile", "air defense", "navy", "fleet", "submarine",
        "weapons", "arms", "defense industry", "security",
        "минобороны", "военн", "войск", "учени", "маневр",
        "ракет", "пво", "флот", "оруж",
    ],
    "energy": [
        "energy", "oil", "gas", "lng", "pipeline", "gazprom",
        "rosneft", "novatek", "opec", "opec+", "refinery",
        "electricity", "power grid", "nuclear power",
        "coal", "fuel", "energy exports", "petroleum",
        "энерг", "нефт", "газ", "спг", "газпром", "роснефт",
        "новатэк", "опек", "атомн", "топлив",
    ],
    "economy": [
        "economy", "economic", "gdp", "inflation", "interest rate",
        "central bank", "trade", "exports", "imports", "industry",
        "manufacturing", "investment", "budget", "deficit",
        "banking", "ruble", "sanctions", "market", "employment",
        "эконом", "ввп", "инфляц", "центробанк", "торгов",
        "экспорт", "импорт", "промышлен", "инвестиц", "бюджет",
        "банк", "рубл", "санкц", "рын",
    ],
    "local_events": [
        "fire", "flood", "earthquake", "storm", "wildfire",
        "explosion", "accident", "crash", "evacuation",
        "emergency", "disaster", "landslide", "outage",
        "collapse", "rescue", "injured", "killed",
        "пожар", "наводнен", "землетрясен", "шторм",
        "взрыв", "авари", "крушен", "эвакуац",
        "чс", "чрезвычайн", "бедств", "спасател",
    ],
}

# Strict Russia gate:
# Keep only articles that explicitly mention Russia/Russian
# in English or Russian.
RUSSIA_PATTERNS = [
    re.compile(r"\brussi\w*\b", re.IGNORECASE),  # Russia, Russian, Russians
    re.compile(r"росси\w*", re.IGNORECASE),      # Россия, российский, россияне...
    re.compile(r"русск\w*", re.IGNORECASE),      # русский, русские...
]

# ============ OPTIONAL TRANSLATION ============

try:
    from deep_translator import GoogleTranslator
    HAS_TRANSLATOR = True
except Exception:
    HAS_TRANSLATOR = False

# ================= HELPERS =================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fetch_text(url: str) -> Optional[str]:
    sess = requests.Session()
    sess.headers.update(HEADERS)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = sess.get(url, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and r.text:
                return r.text
            print(f"[WARN] HTTP {r.status_code} from {url}")
        except requests.RequestException as e:
            print(f"[WARN] Fetch error ({attempt}/{MAX_RETRIES}) for {url}: {e}")
        time.sleep(RETRY_SLEEP * attempt)

    return None


def _parse_entry_datetime(entry: dict) -> Optional[datetime]:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        st = entry.get(key)
        if st:
            try:
                return datetime(*st[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    for key in ("published", "updated", "created", "pubDate"):
        val = entry.get(key)
        if val:
            try:
                return pd.to_datetime(val, utc=True, errors="raise").to_pydatetime()
            except Exception:
                continue

    return None


def normalize_space(s: str) -> str:
    return " ".join((s or "").split()).strip()


def two_sentence_lead(text: str) -> str:
    t = normalize_space(re.sub(r"<[^>]+>", " ", text or ""))
    if not t:
        return ""

    parts = re.split(r"(?<=[.!?])\s+", t)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}".strip()
    return t[:280].rstrip()


def translate_text(text: str) -> str:
    if not text:
        return ""
    if not HAS_TRANSLATOR:
        return text
    try:
        return GoogleTranslator(source="auto", target="en").translate(text)
    except Exception:
        return text


def hash_key(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()


def within_hours(dt_utc: datetime, now_utc: datetime, keep_hours: int) -> bool:
    return dt_utc >= (now_utc - timedelta(hours=keep_hours))


def mentions_russia(text: str) -> bool:
    txt = normalize_space((text or "").lower())
    if not txt:
        return False
    return any(pattern.search(txt) for pattern in RUSSIA_PATTERNS)


def article_is_russia_related(article: dict) -> bool:
    # Check raw fields first so Russian-language matches are preserved.
    raw_text = " ".join([
        article.get("title_raw", ""),
        article.get("lead_raw", ""),
    ])

    if mentions_russia(raw_text):
        return True

    # Fallback: also check translated English fields.
    en_text = " ".join([
        article.get("title_en", ""),
        article.get("lead_en", ""),
    ])

    return mentions_russia(en_text)


def classify_topics(article: dict) -> dict:
    txt = f"{article.get('title_raw', '')} {article.get('lead_raw', '')}".lower()

    scores = {}
    for topic, keywords in TOPIC_KEYWORDS.items():
        scores[topic] = sum(1 for kw in keywords if kw in txt)

    labels = []

    if scores["diplomacy"] >= 1:
        labels.append("diplomacy")
    if scores["military"] >= 1:
        labels.append("military")
    if scores["energy"] >= 1:
        labels.append("energy")
    if scores["economy"] >= 2:
        labels.append("economy")
    if scores["local_events"] >= 2:
        labels.append("local_events")

    primary = None
    if any(v > 0 for v in scores.values()):
        primary = max(scores, key=scores.get)

    article["topic_scores"] = scores
    article["topics"] = labels
    article["primary_topic"] = primary
    return article

# ================= RSS COLLECTOR =================

def collect_from_rss(source_name: str, feeds: List[str]) -> List[dict]:
    rows: List[dict] = []

    for feed_url in feeds:
        txt = fetch_text(feed_url)
        if not txt:
            continue

        d = feedparser.parse(txt)

        for e in d.entries:
            title = normalize_space(e.get("title", ""))
            url = normalize_space(e.get("link", ""))
            summary = e.get("summary", "") or e.get("description", "") or ""

            dt = _parse_entry_datetime(e)
            if not dt:
                continue

            rows.append({
                "source": source_name,
                "url": url,
                "published_utc": dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title_raw": title,
                "lead_raw": two_sentence_lead(summary),
            })

        if rows:
            break

    return rows

# ================= STORE LOGIC =================

def load_existing_json(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("articles"), list):
            return data["articles"]
        if isinstance(data, list):
            return data
    except Exception as e:
        print(f"[WARN] Failed loading existing JSON: {e}")
    return []


def prune_and_dedupe(rows: List[dict]) -> List[dict]:
    now = _utcnow()
    kept: List[dict] = []

    for r in rows:
        try:
            dt = pd.to_datetime(r.get("published_utc"), utc=True, errors="raise").to_pydatetime()
        except Exception:
            continue

        if within_hours(dt, now, KEEP_HOURS):
            kept.append(r)

    best = {}
    for r in kept:
        key = hash_key(r.get("source", ""), r.get("url", ""))
        prev = best.get(key)

        if not prev:
            best[key] = r
        else:
            if len(r.get("lead_raw", "") or "") > len(prev.get("lead_raw", "") or ""):
                best[key] = r

    final = list(best.values())
    final.sort(key=lambda x: x.get("published_utc", ""), reverse=True)
    return final


def enrich_translate(rows: List[dict]) -> List[dict]:
    out: List[dict] = []

    for r in rows:
        title_en = translate_text(r.get("title_raw", ""))
        lead_en = translate_text(r.get("lead_raw", ""))

        out.append({
            "id": hash_key(r.get("source", ""), r.get("url", ""), r.get("published_utc", "")),
            "source": r.get("source", ""),
            "url": r.get("url", ""),
            "published_utc": r.get("published_utc", ""),
            "title_en": title_en,
            "lead_en": two_sentence_lead(lead_en),
            "title_raw": r.get("title_raw", ""),
            "lead_raw": r.get("lead_raw", ""),
        })

    return out


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

# ================= MAIN =================

def main() -> int:
    now = _utcnow()
    print(f"[INFO] Run at {now.strftime('%Y-%m-%dT%H:%M:%SZ')} | keep_hours={KEEP_HOURS}")

    collected: List[dict] = []

    for name, cfg in SOURCES.items():
        print(f"[INFO] RSS: {name}")
        rows = collect_from_rss(name, cfg["feeds"])
        print(f"[INFO] {name}: {len(rows)} rows")
        collected.extend(rows)

    if not collected:
        print("[WARN] No articles collected.")
        return 0

    existing = load_existing_json(OUT_JSON)
    merged = existing + collected
    merged = prune_and_dedupe(merged)
    enriched = enrich_translate(merged)

    # Hard Russia filter
    filtered = [a for a in enriched if article_is_russia_related(a)]
    print(f"[INFO] Russia-related only: {len(filtered)} / {len(enriched)} kept")

    classified = [classify_topics(a) for a in filtered]

    payload = {
        "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "keep_hours": KEEP_HOURS,
        "count": len(classified),
        "articles": classified,
    }

    atomic_write_json(OUT_JSON, payload)

    topic_names = ["diplomacy", "military", "energy", "economy", "local_events"]

    for topic in topic_names:
        subset = [a for a in classified if topic in a.get("topics", [])]
        topic_payload = {
            "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "topic": topic,
            "count": len(subset),
            "articles": subset,
        }
        atomic_write_json(Path(f"data/articles_{topic}.json"), topic_payload)

    print(f"[OK] Wrote {len(classified)} Russia-related articles -> {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
