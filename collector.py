# -*- coding: utf-8 -*-
"""
Russian Media Tracker — Collector + Translator + JSON Store (48h rolling)

Targets:
- Meduza
- RT
- Russia Beyond
- TASS
- The Moscow Times

Outputs:
- data/articles_latest.json
- Keeps only last 48 hours
- Translates all text fields into English
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
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

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
TEST_MODE = os.getenv("TEST_MODE", "0") == "1"


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
            r = sess.get(url, timeout=TIMEOUT)
            if r.status_code == 200 and r.text:
                return r.text
        except requests.RequestException:
            pass
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
                return pd.to_datetime(val, utc=True).to_pydatetime()
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
        return f"{parts[0]} {parts[1]}"
    return t[:280]


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
            summary = e.get("summary", "") or ""

            dt = _parse_entry_datetime(e)
            if not dt:
                continue

            rows.append({
                "source": source_name,
                "url": url,
                "published_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
        return data.get("articles", [])
    except Exception:
        return []


def prune_and_dedupe(rows: List[dict]) -> List[dict]:
    now = _utcnow()
    kept = []

    for r in rows:
        try:
            dt = pd.to_datetime(r.get("published_utc"), utc=True)
        except Exception:
            continue

        if within_hours(dt.to_pydatetime(), now, KEEP_HOURS):
            kept.append(r)

    best = {}
    for r in kept:
        key = hash_key(r.get("source"), r.get("url"))
        best[key] = r

    final = list(best.values())
    final.sort(key=lambda x: x.get("published_utc"), reverse=True)
    return final


def enrich_translate(rows: List[dict]) -> List[dict]:
    out = []

    for r in rows:
        title_en = translate_text(r.get("title_raw", ""))
        lead_en = translate_text(r.get("lead_raw", ""))

        out.append({
            "id": hash_key(r.get("source"), r.get("url"), r.get("published_utc")),
            "source": r.get("source"),
            "url": r.get("url"),
            "published_utc": r.get("published_utc"),
            "title_en": title_en,
            "lead_en": two_sentence_lead(lead_en),
            "title_raw": r.get("title_raw"),
            "lead_raw": r.get("lead_raw"),
        })

    return out


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ================= MAIN =================
def should_run_now():
    et = datetime.now(ZoneInfo("America/New_York"))
    return et.hour in {7, 12, 17, 22}
def main() -> int:
    if not should_run_now():
        print("Not a scheduled ET hour — exiting.")
        return 0
    now = _utcnow()
    print(f"[INFO] Run at {now.strftime('%Y-%m-%dT%H:%M:%SZ')} | keep_hours={KEEP_HOURS}")

    collected = []

    for name, cfg in SOURCES.items():
        print(f"[INFO] RSS: {name}")
        collected.extend(collect_from_rss(name, cfg["feeds"]))

    if not collected:
        print("[WARN] No articles collected.")
        return 0

    existing = load_existing_json(OUT_JSON)
    merged = existing + collected
    merged = prune_and_dedupe(merged)
    enriched = enrich_translate(merged)

    payload = {
        "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "keep_hours": KEEP_HOURS,
        "count": len(enriched),
        "articles": enriched,
    }

    atomic_write_json(OUT_JSON, payload)

    print(f"[OK] Wrote {len(enriched)} articles -> {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
