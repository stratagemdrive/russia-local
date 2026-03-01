# -*- coding: utf-8 -*-
"""
Russian Media Tracker — Collector + Translator + JSON Store (48h rolling)

Targets:
- Meduza
- RT
- Russia Beyond
- Sputnik
- TASS
- The Moscow Times

Outputs:
- data/articles_latest.json  (single rolling file)
- Keeps only last 48 hours (relative to now in UTC)
- Translates all text fields into English
- Designed to run on a schedule (cron / GitHub Actions)

Notes:
- Prefers RSS (feedparser). Falls back to HTML scraping where necessary.
- Translation uses deep_translator GoogleTranslator if installed.
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import hashlib
import requests
import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ----------------- CONFIG -----------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml, text/html, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.7",
    "Connection": "keep-alive",
}

TIMEOUT = 20
MAX_RETRIES = 3
RETRY_SLEEP = 2.0

# Rolling retention window
KEEP_HOURS = int(os.getenv("KEEP_HOURS", "48"))

# Output file (rolling JSON)
OUT_JSON = Path(os.getenv("OUT_JSON", "data/articles_latest.json"))

# Optional relevance filters (keep your existing logic if you want it)
TEST_MODE = os.getenv("TEST_MODE", "0") == "1"

KEYWORDS_INCLUDE: List[str] = [
    # English
    "ukraine", "ukrainian", "kyiv", "kiev", "donbas", "donetsk", "luhansk",
    "kharkiv", "kherson", "zaporizh", "crimea", "front line",
    "nato", "alliance", "strike", "missile", "mobilization", "air defense",
    "drone", "uav", "war", "offensive", "counteroffensive",
    # Russian stems
    "украин", "киев", "донбасс", "донецк", "луганск", "харьков", "херсон",
    "запорож", "крым", "сво", "спецоперац", "фронт", "обстрел", "удар",
    "ракет", "мобилизац", "нато", "альянс", "запад", "всу", "пво", "бпла",
    "дрон", "артилл", "наступлен", "контрнаступ",
]

KEYWORDS_EXCLUDE: List[str] = [
    "football", "hockey", "tennis", "soccer", "showbiz", "weather", "movie", "film festival",
    "футбол", "хоккей", "теннис", "шоу-бизнес", "погода", "кинофестиваль",
]

# Feeds (preferred). Some sites need HTML fallback.
SOURCES: Dict[str, Dict] = {
    "TASS (EN)": {
        "type": "rss",
        "feeds": ["https://tass.com/rss/v2.xml"],
        "base": "https://tass.com",
    },
    "TASS (RU)": {
        "type": "rss",
        "feeds": ["https://tass.ru/rss/v2.xml"],
        "base": "https://tass.ru",
    },
    "RT": {
        "type": "rss",
        "feeds": ["https://www.rt.com/rss/news/"],
        "base": "https://www.rt.com",
    },
    "Meduza (EN)": {
        "type": "rss",
        # commonly referenced; if blocked sometimes, RSS fallback list helps
        "feeds": [
            "https://meduza.io/rss/en/all",
            "https://meduza.io/rss/en/news",
            "https://meduza.io/rss/all",
            "https://meduza.io/rss/news",
        ],
        "base": "https://meduza.io",
    },
    "Russia Beyond": {
        "type": "rss",
        "feeds": ["https://www.rbth.com/rss"],
        "base": "https://www.rbth.com",
    },
    "The Moscow Times": {
        "type": "rss",
        "feeds": [
            "https://www.themoscowtimes.com/rss/news",
            "https://www.themoscowtimes.com/rss",
            "https://www.themoscowtimes.com/page/rss",
        ],
        "base": "https://www.themoscowtimes.com",
    },
    "Sputnik": {
        # RSS endpoints can be inconsistent/blocked; treat as HTML-first
        "type": "html",
        "base": "https://sputnikglobe.com",
        "start_urls": [
            "https://sputnikglobe.com/news/",
            "https://sputnikglobe.com/world/",
            "https://sputnikglobe.com/russia/",
            "https://sputnikglobe.com/military/",
            "https://sputnikglobe.com/economy/",
        ],
        # optional RSS candidates (if they work in your environment)
        "feeds": [
            "https://sputnikglobe.com/export/rss2/index.xml",
            "https://sputnikglobe.com/export/rss2/archive/index.xml",
        ],
    },
}

# ----------------- Optional translation -----------------

try:
    from deep_translator import GoogleTranslator
    HAS_TRANSLATOR = True
except Exception:
    HAS_TRANSLATOR = False


# ----------------- Helpers -----------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fetch_text(url: str, referer: Optional[str] = None) -> Optional[str]:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    if referer:
        sess.headers.update({"Referer": referer})

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
    # 1) feedparser structured
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        st = entry.get(key)
        if st:
            try:
                return datetime(*st[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    # 2) string parse fallback
    for key in ("published", "updated", "created", "pubDate", "date"):
        val = entry.get(key)
        if val:
            try:
                return pd.to_datetime(val, utc=True, errors="raise").to_pydatetime().astimezone(timezone.utc)
            except Exception:
                continue

    return None


def normalize_space(s: str) -> str:
    return " ".join((s or "").split()).strip()


def two_sentence_lead(text: str) -> str:
    """
    Try to compress summary/description into ~2 sentences.
    If no obvious sentence boundaries, truncate to ~280 chars.
    """
    t = normalize_space(re.sub(r"<[^>]+>", " ", text or ""))
    if not t:
        return ""

    # split on sentence-ish boundaries
    parts = re.split(r"(?<=[.!?])\s+", t)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}".strip()
    if len(parts) == 1:
        return parts[0][:280].rstrip()
    return t[:280].rstrip()


def translate_text(text: str, target_lang: str = "en") -> str:
    if not text:
        return ""
    if not HAS_TRANSLATOR:
        return text
    try:
        return GoogleTranslator(source="auto", target=target_lang).translate(text)
    except Exception:
        return text


def looks_relevant(article: dict) -> bool:
    if TEST_MODE:
        return True
    txt = f"{article.get('title_raw','')} {article.get('lead_raw','')}".lower()
    if any(x in txt for x in KEYWORDS_EXCLUDE):
        return False
    return any(x in txt for x in KEYWORDS_INCLUDE)


def hash_key(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()


def within_hours(dt_utc: datetime, now_utc: datetime, keep_hours: int) -> bool:
    return dt_utc >= (now_utc - timedelta(hours=keep_hours))


# ----------------- RSS collectors -----------------

def collect_from_rss(source_name: str, feeds: List[str]) -> List[dict]:
    rows: List[dict] = []

    for feed_url in feeds:
        txt = fetch_text(feed_url, referer=urlparse(feed_url).scheme + "://" + urlparse(feed_url).netloc + "/")
        if not txt:
            print(f"[WARN] RSS fetch failed: {source_name} -> {feed_url}")
            continue

        d = feedparser.parse(txt)
        for e in d.entries:
            title = normalize_space(e.get("title", ""))
            url = normalize_space(e.get("link", ""))
            summary = e.get("summary", "") or e.get("description", "") or ""

            dt = _parse_entry_datetime(e)
            if not dt:
                # Skip undated items (hard to prune reliably)
                continue

            rows.append({
                "source": source_name,
                "url": url,
                "published_utc": dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title_raw": title,
                "lead_raw": two_sentence_lead(summary),
                "collector": "rss",
            })

        # if a feed works, you can stop early for that source to avoid duplicates
        if rows:
            break

    return rows


# ----------------- HTML collectors (fallback) -----------------

def _extract_time_from_tag(tag) -> Optional[datetime]:
    """
    Try to interpret <time datetime="..."> or similar patterns as UTC.
    If timezone not specified, parse as UTC (best-effort).
    """
    if not tag:
        return None

    dt_str = tag.get("datetime") or tag.get("content") or tag.get_text(strip=True)
    dt_str = normalize_space(dt_str)
    if not dt_str:
        return None
    try:
        dt = pd.to_datetime(dt_str, utc=True, errors="raise").to_pydatetime()
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def collect_sputnik_html(start_urls: List[str]) -> List[dict]:
    """
    Sputnik HTML varies; this is a pragmatic scraper:
    - Finds links that look like article pages
    - Attempts to pull title + time + first paragraph/meta description
    """
    out: List[dict] = []
    seen_urls = set()

    for u in start_urls:
        html = fetch_text(u, referer="https://sputnikglobe.com/")
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        # candidate links: keep internal, likely-article URLs
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href:
                continue
            if href.startswith("/"):
                full = "https://sputnikglobe.com" + href
            else:
                full = href

            pu = urlparse(full)
            if pu.netloc and "sputnikglobe.com" not in pu.netloc:
                continue

            # heuristic: most article pages contain a numeric id at end
            if not re.search(r"/\d{6,}/?$", pu.path):
                continue

            if full in seen_urls:
                continue
            seen_urls.add(full)

            # fetch the article page (light throttle by limiting)
            if len(out) >= 60:
                break

            art_html = fetch_text(full, referer=u)
            if not art_html:
                continue
            art = BeautifulSoup(art_html, "html.parser")

            # title
            title = normalize_space(
                (art.select_one("h1") or art.select_one("meta[property='og:title']") or {}).get_text(strip=True)
                if art.select_one("h1")
                else (art.select_one("meta[property='og:title']")["content"].strip()
                      if art.select_one("meta[property='og:title']") and art.select_one("meta[property='og:title']").get("content") else "")
            )

            # time
            dt = None
            ttag = art.select_one("time")
            dt = _extract_time_from_tag(ttag) if ttag else None
            if not dt:
                # try meta article:published_time
                m = art.select_one("meta[property='article:published_time']")
                if m and m.get("content"):
                    try:
                        dt = pd.to_datetime(m["content"], utc=True, errors="raise").to_pydatetime().astimezone(timezone.utc)
                    except Exception:
                        dt = None

            if not title or not dt:
                continue

            # lead: meta description first, else first paragraph
            lead = ""
            md = art.select_one("meta[name='description']")
            if md and md.get("content"):
                lead = md["content"]
            else:
                p = art.select_one("article p") or art.select_one(".article__text p") or art.select_one("p")
                lead = p.get_text(" ", strip=True) if p else ""

            out.append({
                "source": "Sputnik",
                "url": full,
                "published_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title_raw": title,
                "lead_raw": two_sentence_lead(lead),
                "collector": "html",
            })

        if len(out) >= 60:
            break

    return out


# ----------------- Store: merge + prune + dedupe -----------------

def load_existing_json(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("articles"), list):
            return data["articles"]
        if isinstance(data, list):
            return data
    except Exception:
        return []
    return []


def prune_and_dedupe(rows: List[dict], keep_hours: int) -> List[dict]:
    now = _utcnow()

    kept: List[dict] = []
    for r in rows:
        try:
            dt = pd.to_datetime(r.get("published_utc", ""), utc=True, errors="raise").to_pydatetime().astimezone(timezone.utc)
        except Exception:
            continue
        if within_hours(dt, now, keep_hours):
            kept.append(r)

    # dedupe by stable key: source + url (primary), else source + title + time
    best: Dict[str, dict] = {}
    for r in kept:
        key = hash_key(r.get("source",""), r.get("url","")) if r.get("url") else hash_key(
            r.get("source",""), r.get("title_raw",""), r.get("published_utc","")
        )
        prev = best.get(key)
        if not prev:
            best[key] = r
        else:
            # keep whichever has a longer lead (usually richer)
            if len((r.get("lead_raw") or "")) > len((prev.get("lead_raw") or "")):
                best[key] = r

    # sort newest-first
    final = list(best.values())
    final.sort(key=lambda x: x.get("published_utc", ""), reverse=True)
    return final


def enrich_translate(rows: List[dict]) -> List[dict]:
    """
    Translate title + lead into English; keep raw fields too.
    Add convenience fields (domain, published_epoch).
    """
    out: List[dict] = []
    for r in rows:
        url = r.get("url", "")
        domain = urlparse(url).netloc.lower()

        # translate
        title_en = translate_text(r.get("title_raw", ""))
        lead_en = translate_text(r.get("lead_raw", ""))

        # normalize 2-sentence again post-translation (Google sometimes adds odd spacing)
        lead_en = two_sentence_lead(lead_en)

        # epoch
        epoch = None
        try:
            dt = pd.to_datetime(r.get("published_utc",""), utc=True, errors="raise")
            epoch = int(dt.timestamp())
        except Exception:
            epoch = None

        out.append({
            "id": hash_key(r.get("source",""), url, r.get("published_utc",""), r.get("title_raw","")),
            "source": r.get("source", ""),
            "source_domain": domain,
            "url": url,
            "published_utc": r.get("published_utc", ""),
            "published_epoch": epoch,
            "title_en": title_en,
            "lead_en": lead_en,
            "title_raw": r.get("title_raw", ""),
            "lead_raw": r.get("lead_raw", ""),
            "collector": r.get("collector", ""),
        })
    return out


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ----------------- Main -----------------

def main() -> int:
    now = _utcnow()
    print(f"[INFO] Run at {now.strftime('%Y-%m-%dT%H:%M:%SZ')} | keep_hours={KEEP_HOURS}")

    collected: List[dict] = []

    # 1) RSS sources
    for name, cfg in SOURCES.items():
        if cfg.get("type") != "rss":
            continue
        feeds = cfg.get("feeds") or []
        if not feeds:
            continue
        print(f"[INFO] RSS: {name}")
        collected.extend(collect_from_rss(name, feeds))

    # 2) HTML sources (Sputnik)
    for name, cfg in SOURCES.items():
        if cfg.get("type") != "html":
            continue
        print(f"[INFO] HTML: {name}")
        if name == "Sputnik":
            collected.extend(collect_sputnik_html(cfg.get("start_urls", [])))

    if not collected:
        print("[WARN] No articles collected (all sources failed).")
        return 0

    # optional relevance filtering
    if not TEST_MODE:
        before = len(collected)
        collected = [r for r in collected if looks_relevant(r)]
        print(f"[INFO] Relevance kept {len(collected)}/{before}")

    # merge with existing store
    existing = load_existing_json(OUT_JSON)
    # existing is already enriched; convert back to minimal compatible rows for prune/dedupe
    existing_min = []
    for e in existing:
        existing_min.append({
            "source": e.get("source",""),
            "url": e.get("url",""),
            "published_utc": e.get("published_utc",""),
            "title_raw": e.get("title_raw",""),
            "lead_raw": e.get("lead_raw",""),
            "collector": e.get("collector","store"),
        })

    merged = existing_min + collected
    merged = prune_and_dedupe(merged, KEEP_HOURS)
    enriched = enrich_translate(merged)

    payload = {
        "updated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "keep_hours": KEEP_HOURS,
        "count": len(enriched),
        "articles": enriched,
    }

    atomic_write_json(OUT_JSON, payload)
    print(f"[OK] Wrote {len(enriched)} articles -> {OUT_JSON.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
