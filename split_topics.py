import json
from pathlib import Path

MASTER = Path("data/articles_latest.json")

TOPICS = [
    "diplomacy",
    "military",
    "energy",
    "economy",
    "local_events"
]


def atomic_write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def main():

    if not MASTER.exists():
        print("Master JSON not found.")
        return

    data = json.loads(MASTER.read_text(encoding="utf-8"))

    updated = data.get("updated_at_utc")
    keep_hours = data.get("keep_hours")
    articles = data.get("articles", [])

    for topic in TOPICS:

        subset = [
            a for a in articles
            if a.get("topic_scores", {}).get(topic, 0) >= 1
        ]

        payload = {
            "updated_at_utc": updated,
            "keep_hours": keep_hours,
            "topic": topic,
            "count": len(subset),
            "articles": subset
        }

        out = Path(f"data/articles_{topic}.json")
        atomic_write_json(out, payload)

        print(f"Wrote {len(subset)} → {out}")


if __name__ == "__main__":
    main()
