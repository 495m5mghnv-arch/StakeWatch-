import json, csv, re, os
from datetime import datetime, timezone
import requests
import feedparser

SEC_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom"
FORM_WHITELIST = {"SC 13D", "SC 13G", "8-K"}

# Pflicht: aussagekräftiger User-Agent
USER_AGENT = os.getenv("SEC_USER_AGENT", "Leonard Klauss leonard@example.com")

def extract_form_type(title: str) -> str:
    # Titel sieht meist aus wie: "8-K - COMPANY NAME (CIK...)"
    m = re.match(r"^([A-Z0-9/\- ]+?)\s+-\s+", title.strip())
    return m.group(1).strip() if m else ""

def load_seen(path="seen.json"):
    if not os.path.exists(path):
        return set()
    try:
        return set(json.load(open(path, "r", encoding="utf-8")))
    except Exception:
        return set()

def save_seen(seen, path="seen.json"):
    json.dump(sorted(list(seen)), open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def load_events(path="data.json"):
    if not os.path.exists(path):
        return []
    try:
        return json.load(open(path, "r", encoding="utf-8"))
    except Exception:
        return []

def save_events(events, path="data.json"):
    json.dump(events, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def save_csv(events, path="events.csv"):
    cols = ["time_utc", "form", "title", "link"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in events:
            w.writerow({k: e.get(k, "") for k in cols})

def main():
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    r = requests.get(SEC_RSS_URL, headers=headers, timeout=30)
    r.raise_for_status()
    feed = feedparser.parse(r.text)

    seen = load_seen()
    events = load_events()

    new_count = 0
    now = datetime.now(timezone.utc).isoformat()

    for entry in feed.entries:
        entry_id = entry.get("id") or entry.get("link")
        if not entry_id or entry_id in seen:
            continue

        title = (entry.get("title") or "").strip()
        form = extract_form_type(title)
        link = (entry.get("link") or "").strip()

        seen.add(entry_id)

        if form in FORM_WHITELIST:
            events.insert(0, {
                "time_utc": now,
                "form": form,
                "title": title,
                "link": link
            })
            new_count += 1

    # begrenzen, damit es nicht endlos wächst
    events = events[:2000]

    save_seen(seen)
    save_events(events)
    save_csv(events)

    print(f"Done. New relevant events: {new_count}. Total stored: {len(events)}")

if __name__ == "__main__":
    main()
