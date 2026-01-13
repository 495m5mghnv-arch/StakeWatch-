import os
import re
import json
import csv
from datetime import datetime, timezone

import requests
import feedparser

USER_AGENT = os.getenv("SEC_USER_AGENT", "Leonard Klauss leonard@example.com")

SEC_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom"
SEC_FORMS = {"SC 13D", "SC 13G", "8-K"}

DE_NEWS_RSS = "https://api.boerse-frankfurt.de/v1/feeds/news.rss"

MAX_EVENTS = 2000


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_csv(path, events):
    cols = ["time_utc", "region", "source", "event", "buyer", "target", "percent", "title", "link"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in events:
            w.writerow({k: e.get(k, "") for k in cols})


def extract_sec_form(title):
    m = re.match(r"^([A-Z0-9/\- ]+?)\s+-\s+", (title or "").strip())
    return m.group(1).strip() if m else ""


def html_to_text(html):
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
    txt = re.sub(r"<br\s*/?>", "\n", txt, flags=re.I)
    txt = re.sub(r"</p\s*>", "\n", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"&nbsp;", " ", txt)
    txt = re.sub(r"\s+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


def de_extract_issuer_from_title(title):
    t = title or ""
    m = re.search(r"Stimmrechte\s*:\s*(.+?)(?:\s+-\s+|\s+\||$)", t)
    return (m.group(1).strip() if m else "").strip()


def de_parse_percent_new(text):
    # Nimmt aus der Zeile "neu ..." die letzte Prozentzahl
    for line in text.splitlines():
        l = line.strip().lower()
        if l.startswith("neu "):
            perc = re.findall(r"(\d+(?:[.,]\d+)?)\s*%", line)
            if perc:
                s = perc[-1].replace(",", ".")
                try:
                    return float(s)
                except Exception:
                    return None
    return None


def de_parse_notifier(text):
    m = re.search(r"Juristische Person\s*:\s*(.+)", text)
    if m:
        return m.group(1).strip()
    m = re.search(r"Natürliche Person\s*:\s*(.+)", text)
    if m:
        return m.group(1).strip()
    return ""


def collect_sec():
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    r = requests.get(SEC_RSS_URL, headers=headers, timeout=30)
    r.raise_for_status()
    feed = feedparser.parse(r.text)

    out = []
    for entry in feed.entries:
        title = (getattr(entry, "title", "") or "").strip()
        link = (getattr(entry, "link", "") or "").strip()
        form = extract_sec_form(title)
        if form not in SEC_FORMS:
            continue

        out.append({
            "time_utc": now_utc_iso(),
            "region": "US",
            "source": "SEC",
            "event": form,
            "buyer": "",
            "target": "",
            "percent": "",
            "title": title,
            "link": link
        })
    return out


def collect_de():
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    r = requests.get(DE_NEWS_RSS, headers=headers, timeout=30)
    r.raise_for_status()
    feed = feedparser.parse(r.text)

    out = []
    for entry in feed.entries[:120]:
        title = (getattr(entry, "title", "") or "").strip()
        link = (getattr(entry, "link", "") or "").strip()

        if "EQS" not in title:
            continue
        if ("Stimmrechte" not in title) and ("Stimmrechtsmitteilung" not in title):
            continue

        issuer = de_extract_issuer_from_title(title)

        # Best effort: Artikel laden, wenn möglich
        notifier = ""
        percent_new = ""
        rr = requests.get(link, headers={"User-Agent": USER_AGENT}, timeout=30)
        if rr.ok:
            text = html_to_text(rr.text)
            pn = de_parse_percent_new(text)
            if pn is not None:
                percent_new = str(pn)
            notifier = de_parse_notifier(text)

        out.append({
            "time_utc": now_utc_iso(),
            "region": "DE",
            "source": "DeutscheBoerseRSS",
            "event": "Stimmrechte",
            "buyer": notifier,
            "target": issuer,
            "percent": percent_new,
            "title": title,
            "link": link
        })

    return out


def main():
    seen = set(load_json("seen.json", []))
    events = load_json("data.json", [])

    new_items = []
    items = collect_sec() + collect_de()

    for item in items:
        key = item.get("link") or item.get("title")
        if not key or key in seen:
            continue
        seen.add(key)
        new_items.append(item)

    if new_items:
        events = new_items + events

    events = events[:MAX_EVENTS]

    save_json("seen.json", sorted(list(seen)))
    save_json("data.json", events)
    save_csv("events.csv", events)

    print(f"Done. New: {len(new_items)} | Total: {len(events)}")


if __name__ == "__main__":
    main()
