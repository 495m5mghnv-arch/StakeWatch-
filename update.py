import os
import re
import json
import csv
from datetime import datetime, timezone

import requests
import feedparser

USER_AGENT = os.getenv("SEC_USER_AGENT", "Leonard Klauss leonard@example.com")

# USA (SEC current filings feed)
SEC_RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom"
SEC_FORMS = {"SC 13D", "SC 13G", "8-K"}

# Deutschland (Deutsche Börse / Börse Frankfurt News RSS, enthält EQS-Meldungen)
DE_NEWS_RSS = "https://api.boerse-frankfurt.de/v1/feeds/news.rss"

MAX_EVENTS = 2000


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_csv(path: str, events):
    cols = ["time_utc", "region", "source", "event", "buyer", "target", "percent", "title", "link"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in events:
            w.writerow({k: e.get(k, "") for k in cols})


def extract_sec_form(title: str) -> str:
    # z.B. "8-K - COMPANY NAME (CIK...)"
    m = re.match(r"^([A-Z0-9/\- ]+?)\s+-\s+", (title or "").strip())
    return m.group(1).strip() if m else ""


def html_to_text(html: str) -> str:
    # Minimaler HTML->Text (ohne externe Libraries)
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.I | re.S)
    txt = re.sub(r"<br\s*/?>", "\n", txt, flags=re.I)
    txt = re.sub(r"</p\s*>", "\n", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"&nbsp;", " ", txt)
    txt = re.sub(r"\s+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


def de_extract_issuer_from_title(title: str) -> str:
    # Häufig: "EQS-Stimmrechte: <Issuer> - ..."
    t = title or ""
    m = re.search(r"Stimmrechte\s*:\s*(.+?)(?:\s+-\s+|\s+\||$)", t)
    return (m.group(1).strip() if m else "").strip()


def de_parse_percent_from_text(text: str):
    # Sucht nach Zeile "neu ..." und nimmt die letzte Prozentzahl
    def last_percent_in_line(line: str):
        perc = re.findall(r"(\d+(?:[.,]\d+)?)\s*%", line)
        if not perc:
            return None
        s = perc[-1].replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    percent_new = None
    percent_old = None
    for line in text.splitlines():
        l = line.strip().lower()
        if l.startswith("neu "):
            percent_new = last_percent_in_line(line)
        if l.startswith("letzte "):
            percent_old = last_percent_in_line(line)

    return percent_new, percent_old


def de_parse_notifier_from_text(text: str) -> str:
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

        # Filter: EQS Stimmrechte / Stimmrechtsmitteilung
        if "EQS" not in title:
            continue
        if ("Stimmrechte" not in title) and ("Stimmrechtsmitteilung" not in title):
            continue

        issuer = de_extract_issuer_from_title(title)
        notifier = ""
        percent_new = ""

        # Optional: Artikel holen und Prozent + Melder extrahieren (best effort)
        try:
            rr = requests.get(link, headers={"User-Agent": USER_AGENT}, timeout=30)
            rr.raise_for_status()
            text = html_to_text(rr.text)

            pn, _po = de_parse_percent_from_text(text)
            if pn is not None:
                percent_new = str(pn)

            notifier = de_parse_notifier_from_text(text)
