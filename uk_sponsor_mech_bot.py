"""
Daily UK sponsor-licensed mechanical job digest -> Telegram
Format per line: Job title + company name + location + job posting link

Requirements:
  pip install requests beautifulsoup4 pandas rapidfuzz

Env vars:
  TELEGRAM_BOT_TOKEN = api telegram
  TELEGRAM_CHAT_ID = id telegram
  ADZUNA_APP_ID = id
  ADZUNA_APP_KEY = key
"""

import os
import re
import time
import json
import sqlite3
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv
load_dotenv()

import requests
import random
from datetime import datetime
import zoneinfo
import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import process, fuzz

SPONSOR_PAGE = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"

ADZUNA_ENDPOINT = "https://api.adzuna.com/v1/api/jobs/gb/search/1"
DB_PATH = os.path.join(os.path.dirname(__file__), "seen_jobs.sqlite3")

# Tune these search terms any time you want
QUERIES = [
    "mechanical engineer",
    "mechanical design engineer",
    "design engineer mechanical",
    "cad engineer solidworks",
    "product design engineer mechanical",
    "manufacturing engineer mechanical",
    "maintenance engineer mechanical",
]

FUZZY_THRESHOLD = 92
MAX_JOBS_PER_QUERY = 50

def get_meta(con, key: str) -> str | None:
    cur = con.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_meta(con, key: str, value: str) -> None:
    con.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", (key, value))
    con.commit()

def london_today_str() -> str:
    tz = zoneinfo.ZoneInfo("Europe/London")
    return datetime.now(tz).strftime("%Y-%m-%d")

def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def normalize_company(name: str) -> str:
    if not name:
        return ""
    name = name.lower()
    name = name.replace("&", " and ")
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    name = re.sub(r"\b(ltd|limited|plc|llp|group|holdings|co|company|inc)\b", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def get_latest_sponsor_csv_url() -> str:
    html = requests.get(SPONSOR_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    link = soup.select_one('a[href*="assets.publishing.service.gov.uk"][href$=".csv"]')
    if not link:
        # fallback: sometimes the GOV.UK page structure changes; try any .csv link
        link = soup.select_one('a[href$=".csv"]')
    if not link:
        raise RuntimeError("Could not find sponsor CSV link on the GOV.UK sponsor register page.")
    return link["href"]

def load_sponsors() -> pd.DataFrame:
    csv_url = get_latest_sponsor_csv_url()
    df = pd.read_csv(csv_url)

    # Defensive column mapping (GOV.UK sometimes changes headers slightly)
    cols = {c.strip().lower(): c for c in df.columns}
    org_col = cols.get("organisation name") or cols.get("organization name")
    route_col = cols.get("route")

    if not org_col:
        raise RuntimeError(f"Unexpected sponsor CSV columns (no org name found): {df.columns.tolist()}")
    if not route_col:
        # routes are helpful but not mandatory for matching; create placeholder if missing
        df["route"] = ""
        route_col = "route"

    df = df.rename(columns={org_col: "org_name", route_col: "route"})
    df["org_norm"] = df["org_name"].astype(str).map(normalize_company)

    grouped = df.groupby(["org_name", "org_norm"], as_index=False)["route"].agg(lambda s: sorted(set(map(str, s))))
    return grouped

def sponsor_match(employer: str, sponsors_df: pd.DataFrame) -> Optional[Dict]:
    emp_norm = normalize_company(employer)
    if not emp_norm:
        return None

    # exact normalized match
    exact = sponsors_df[sponsors_df["org_norm"] == emp_norm]
    if len(exact) > 0:
        row = exact.iloc[0].to_dict()
        row["match_type"] = "exact"
        return row

    # fuzzy fallback
    choices = sponsors_df["org_norm"].tolist()
    best = process.extractOne(emp_norm, choices, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= FUZZY_THRESHOLD:
        matched_norm = best[0]
        row = sponsors_df[sponsors_df["org_norm"] == matched_norm].iloc[0].to_dict()
        row["match_type"] = f"fuzzy({best[1]})"
        return row

    return None

def fetch_adzuna_jobs(app_id: str, app_key: str, what: str, results_per_page: int = 50) -> List[Dict]:
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
        "what": what,
        "content-type": "application/json",
    }
    r = requests.get(ADZUNA_ENDPOINT, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("results", [])

def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            job_key TEXT PRIMARY KEY,
            first_seen INTEGER
        )
    """)
    con.commit()
    return con

def already_seen(con: sqlite3.Connection, job_key: str) -> bool:
    cur = con.execute("SELECT 1 FROM seen WHERE job_key = ?", (job_key,))
    return cur.fetchone() is not None

def mark_seen(con: sqlite3.Connection, job_key: str) -> None:
    con.execute("INSERT OR IGNORE INTO seen(job_key, first_seen) VALUES (?, ?)", (job_key, int(time.time())))
    con.commit()

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}

    for attempt in range(1, 6):  # up to 5 tries
        try:
            r = requests.post(url, json=payload, timeout=30)

            # Handle Telegram rate limits (429 with retry_after)
            if r.status_code == 429:
                try:
                    data = r.json()
                    retry_after = int(data.get("parameters", {}).get("retry_after", 3))
                except Exception:
                    retry_after = 3
                time.sleep(retry_after + 1)
                continue

            r.raise_for_status()
            return

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            # backoff + jitter
            sleep_s = min(30, 2 ** attempt) + random.random()
            time.sleep(sleep_s)
            continue

    # If we get here, all retries failed
    raise RuntimeError("Failed to send Telegram message after multiple retries.")

def chunk_lines(lines: List[str], max_chars: int = 3800) -> List[str]:
    # Telegram hard limit is 4096 chars; keep buffer for safety
    chunks = []
    buf = ""
    for line in lines:
        if len(buf) + len(line) + 1 > max_chars:
            if buf.strip():
                chunks.append(buf.strip())
            buf = line + "\n"
        else:
            buf += line + "\n"
    if buf.strip():
        chunks.append(buf.strip())
    return chunks

def main():
    bot_token = must_env("TELEGRAM_BOT_TOKEN")
    chat_id = must_env("TELEGRAM_CHAT_ID")
    adzuna_id = must_env("ADZUNA_APP_ID")
    adzuna_key = must_env("ADZUNA_APP_KEY")


    sponsors = load_sponsors()
    con = init_db()

    today = london_today_str()
    last_run = get_meta(con, "last_run_date")
    if last_run == today:
    return  # already ran today
    set_meta(con, "last_run_date", today)

    new_lines: List[str] = []

    for q in QUERIES:
        jobs = fetch_adzuna_jobs(adzuna_id, adzuna_key, what=q, results_per_page=MAX_JOBS_PER_QUERY)
        for j in jobs:
            title = (j.get("title") or "").strip()
            company = ((j.get("company") or {}).get("display_name") or "").strip()
            location = ((j.get("location") or {}).get("display_name") or "").strip()
            link = (j.get("redirect_url") or "").strip()

            if not title or not company or not link:
                continue

            # build a stable key (use Adzuna id if present, else link)
            job_key = str(j.get("id") or link)

            if already_seen(con, job_key):
                continue

            # sponsor filter
            m = sponsor_match(company, sponsors)
            if not m:
                continue

            # ✅ required output format (single line)
            line = f"{title} | {company} | {location} | {link}"
            new_lines.append(line)

            mark_seen(con, job_key)

    if not new_lines:
        tg_send(bot_token, chat_id, "No new sponsor-licensed mechanical jobs found today.")
        return

    # Optional: sort for nicer digests
    new_lines = sorted(set(new_lines), key=lambda s: s.lower())

    header = "Today’s new sponsor-licensed mechanical jobs:\n"
    chunks = chunk_lines([header] + new_lines)

    for i, msg in enumerate(chunks):
        tg_send(bot_token, chat_id, msg)
    time.sleep(1.1)  # keep under 1 msg/sec for the same chat


if __name__ == "__main__":
    main()
