"""
Daily UK sponsor-licensed engineering job digest -> Telegram
Format per line: Job title + company name + location + job posting link

Requirements:
  pip install requests beautifulsoup4 pandas rapidfuzz python-dotenv

Env vars:
  TELEGRAM_BOT_TOKEN = Telegram bot token
  TELEGRAM_CHAT_ID = main target chat id (channel/group)
  TELEGRAM_ADMIN_CHAT_ID = optional admin DM chat id (for debug summary)
  ADZUNA_APP_ID = Adzuna app id
  ADZUNA_APP_KEY = Adzuna app key
  DEBUG_SEND_SUMMARY = optional (1/0), default 1
"""

import os
import re
import time
import sqlite3
import random
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional, Dict, List, Iterable, Set, Any, Tuple

from dotenv import load_dotenv
load_dotenv()

import requests
import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import process, fuzz


SPONSOR_PAGE = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"
ADZUNA_ENDPOINT_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/gb/search/{page}"
DB_PATH = os.path.join(os.path.dirname(__file__), "seen_jobs.sqlite3")

# ---------------------------------
# Search queries (tune anytime)
# ---------------------------------
QUERIES = [
    "mechatronics engineer",
    "electromechanical engineer",
    "automation engineer manufacturing",
    "industrial automation engineer",
    "controls engineer plc",
    "junior controls engineer",
    "robotics integration engineer",
    "robotics applications engineer",
    "product development engineer electromechanical",
    "npi engineer manufacturing",
    "digital manufacturing engineer",
    "industry 4.0 engineer",
    "manufacturing systems engineer",
    "manufacturing process engineer automation",
    "test validation engineer electromechanical",
    "product test engineer mechanical",
    "reliability engineer manufacturing",
    "simulation engineer mechanical matlab",
    "cae engineer ansys",
    "commissioning engineer automation",
]

# Adzuna search tuning
ADZUNA_PAGES_PER_QUERY = 3
MAX_JOBS_PER_QUERY = 50
ADZUNA_MAX_DAYS_OLD = 14  # keep digest reasonably fresh

# Sponsor name fuzzy threshold (rapidfuzz token_sort_ratio out of 100)
FUZZY_THRESHOLD = 92

# ---------------------------------
# Relevance / scoring config lists
# ---------------------------------
TARGET_JOB_TITLES = [
    # Mechatronics / Electromechanical
    "Mechatronics Engineer",
    "Electromechanical Engineer",
    "Mechatronics Design Engineer",
    "Electro-Mechanical Design Engineer",
    "Systems Integration Engineer",
    "Electromechanical Systems Engineer",

    # Automation / Controls
    "Automation Engineer",
    "Industrial Automation Engineer",
    "Manufacturing Automation Engineer",
    "Controls Engineer",
    "Controls and Automation Engineer",
    "Control Systems Engineer",
    "Junior Controls Engineer",
    "PLC Engineer",
    "Automation Project Engineer",
    "Commissioning Engineer",

    # Robotics
    "Robotics Engineer",
    "Robotics Integration Engineer",
    "Robotics Applications Engineer",
    "Robot Integration Engineer",
    "Robot Cell Integration Engineer",
    "Automation and Robotics Engineer",

    # Product Development / R&D
    "Product Development Engineer",
    "Electromechanical Product Development Engineer",
    "Design and Development Engineer",
    "R&D Engineer",
    "Research and Development Engineer",
    "NPI Engineer",
    "New Product Introduction Engineer",

    # Manufacturing / Digital Manufacturing
    "Manufacturing Engineer",
    "Manufacturing Process Engineer",
    "Process Engineer",
    "Production Engineer",
    "Digital Manufacturing Engineer",
    "Industry 4.0 Engineer",
    "Smart Manufacturing Engineer",
    "Manufacturing Systems Engineer",
    "Industrial Digitalization Engineer",
    "Continuous Improvement Engineer",

    # Test / Validation / Reliability
    "Test Engineer",
    "Validation Engineer",
    "Test and Validation Engineer",
    "Verification and Validation Engineer",
    "V&V Engineer",
    "Product Test Engineer",
    "Reliability Engineer",
    "Reliability Test Engineer",

    # Simulation / Analysis
    "CAE Engineer",
    "Simulation Engineer",
    "Modelling and Simulation Engineer",
    "Design Analysis Engineer",
    "Computational Engineer",

    # Technical / Field / Applications
    "Field Application Engineer",
    "Applications Engineer",
    "Technical Solutions Engineer",
    "Field Service Engineer",
]

TITLE_KEYWORDS = [
    # Core identity
    "mechatronics",
    "electromechanical",
    "electro-mechanical",
    "automation",
    "controls",
    "control systems",
    "plc",
    "robotics",
    "systems integration",
    "integration engineer",

    # Product / design / development
    "product development",
    "design and development",
    "r&d",
    "research and development",
    "npi",
    "new product introduction",

    # Manufacturing / process / digital
    "manufacturing engineer",
    "manufacturing process",
    "process engineer",
    "production engineer",
    "digital manufacturing",
    "smart manufacturing",
    "industry 4.0",
    "manufacturing systems",
    "industrial digitalization",
    "continuous improvement",

    # Test / validation / reliability
    "test engineer",
    "validation engineer",
    "verification",
    "v&v",
    "product test",
    "reliability",

    # Analysis / simulation
    "cae",
    "simulation engineer",
    "modelling",
    "modeling",
    "design analysis",

    # Customer-facing industrial roles
    "applications engineer",
    "field application",
    "field service engineer",
    "technical solutions engineer",
    "commissioning engineer",
]

DESCRIPTION_INCLUDE_KEYWORDS = [
    # Mechanical / product
    "mechanical design",
    "product development",
    "electromechanical",
    "mechatronics",
    "prototype",
    "design for manufacture",
    "dfm",
    "testing",
    "validation",
    "root cause analysis",

    # Automation / controls
    "automation",
    "industrial automation",
    "plc",
    "scada",
    "control systems",
    "instrumentation",
    "commissioning",
    "sensors",
    "actuators",

    # Manufacturing / process
    "manufacturing",
    "process improvement",
    "continuous improvement",
    "production",
    "lean",
    "digital manufacturing",
    "industry 4.0",
    "manufacturing systems",

    # Software / analysis edge
    "python",
    "matlab",
    "data analysis",
    "automation scripts",
    "simulation",
    "ansys",
    "solidworks",
    "fusion 360",
]

EXCLUDE_KEYWORDS = [
    # Too senior (early-career focus)
    "senior",
    "lead",
    "principal",
    "head of",
    "director",

    # Pure software roles (not primary target)
    "frontend",
    "backend",
    "full stack",
    "react developer",
    "android developer",
    "ios developer",
    "web developer",

    # Unrelated engineering domains (optional)
    "civil engineer",
    "architectural",
    "quantity surveyor",

    # Sales-heavy (optional)
    "sales engineer",
    "business development",
]

SPONSORSHIP_RELEVANCE_HINTS = [
    "visa sponsorship",
    "sponsorship available",
    "skilled worker visa",
    "right to work uk",
    "relocation support",
]

CORE_TITLES_HIGH_PRIORITY = [
    "Mechatronics Engineer",
    "Electromechanical Engineer",
    "Automation Engineer",
    "Industrial Automation Engineer",
    "Controls Engineer",
    "Robotics Integration Engineer",
    "Product Development Engineer",
    "NPI Engineer",
    "Digital Manufacturing Engineer",
    "Manufacturing Systems Engineer",
    "Test and Validation Engineer",
    "Manufacturing Process Engineer",
]


# ------------------------------
# Basic helpers
# ------------------------------
def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def inc(stats: Dict[str, Any], key: str, amount: int = 1) -> None:
    stats[key] = int(stats.get(key, 0)) + amount


def add_reason_count(stats: Dict[str, Any], reason: str) -> None:
    reason_counts = stats.setdefault("score_reject_reasons", {})
    reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1


def normalize_company(name: str) -> str:
    if not name:
        return ""
    name = name.lower()
    name = name.replace("&", " and ")
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    name = re.sub(r"\b(ltd|limited|plc|llp|group|holdings|co|company|inc)\b", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    t = text.lower()
    t = t.replace("&", " and ")
    t = t.replace("/", " ")
    t = t.replace("-", " ")
    t = re.sub(r"[^a-z0-9+.#\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def get_latest_sponsor_csv_url() -> str:
    html = requests.get(SPONSOR_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    link = soup.select_one('a[href*="assets.publishing.service.gov.uk"][href$=".csv"]')
    if not link:
        link = soup.select_one('a[href$=".csv"]')
    if not link:
        raise RuntimeError("Could not find sponsor CSV link on the GOV.UK sponsor register page.")
    return link["href"]


def load_sponsors() -> pd.DataFrame:
    csv_url = get_latest_sponsor_csv_url()
    df = pd.read_csv(csv_url)

    cols = {c.strip().lower(): c for c in df.columns}
    org_col = cols.get("organisation name") or cols.get("organization name")
    route_col = cols.get("route")

    if not org_col:
        raise RuntimeError(f"Unexpected sponsor CSV columns (no org name found): {df.columns.tolist()}")
    if not route_col:
        df["route"] = ""
        route_col = "route"

    df = df.rename(columns={org_col: "org_name", route_col: "route"})
    df["org_norm"] = df["org_name"].astype(str).map(normalize_company)

    grouped = df.groupby(["org_name", "org_norm"], as_index=False)["route"].agg(lambda s: sorted(set(map(str, s))))
    return grouped


def sponsor_match(employer: str, sponsors_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    emp_norm = normalize_company(employer)
    if not emp_norm:
        return None

    exact = sponsors_df[sponsors_df["org_norm"] == emp_norm]
    if len(exact) > 0:
        row = exact.iloc[0].to_dict()
        row["match_type"] = "exact"
        return row

    choices = sponsors_df["org_norm"].tolist()
    best = process.extractOne(emp_norm, choices, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= FUZZY_THRESHOLD:
        matched_norm = best[0]
        row = sponsors_df[sponsors_df["org_norm"] == matched_norm].iloc[0].to_dict()
        row["match_type"] = f"fuzzy({best[1]})"
        return row

    return None


def fetch_adzuna_jobs(
    app_id: str,
    app_key: str,
    what: str,
    *,
    page: int = 1,
    results_per_page: int = 50,
    max_days_old: Optional[int] = None,
) -> List[Dict[str, Any]]:
    endpoint = ADZUNA_ENDPOINT_TEMPLATE.format(page=page)
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
        "what": what,
        "content-type": "application/json",
    }
    if max_days_old is not None:
        params["max_days_old"] = max_days_old

    r = requests.get(endpoint, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("results", [])


def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            job_key TEXT PRIMARY KEY,
            first_seen INTEGER
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

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

    for attempt in range(1, 6):
        try:
            r = requests.post(url, json=payload, timeout=30)

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
            sleep_s = min(30, 2 ** attempt) + random.random()
            time.sleep(sleep_s)
            continue

    raise RuntimeError("Failed to send Telegram message after multiple retries.")


def get_target_chat_ids() -> List[str]:
    ids: List[str] = []

    main_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if main_id:
        ids.append(main_id)

    admin_id = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()
    if admin_id and admin_id not in ids:
        ids.append(admin_id)

    if not ids:
        raise RuntimeError("No Telegram chat IDs configured.")
    return ids


def get_admin_chat_id() -> str:
    return os.getenv("TELEGRAM_ADMIN_CHAT_ID", "").strip()


def tg_send_multi(bot_token: str, chat_ids: List[str], text: str) -> None:
    for cid in chat_ids:
        tg_send(bot_token, cid, text)
        time.sleep(1.1)  # avoid flooding / resets


def send_admin_debug(bot_token: str, text: str) -> None:
    admin_id = get_admin_chat_id()
    if not admin_id:
        return
    tg_send(bot_token, admin_id, text)


def chunk_lines(lines: List[str], max_chars: int = 3800) -> List[str]:
    chunks: List[str] = []
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


# ------------------------------
# UK / job-title filters
# ------------------------------
def is_mech_related_title(title: str) -> bool:
    t = (title or "").lower()

    include_keywords = [
        "mechanical",
        "mechatronics",
        "electromechanical",
        "electro-mechanical",
        "automation engineer",
        "industrial automation",
        "controls engineer",
        "control systems engineer",
        "plc engineer",
        "robotics engineer",
        "robotics integration",
        "commissioning engineer",
        "manufacturing engineer",
        "manufacturing process engineer",
        "process engineer",
        "production engineer",
        "npi engineer",
        "product development engineer",
        "validation engineer",
        "test engineer",
        "reliability engineer",
        "simulation engineer",
        "cae engineer",
        "design engineer",
        "design and development engineer",
        "field service engineer",
        "applications engineer",
    ]

    exclude_keywords = [
        "sales",
        "inside sales",
        "business development",
        "account manager",
        "recruiter",
        "talent acquisition",
        "customer service",
        "marketing",
        "finance",
        " hr ",
        "human resources",
        "teacher",
        "nurse",
    ]

    return any(k in t for k in include_keywords) and not any(k in t for k in exclude_keywords)


def is_uk_job(title: str, company: str, location: str, description: str = "") -> bool:
    text = f"{title} {company} {location} {description}".lower()
    loc = (location or "").lower()

    # Strong location-field UK signals
    strong_uk_loc_signals = ["united kingdom", "england", "scotland", "wales", "northern ireland", ", uk", " uk"]
    if any(x in loc for x in strong_uk_loc_signals):
        return True

    uk_signals = [
        "london", "manchester", "birmingham", "bristol", "leeds", "glasgow", "edinburgh",
        "liverpool", "sheffield", "nottingham", "newcastle", "southampton", "coventry",
        "milton keynes", "cambridge", "oxford", "remote uk", "uk remote"
    ]

    non_uk_signals = [
        "united states", " usa", " us ", "new york", "california", "texas", "seattle",
        "austin", "boston", "chicago", "toronto", "canada", "australia", "singapore"
    ]

    padded = f" {text} "
    has_uk = any(x in padded for x in uk_signals) or any(x in padded for x in [" united kingdom ", " uk "])
    has_non_uk = any(x in padded for x in non_uk_signals)

    if has_uk and not has_non_uk:
        return True
    if has_non_uk and not has_uk:
        return False

    # Adzuna GB is UK-biased; allow unknowns to avoid false negatives
    return True


# ------------------------------
# Relevance scoring helpers
# ------------------------------
@dataclass
class MatchConfig:
    fuzzy_title_threshold: float = 0.86
    min_description_hits_for_non_exact_title: int = 2
    min_score_to_alert: int = 28

    score_exact_title: int = 40
    score_fuzzy_title: int = 30
    score_high_priority_title: int = 12
    score_title_keyword_hit: int = 8
    score_description_keyword_hit: int = 3
    score_sponsorship_hint_hit: int = 2
    score_bonus_skill_hit: int = 2

    cap_title_keyword_hits: int = 3
    cap_description_keyword_hits: int = 6
    cap_sponsorship_hint_hits: int = 2
    cap_bonus_skill_hits: int = 4

    bonus_skill_keywords: List[str] = field(default_factory=lambda: [
        "python", "matlab", "plc", "scada", "controls", "automation",
        "mechatronics", "electromechanical", "digital manufacturing", "industry 4.0",
    ])


def contains_phrase(text_norm: str, phrase_norm: str) -> bool:
    return phrase_norm in text_norm


def keyword_hits(text_norm: str, keywords: Iterable[str]) -> List[str]:
    hits: List[str] = []
    for kw in keywords:
        k = normalize_text(kw)
        if not k:
            continue
        if contains_phrase(text_norm, k):
            hits.append(kw)
    return hits


def best_fuzzy_title_match(title_norm: str, target_titles: Iterable[str]) -> Tuple[float, Optional[str]]:
    best_score = 0.0
    best_title: Optional[str] = None
    for t in target_titles:
        t_norm = normalize_text(t)
        if not t_norm:
            continue
        score = SequenceMatcher(None, title_norm, t_norm).ratio()
        if score > best_score:
            best_score = score
            best_title = t
    return best_score, best_title


def company_in_sponsor_list(company_name: str, sponsor_companies_norm: Optional[Set[str]]) -> bool:
    if sponsor_companies_norm is None:
        return True
    return normalize_text(company_name) in sponsor_companies_norm


def score_job_posting(
    job: Dict[str, Any],
    *,
    target_job_titles: List[str],
    title_keywords: List[str],
    description_include_keywords: List[str],
    exclude_keywords: List[str],
    sponsorship_relevance_hints: Optional[List[str]] = None,
    core_titles_high_priority: Optional[List[str]] = None,
    sponsor_companies_norm: Optional[Set[str]] = None,
    config: Optional[MatchConfig] = None,
) -> Dict[str, Any]:
    cfg = config or MatchConfig()
    sponsorship_relevance_hints = sponsorship_relevance_hints or []
    core_titles_high_priority = core_titles_high_priority or []

    title = str(job.get("title", "") or "")
    desc = str(job.get("description", "") or "")
    company = str(job.get("company", "") or "")

    title_norm = normalize_text(title)
    desc_norm = normalize_text(desc)
    company_norm = normalize_text(company)
    combined_norm = f"{title_norm} {desc_norm}".strip()

    debug_reasons: List[str] = []
    score = 0

    sponsor_ok = company_in_sponsor_list(company, sponsor_companies_norm)
    if not sponsor_ok:
        return {
            "accepted": False,
            "score": 0,
            "reject_reason": "company_not_in_sponsor_list",
            "debug": {
                "company": company,
                "company_norm": company_norm,
                "reasons": ["Company did not match sponsor list"],
            },
        }
    debug_reasons.append("company matched sponsor list" if sponsor_companies_norm else "sponsor check skipped")

    exclude_hits = keyword_hits(combined_norm, exclude_keywords)
    if exclude_hits:
        return {
            "accepted": False,
            "score": 0,
            "reject_reason": "exclude_keyword",
            "debug": {
                "title": title,
                "company": company,
                "exclude_hits": exclude_hits,
                "reasons": [f"Excluded by keywords: {exclude_hits}"],
            },
        }

    target_titles_norm = {normalize_text(t): t for t in target_job_titles}
    exact_title_match = title_norm in target_titles_norm
    if exact_title_match:
        score += cfg.score_exact_title
        debug_reasons.append(f"exact title match: {target_titles_norm[title_norm]}")

    fuzzy_score, fuzzy_title = best_fuzzy_title_match(title_norm, target_job_titles)
    fuzzy_title_match = (fuzzy_score >= cfg.fuzzy_title_threshold) and not exact_title_match
    if fuzzy_title_match:
        score += cfg.score_fuzzy_title
        debug_reasons.append(f"fuzzy title match: {fuzzy_title} ({fuzzy_score:.2f})")

    core_titles_norm = {normalize_text(t) for t in core_titles_high_priority}
    if title_norm in core_titles_norm:
        score += cfg.score_high_priority_title
        debug_reasons.append("high-priority title boost")

    title_kw_hits = list(dict.fromkeys(keyword_hits(title_norm, title_keywords)))
    title_kw_count_used = min(len(title_kw_hits), cfg.cap_title_keyword_hits)
    if title_kw_count_used:
        score += title_kw_count_used * cfg.score_title_keyword_hit
        debug_reasons.append(f"title keyword hits: {title_kw_hits[:cfg.cap_title_keyword_hits]}")

    desc_kw_hits = list(dict.fromkeys(keyword_hits(desc_norm, description_include_keywords)))
    desc_kw_count_used = min(len(desc_kw_hits), cfg.cap_description_keyword_hits)
    if desc_kw_count_used:
        score += desc_kw_count_used * cfg.score_description_keyword_hit
        debug_reasons.append(f"description keyword hits: {desc_kw_hits[:cfg.cap_description_keyword_hits]}")

    sponsor_hint_hits = list(dict.fromkeys(keyword_hits(desc_norm, sponsorship_relevance_hints)))
    sponsor_hint_count_used = min(len(sponsor_hint_hits), cfg.cap_sponsorship_hint_hits)
    if sponsor_hint_count_used:
        score += sponsor_hint_count_used * cfg.score_sponsorship_hint_hit
        debug_reasons.append(f"sponsorship hints: {sponsor_hint_hits[:cfg.cap_sponsorship_hint_hits]}")

    bonus_skill_hits = list(dict.fromkeys(keyword_hits(combined_norm, cfg.bonus_skill_keywords)))
    bonus_skill_count_used = min(len(bonus_skill_hits), cfg.cap_bonus_skill_hits)
    if bonus_skill_count_used:
        score += bonus_skill_count_used * cfg.score_bonus_skill_hit
        debug_reasons.append(f"bonus skill hits: {bonus_skill_hits[:cfg.cap_bonus_skill_hits]}")

    title_relevant = exact_title_match or fuzzy_title_match or (len(title_kw_hits) >= 1)
    desc_relevant = len(desc_kw_hits) >= cfg.min_description_hits_for_non_exact_title
    if exact_title_match or fuzzy_title_match:
        desc_relevant = True

    accepted = bool(title_relevant and desc_relevant and score >= cfg.min_score_to_alert)

    reject_reason = None
    if not accepted:
        if not title_relevant:
            reject_reason = "title_not_relevant"
        elif not desc_relevant:
            reject_reason = "description_not_relevant_enough"
        else:
            reject_reason = "score_below_threshold"

    return {
        "accepted": accepted,
        "score": score,
        "reject_reason": reject_reason,
        "debug": {
            "company": company,
            "title": title,
            "title_norm": title_norm,
            "exact_title_match": exact_title_match,
            "fuzzy_title_match": fuzzy_title_match,
            "fuzzy_title_best": fuzzy_title,
            "fuzzy_title_score": round(fuzzy_score, 3),
            "title_keyword_hits": title_kw_hits,
            "description_keyword_hits": desc_kw_hits,
            "sponsorship_hint_hits": sponsor_hint_hits,
            "bonus_skill_hits": bonus_skill_hits,
            "reasons": debug_reasons,
        },
    }


# ------------------------------
# Debug summary helpers
# ------------------------------
def build_debug_summary(stats: Dict[str, Any], status: str, error_text: str = "") -> str:
    lines: List[str] = []
    lines.append(f"🔎 Job bot debug summary ({status})")
    lines.append(f"Queries: {stats.get('queries', 0)}")
    lines.append(f"Pages checked: {stats.get('pages_checked', 0)}")
    lines.append(f"Non-empty pages: {stats.get('pages_nonempty', 0)}")
    lines.append(f"Jobs fetched total: {stats.get('jobs_fetched', 0)}")
    lines.append(f"Skipped missing fields: {stats.get('skip_missing_fields', 0)}")
    lines.append(f"Skipped title filter: {stats.get('skip_title_filter', 0)}")
    lines.append(f"Skipped non-UK filter: {stats.get('skip_non_uk', 0)}")
    lines.append(f"Skipped already seen: {stats.get('skip_seen', 0)}")
    lines.append(f"Skipped sponsor mismatch: {stats.get('skip_sponsor', 0)}")
    lines.append(f"Skipped score/relevance: {stats.get('skip_score', 0)}")
    lines.append(f"Accepted before dedupe: {stats.get('accepted_before_dedupe', 0)}")
    lines.append(f"Unique lines to send: {stats.get('unique_lines', 0)}")
    lines.append(f"Digest chunks sent: {stats.get('digest_chunks', 0)}")
    lines.append(f"Target chats used: {stats.get('chat_targets', 0)}")

    reasons = stats.get("score_reject_reasons", {}) or {}
    if reasons:
        top = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:5]
        reason_text = ", ".join([f"{k}={v}" for k, v in top])
        lines.append(f"Top score rejects: {reason_text}")

    if stats.get("sample_sent"):
        lines.append("Sample sent:")
        for s in stats["sample_sent"][:3]:
            lines.append(f"- {s}")

    if error_text:
        lines.append("Error:")
        lines.append(error_text[:1500])

    msg = "\n".join(lines)

    # Keep debug summary within Telegram limits
    if len(msg) > 3500:
        msg = msg[:3450] + "\n...(truncated)"
    return msg


def maybe_send_debug_summary(bot_token: str, stats: Dict[str, Any], status: str, error_text: str = "") -> None:
    if not get_admin_chat_id():
        return
    if not env_flag("DEBUG_SEND_SUMMARY", True):
        return
    msg = build_debug_summary(stats, status=status, error_text=error_text)
    send_admin_debug(bot_token, msg)


def main() -> None:
    bot_token = must_env("TELEGRAM_BOT_TOKEN")
    chat_ids = get_target_chat_ids()
    adzuna_id = must_env("ADZUNA_APP_ID")
    adzuna_key = must_env("ADZUNA_APP_KEY")

    stats: Dict[str, Any] = {
        "queries": 0,
        "pages_checked": 0,
        "pages_nonempty": 0,
        "jobs_fetched": 0,
        "skip_missing_fields": 0,
        "skip_title_filter": 0,
        "skip_non_uk": 0,
        "skip_seen": 0,
        "skip_sponsor": 0,
        "skip_score": 0,
        "accepted_before_dedupe": 0,
        "unique_lines": 0,
        "digest_chunks": 0,
        "chat_targets": len(chat_ids),
        "score_reject_reasons": {},
        "sample_sent": [],
    }

    try:
        sponsors = load_sponsors()
        con = init_db()

        new_lines: List[str] = []

        match_cfg = MatchConfig(
            fuzzy_title_threshold=0.86,
            min_description_hits_for_non_exact_title=2,
            min_score_to_alert=28,
        )

        for q in QUERIES:
            inc(stats, "queries")
            for page in range(1, ADZUNA_PAGES_PER_QUERY + 1):
                inc(stats, "pages_checked")

                jobs = fetch_adzuna_jobs(
                    adzuna_id,
                    adzuna_key,
                    what=q,
                    page=page,
                    results_per_page=MAX_JOBS_PER_QUERY,
                    max_days_old=ADZUNA_MAX_DAYS_OLD,
                )

                if not jobs:
                    # Stop paging this query if current page is empty
                    break

                inc(stats, "pages_nonempty")
                inc(stats, "jobs_fetched", len(jobs))

                for j in jobs:
                    title = (j.get("title") or "").strip()
                    company = ((j.get("company") or {}).get("display_name") or "").strip()
                    location = ((j.get("location") or {}).get("display_name") or "").strip()
                    link = (j.get("redirect_url") or "").strip()
                    description = (j.get("description") or "").strip()

                    if not title or not company or not link:
                        inc(stats, "skip_missing_fields")
                        continue

                    if not is_mech_related_title(title):
                        inc(stats, "skip_title_filter")
                        continue

                    if not is_uk_job(title, company, location, description):
                        inc(stats, "skip_non_uk")
                        continue

                    job_key = str(j.get("id") or link)
                    if already_seen(con, job_key):
                        inc(stats, "skip_seen")
                        continue

                    sponsor_info = sponsor_match(company, sponsors)
                    if not sponsor_info:
                        inc(stats, "skip_sponsor")
                        continue

                    job_for_score = {
                        "title": title,
                        "company": company,
                        "description": description,
                        "location": location,
                        "url": link,
                    }

                    score_result = score_job_posting(
                        job_for_score,
                        target_job_titles=TARGET_JOB_TITLES,
                        title_keywords=TITLE_KEYWORDS,
                        description_include_keywords=DESCRIPTION_INCLUDE_KEYWORDS,
                        exclude_keywords=EXCLUDE_KEYWORDS,
                        sponsorship_relevance_hints=SPONSORSHIP_RELEVANCE_HINTS,
                        core_titles_high_priority=CORE_TITLES_HIGH_PRIORITY,
                        sponsor_companies_norm=None,  # sponsor check already done above
                        config=match_cfg,
                    )

                    if not score_result["accepted"]:
                        inc(stats, "skip_score")
                        add_reason_count(stats, str(score_result.get("reject_reason") or "unknown"))
                        continue

                    line = f"{title} | {company} | {location} | {link}"
                    new_lines.append(line)
                    inc(stats, "accepted_before_dedupe")

                    if len(stats["sample_sent"]) < 3:
                        stats["sample_sent"].append(line)

                    mark_seen(con, job_key)

        if not new_lines:
            stats["unique_lines"] = 0
            stats["digest_chunks"] = 1
            tg_send_multi(bot_token, chat_ids, "No new sponsor-licensed engineering jobs found today.")
            maybe_send_debug_summary(bot_token, stats, status="ok_no_new_jobs")
            return

        new_lines = sorted(set(new_lines), key=lambda s: s.lower())
        stats["unique_lines"] = len(new_lines)

        header = "Today’s new sponsor-licensed engineering jobs:\n"
        chunks = chunk_lines([header] + new_lines)
        stats["digest_chunks"] = len(chunks)

        for msg in chunks:
            tg_send_multi(bot_token, chat_ids, msg)

        maybe_send_debug_summary(bot_token, stats, status="ok_sent")

    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        try:
            maybe_send_debug_summary(bot_token, stats, status="error", error_text=error_text)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
