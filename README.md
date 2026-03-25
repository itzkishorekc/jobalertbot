# UK Sponsorship Job Alert Bot (Engineering-Focused)

A Python Telegram bot that helps users find **UK job openings** at companies listed on the **UK Government Skilled Worker sponsor register**.

It searches job listings (Adzuna), filters results by:
1. **Sponsor company match** (UK GOV sponsor list)
2. **Target job titles / keywords**
3. **Role relevance scoring** (title + description matching)
4. **Exclude keywords** (to reduce noise)

Then it sends alerts to Telegram.

---

## Why this project exists and why it was made

Searching for visa-sponsoring jobs manually is time-consuming and noisy.

This bot automates the workflow:
- Find new jobs
- Check whether the employer appears on the UK sponsor register
- Filter for your target career path (e.g., Mechanical, Electrical, Civil, Software)
- Send clean alerts to Telegram

---

## Features

- ✅ UK GOV sponsor register integration (CSV from GOV.UK page)
- ✅ Job search via Adzuna API
- ✅ Fuzzy sponsor-company matching
- ✅ Title + description relevance scoring
- ✅ Telegram alerts (group/channel + optional admin/debug chat)
- ✅ SQLite “seen jobs” tracking (avoids duplicates)
- ✅ Debug mode to inspect why jobs were rejected

---

## Setup

### 1) Install dependencies
```bash
pip install requests beautifulsoup4 pandas rapidfuzz python-dotenv
```
### 2) Create .env

```
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_channel_or_group_id
TELEGRAM_ADMIN_CHAT_ID=your_personal_chat_id   # optional but recommended

ADZUNA_APP_ID=your_adzuna_app_id
ADZUNA_APP_KEY=your_adzuna_app_key

# Optional debug mode
DEBUG_REJECTIONS=1
DEBUG_REJECTIONS_LIMIT=15
```

3) Run
```
python uk_sponsor_mech_bot.py
```
Output format (Telegram)

Each job is sent in this format:

Job Title | Company | Location | Job Link

Example:
Automation Engineer | Example Robotics Ltd | Cambridge | https://...

How to customize for your branch of study

This project is designed to be easy to adapt.

Main things to edit

Inside the Python file, update these lists:

QUERIES → what the job API searches for

TARGET_JOB_TITLES → exact/near-exact job titles you want

TITLE_KEYWORDS → keywords for fuzzy title matching

DESCRIPTION_INCLUDE_KEYWORDS → terms expected in relevant job descriptions

EXCLUDE_KEYWORDS → terms to ignore (noise)

Example customizations by branch
Mechanical / Mechatronics / Manufacturing (default style)

Good keywords:

mechatronics

electromechanical

automation engineer

controls engineer

manufacturing engineer

product development

test/validation

PLC / SCADA

digital manufacturing / Industry 4.0

Electrical / Electronics / Embedded

Suggested additions:

electrical engineer

electronics engineer

embedded systems

firmware

PCB

power electronics

instrumentation

control systems

VHDL / Verilog (if relevant)

MATLAB / Simulink

Civil / Construction / Infrastructure

Suggested replacements:

civil engineer

structural engineer

site engineer

project engineer (civil)

highways / transport

geotechnical

drainage

AutoCAD / Revit / BIM

NEC contracts (optional)

construction design management

Tip: Remove civil engineer from EXCLUDE_KEYWORDS if adapting for civil roles.

Software / Data / AI

Suggested replacements:

software engineer

backend engineer

data engineer

machine learning engineer

python developer

cloud engineer

SQL

APIs

Docker / Kubernetes

AWS / Azure / GCP

Tip: Remove software-related terms from EXCLUDE_KEYWORDS if using this for software jobs.

Biomedical / Chemical / Pharma / Process

Suggested additions:

process engineer

validation engineer

quality engineer

GMP

CAPA

root cause analysis

process improvement

instrumentation

documentation / compliance

Scoring system (how filtering works)

The bot scores each job using:

Exact title match (high score)

Fuzzy title match (medium-high score)

Title keyword hits

Description keyword hits

Optional bonus keywords (e.g., Python, PLC, MATLAB)

It rejects jobs if:

excluded keywords are found, or

the score is below threshold, or

the title/description is not relevant enough

Tuning tips

Too many irrelevant alerts → increase min_score_to_alert

Missing good jobs → lower min_score_to_alert

Too broad → tighten TITLE_KEYWORDS and DESCRIPTION_INCLUDE_KEYWORDS

Too narrow → add more synonyms / title variants

Debug mode (recommended during tuning) STILL IN DEVELOPMENT



[Enable:

DEBUG_REJECTIONS=1
DEBUG_REJECTIONS_LIMIT=15

The bot will send a debug summary to TELEGRAM_ADMIN_CHAT_ID showing:

rejected job title/company

rejection reason

score

top matching/reject clues

This makes it easier to tune filters for your field.]

Project structure (simple)

uk_sponsor_mech_bot.py → main bot script

.env → secrets/config (not committed)

seen_jobs.sqlite3 → local database of already-sent jobs

Contributions welcome

If you want to help improve this project, good contribution ideas include:

Better sponsor company alias matching (Ltd/Limited/Group edge cases)

More job sources (Indeed, LinkedIn, Reed, etc. where allowed by API/TOS)

Branch presets (Mechanical/Electrical/Civil/Software)

Smarter ranking (weights, ML scoring, learning from accepted/rejected jobs)

Docker support / scheduler setup (cron, GitHub Actions, VPS)

Unit tests for matching and scoring logic

Contribution guideline (simple)

Fork the repo

Create a branch (feature/your-change)

Make changes

Disclaimer

This project is an automation/filtering helper and may miss some jobs or include false positives.
Always verify:

visa sponsorship availability

role eligibility

latest immigration requirements

job listing accuracy
