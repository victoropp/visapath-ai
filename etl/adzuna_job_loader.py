# adzuna_job_loader.py – v0.6  (ASCII-only, future-proof)
"""
Dynamic Adzuna job fetcher for VisaPath AI.

• Accepts comma-separated lists of job titles and cities.
• Reads ADZUNA_APP_ID / ADZUNA_APP_KEY from .env or env vars.
• Paginates (100 results / page), handles 429/5xx with exponential back-off.
• Optional --visa_only keyword filter (visa|sponsor|skilled worker).
• Saves raw JSON and flattened CSV to ./data/<slug>.*

Run, for example:
    python adzuna_job_loader.py -s "data analyst, project manager" \
                                -c "London,Remote" --pages 5
"""
from __future__ import annotations

import argparse
import csv
import itertools
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# ─────────────────────────── ENV / CLI ────────────────────────────────
load_dotenv()


def _parse_csv(arg: str) -> list[str]:
    """Split comma-separated CLI argument → trimmed list (no empties)."""
    return [t.strip() for t in arg.split(",") if t.strip()]


def _cli() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("-s", "--search", default="software engineer",
                   help="Comma-separated list of job titles / keywords")
    p.add_argument("-c", "--city", default="London",
                   help="Comma-separated list of locations (Remote allowed)")
    p.add_argument("--country", default="gb",
                   help="ISO-2 country code (e.g. gb, us, au)")
    p.add_argument("--radius_km", type=int, default=25,
                   help="Search radius around city centre (km)")
    p.add_argument("--pages", type=int, default=3,
                   help="Max pages per term/location (100 results each)")
    p.add_argument("--visa_only", action="store_true",
                   help="Keep ads mentioning visa/sponsor/skilled worker")
    p.add_argument("--outdir", default="data")
    return p.parse_args()


ARGS = _cli()
TERMS = _parse_csv(ARGS.search)
CITIES = _parse_csv(ARGS.city)

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")
if not (APP_ID and APP_KEY):
    sys.exit("  Set ADZUNA_APP_ID and ADZUNA_APP_KEY in .env or env vars")

OUT_DIR = Path(ARGS.outdir)
OUT_DIR.mkdir(exist_ok=True)

STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
LOG_ARROW = "->"                                   # ASCII-only arrow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                   # console
        logging.FileHandler(OUT_DIR / f"adzuna_{STAMP}.log",
                            encoding="utf-8")                      # UTF-8 file
    ],
)

# ─────────────────────────── HELPERS ──────────────────────────────────
BASE_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"


def _slugify(text: str, maxlen: int = 60) -> str:
    text = re.sub(r"[^A-Za-z0-9\-_. ]+", "", text)
    text = re.sub(r"\s+", "_", text).strip("_")
    return text[:maxlen] or "blank"


def _fetch_page(term: str, city: str, page: int) -> list[dict]:
    url = BASE_URL.format(country=ARGS.country, page=page)
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 100,        # Adzuna maximum
        "what": term,
        "where": city,
        "distance": ARGS.radius_km,
        "content-type": "application/json",
    }
    for attempt in range(4):
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("results", [])
        if resp.status_code in {429, 500, 502, 503}:
            wait = 2 ** attempt
            logging.warning("Retry %s in %ss (%s)", attempt + 1, wait,
                            resp.status_code)
            time.sleep(wait)
            continue
        resp.raise_for_status()
    return []


CSV_FIELDS = [
    "id", "title", "company", "location", "created", "description",
    "latitude", "longitude", "salary_min", "salary_max", "redirect_url",
]

# ─────────────────────────── MAIN LOOP ────────────────────────────────
for term, city in itertools.product(TERMS, CITIES):
    slug = f"{_slugify(term)}_{_slugify(city)}_{STAMP}"
    json_path = OUT_DIR / f"{slug}.json"
    csv_path = OUT_DIR / f"{slug}.csv"
    all_jobs: list[dict] = []

    for page in range(1, ARGS.pages + 1):
        jobs = _fetch_page(term, city, page)
        if not jobs:
            break

        if ARGS.visa_only:
            jobs = [
                j for j in jobs
                if any(k in j.get("description", "").lower()
                       for k in ("visa", "sponsor", "skilled worker"))
            ]

        all_jobs.extend(jobs)
        logging.info("%s %s page %d %s %d jobs (total %d)",
                     slug, LOG_ARROW, page, LOG_ARROW,
                     len(jobs), len(all_jobs))

    # --- JSON
    with open(json_path, "w", encoding="utf-8") as f_json:
        json.dump(all_jobs, f_json, indent=2, ensure_ascii=False)

    # --- CSV
    with open(csv_path, "w", encoding="utf-8", newline="") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(CSV_FIELDS)
        for j in all_jobs:
            loc = j.get("location", {}).get("display_name", "")
            writer.writerow([
                j.get("id"), j.get("title"),
                j.get("company", {}).get("display_name"),
                loc, j.get("created"), j.get("description"),
                j.get("latitude"), j.get("longitude"),
                j.get("salary_min"), j.get("salary_max"),
                j.get("redirect_url"),
            ])

    logging.info("Saved %d jobs %s %s / %s",
                 len(all_jobs), LOG_ARROW,
                 json_path.name, csv_path.name)

print("  Completed every search/location combination.")
