"""
etl_sponsor_register.py – v1.2.4
Download & clean the UK Home-Office sponsor register.

Creates timestamped outputs in /data/:
  • sponsor_register_raw_YYYY-MM-DD.csv/.xlsx     (original download)
  • sponsor_register_clean_YYYY-MM-DD.csv         (comma, BOM, Excel-friendly)
  • sponsor_register_clean_excel_YYYY-MM-DD.csv   (semicolon, BOM – Excel-safe)
  • sponsor_register_clean_YYYY-MM-DD.xlsx        (native workbook)
"""

import os, re, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from pathlib import Path

# ───── CONFIG ─────────────────────────────────────────────────────────
BASE_DIR = r"C:\Users\victo\Documents\Data_Science_Projects\visapath-ai"
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

PAGE_URL = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"
HEADERS  = {"User-Agent": "Mozilla/5.0 (VisaPath ETL 1.2.4)"}
TODAY    = date.today().isoformat()

# ───── 1. Locate current asset link ───────────────────────────────────
print(" Looking for latest sponsor register …")
html  = requests.get(PAGE_URL, headers=HEADERS, timeout=30).text
soup  = BeautifulSoup(html, "html.parser")
href  = next((a["href"] for a in soup.select("a[href]")
              if re.search(r"Worker_and_Temporary_Worker\.(csv|xlsx)$", a["href"])),
             None)
if not href:
    raise RuntimeError(" Could not find register link on GOV.UK")

ext = href.split(".")[-1].lower()
raw_path = os.path.join(DATA_DIR, f"sponsor_register_raw_{TODAY}.{ext}")

# ───── 2. Download only if new (check file size) ─────────────────────
print(" Checking for updates …")
new_file_size = int(requests.head(href, headers=HEADERS).headers.get("Content-Length", 0))

if os.path.exists(raw_path):
    local_file_size = os.path.getsize(raw_path)
    if local_file_size == new_file_size:
        print(" Latest version already downloaded.")
    else:
        print(" File updated – redownloading …")
        os.remove(raw_path)  # delete old partial
else:
    print("  New file available – downloading …")

if not os.path.exists(raw_path):
    with requests.get(href, headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(raw_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
    print(f" Saved: {raw_path}")

# ───── 3. Load into DataFrame ─────────────────────────────────────────
print(" Loading data …")
if ext == "csv":
    df = pd.read_csv(raw_path, dtype=str)
else:
    df = pd.read_excel(raw_path, engine="openpyxl")

# ───── 4. Clean data ──────────────────────────────────────────────────
df = df.rename(columns=str.strip)
df.columns = (df.columns.str.lower()
                        .str.replace(r"[^\w]+", "_", regex=True)
                        .str.strip("_"))

df = df.dropna(subset=["organisation_name", "town_city"]).drop_duplicates()

# ───── 5. Save cleaned versions ───────────────────────────────────────
csv_main   = os.path.join(DATA_DIR, f"sponsor_register_clean_{TODAY}.csv")
csv_excel  = os.path.join(DATA_DIR, f"sponsor_register_clean_excel_{TODAY}.csv")
xlsx_path  = os.path.join(DATA_DIR, f"sponsor_register_clean_{TODAY}.xlsx")

# (a) Comma CSV – for pipelines / Neo4j
with open(csv_main, "w", encoding="utf-8-sig", newline="") as f:
    f.write("sep=,\n")
    df.to_csv(f, index=False, encoding="utf-8-sig")

# (b) Semicolon CSV – for Excel preview
with open(csv_excel, "w", encoding="utf-8-sig", newline="") as f:
    f.write("sep=;\n")
    df.to_csv(f, sep=";", index=False, encoding="utf-8-sig")

# (c) Excel XLSX
with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
    df.to_excel(writer, index=False, sheet_name="AllRoutes")

# ───── 6. Done ────────────────────────────────────────────────────────
print("\n Outputs saved:")
print(f"   Raw file:         {raw_path}")
print(f"   Clean CSV:        {csv_main}")
print(f"   Clean Excel CSV:  {csv_excel}")
print(f"   Clean XLSX:       {xlsx_path}")
