#!/usr/bin/env python3
import os, time, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from zoneinfo import ZoneInfo

# ---------------- CONFIG ----------------
BASE_URL = "https://www.relianceiccrankings.com/datespecific/{format}/{category}/{date}/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
FORMATS = ["odi", "test"]
CATEGORIES = ["batting", "bowling"]
OUTPUT_FILE = "ICC_Rankings.csv.gz"   # or "ICC_Rankings.csv"
MAX_RETRIES = 3

session = requests.Session()
session.headers.update(HEADERS)


# ---------------- HELPERS ----------------
def last_tuesday_ist(today_utc=None, strict=True):
    """Return last Tuesday (IST)."""
    if today_utc is None:
        today_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    now_ist = today_utc.astimezone(ZoneInfo("Asia/Kolkata"))
    wd = now_ist.weekday()  # Mon=0, Tue=1,...
    delta = (wd - 1) % 7
    if strict and wd == 1:  # today is Tuesday
        delta = 7
    return (now_ist.date() - timedelta(days=delta))


def scrape_date(date_obj, fmt, category):
    date_str = date_obj.strftime("%Y/%m/%d")
    for _ in range(MAX_RETRIES):
        try:
            r = session.get(BASE_URL.format(format=fmt, category=category, date=date_str), timeout=10)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table tr")[1:101]
            if not rows:
                return []
            return [
                [date_str, fmt, category,
                 cols[0].get_text(strip=True),
                 cols[1].get_text(strip=True),
                 cols[2].get_text(strip=True)]
                for cols in (row.find_all("td") for row in rows)
                if len(cols) >= 3
            ]
        except Exception:
            time.sleep(1)
    return []


# ---------------- MAIN ----------------
def main():
    # Load master file
    if os.path.exists(OUTPUT_FILE):
        df_master = pd.read_csv(OUTPUT_FILE)
        df_master["Date"] = pd.to_datetime(df_master["Date"], errors="coerce")
        print("Loaded existing master file.")
    else:
        df_master = pd.DataFrame(columns=["Date","Format","Category","Rank","Player","Rating"])
        print("No existing file, starting fresh.")

    # Determine scraping range
    end_date = last_tuesday_ist()
    jobs = []
    for fmt in FORMATS:
        for cat in CATEGORIES:
            df_sub = df_master[(df_master["Format"] == fmt) & (df_master["Category"] == cat)]
            if not df_sub.empty:
                last_date = df_sub["Date"].max().date()
            else:
                last_date = datetime(1971,1,1).date() - timedelta(days=1)
            if last_date >= end_date:
                continue
            start_date = last_date + timedelta(days=1)
            new_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
            for d in new_dates:
                jobs.append((d, fmt, cat))

    if not jobs:
        print("Nothing new to scrape.")
        return

    print(f"Scraping {len(jobs)} jobs from multiple format-category-date combinations.")

    # Multiprocessing scrape
    results = []
    def callback(res): 
        if res: results.extend(res)

    with Pool(processes=cpu_count()) as pool:
        for d, fmt, cat in jobs:
            pool.apply_async(scrape_date, args=(d, fmt, cat), callback=callback)
        pool.close()
        pool.join()

    if not results:
        print("No new data scraped.")
        return

    # Append + deduplicate
    new_df = pd.DataFrame(results, columns=["Date","Format","Category","Rank","Player","Rating"])
    new_df["Date"] = pd.to_datetime(new_df["Date"], errors="coerce")
    
    print(f"New rows scraped this run: {len(new_df)}")

    combined = pd.concat([df_master, new_df], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["Date","Format","Category","Rank","Player","Rating"]
    ).sort_values(["Format","Category","Date","Rank"])

    # Save
    combined["Date"] = combined["Date"].dt.strftime("%Y/%m/%d")
    combined.to_csv(OUTPUT_FILE, index=False)

    print(f"Updated master file → {OUTPUT_FILE} ({len(combined):,} rows total)")


if __name__ == "__main__":
    main()
