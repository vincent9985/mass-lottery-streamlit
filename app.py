import streamlit as st
import requests
import time
import pandas as pd
from datetime import date

st.set_page_config(
    page_title="Mass Lottery Winners Scraper",
    layout="centered"
)

st.title("Mass Lottery Winners Scraper")

st.markdown(
    "Leave **Cities** blank to fetch **all cities**. "
    "Enter comma-separated cities (e.g. `Quincy, N Quincy`) to filter."
)

# ---------------- UI ----------------
cities_input = st.text_input(
    "Cities (optional, comma separated)",
    value=""
)

date_from = st.date_input(
    "Date From",
    value=date(2025, 1, 1)
)

date_to = st.date_input(
    "Date To",
    value=date.today()
)

run = st.button("Run Scraper")

log_box = st.empty()
progress_bar = st.progress(0)

# ---------------- Helpers ----------------
def log(msg):
    log_box.text(msg)

def fetch_page(base_url, start_index, page_size):
    url = f"{base_url}&start_index={start_index}&count={page_size}"
    r = requests.get(url, timeout=30)
    if not r.ok:
        raise Exception(f"HTTP {r.status_code}")
    data = r.json()
    if "pageOfWinners" not in data:
        raise Exception("Bad response, missing pageOfWinners")
    return data

# ---------------- Main Logic ----------------
if run:
    log("Starting scrape...")
    progress_bar.progress(0)

    # Parse cities
    cities = [c.strip() for c in cities_input.split(",") if c.strip()]
    cities_param = ",".join(cities)

    # ---- Build BASE safely ----
    BASE = (
        "https://www.masslottery.com/api/v1/winners/query"
        f"?date_from={date_from}"
        f"&date_to={date_to}"
        "&prize_amounts=600-4999,5000-9999,10000-24999,25000-49999,"
        "50000-99999,100000-999999,1000000-"
    )

    if cities_param:
        BASE += f"&cities={cities_param}"
        log(f"Filtering cities: {cities_param}")
    else:
        log("No city filter applied (ALL cities)")

    BASE += "&sort=newestFirst"

    PAGE_SIZE = 200
    MAX_RETRIES = 3

    # ---- First page ----
    first = fetch_page(BASE, 0, PAGE_SIZE)
    total = first["totalNumberOfWinners"]
    all_rows = list(first["pageOfWinners"])

    log(f"Total winners reported by API: {total}")

    # ---- Pagination ----
    offsets = list(range(PAGE_SIZE, total, PAGE_SIZE))
    processed = min(PAGE_SIZE, total)

    for start in offsets:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data = fetch_page(BASE, start, PAGE_SIZE)
                all_rows.extend(data["pageOfWinners"])
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    st.error(f"Failed at offset {start}: {e}")
                time.sleep(0.3 * attempt)

        processed = min(start + PAGE_SIZE, total)
        progress_bar.progress(processed / total)
        log(f"Fetched {processed} / {total}")
        time.sleep(0.05)

    # ---- Deduplicate ----
    seen = set()
    deduped = []

    for r in all_rows:
        key = (
            r.get("date_of_win"),
            r.get("prize_amount_usd"),
            r.get("name"),
            r.get("retailer"),
            r.get("retailer_location"),
        )
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    df = pd.DataFrame(deduped)

    log(f"Done. Final rows: {len(df)}")
    progress_bar.progress(1.0)

    st.success(f"Scrape complete. Rows: {len(df)}")

    st.download_button(
        label="Download CSV",
        data=df.to_csv(index=False),
        file_name="masslottery_winners.csv",
        mime="text/csv"
    )

    st.dataframe(df.head(50))
