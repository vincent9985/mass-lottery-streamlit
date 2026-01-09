import streamlit as st
import requests
import time
import pandas as pd
from datetime import date

st.set_page_config(page_title="Mass Lottery Winners Scraper", layout="centered")
st.title("Mass Lottery Winners Scraper")

st.markdown(
    "Cities are optional. Leave blank to fetch all cities. "
    "Date filter is always applied. This app will show the exact request URL used."
)

cities_input = st.text_input("Cities (optional, comma separated)", value="")
date_from = st.date_input("Date From", value=date(2025, 1, 1))
date_to = st.date_input("Date To", value=date.today())
run = st.button("Run Scraper")

log_box = st.empty()
progress_bar = st.progress(0)
debug_box = st.empty()

def log(msg: str):
    log_box.text(msg)

SESSION = requests.Session()
API_URL = "https://www.masslottery.com/api/v1/winners/query"

PRIZE_AMOUNTS = "600-4999,5000-9999,10000-24999,25000-49999,50000-99999,100000-999999,1000000-"

PAGE_SIZE = 200
MAX_RETRIES = 3

def fetch_page(params: dict, start_index: int):
    p = dict(params)
    p["start_index"] = start_index
    p["count"] = PAGE_SIZE

    r = SESSION.get(API_URL, params=p, timeout=30)
    if not r.ok:
        raise Exception(f"HTTP {r.status_code} | {r.text[:200]}")
    data = r.json()
    if "pageOfWinners" not in data or "totalNumberOfWinners" not in data:
        raise Exception("Bad response, missing keys")
    return data, r.url

if run:
    if date_to < date_from:
        st.error("Date To must be on or after Date From.")
        st.stop()

    cities = [c.strip() for c in cities_input.split(",") if c.strip()]

    params = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "prize_amounts": PRIZE_AMOUNTS,
        "sort": "newestFirst",
    }
    if cities:
        params["cities"] = ",".join(cities)

    log("Starting scrape...")
    progress_bar.progress(0)

    # First page
    first, first_url = fetch_page(params, 0)
    total = int(first["totalNumberOfWinners"])
    all_rows = list(first["pageOfWinners"])

    debug_box.code(first_url, language="text")
    log(f"Total winners reported by API: {total}")

    offsets = list(range(PAGE_SIZE, total, PAGE_SIZE))

    for start in offsets:
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data, _ = fetch_page(params, start)
                all_rows.extend(data["pageOfWinners"])
                last_err = None
                break
            except Exception as e:
                last_err = e
                time.sleep(0.35 * attempt)

        if last_err:
            st.error(f"Failed at offset {start}: {last_err}")
            break

        fetched_so_far = min(start + PAGE_SIZE, total)
        progress_bar.progress(fetched_so_far / max(total, 1))
        log(f"Fetched {fetched_so_far} / {total}")
        time.sleep(0.03)

    # Dedup
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
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    df = pd.DataFrame(deduped)

    # Show actual date range returned
    if not df.empty and "date_of_win" in df.columns:
        df_dates = pd.to_datetime(df["date_of_win"], errors="coerce")
        min_d = df_dates.min()
        max_d = df_dates.max()
        st.info(
            f"Returned date_of_win range: "
            f"{min_d.date() if pd.notna(min_d) else 'N/A'} to {max_d.date() if pd.notna(max_d) else 'N/A'}"
        )

    log(f"Done. Final rows: {len(df)}")
    progress_bar.progress(1.0)

    # ---------------- ANALYSIS SECTION (INSERTED HERE) ----------------
    with st.expander("Analysis", expanded=True):
        st.subheader("Quick stats")

        if df.empty:
            st.warning("No rows to analyze.")
        else:
            # Make sure types are correct
            if "prize_amount_usd" in df.columns:
                df["prize_amount_usd"] = pd.to_numeric(df["prize_amount_usd"], errors="coerce")

            if "date_of_win" in df.columns:
                df["date_of_win"] = pd.to_datetime(df["date_of_win"], errors="coerce")
                df["weekday"] = df["date_of_win"].dt.day_name()
                df["month"] = df["date_of_win"].dt.to_period("M").astype(str)

            total_rows = len(df)
            total_payout = float(df["prize_amount_usd"].sum(skipna=True)) if "prize_amount_usd" in df.columns else 0.0
            median_payout = float(df["prize_amount_usd"].median(skipna=True)) if "prize_amount_usd" in df.columns else 0.0
            unique_retailers = int(df["retailer"].nunique(dropna=True)) if "retailer" in df.columns else 0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rows", f"{total_rows:,}")
            c2.metric("Total payout", f"${total_payout:,.0f}")
            c3.metric("Median prize", f"${median_payout:,.0f}")
            c4.metric("Unique retailers", f"{unique_retailers:,}")

            st.divider()

            # Prize buckets, very lottery-relevant
            st.subheader("Prize buckets")
            def bucketize(x):
                try:
                    x = float(x)
                except Exception:
                    return "Unknown"
                if x < 1000:
                    return "< $1k"
                if x < 5000:
                    return "$1k - $4,999"
                if x < 10000:
                    return "$5k - $9,999"
                if x < 25000:
                    return "$10k - $24,999"
                if x < 50000:
                    return "$25k - $49,999"
                if x < 100000:
                    return "$50k - $99,999"
                if x < 1000000:
                    return "$100k - $999,999"
                return "$1M+"

            if "prize_amount_usd" in df.columns:
                df["prize_bucket"] = df["prize_amount_usd"].apply(bucketize)
                bucket_stats = (
                    df.groupby("prize_bucket")
                    .agg(
                        wins=("prize_amount_usd", "count"),
                        total_payout=("prize_amount_usd", "sum"),
                        median_payout=("prize_amount_usd", "median"),
                    )
                    .sort_values("wins", ascending=False)
                )
                st.bar_chart(bucket_stats["wins"])
                st.dataframe(bucket_stats)

            st.divider()

            # Wins by city
            st.subheader("Wins by city")
            if "retailer_location" in df.columns and "prize_amount_usd" in df.columns:
                city_stats = (
                    df.groupby("retailer_location")
                    .agg(
                        wins=("prize_amount_usd", "count"),
                        total_payout=("prize_amount_usd", "sum"),
                        avg_payout=("prize_amount_usd", "mean"),
                    )
                    .sort_values("wins", ascending=False)
                )
                st.dataframe(city_stats.head(50))

            # Top retailers
            st.subheader("Top winning retailers")
            if "retailer" in df.columns and "prize_amount_usd" in df.columns:
                retailer_stats = (
                    df.groupby("retailer")
                    .agg(
                        wins=("prize_amount_usd", "count"),
                        total_payout=("prize_amount_usd", "sum"),
                        avg_payout=("prize_amount_usd", "mean"),
                    )
                    .sort_values("wins", ascending=False)
                )
                st.dataframe(retailer_stats.head(25))

            # Games that pay out most often
            st.subheader("Games that pay out most often")
            if "name" in df.columns and "prize_amount_usd" in df.columns:
                game_stats = (
                    df.groupby("name")
                    .agg(
                        wins=("prize_amount_usd", "count"),
                        median_payout=("prize_amount_usd", "median"),
                        avg_payout=("prize_amount_usd", "mean"),
                    )
                    .sort_values("wins", ascending=False)
                )
                st.dataframe(game_stats.head(25))

            st.divider()

            # Day of week pattern
            st.subheader("Day of week pattern")
            if "weekday" in df.columns and "prize_amount_usd" in df.columns:
                order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                weekday_stats = (
                    df.groupby("weekday")
                    .agg(
                        wins=("prize_amount_usd", "count"),
                        avg_payout=("prize_amount_usd", "mean"),
                    )
                    .reindex(order)
                )
                st.bar_chart(weekday_stats["wins"])
                st.dataframe(weekday_stats)

            # Monthly trend
            st.subheader("Monthly trend")
            if "month" in df.columns and "prize_amount_usd" in df.columns:
                monthly = (
                    df.groupby("month")
                    .agg(
                        wins=("prize_amount_usd", "count"),
                        total_payout=("prize_amount_usd", "sum"),
                    )
                    .sort_index()
                )
                st.line_chart(monthly["wins"])
                st.dataframe(monthly)

    # ---------------- EXPORT + RAW PREVIEW ----------------
    st.download_button(
        "Download CSV",
        data=df.to_csv(index=False),
        file_name="masslottery_winners.csv",
        mime="text/csv",
    )

    st.subheader("Preview")
    st.dataframe(df.head(50))
