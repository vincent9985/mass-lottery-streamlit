import streamlit as st
import requests
import time
import pandas as pd
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Mass Lottery Winners", layout="wide")
st.title("Mass Lottery Winners")
st.caption(
    "Fetch winner entries from MassLottery, preview immediately, then load the full dataset. "
    "Cities are optional, leave blank for all cities."
)

# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Filters")
    cities_input = st.text_area(
        "Cities (optional, comma separated)",
        value="Quincy, N Quincy",
        height=70
    )
    date_from = st.date_input("Date From", value=date(2025, 8, 1))
    date_to = st.date_input("Date To", value=date(2026, 1, 7))

    st.divider()
    st.subheader("Speed")
    MAX_WORKERS = st.slider("Concurrent requests", 1, 6, 4, help="Try 3–4. If you see errors, lower it.")
    run = st.button("Run", use_container_width=True)

# ---------------- Constants ----------------
SESSION = requests.Session()
API_URL = "https://www.masslottery.com/api/v1/winners/query"
PAGE_SIZE = 200
MAX_RETRIES = 3

status = st.empty()
progress_bar = st.progress(0)
debug_box = st.empty()

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

def bucketize(x):
    try:
        x = float(x)
    except Exception:
        return "Unknown"
    if x < 100: return "< $100"
    if x < 300: return "$100 - $299"
    if x < 600: return "$300 - $599"
    if x < 1000: return "$600 - $999"
    if x < 5000: return "$1k - $4,999"
    if x < 10000: return "$5k - $9,999"
    if x < 25000: return "$10k - $24,999"
    if x < 50000: return "$25k - $49,999"
    if x < 100000: return "$50k - $99,999"
    if x < 1000000: return "$100k - $999,999"
    return "$1M+"

def fmt_dollar(x):
    return f"${x:,.0f}" if pd.notna(x) else "—"

def fmt_count(x):
    try:
        return f"{int(x):,}"
    except Exception:
        return "—"

# ---------------- Cached scrape ----------------
@st.cache_data(ttl=60 * 60, show_spinner=False)
def cached_scrape(date_from_s: str, date_to_s: str, cities_s: str, max_workers: int):
    cities = [c.strip() for c in cities_s.split(",") if c.strip()]

    params = {
        "date_from": date_from_s,
        "date_to": date_to_s,
        "sort": "newestFirst",
    }
    if cities:
        params["cities"] = ",".join(cities)

    # First page
    first, first_url = fetch_page(params, 0)
    total = int(first["totalNumberOfWinners"])
    all_rows = list(first["pageOfWinners"])

    offsets = list(range(PAGE_SIZE, total, PAGE_SIZE))

    def fetch_with_retry(start):
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                data, _ = fetch_page(params, start)
                return start, data["pageOfWinners"]
            except Exception as e:
                last_err = e
                time.sleep(0.25 * attempt)
        raise last_err

    if offsets:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(fetch_with_retry, s) for s in offsets]
            for fut in as_completed(futures):
                _, rows = fut.result()
                all_rows.extend(rows)

    return {
        "first_url": first_url,
        "total": total,
        "rows": all_rows,
    }

# ---------------- Main ----------------
if run:
    if date_to < date_from:
        st.error("Date To must be on or after Date From.")
        st.stop()

    date_from_s = date_from.isoformat()
    date_to_s = date_to.isoformat()
    cities_s = cities_input.strip()

    status.info("Fetching first page…")
    progress_bar.progress(0)

    # Preview: fetch just first page (not cached yet)
    cities_preview = [c.strip() for c in cities_s.split(",") if c.strip()]
    params_preview = {
        "date_from": date_from_s,
        "date_to": date_to_s,
        "sort": "newestFirst",
    }
    if cities_preview:
        params_preview["cities"] = ",".join(cities_preview)

    first, first_url = fetch_page(params_preview, 0)
    total = int(first["totalNumberOfWinners"])
    debug_box.code(first_url, language="text")

    preview_df = pd.DataFrame(first["pageOfWinners"])
    st.subheader("Preview (first 200 rows)")
    st.dataframe(preview_df, use_container_width=True, height=320)

    status.info(f"Preview loaded. Total entries reported by API: {total:,}. Loading full dataset…")

    # Full scrape (cached)
    result = cached_scrape(date_from_s, date_to_s, cities_s, MAX_WORKERS)
    all_rows = result["rows"]
    df = pd.DataFrame(all_rows)  # no dedupe

    # Clean types
    df["prize_amount_usd"] = pd.to_numeric(df.get("prize_amount_usd"), errors="coerce")
    df["date_of_win"] = pd.to_datetime(df.get("date_of_win"), errors="coerce")
    df["weekday"] = df["date_of_win"].dt.day_name()
    df["month"] = df["date_of_win"].dt.to_period("M").astype(str)
    df["prize_bucket"] = df["prize_amount_usd"].apply(bucketize)

    min_d = df["date_of_win"].min()
    max_d = df["date_of_win"].max()

    progress_bar.progress(1.0)
    status.success("Done.")

    st.info(
        f"Requested: {date_from_s} to {date_to_s} | "
        f"Returned (min, max date_of_win): "
        f"{min_d.date() if pd.notna(min_d) else 'N/A'} to {max_d.date() if pd.notna(max_d) else 'N/A'}"
    )

    # KPIs
    total_rows = len(df)
    total_payout = df["prize_amount_usd"].sum(skipna=True)
    median_payout = df["prize_amount_usd"].median(skipna=True)
    unique_retailers = df["retailer"].nunique(dropna=True) if "retailer" in df.columns else 0
    unique_cities = df["retailer_location"].nunique(dropna=True) if "retailer_location" in df.columns else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Winning entries", fmt_count(total_rows))
    k2.metric("Total payout", fmt_dollar(total_payout))
    k3.metric("Median prize", fmt_dollar(median_payout))
    k4.metric("Retailers", fmt_count(unique_retailers))
    k5.metric("Cities", fmt_count(unique_cities))

    st.divider()

    tab_overview, tab_places, tab_games, tab_time, tab_data = st.tabs(
        ["Overview", "Places", "Games", "Time", "Raw data"]
    )

    with tab_overview:
        st.subheader("Prize bucket mix")
        bucket_stats = (
            df.groupby("prize_bucket")
            .agg(wins=("prize_amount_usd", "count"),
                 total_payout=("prize_amount_usd", "sum"),
                 median_payout=("prize_amount_usd", "median"))
            .sort_values("wins", ascending=False)
        )
        st.bar_chart(bucket_stats["wins"])

        show = bucket_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        show["median_payout"] = show["median_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

    with tab_places:
        st.subheader("Wins by city")
        city_stats = (
            df.groupby("retailer_location")
            .agg(wins=("prize_amount_usd", "count"),
                 total_payout=("prize_amount_usd", "sum"),
                 avg_payout=("prize_amount_usd", "mean"))
            .sort_values("wins", ascending=False)
        )
        show = city_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

        st.subheader("Top retailers")
        retailer_stats = (
            df.groupby("retailer")
            .agg(wins=("prize_amount_usd", "count"),
                 total_payout=("prize_amount_usd", "sum"),
                 avg_payout=("prize_amount_usd", "mean"))
            .sort_values("wins", ascending=False)
        )
        show = retailer_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show.head(50), use_container_width=True)

    with tab_games:
        st.subheader("Top games by number of entries")
        game_stats = (
            df.groupby("name")
            .agg(wins=("prize_amount_usd", "count"),
                 median_payout=("prize_amount_usd", "median"),
                 avg_payout=("prize_amount_usd", "mean"))
            .sort_values("wins", ascending=False)
        )
        show = game_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["median_payout"] = show["median_payout"].map(fmt_dollar)
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show.head(50), use_container_width=True)

    with tab_time:
        st.subheader("Wins by day of week")
        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekday_stats = (
            df.groupby("weekday")
            .agg(wins=("prize_amount_usd", "count"),
                 avg_payout=("prize_amount_usd", "mean"))
            .reindex(order)
        )
        st.bar_chart(weekday_stats["wins"])

        show = weekday_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

        st.subheader("Monthly trend")
        monthly = (
            df.groupby("month")
            .agg(wins=("prize_amount_usd", "count"),
                 total_payout=("prize_amount_usd", "sum"))
            .sort_index()
        )
        st.line_chart(monthly["wins"])

        show = monthly.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

    with tab_data:
        st.download_button(
            "Download CSV",
            data=df.to_csv(index=False),
            file_name="masslottery_winners.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.dataframe(df, use_container_width=True, height=520)
