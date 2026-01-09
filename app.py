import streamlit as st
import requests
import time
import pandas as pd
from datetime import date

# ---------------- Page setup ----------------
st.set_page_config(page_title="Mass Lottery Winners", layout="wide")
st.title("Mass Lottery Winners")

st.caption(
    "Pull winner data from MassLottery, then explore trends by city, retailer, game, and time. "
    "Cities are optional, leave blank for all cities."
)

# ---------------- Sidebar controls ----------------
with st.sidebar:
    st.header("Filters")

    cities_input = st.text_area(
        "Cities (optional, comma separated)",
        value="Quincy, N Quincy",
        height=70,
        help="Example: Quincy, N Quincy. Leave blank for all cities."
    )

    date_from = st.date_input("Date From", value=date(2024, 1, 1))
    date_to = st.date_input("Date To", value=date(2026, 1, 8))

    run = st.button("Run", use_container_width=True)

# ---------------- Constants ----------------
SESSION = requests.Session()
API_URL = "https://www.masslottery.com/api/v1/winners/query"
PAGE_SIZE = 200
MAX_RETRIES = 3

# ---------------- Helpers ----------------
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
    if x < 100:
        return "< $100"
    if x < 300:
        return "$100 - $299"
    if x < 600:
        return "$300 - $599"
    if x < 1000:
        return "$600 - $999"
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

def fmt_dollar(x):
    return f"${x:,.0f}" if pd.notna(x) else "—"

def fmt_count(x):
    try:
        return f"{int(x):,}"
    except Exception:
        return "—"

# ---------------- Main ----------------
if run:
    if date_to < date_from:
        st.error("Date To must be on or after Date From.")
        st.stop()

    cities = [c.strip() for c in cities_input.split(",") if c.strip()]

    # NOTE: no prize_amounts param, this pulls ALL prize amounts
    params = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "sort": "newestFirst",
    }
    if cities:
        params["cities"] = ",".join(cities)

    status.info("Starting scrape…")
    progress_bar.progress(0)

    first, first_url = fetch_page(params, 0)
    total = int(first["totalNumberOfWinners"])
    all_rows = list(first["pageOfWinners"])

    debug_box.code(first_url, language="text")
    status.info(f"Total winners reported by API: {total:,}")

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
        status.info(f"Fetched {fetched_so_far:,} / {total:,}")
        time.sleep(0.02)

    # Deduplicate
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

    if df.empty:
        st.warning("No rows returned.")
        st.stop()

    # Clean types
    df["prize_amount_usd"] = pd.to_numeric(df.get("prize_amount_usd"), errors="coerce")
    df["date_of_win"] = pd.to_datetime(df.get("date_of_win"), errors="coerce")
    df["weekday"] = df["date_of_win"].dt.day_name()
    df["month"] = df["date_of_win"].dt.to_period("M").astype(str)
    df["prize_bucket"] = df["prize_amount_usd"].apply(bucketize)

    min_d = df["date_of_win"].min()
    max_d = df["date_of_win"].max()
    status.success("Done.")
    st.info(
        f"Returned date range: {min_d.date() if pd.notna(min_d) else 'N/A'} to {max_d.date() if pd.notna(max_d) else 'N/A'}"
    )

    # ---------------- KPI row (formatted) ----------------
    total_rows = len(df)
    total_payout = df["prize_amount_usd"].sum(skipna=True)
    median_payout = df["prize_amount_usd"].median(skipna=True)
    unique_retailers = df["retailer"].nunique(dropna=True) if "retailer" in df.columns else 0
    unique_cities = df["retailer_location"].nunique(dropna=True) if "retailer_location" in df.columns else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Wins", fmt_count(total_rows))
    k2.metric("Total payout", fmt_dollar(total_payout))
    k3.metric("Median prize", fmt_dollar(median_payout))
    k4.metric("Retailers", fmt_count(unique_retailers))
    k5.metric("Cities", fmt_count(unique_cities))

    st.divider()

    # ---------------- Tabs ----------------
    tab_overview, tab_places, tab_games, tab_time, tab_data = st.tabs(
        ["Overview", "Places", "Games", "Time", "Raw data"]
    )

    with tab_overview:
        st.subheader("Prize bucket mix")

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

        bucket_display = bucket_stats.copy()
        bucket_display["wins"] = bucket_display["wins"].map(lambda x: f"{x:,}")
        bucket_display["total_payout"] = bucket_display["total_payout"].map(fmt_dollar)
        bucket_display["median_payout"] = bucket_display["median_payout"].map(fmt_dollar)
        st.dataframe(bucket_display, use_container_width=True)

    with tab_places:
        st.subheader("Wins by city")

        city_stats = (
            df.groupby("retailer_location")
            .agg(
                wins=("prize_amount_usd", "count"),
                total_payout=("prize_amount_usd", "sum"),
                avg_payout=("prize_amount_usd", "mean"),
            )
            .sort_values("wins", ascending=False)
        )

        city_display = city_stats.copy()
        city_display["wins"] = city_display["wins"].map(lambda x: f"{x:,}")
        city_display["total_payout"] = city_display["total_payout"].map(fmt_dollar)
        city_display["avg_payout"] = city_display["avg_payout"].map(fmt_dollar)
        st.dataframe(city_display, use_container_width=True)

        st.subheader("Top winning retailers")

        retailer_stats = (
            df.groupby("retailer")
            .agg(
                wins=("prize_amount_usd", "count"),
                total_payout=("prize_amount_usd", "sum"),
                avg_payout=("prize_amount_usd", "mean"),
            )
            .sort_values("wins", ascending=False)
        )

        retailer_display = retailer_stats.copy()
        retailer_display["wins"] = retailer_display["wins"].map(lambda x: f"{x:,}")
        retailer_display["total_payout"] = retailer_display["total_payout"].map(fmt_dollar)
        retailer_display["avg_payout"] = retailer_display["avg_payout"].map(fmt_dollar)
        st.dataframe(retailer_display.head(50), use_container_width=True)

    with tab_games:
        st.subheader("Games that pay out most often")

        game_stats = (
            df.groupby("name")
            .agg(
                wins=("prize_amount_usd", "count"),
                median_payout=("prize_amount_usd", "median"),
                avg_payout=("prize_amount_usd", "mean"),
            )
            .sort_values("wins", ascending=False)
        )

        game_display = game_stats.copy()
        game_display["wins"] = game_display["wins"].map(lambda x: f"{x:,}")
        game_display["median_payout"] = game_display["median_payout"].map(fmt_dollar)
        game_display["avg_payout"] = game_display["avg_payout"].map(fmt_dollar)
        st.dataframe(game_display.head(50), use_container_width=True)

    with tab_time:
        st.subheader("Wins by day of week")

        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekday_stats = (
            df.groupby("weekday")
            .agg(wins=("prize_amount_usd", "count"), avg_payout=("prize_amount_usd", "mean"))
            .reindex(order)
        )

        st.bar_chart(weekday_stats["wins"])

        weekday_display = weekday_stats.copy()
        weekday_display["wins"] = weekday_display["wins"].map(lambda x: f"{x:,}")
        weekday_display["avg_payout"] = weekday_display["avg_payout"].map(fmt_dollar)
        st.dataframe(weekday_display, use_container_width=True)

        st.subheader("Monthly trend")

        monthly = (
            df.groupby("month")
            .agg(wins=("prize_amount_usd", "count"), total_payout=("prize_amount_usd", "sum"))
            .sort_index()
        )

        st.line_chart(monthly["wins"])

        monthly_display = monthly.copy()
        monthly_display["wins"] = monthly_display["wins"].map(lambda x: f"{x:,}")
        monthly_display["total_payout"] = monthly_display["total_payout"].map(fmt_dollar)
        st.dataframe(monthly_display, use_container_width=True)

    with tab_data:
        st.subheader("Download")
        st.download_button(
            "Download CSV",
            data=df.to_csv(index=False),
            file_name="masslottery_winners.csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.subheader("Preview")
        st.dataframe(df, use_container_width=True, height=520)
