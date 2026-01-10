import streamlit as st
import requests
import time
import pandas as pd
from datetime import date
from cities import ALL_CITIES
from datetime import date, timedelta



# ---------------- Page setup ----------------
st.set_page_config(page_title="Mass Lottery Winners", layout="wide")
st.title("Mass Lottery Winners")

st.caption(
    "Fetch Mass Lottery winning entries and analyze them by city, retailer, game, and time. "
    "Cities are optional, leave blank for all cities."
)

# ---------------- date filter ----------------

def first_day_of_month(d: date) -> date:
    return d.replace(day=1)

def add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, [31,
        29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    ][m - 1])
    return date(y, m, day)

def last_day_of_month(d: date) -> date:
    return add_months(first_day_of_month(d), 1) - timedelta(days=1)

def quarter_start(d: date) -> date:
    q = (d.month - 1) // 3  # 0..3
    m = q * 3 + 1
    return date(d.year, m, 1)
    
# ---------------- Sidebar ----------------
with st.sidebar:
    st.header("Filters")

    cities_selected = st.multiselect(
        "City",
        options=ALL_CITIES,
        default=["Quincy", "N Quincy"],
        help="Select one or more cities. Leave empty for all cities."
    )

    today = date.today()

    preset = st.selectbox(
        "Date range preset",
        [
            "Custom",
            "Last 7 days",
            "Last 30 days",
            "This month",
            "Last month",
            "This quarter",
            "Last quarter",
            "This year",
            "Last year",
        ],
        index=0
    )

    if preset == "Last 7 days":
        preset_from, preset_to = today - timedelta(days=7), today
    elif preset == "Last 30 days":
        preset_from, preset_to = today - timedelta(days=30), today
    elif preset == "This month":
        preset_from, preset_to = first_day_of_month(today), today
    elif preset == "Last month":
        lm = add_months(today, -1)
        preset_from, preset_to = first_day_of_month(lm), last_day_of_month(lm)
    elif preset == "This quarter":
        preset_from, preset_to = quarter_start(today), today
    elif preset == "Last quarter":
        this_q_start = quarter_start(today)
        last_q_end = this_q_start - timedelta(days=1)
        preset_from, preset_to = quarter_start(last_q_end), last_q_end
    elif preset == "This year":
        preset_from, preset_to = date(today.year, 1, 1), today
    elif preset == "Last year":
        preset_from, preset_to = date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)
    else:
        preset_from, preset_to = None, None

    date_from = st.date_input(
        "Date From",
        value=preset_from if preset_from else date(2026, 1, 1)
    )

    date_to = st.date_input(
        "Date To",
        value=preset_to if preset_to else date(2026, 1, 8)
    )

    run = st.button("Run", use_container_width=True)

# ---------------- Constants ----------------
SESSION = requests.Session()
API_URL = "https://www.masslottery.com/api/v1/winners/query"
PAGE_SIZE = 200
MAX_RETRIES = 3

status = st.empty()
progress_bar = st.progress(0)
debug_box = st.empty()


# ---------------- Helpers ----------------
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

# ---------------- Main ----------------
if run:
    if date_to < date_from:
        st.error("Date To must be on or after Date From.")
        st.stop()

    params = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "sort": "newestFirst",
    }
    if cities_selected:
        params["cities"] = ",".join(cities_selected)

    status.info("Starting scrape…")
    progress_bar.progress(0)

    # First page
    first, first_url = fetch_page(params, 0)
    total = int(first["totalNumberOfWinners"])
    all_rows = list(first["pageOfWinners"])

    debug_box.code(first_url, language="text")
    status.info(f"Total entries reported by API: {total:,}")

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

    # No deduplication, keep all rows
    df = pd.DataFrame(all_rows)

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
        f"Requested: {date_from.isoformat()} to {date_to.isoformat()} | "
        f"Returned (min, max date_of_win): "
        f"{min_d.date() if pd.notna(min_d) else 'N/A'} to {max_d.date() if pd.notna(max_d) else 'N/A'}"
    )

    # ---------------- KPIs ----------------
    total_rows = len(df)
    total_payout = df["prize_amount_usd"].sum(skipna=True)
    median_payout = df["prize_amount_usd"].median(skipna=True)
    unique_retailers = df["retailer"].nunique(dropna=True)
    unique_cities = df["retailer_location"].nunique(dropna=True)

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Winning entries", fmt_count(total_rows))
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

        show = bucket_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        show["median_payout"] = show["median_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

    with tab_places:
        city_stats = (
            df.groupby("retailer_location")
            .agg(
                wins=("prize_amount_usd", "count"),
                total_payout=("prize_amount_usd", "sum"),
                avg_payout=("prize_amount_usd", "mean"),
            )
            .sort_values("wins", ascending=False)
        )

        show = city_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

        retailer_stats = (
            df.groupby("retailer")
            .agg(
                wins=("prize_amount_usd", "count"),
                total_payout=("prize_amount_usd", "sum"),
                avg_payout=("prize_amount_usd", "mean"),
            )
            .sort_values("wins", ascending=False)
        )

        show = retailer_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["total_payout"] = show["total_payout"].map(fmt_dollar)
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show.head(50), use_container_width=True)

    with tab_games:
        game_stats = (
            df.groupby("name")
            .agg(
                wins=("prize_amount_usd", "count"),
                median_payout=("prize_amount_usd", "median"),
                avg_payout=("prize_amount_usd", "mean"),
            )
            .sort_values("wins", ascending=False)
        )

        show = game_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["median_payout"] = show["median_payout"].map(fmt_dollar)
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show.head(50), use_container_width=True)

    with tab_time:
        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        weekday_stats = (
        df.groupby("weekday")
          .agg(
              wins=("prize_amount_usd", "count"),
              avg_payout=("prize_amount_usd", "mean"),
          )
          .reindex(order, fill_value=0)
        )


        st.bar_chart(weekday_stats["wins"])

        show = weekday_stats.copy()
        show["wins"] = show["wins"].map(lambda x: f"{x:,}")
        show["avg_payout"] = show["avg_payout"].map(fmt_dollar)
        st.dataframe(show, use_container_width=True)

        monthly = (
            df.groupby("month")
            .agg(
                wins=("prize_amount_usd", "count"),
                total_payout=("prize_amount_usd", "sum"),
            )
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
