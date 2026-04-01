"""
Phase 0 — Data Generation Script
WBD APAC — Title Performance Analyst
Generates ~170K rows of realistic Max/HBO Max streaming data in DuckDB

Tables:
  titles               —  59 titles (series + movies)
  viewership_daily     —  ~51K rows (title × market × day)
  viewership_episode   —  ~3.4K rows (title × episode × day)
  subscriber_viewing   —  ~37K rows (subscriber × title × date)
  title_benchmarks     —  59 rows  (genre averages + comparable title refs)

Markets: AU, SG, HK, IN, JP, KR, TW, TH, PH, MY
Date range: 90 days ending 31 March 2026

─────────────────────────────────────────────────────────────────────
EXTENSION ZONE GUIDE
─────────────────────────────────────────────────────────────────────
Each table has an EXTENSION ZONE block at the bottom of its section.
To add a new column:
  1. Add the value to the row dict inside the loop (search "EXT-ADD")
  2. Uncomment the matching line in the EXTENSION ZONE block below it
  3. Re-run this script — DuckDB will pick up the new column automatically

Future columns already pre-built and ready to activate:
  titles             → content_rating, country_of_origin, available_since_date
  viewership_daily   → device_type, new_viewer_starts, avg_session_duration_mins
  viewership_episode → market, season_number
  subscriber_viewing → device_type, days_since_subscription_start
  New table ready    → viewership_hourly (activates C2: time-of-day analysis)
─────────────────────────────────────────────────────────────────────
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

random.seed(42)
np.random.seed(42)

DB_PATH = os.path.join(os.path.dirname(__file__), "title_performance.duckdb")

print("=" * 65)
print("WBD APAC — Title Performance Data Generation")
print("Max / HBO Max Streaming | 10 APAC Markets")
print("=" * 65)

con = duckdb.connect(DB_PATH)
print(f"\n✓ Connected to DuckDB at: {DB_PATH}")


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

MARKETS = ["AU", "SG", "HK", "IN", "JP", "KR", "TW", "TH", "PH", "MY"]

# Base audience size multiplier per market (AU = largest SVOD base in APAC)
MARKET_SIZE = {
    "AU": 1.40, "SG": 1.20, "HK": 0.75, "IN": 0.90,
    "JP": 1.10, "KR": 0.95, "TW": 0.80, "TH": 0.70,
    "PH": 0.65, "MY": 0.65,
}

# Completion rate affinity per market (JP/KR very high, IN moderate)
MARKET_COMPLETION = {
    "AU": 1.05, "SG": 1.00, "HK": 0.98, "IN": 0.88,
    "JP": 1.12, "KR": 1.08, "TW": 1.02, "TH": 0.92,
    "PH": 0.90, "MY": 0.91,
}

END_DATE   = datetime(2026, 3, 31)
START_DATE = END_DATE - timedelta(days=89)
DATE_RANGE = [START_DATE + timedelta(days=i) for i in range(90)]

# ─────────────────────────────────────────────────────────────
# EXTENSION ZONE — CONSTANTS
# ─────────────────────────────────────────────────────────────
# Uncomment these when activating device_type or hourly columns

# DEVICE_TYPES   = ["Smart TV", "Mobile", "Tablet", "Web Browser", "Games Console"]
# DEVICE_WEIGHTS = [0.42, 0.28, 0.12, 0.12, 0.06]

# HOURS_PRIME      = list(range(19, 23))   # 7pm–11pm
# HOURS_DAYTIME    = list(range(10, 17))   # 10am–5pm
# HOURS_LATENIGHT  = list(range(23, 24)) + list(range(0, 2))


# ─────────────────────────────────────────────────────────────
# STEP 1: TITLES TABLE (59 titles)
# Columns: title_id, title_name, genre, format, release_date,
#          seasons, episodes_count, language, is_hbo_original
# ─────────────────────────────────────────────────────────────

# fmt: (title_id, title_name, genre, format, release_date, seasons, episodes_count, language, is_hbo_original)
TITLES_RAW = [
    # Drama — HBO Originals (English)
    (1,  "House of the Dragon S1",       "Drama",       "Series", "2022-08-21", 1, 10,  "EN", 1),
    (2,  "House of the Dragon S2",       "Drama",       "Series", "2024-06-16", 1, 8,   "EN", 1),
    (3,  "The White Lotus S1",           "Drama",       "Series", "2021-07-11", 1, 6,   "EN", 1),
    (4,  "The White Lotus S2",           "Drama",       "Series", "2022-10-30", 1, 7,   "EN", 1),
    (5,  "The White Lotus S3",           "Drama",       "Series", "2025-02-16", 1, 8,   "EN", 1),
    (6,  "Succession S4",                "Drama",       "Series", "2023-03-26", 1, 10,  "EN", 1),
    (7,  "True Detective Night Country", "Drama",       "Series", "2024-01-14", 1, 6,   "EN", 1),
    (8,  "Sharp Objects",                "Drama",       "Series", "2018-07-08", 1, 8,   "EN", 1),
    (9,  "Big Little Lies S1",           "Drama",       "Series", "2017-02-19", 1, 7,   "EN", 1),
    (10, "Mare of Easttown",             "Drama",       "Series", "2021-04-18", 1, 7,   "EN", 1),
    (11, "Euphoria S1",                  "Drama",       "Series", "2019-06-16", 1, 8,   "EN", 1),
    (12, "Euphoria S2",                  "Drama",       "Series", "2022-01-09", 1, 8,   "EN", 1),
    (13, "Industry S1",                  "Drama",       "Series", "2020-11-09", 1, 8,   "EN", 1),
    (14, "Industry S2",                  "Drama",       "Series", "2022-08-01", 1, 8,   "EN", 1),
    (15, "The Idol",                     "Drama",       "Series", "2023-06-04", 1, 5,   "EN", 1),
    # Fantasy / Sci-Fi
    (16, "Game of Thrones S1-8",         "Fantasy",     "Series", "2011-04-17", 8, 73,  "EN", 1),
    (17, "The Last of Us S1",            "Fantasy",     "Series", "2023-01-15", 1, 9,   "EN", 1),
    (18, "The Last of Us S2",            "Fantasy",     "Series", "2025-04-13", 1, 7,   "EN", 1),
    (19, "Westworld S1",                 "Sci-Fi",      "Series", "2016-10-02", 1, 10,  "EN", 1),
    (20, "His Dark Materials S1",        "Fantasy",     "Series", "2019-11-03", 1, 8,   "EN", 1),
    (21, "Station Eleven",               "Sci-Fi",      "Series", "2021-12-16", 1, 10,  "EN", 1),
    (22, "Lovecraft Country",            "Horror",      "Series", "2020-08-16", 1, 10,  "EN", 1),
    (23, "The Nevers",                   "Sci-Fi",      "Series", "2021-04-11", 1, 12,  "EN", 1),
    # Crime / Thriller
    (24, "The Wire S1-5",                "Crime",       "Series", "2002-06-02", 5, 60,  "EN", 1),
    (25, "Boardwalk Empire S1",          "Crime",       "Series", "2010-09-19", 1, 12,  "EN", 1),
    (26, "Watchmen",                     "Crime",       "Series", "2019-10-20", 1, 9,   "EN", 1),
    (27, "The Undoing",                  "Thriller",    "Series", "2020-10-25", 1, 6,   "EN", 1),
    (28, "True Blood S1",                "Horror",      "Series", "2008-09-07", 1, 12,  "EN", 1),
    (29, "Perry Mason S1",               "Crime",       "Series", "2020-06-21", 1, 8,   "EN", 1),
    (30, "We Own This City",             "Crime",       "Series", "2022-04-25", 1, 6,   "EN", 1),
    # Comedy
    (31, "Barry S1",                     "Comedy",      "Series", "2018-03-25", 1, 8,   "EN", 1),
    (32, "Barry S2",                     "Comedy",      "Series", "2019-03-31", 1, 8,   "EN", 1),
    (33, "Barry S3",                     "Comedy",      "Series", "2022-04-24", 1, 8,   "EN", 1),
    (34, "Hacks S1",                     "Comedy",      "Series", "2021-05-13", 1, 10,  "EN", 1),
    (35, "Hacks S2",                     "Comedy",      "Series", "2023-05-04", 1, 8,   "EN", 1),
    (36, "Curb Your Enthusiasm S12",     "Comedy",      "Series", "2024-02-04", 1, 10,  "EN", 1),
    (37, "The Flight Attendant S1",      "Comedy",      "Series", "2020-11-26", 1, 8,   "EN", 1),
    (38, "Insecure S5",                  "Comedy",      "Series", "2021-10-24", 1, 10,  "EN", 1),
    # Documentary
    (39, "The Jinx Part 2",              "Documentary", "Series", "2024-04-21", 1, 6,   "EN", 1),
    (40, "Allen v. Farrow",              "Documentary", "Series", "2021-02-21", 1, 4,   "EN", 1),
    (41, "McEnroe",                      "Documentary", "Movie",  "2024-07-19", 0, 1,   "EN", 0),
    (42, "The Vow S1",                   "Documentary", "Series", "2020-08-23", 1, 9,   "EN", 0),
    (43, "Atlanta's Missing Murdered",   "Documentary", "Series", "2020-05-01", 1, 6,   "EN", 1),
    # Asian Originals — multilingual
    (44, "Pachinko S1",                  "K-Drama",     "Series", "2022-03-25", 1, 8,   "KO", 1),
    (45, "Pachinko S2",                  "K-Drama",     "Series", "2024-08-23", 1, 8,   "KO", 1),
    (46, "My Brilliant Friend S4",       "Drama",       "Series", "2024-02-05", 1, 8,   "IT", 1),
    (47, "Tokyo Vice S1",                "Crime",       "Series", "2022-04-07", 1, 8,   "JA", 1),
    (48, "Tokyo Vice S2",                "Crime",       "Series", "2024-02-08", 1, 9,   "JA", 1),
    (49, "The Sympathizer",              "Drama",       "Series", "2024-04-14", 1, 7,   "EN", 1),
    (50, "Expats",                       "Drama",       "Series", "2024-01-26", 1, 6,   "EN", 1),
    # Movies — WB Theatrical (licensed, not originals)
    (51, "Dune Part One",                "Sci-Fi",      "Movie",  "2021-10-22", 0, 1,   "EN", 0),
    (52, "Dune Part Two",                "Sci-Fi",      "Movie",  "2024-03-01", 0, 1,   "EN", 0),
    (53, "The Batman",                   "Action",      "Movie",  "2022-03-04", 0, 1,   "EN", 0),
    (54, "Barbie",                       "Comedy",      "Movie",  "2023-07-21", 0, 1,   "EN", 0),
    (55, "Wonka",                        "Comedy",      "Movie",  "2023-12-15", 0, 1,   "EN", 0),
    (56, "Aquaman Lost Kingdom",         "Action",      "Movie",  "2023-12-22", 0, 1,   "EN", 0),
    (57, "The Flash",                    "Action",      "Movie",  "2023-06-16", 0, 1,   "EN", 0),
    (58, "Elvis",                        "Drama",       "Movie",  "2022-06-24", 0, 1,   "EN", 0),
    (59, "Oppenheimer",                  "Drama",       "Movie",  "2023-07-21", 0, 1,   "EN", 0),
]

titles_df = pd.DataFrame(TITLES_RAW, columns=[
    "title_id", "title_name", "genre", "format", "release_date",
    "seasons", "episodes_count",
    # ── V1 additions ──
    "language",        # Primary audio language: EN / KO / JA / IT / etc.
    "is_hbo_original", # 1 = HBO/Max Original  |  0 = Licensed/Theatrical
])

# ── EXTENSION ZONE — titles ────────────────────────────────────────────────
# To activate: add value to each tuple in TITLES_RAW, then uncomment below.
#
# "content_rating"      — e.g. "MA15+", "M", "PG", "R"
#                         Needed for: audience segmentation, parental controls analysis
#
# "country_of_origin"   — e.g. "US", "KR", "JP", "IT"
#                         Needed for: local vs. imported content performance split
#
# "available_since_date"— Date title became available on Max APAC (≠ global release_date)
#                         Needed for: accurate day-1/7/30 launch tracking in APAC
#
# "production_budget_usd" — Estimated production budget (millions)
#                           Needed for: Project 4 — Content ROI Attribution Engine
#
# "acquisition_cost_usd"  — What WBD paid to license this title for APAC
#                           Needed for: Project 4 — Content ROI Attribution Engine
# ──────────────────────────────────────────────────────────────────────────

con.execute("DROP TABLE IF EXISTS titles")
con.execute("CREATE TABLE titles AS SELECT * FROM titles_df")
print(f"✓ titles: {len(titles_df)} rows  "
      f"[columns: {', '.join(titles_df.columns)}]")


# ─────────────────────────────────────────────────────────────
# PERFORMANCE TIERS — controls realistic behaviour per title
# ─────────────────────────────────────────────────────────────
#  champion     — high starts, high completion, positive WoW trend
#  solid        — decent starts, good completion, stable
#  underperform — low starts OR low completion (below genre benchmark)
#  sleeper      — low starts but very high completion (niche, word-of-mouth)
#  at_risk      — started strong, clearly declining WoW
#  new_launch   — recent release, building momentum

TITLE_TIERS = {
    2: "champion", 5: "champion", 17: "champion", 18: "champion",
    52: "champion", 54: "champion", 6: "champion",
    1: "solid", 3: "solid", 4: "solid", 16: "solid", 24: "solid",
    31: "solid", 51: "solid", 53: "solid", 26: "solid", 44: "solid", 47: "solid",
    15: "underperform", 23: "underperform", 56: "underperform",
    57: "underperform", 28: "underperform",
    10: "sleeper", 27: "sleeper", 30: "sleeper", 50: "sleeper", 49: "sleeper",
    12: "at_risk", 14: "at_risk", 36: "at_risk", 20: "at_risk",
    45: "new_launch", 48: "new_launch", 39: "new_launch",
}

def get_tier(title_id):
    return TITLE_TIERS.get(title_id, "solid")

TIER_PARAMS = {
    "champion":    {"starts": 8500,  "completion": 0.74, "trend": +0.03},
    "solid":       {"starts": 4200,  "completion": 0.62, "trend": +0.00},
    "underperform":{"starts": 1800,  "completion": 0.38, "trend": -0.01},
    "sleeper":     {"starts": 900,   "completion": 0.81, "trend": +0.01},
    "at_risk":     {"starts": 3500,  "completion": 0.55, "trend": -0.04},
    "new_launch":  {"starts": 2200,  "completion": 0.65, "trend": +0.05},
}


# ─────────────────────────────────────────────────────────────
# HELPER: APAC seasonality
# ─────────────────────────────────────────────────────────────

def apac_seasonality(date):
    """Weekend viewing spike + CNY seasonal bump."""
    factor = 1.0
    if date.weekday() in (4, 5):   # Friday/Saturday — peak
        factor *= 1.22
    elif date.weekday() == 6:       # Sunday — strong
        factor *= 1.15
    elif date.weekday() == 0:       # Monday — dip
        factor *= 0.88
    if date.month == 1 and date.day >= 22:   # CNY run-up
        factor *= 1.18
    elif date.month == 2 and date.day <= 10: # CNY peak
        factor *= 1.15
    return factor


# ─────────────────────────────────────────────────────────────
# STEP 2: VIEWERSHIP_DAILY
# Columns: date, title_id, market, starts, completions,
#          watch_time_mins, completion_rate,
#          unique_viewers, returning_viewers
# ─────────────────────────────────────────────────────────────

print("\n⏳ Generating viewership_daily...")
daily_rows = []

for _, title in titles_df.iterrows():
    tid    = title["title_id"]
    tier   = get_tier(tid)
    params = TIER_PARAMS[tier]

    try:
        rel_date = datetime.strptime(title["release_date"], "%Y-%m-%d")
    except Exception:
        rel_date = START_DATE

    for market in MARKETS:
        msize = MARKET_SIZE[market]
        mcomp = MARKET_COMPLETION[market]

        base_starts = params["starts"] * msize
        base_comp   = min(0.95, params["completion"] * mcomp)
        trend       = params["trend"]

        # Market-specific title affinities
        if tid in (3, 4, 5) and market == "IN":
            base_starts *= 0.50; base_comp *= 0.85
        if tid in (44, 45, 49, 50) and market in ("KR", "JP"):
            base_starts *= 1.60; base_comp *= 1.08
        if tid in (47, 48) and market in ("JP", "KR", "TW"):
            base_starts *= 1.45
        if tid in (54, 55) and market in ("IN", "PH", "TH"):
            base_starts *= 1.30

        for day_num, date in enumerate(DATE_RANGE):
            if date < rel_date:
                continue
            if random.random() < 0.04:  # ~4% natural data gaps
                continue

            season       = apac_seasonality(date)
            week         = day_num // 7
            trend_factor = max(0.3, min(2.5, 1.0 + trend * week))

            days_since_release = (date - rel_date).days
            library_decay = (
                max(0.2, 1.0 - (days_since_release - 365) * 0.001)
                if days_since_release > 365 else 1.0
            )

            starts = max(1, int(
                base_starts * season * trend_factor * library_decay
                * random.uniform(0.88, 1.12)
            ))
            comp_rate = min(0.97, max(0.05,
                base_comp * random.uniform(0.93, 1.07)
            ))
            completions      = int(starts * comp_rate)
            watch_time_mins  = int(starts * comp_rate * random.uniform(42, 68))
            unique_viewers   = int(starts * random.uniform(0.72, 0.95))
            returning_viewers= int(unique_viewers * random.uniform(0.20, 0.55))

            # EXT-ADD: uncomment the matching EXTENSION ZONE lines below to activate
            row = {
                "date":               date.strftime("%Y-%m-%d"),
                "title_id":           tid,
                "market":             market,
                "starts":             starts,
                "completions":        completions,
                "watch_time_mins":    watch_time_mins,
                "completion_rate":    round(comp_rate, 4),
                "unique_viewers":     unique_viewers,
                "returning_viewers":  returning_viewers,
                # "device_type":      random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0],
                # "new_viewer_starts":  int(starts * random.uniform(0.30, 0.60)),
                # "avg_session_duration_mins": round(random.uniform(22, 55), 1),
            }
            daily_rows.append(row)

daily_df = pd.DataFrame(daily_rows)
con.execute("DROP TABLE IF EXISTS viewership_daily")
con.execute("CREATE TABLE viewership_daily AS SELECT * FROM daily_df")
print(f"✓ viewership_daily: {len(daily_df):,} rows  "
      f"[columns: {', '.join(daily_df.columns)}]")

# ── EXTENSION ZONE — viewership_daily ─────────────────────────────────────
# To activate: uncomment the matching "EXT-ADD" line in the loop above.
#
# "device_type"               — Smart TV / Mobile / Tablet / Web / Console
#                               Needs: DEVICE_TYPES + DEVICE_WEIGHTS constants above
#                               Enables: device mix analysis per title/market
#
# "new_viewer_starts"         — Starts from first-time viewers of this title (that day)
#                               Enables: discovery rate vs. binge-return analysis
#
# "avg_session_duration_mins" — Average watch session length in minutes
#                               Enables: engagement depth analysis (not just starts/completions)
# ──────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# STEP 3: VIEWERSHIP_EPISODE
# Columns: date, title_id, episode_number,
#          starts, completions, avg_watch_time_mins, drop_off_point_pct
# ─────────────────────────────────────────────────────────────

print("⏳ Generating viewership_episode...")
episode_rows = []

SERIES_TITLES = titles_df[
    (titles_df["format"] == "Series") & (titles_df["episodes_count"] > 1)
].head(40)

for _, title in SERIES_TITLES.iterrows():
    tid    = title["title_id"]
    tier   = get_tier(tid)
    params = TIER_PARAMS[tier]
    n_eps  = min(int(title["episodes_count"]), 12)

    try:
        rel_date = datetime.strptime(title["release_date"], "%Y-%m-%d")
    except Exception:
        rel_date = START_DATE

    ep_base_starts = params["starts"] * 8
    ep_base_comp   = params["completion"]

    ep_multipliers = []
    for ep in range(1, n_eps + 1):
        if ep == 1:
            mult = 1.0
        elif ep == n_eps:
            mult = random.uniform(0.55, 0.80)
        else:
            decay = 1.0 - (ep - 1) * random.uniform(0.04, 0.09)
            mult  = max(0.3, decay)
        ep_multipliers.append(mult)

    drop_ep = None
    if tier == "underperform":
        drop_ep = random.randint(2, 4)
        for i in range(drop_ep, n_eps):
            ep_multipliers[i] *= random.uniform(0.40, 0.60)

    ep_dates = [rel_date + timedelta(days=d) for d in range(0, 45, 3)]
    ep_dates = [d for d in ep_dates if START_DATE <= d <= END_DATE]
    if not ep_dates:
        ep_dates = DATE_RANGE[::5][:10]

    for ep_num, ep_mult in enumerate(ep_multipliers, 1):
        for date in ep_dates:
            if date < rel_date:
                continue
            starts = max(1, int(
                ep_base_starts * ep_mult
                * apac_seasonality(date)
                * random.uniform(0.88, 1.12)
            ))
            ep_comp     = min(0.97, ep_base_comp * ep_mult * random.uniform(0.90, 1.10))
            completions = int(starts * ep_comp)
            avg_watch   = random.uniform(35, 58)

            if tier == "underperform" and drop_ep and ep_num >= drop_ep:
                drop_pct = random.uniform(0.18, 0.42)
            elif tier in ("champion", "sleeper"):
                drop_pct = random.uniform(0.72, 0.95)
            else:
                drop_pct = random.uniform(0.45, 0.75)

            # EXT-ADD: uncomment matching EXTENSION ZONE lines below to activate
            row = {
                "date":                date.strftime("%Y-%m-%d"),
                "title_id":            tid,
                "episode_number":      ep_num,
                "starts":              starts,
                "completions":         completions,
                "avg_watch_time_mins": round(avg_watch, 1),
                "drop_off_point_pct":  round(drop_pct, 4),
                # "market":            random.choice(MARKETS),
                # "season_number":     int(title["seasons"]) if title["seasons"] > 0 else 1,
            }
            episode_rows.append(row)

episode_df = pd.DataFrame(episode_rows)
con.execute("DROP TABLE IF EXISTS viewership_episode")
con.execute("CREATE TABLE viewership_episode AS SELECT * FROM episode_df")
print(f"✓ viewership_episode: {len(episode_df):,} rows  "
      f"[columns: {', '.join(episode_df.columns)}]")

# ── EXTENSION ZONE — viewership_episode ───────────────────────────────────
# To activate: uncomment the matching "EXT-ADD" line in the loop above.
#
# "market"         — Which APAC market this episode record belongs to
#                    Enables: episode-level market splits (e.g. which markets drop off at Ep3)
#                    Note: currently episode data is aggregated across all markets.
#                    Activating this will multiply rows by 10 (~34K rows total)
#
# "season_number"  — Season number for multi-season titles
#                    Enables: season-over-season episode comparison
# ──────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# STEP 4: SUBSCRIBER_VIEWING
# Columns: subscriber_id, title_id, date, watch_time_mins,
#          completed, subscriber_segment, plan_type, market,
#          is_churned, subscription_start_date
# ─────────────────────────────────────────────────────────────

print("⏳ Generating subscriber_viewing...")

SEGMENTS    = ["New", "Returning", "Lapsed-Reactivated", "At-Risk", "Loyal"]
SEG_WEIGHTS = [0.25, 0.35, 0.10, 0.15, 0.15]
PLAN_TYPES  = ["Monthly-Standard", "Monthly-Premium", "Annual-Standard", "Annual-Premium"]
PLAN_WEIGHTS= [0.35, 0.25, 0.25, 0.15]

# Churn probability by segment (used for is_churned flag)
CHURN_PROB = {
    "New":                0.05,
    "Returning":          0.08,
    "Lapsed-Reactivated": 0.15,
    "At-Risk":            0.35,
    "Loyal":              0.02,
}

# How long ago they subscribed (days before END_DATE)
TENURE_RANGE = {
    "New":                (5,   60),
    "Returning":          (365, 1095),
    "Lapsed-Reactivated": (10,  30),
    "At-Risk":            (180, 540),
    "Loyal":              (730, 1825),
}

N_SUBSCRIBERS = 2200
subscribers   = []

for sid in range(1, N_SUBSCRIBERS + 1):
    market  = random.choices(MARKETS, weights=[14, 12, 8, 9, 11, 10, 8, 7, 7, 7])[0]
    segment = random.choices(SEGMENTS, weights=SEG_WEIGHTS)[0]
    plan    = random.choices(PLAN_TYPES, weights=PLAN_WEIGHTS)[0]

    n_titles_watched = {
        "Loyal":              random.randint(8, 18),
        "Returning":          random.randint(4, 12),
        "New":                random.randint(2, 7),
        "At-Risk":            random.randint(1, 4),
        "Lapsed-Reactivated": random.randint(2, 8),
    }[segment]

    # is_churned: stochastic by segment
    is_churned = 1 if random.random() < CHURN_PROB[segment] else 0

    # subscription_start_date: realistic tenure per segment
    lo, hi = TENURE_RANGE[segment]
    days_ago = random.randint(lo, hi)
    sub_start = (END_DATE - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    subscribers.append((sid, market, segment, plan, n_titles_watched, is_churned, sub_start))

sub_rows  = []
title_ids = list(titles_df["title_id"])

for (sid, market, segment, plan, n_titles, is_churned, sub_start) in subscribers:
    watched_titles = random.sample(title_ids, min(n_titles, len(title_ids)))
    for tid in watched_titles:
        tier   = get_tier(tid)
        params = TIER_PARAMS[tier]
        n_sessions = random.randint(1, 4)

        for _ in range(n_sessions):
            date     = random.choice(DATE_RANGE)
            comp_base = params["completion"]
            if segment == "Loyal":
                comp_base = min(0.97, comp_base * 1.15)
            elif segment == "At-Risk":
                comp_base = max(0.10, comp_base * 0.65)
            elif segment == "New":
                comp_base *= 0.90

            completed  = 1 if random.random() < comp_base else 0
            watch_mins = random.uniform(35, 68) if completed else random.uniform(8, 35)

            # EXT-ADD: uncomment matching EXTENSION ZONE lines below to activate
            row = {
                "subscriber_id":        sid,
                "title_id":             tid,
                "date":                 date.strftime("%Y-%m-%d"),
                "watch_time_mins":      round(watch_mins, 1),
                "completed":            completed,
                "subscriber_segment":   segment,
                "plan_type":            plan,
                "market":               market,
                # ── V1 additions ──
                "is_churned":           is_churned,
                "subscription_start_date": sub_start,
                # ── Extension zone (uncomment to activate) ──
                # "device_type":        random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0],
                # "days_since_sub_start": (datetime.strptime(date.strftime("%Y-%m-%d"), "%Y-%m-%d") - datetime.strptime(sub_start, "%Y-%m-%d")).days,
            }
            sub_rows.append(row)

sub_df = pd.DataFrame(sub_rows)
con.execute("DROP TABLE IF EXISTS subscriber_viewing")
con.execute("CREATE TABLE subscriber_viewing AS SELECT * FROM sub_df")
print(f"✓ subscriber_viewing: {len(sub_df):,} rows  "
      f"[columns: {', '.join(sub_df.columns)}]")

# ── EXTENSION ZONE — subscriber_viewing ───────────────────────────────────
# To activate: uncomment the matching "EXT-ADD" line in the loop above.
#
# "device_type"              — Smart TV / Mobile / Tablet / Web / Console
#                              Needs: DEVICE_TYPES constant (uncomment at top)
#                              Enables: device-level engagement by segment
#
# "days_since_sub_start"     — How many days into their subscription this view happened
#                              Enables: lifecycle analysis (do people churn after day 30?)
#
# Future columns to add when real data becomes available:
# "referral_source"          — How the subscriber first signed up (Organic/Paid/Bundle)
# "cancel_reason"            — For churned subscribers: why they left (survey data)
# "household_size"           — Number of profiles on the account
# ──────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# STEP 5: TITLE_BENCHMARKS
# Columns: title_id, comparable_title_ids,
#          genre_avg_completion_rate,
#          genre_avg_starts_day7, genre_avg_starts_day30
# ─────────────────────────────────────────────────────────────

print("⏳ Generating title_benchmarks...")

genre_stats = (
    daily_df.merge(titles_df[["title_id", "genre"]], on="title_id")
    .groupby("genre")
    .agg(
        genre_avg_completion_rate=("completion_rate", "mean"),
        genre_avg_starts_day7    =("starts", lambda x: x.mean() * 7),
        genre_avg_starts_day30   =("starts", lambda x: x.mean() * 30),
    )
    .reset_index()
    .round(4)
)

benchmark_rows = []
for _, title in titles_df.iterrows():
    tid   = title["title_id"]
    genre = title["genre"]

    genre_row = genre_stats[genre_stats["genre"] == genre]
    if len(genre_row) == 0:
        gavg_comp = 0.58;  gavg_d7 = 25000;  gavg_d30 = 95000
    else:
        gavg_comp = float(genre_row["genre_avg_completion_rate"].iloc[0])
        gavg_d7   = float(genre_row["genre_avg_starts_day7"].iloc[0])
        gavg_d30  = float(genre_row["genre_avg_starts_day30"].iloc[0])

    same_genre  = titles_df[
        (titles_df["genre"] == genre) & (titles_df["title_id"] != tid)
    ]["title_id"].tolist()
    comparables = random.sample(same_genre, min(3, len(same_genre)))

    benchmark_rows.append({
        "title_id":                  tid,
        "comparable_title_ids":      ",".join(str(c) for c in comparables),
        "genre_avg_completion_rate": round(gavg_comp, 4),
        "genre_avg_starts_day7":     int(gavg_d7),
        "genre_avg_starts_day30":    int(gavg_d30),
    })

bench_df = pd.DataFrame(benchmark_rows)
con.execute("DROP TABLE IF EXISTS title_benchmarks")
con.execute("CREATE TABLE title_benchmarks AS SELECT * FROM bench_df")
print(f"✓ title_benchmarks: {len(bench_df)} rows  "
      f"[columns: {', '.join(bench_df.columns)}]")

# ── EXTENSION ZONE — title_benchmarks ─────────────────────────────────────
# These require real data or additional simulation logic to activate.
#
# "language_avg_completion_rate" — Genre avg split by language (EN vs KO vs JA)
#                                  Enables: fairer benchmarking for non-English titles
#
# "format_avg_starts_day7"       — Genre avg split by format (Series vs Movie)
#                                  Enables: series vs. movie benchmarks in same genre
#
# "original_vs_licensed_avg"     — Separate benchmarks for originals vs theatrical
#                                  Needs: is_hbo_original from titles table (already exists)
# ──────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# EXTENSION ZONE — NEW TABLES (not yet built)
# ─────────────────────────────────────────────────────────────
#
# TABLE: viewership_hourly
# Purpose: Enables C2 ("What time of day is X most watched?")
# Activate: uncomment the block below, add HOURS constants at the top
# Estimated size: ~200K rows (59 titles × 10 markets × 24 hours × sample days)
#
# hourly_rows = []
# SAMPLE_DATES = DATE_RANGE[::7]  # one day per week (13 sample days)
# for _, title in titles_df.iterrows():
#     for market in MARKETS:
#         for date in SAMPLE_DATES:
#             for hour in range(24):
#                 if hour in HOURS_PRIME:
#                     weight = random.uniform(1.8, 3.0)
#                 elif hour in HOURS_DAYTIME:
#                     weight = random.uniform(0.8, 1.4)
#                 elif hour in HOURS_LATENIGHT:
#                     weight = random.uniform(0.6, 1.0)
#                 else:  # late night / early morning
#                     weight = random.uniform(0.1, 0.4)
#                 starts = max(0, int(50 * weight * MARKET_SIZE[market] * random.uniform(0.8, 1.2)))
#                 hourly_rows.append({
#                     "date": date.strftime("%Y-%m-%d"),
#                     "title_id": int(title["title_id"]),
#                     "market": market,
#                     "hour_of_day": hour,
#                     "starts": starts,
#                 })
# hourly_df = pd.DataFrame(hourly_rows)
# con.execute("DROP TABLE IF EXISTS viewership_hourly")
# con.execute("CREATE TABLE viewership_hourly AS SELECT * FROM hourly_df")
# print(f"✓ viewership_hourly: {len(hourly_df):,} rows")
#
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────

print("\n" + "=" * 65)
print("DATABASE SUMMARY — WBD APAC Title Performance")
print("=" * 65)

for table in ["titles", "viewership_daily", "viewership_episode",
              "subscriber_viewing", "title_benchmarks"]:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    cols  = con.execute(f"PRAGMA table_info({table})").fetchall()
    print(f"  {table:<30} {count:>8,} rows  ({len(cols)} columns)")

dates   = con.execute("SELECT MIN(date), MAX(date) FROM viewership_daily").fetchone()
markets = con.execute("SELECT COUNT(DISTINCT market) FROM viewership_daily").fetchone()[0]
tcount  = con.execute("SELECT COUNT(DISTINCT title_id) FROM viewership_daily").fetchone()[0]
genres  = con.execute("SELECT COUNT(DISTINCT genre) FROM titles").fetchone()[0]
langs   = con.execute("SELECT COUNT(DISTINCT language) FROM titles").fetchone()[0]
origs   = con.execute("SELECT COUNT(*) FROM titles WHERE is_hbo_original=1").fetchone()[0]
churn_n = con.execute("SELECT COUNT(DISTINCT subscriber_id) FROM subscriber_viewing WHERE is_churned=1").fetchone()[0]

print(f"\n  Date range:       {dates[0]}  →  {dates[1]}")
print(f"  Markets:          {markets} APAC markets")
print(f"  Titles tracked:   {tcount}")
print(f"  Genres:           {genres}")
print(f"  Languages:        {langs}  (EN / KO / JA / IT)")
print(f"  HBO Originals:    {origs} of 59 titles")
print(f"  Churned subs:     {churn_n} subscribers flagged")
print(f"\n✅ Database saved to: {DB_PATH}")
print("=" * 65)

# Sanity checks
print("\n📊 Top 5 titles by avg daily starts (AU market):")
top = con.execute("""
    SELECT t.title_name, t.language, t.is_hbo_original,
           ROUND(AVG(v.starts),0)           AS avg_daily_starts,
           ROUND(AVG(v.completion_rate)*100,1) AS avg_completion_pct
    FROM viewership_daily v
    JOIN titles t ON t.title_id = v.title_id
    WHERE v.market = 'AU'
    GROUP BY t.title_name, t.language, t.is_hbo_original
    ORDER BY avg_daily_starts DESC
    LIMIT 5
""").fetchall()
for r in top:
    orig = "Original" if r[2] == 1 else "Licensed"
    print(f"  [{r[1]}] [{orig}] {r[0]:<35} {int(r[3]):>6} starts/day  |  {r[4]}% completion")

print("\n📊 Underperformers (avg completion < 45%):")
under = con.execute("""
    SELECT t.title_name, ROUND(AVG(v.completion_rate)*100,1) AS avg_comp
    FROM viewership_daily v
    JOIN titles t ON t.title_id = v.title_id
    GROUP BY t.title_name
    HAVING avg_comp < 45
    ORDER BY avg_comp
    LIMIT 5
""").fetchall()
for r in under:
    print(f"  {r[0]:<35} {r[1]}% completion")

print("\n📊 Churn risk by subscriber segment:")
churn = con.execute("""
    SELECT subscriber_segment,
           COUNT(DISTINCT subscriber_id)                                     AS total_subs,
           COUNT(DISTINCT CASE WHEN is_churned=1 THEN subscriber_id END)     AS churned,
           ROUND(COUNT(DISTINCT CASE WHEN is_churned=1 THEN subscriber_id END)
                 * 100.0 / COUNT(DISTINCT subscriber_id), 1)                 AS churn_pct
    FROM subscriber_viewing
    GROUP BY subscriber_segment
    ORDER BY churn_pct DESC
""").fetchall()
for r in churn:
    print(f"  {r[0]:<22} {r[1]:>5} subs  |  {r[2]:>4} churned  |  {r[3]}% churn rate")

con.close()
print("\n✅ Done. Phase 0 complete.\n")
