"""Phase 13: DuckDB-backed historical explore page.

Reads parquet directly via DuckDB. No Spark, no Redis — pure batch surface.
Pre-canned queries cover the most useful analytical lookups; the free SQL box
lets analysts run anything they want.

DuckDB views are read-only by construction (parquet files on disk). The free
SQL box additionally rejects any DDL/DML keywords as defence in depth.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import config

st.set_page_config(page_title="F1 Explore", layout="wide")
st.title("Explore — historical OLAP")
st.caption("DuckDB queries directly over processed parquet. No Redis, no Kafka.")

LAP_FEATURES_GLOB = str(Path(config.LAP_FEATURES_PARQUET) / "*.parquet")
BASELINES_GLOB = str(Path(config.DEGRADATION_BASELINES_PARQUET) / "*.parquet")
REPLAY_PATH = str(config.RACE_REPLAY_EVENTS_PARQUET)

FORBIDDEN_SQL_KEYWORDS = (
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "ATTACH", "COPY", "PRAGMA", "CALL", "EXPORT",
)


@st.cache_resource
def get_conn() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute(f"CREATE OR REPLACE VIEW lap_features AS SELECT * FROM read_parquet('{LAP_FEATURES_GLOB}')")
    con.execute(f"CREATE OR REPLACE VIEW baselines AS SELECT * FROM read_parquet('{BASELINES_GLOB}')")
    if Path(REPLAY_PATH).exists():
        con.execute(f"CREATE OR REPLACE VIEW replay_events AS SELECT * FROM read_parquet('{REPLAY_PATH}')")
    return con


def safe_query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    upper = sql.upper()
    for kw in FORBIDDEN_SQL_KEYWORDS:
        if kw in upper:
            raise ValueError(f"Forbidden keyword in query: {kw}")
    return con.execute(sql).fetch_df()


CANNED_QUERIES: dict[str, str] = {
    "Driver pace by race (clean laps)": """
        SELECT year, race_name, driver_code,
               COUNT(*) AS clean_laps,
               ROUND(AVG(lap_time_sec), 3) AS avg_lap_time,
               ROUND(AVG(pace_delta), 3) AS avg_pace_delta
        FROM lap_features
        WHERE clean_lap
        GROUP BY year, race_name, driver_code
        ORDER BY year, race_name, avg_lap_time
    """,
    "Compound degradation by track temp bucket": """
        SELECT compound,
               CAST(FLOOR(track_temp_c / 5) * 5 AS INT) AS track_temp_bucket,
               COUNT(*) AS n,
               ROUND(AVG(lap_time_sec), 3) AS avg_lap_time,
               ROUND(AVG(pace_delta), 3) AS avg_pace_delta
        FROM lap_features
        WHERE clean_lap AND track_temp_c IS NOT NULL
        GROUP BY compound, track_temp_bucket
        ORDER BY compound, track_temp_bucket
    """,
    "Top anomalies (replay events, lowest scores)": """
        SELECT year, race_name, driver_code, lap_number, compound, tyre_life,
               ROUND(risk_score, 3) AS risk_score, risk_status,
               ROUND(anomaly_score, 4) AS anomaly_score
        FROM replay_events
        WHERE is_anomaly
        ORDER BY anomaly_score ASC
        LIMIT 25
    """,
    "Stint length distribution per compound": """
        SELECT compound,
               COUNT(*) AS stints,
               ROUND(AVG(laps_in_stint), 2) AS avg_length,
               MIN(laps_in_stint) AS min_length,
               MAX(laps_in_stint) AS max_length
        FROM (
            SELECT year, race_round, driver_code, stint, compound,
                   COUNT(*) AS laps_in_stint
            FROM lap_features
            GROUP BY year, race_round, driver_code, stint, compound
        )
        GROUP BY compound
        ORDER BY avg_length DESC
    """,
    "Risk status distribution by compound (replay events)": """
        SELECT compound, risk_status, COUNT(*) AS n
        FROM replay_events
        GROUP BY compound, risk_status
        ORDER BY compound, n DESC
    """,
    "Wet vs dry lap-time impact": """
        SELECT compound, rainfall, COUNT(*) AS n,
               ROUND(AVG(lap_time_sec), 3) AS avg_lap_time
        FROM lap_features
        WHERE clean_lap
        GROUP BY compound, rainfall
        ORDER BY compound, rainfall
    """,
}


def render_canned(con: duckdb.DuckDBPyConnection) -> None:
    st.subheader("Canned queries")
    name = st.selectbox("Pick a query", list(CANNED_QUERIES.keys()))
    sql = CANNED_QUERIES[name].strip()
    with st.expander("SQL", expanded=False):
        st.code(sql, language="sql")
    try:
        df = safe_query(con, sql)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return
    st.dataframe(df, use_container_width=True, height=400)
    st.caption(f"{len(df)} rows")


def render_free_sql(con: duckdb.DuckDBPyConnection) -> None:
    st.subheader("Free SQL")
    st.caption("Read-only. Tables: `lap_features`, `baselines`, `replay_events`.")
    default = "SELECT compound, COUNT(*) AS laps FROM lap_features WHERE clean_lap GROUP BY compound;"
    sql = st.text_area("SQL", value=default, height=140)
    if st.button("Run"):
        try:
            df = safe_query(con, sql)
        except ValueError as e:
            st.error(str(e))
            return
        except Exception as e:
            st.error(f"Query failed: {e}")
            return
        st.dataframe(df, use_container_width=True, height=400)
        st.caption(f"{len(df)} rows")


def render_corpus_summary(con: duckdb.DuckDBPyConnection) -> None:
    df = con.execute("""
        SELECT
            COUNT(*) AS lap_rows,
            COUNT(DISTINCT (year, race_round)) AS races,
            COUNT(DISTINCT driver_code) AS drivers,
            MIN(year) AS first_year,
            MAX(year) AS last_year
        FROM lap_features
    """).fetch_df()
    cols = st.columns(5)
    cols[0].metric("Lap rows", int(df.iloc[0]["lap_rows"]))
    cols[1].metric("Races", int(df.iloc[0]["races"]))
    cols[2].metric("Drivers", int(df.iloc[0]["drivers"]))
    cols[3].metric("First year", int(df.iloc[0]["first_year"]))
    cols[4].metric("Last year", int(df.iloc[0]["last_year"]))


def main() -> None:
    try:
        con = get_conn()
    except Exception as e:
        st.error(f"Cannot open processed parquet via DuckDB: {e}")
        st.info("Run features/build_lap_features.py and features/build_degradation_baselines.py first.")
        return

    render_corpus_summary(con)
    st.divider()
    render_canned(con)
    st.divider()
    render_free_sql(con)


main()
