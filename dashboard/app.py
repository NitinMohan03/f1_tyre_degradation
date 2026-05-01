"""Phase 8: Streamlit dashboard reading live state from Redis."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import redis
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config

STATUS_COLORS = {
    "LOW": "#22c55e",
    "MEDIUM": "#f59e0b",
    "HIGH": "#ef4444",
    "INSUFFICIENT_DATA": "#9ca3af",
}

st.set_page_config(page_title="F1 Tyre Degradation Monitor", layout="wide")


@st.cache_resource
def get_redis() -> redis.Redis:
    return redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB,
        decode_responses=True,
    )


def _f(v, default=None):
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load_drivers(r: redis.Redis) -> pd.DataFrame:
    keys = sorted(r.keys("driver:*"))
    rows = []
    for k in keys:
        h = r.hgetall(k)
        if not h:
            continue
        rows.append({
            "driver_code": h.get("driver_code", k.split(":")[-1]),
            "constructor_key": h.get("constructor_key", ""),
            "race_name": h.get("race_name", ""),
            "lap_number": int(_f(h.get("lap_number"), 0)),
            "compound": h.get("compound", ""),
            "tyre_life": _f(h.get("tyre_life")),
            "stint": int(_f(h.get("stint"), 0)),
            "lap_time_sec": _f(h.get("lap_time_sec")),
            "pace_delta": _f(h.get("pace_delta")),
            "degradation_slope": _f(h.get("degradation_slope")),
            "field_delta": _f(h.get("field_delta")),
            "risk_score": _f(h.get("risk_score"), 0.0),
            "risk_status": h.get("risk_status", "INSUFFICIENT_DATA"),
            "consecutive_high": int(_f(h.get("consecutive_high"), 0)),
            "track_temp_c": _f(h.get("track_temp_c")),
            "air_temp_c": _f(h.get("air_temp_c")),
            "rainfall": (h.get("rainfall", "") in {"True", "true", "1"}),
            "wind_speed": _f(h.get("wind_speed")),
            "anomaly_score": _f(h.get("anomaly_score")),
            "is_anomaly": (h.get("is_anomaly", "") in {"True", "true", "1"}),
            "timestamp": h.get("timestamp", ""),
        })
    return pd.DataFrame(rows)


def load_history(r: redis.Redis, code: str) -> pd.DataFrame:
    members = r.zrange(config.REDIS_KEY_HISTORY.format(driver_code=code), 0, -1)
    rows = []
    for m in members:
        try:
            rows.append(json.loads(m))
        except json.JSONDecodeError:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("lap")
    return df


def load_alerts(r: redis.Redis, n: int = 50) -> pd.DataFrame:
    items = r.lrange(config.REDIS_KEY_ALERTS, 0, n - 1)
    rows = []
    for it in items:
        try:
            rows.append(json.loads(it))
        except json.JSONDecodeError:
            continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_race(r: redis.Redis) -> dict:
    raw = r.get(config.REDIS_KEY_RACE)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def render_header(race: dict, drivers: pd.DataFrame) -> None:
    cols = st.columns(4)
    cols[0].metric("Race", race.get("race_name", "—"))
    cols[1].metric("Last lap", race.get("last_lap", drivers["lap_number"].max() if not drivers.empty else "—"))
    cols[2].metric("Drivers", len(drivers))
    cols[3].metric("Last update (UTC)", race.get("updated_at", "—")[:19].replace("T", " "))

    if not drivers.empty and "track_temp_c" in drivers.columns:
        track_temp = drivers["track_temp_c"].dropna()
        air_temp = drivers["air_temp_c"].dropna()
        wind = drivers["wind_speed"].dropna()
        wet = bool(drivers["rainfall"].any())
        anomalies = int(drivers["is_anomaly"].sum()) if "is_anomaly" in drivers.columns else 0
        wcols = st.columns(5)
        wcols[0].metric("Track temp", f"{track_temp.mean():.1f}°C" if len(track_temp) else "—")
        wcols[1].metric("Air temp", f"{air_temp.mean():.1f}°C" if len(air_temp) else "—")
        wcols[2].metric("Wind", f"{wind.mean():.1f} m/s" if len(wind) else "—")
        wcols[3].metric("Conditions", "WET" if wet else "Dry")
        wcols[4].metric("ML anomalies (live)", anomalies)


def render_driver_cards(drivers: pd.DataFrame) -> None:
    if drivers.empty:
        st.info("No driver state in Redis yet. Start producer + consumer.")
        return
    drivers = drivers.sort_values(["risk_score"], ascending=False)
    n_cols = 5
    for i in range(0, len(drivers), n_cols):
        cols = st.columns(n_cols)
        for col, (_, row) in zip(cols, drivers.iloc[i:i + n_cols].iterrows()):
            color = STATUS_COLORS.get(row["risk_status"], "#9ca3af")
            lap_time = f"{row['lap_time_sec']:.3f}s" if row["lap_time_sec"] is not None else "—"
            pace = f"{row['pace_delta']:+.3f}s" if row["pace_delta"] is not None else "—"
            tyre = f"{int(row['tyre_life'])}" if row["tyre_life"] is not None else "—"
            risk = f"{row['risk_score']:.2f}"
            anomaly_badge = ""
            if row.get("is_anomaly"):
                a_score = row.get("anomaly_score")
                a_str = f"{a_score:.3f}" if isinstance(a_score, (int, float)) else "?"
                anomaly_badge = f"<div style=\"margin-top:2px;color:#f472b6\">⚠ ML anomaly ({a_str})</div>"
            col.markdown(
                f"""<div style="border-left:6px solid {color};padding:8px 12px;border-radius:4px;background:#111827;color:#f3f4f6">
<div style="font-size:1.2rem;font-weight:700">{row['driver_code']}</div>
<div style="font-size:0.8rem;color:#9ca3af">{row['constructor_key']}</div>
<div style="margin-top:4px"><b>{row['compound']}</b> · age {tyre} · stint {row['stint']}</div>
<div>lap {row['lap_number']} · {lap_time}</div>
<div>pace Δ {pace}</div>
<div style="margin-top:4px"><b>risk {risk}</b> · <span style="color:{color}">{row['risk_status']}</span></div>
{anomaly_badge}
</div>""",
                unsafe_allow_html=True,
            )


def render_driver_trend(r: redis.Redis, drivers: pd.DataFrame) -> None:
    if drivers.empty:
        return
    codes = sorted(drivers["driver_code"].tolist())
    code = st.selectbox("Driver trend", codes)
    hist = load_history(r, code)
    if hist.empty:
        st.info("No history yet.")
        return

    c1, c2 = st.columns(2)
    fig = px.line(hist, x="lap", y="lap_time_sec", title=f"{code} — lap time", markers=True)
    c1.plotly_chart(fig, use_container_width=True)

    fig = px.line(hist, x="lap", y="risk_score", title=f"{code} — risk score", markers=True)
    fig.add_hline(y=config.RISK_THRESHOLD_HIGH, line_dash="dot", annotation_text="HIGH")
    fig.add_hline(y=config.RISK_THRESHOLD_MEDIUM, line_dash="dot", annotation_text="MED")
    c2.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)
    fig = px.line(hist, x="lap", y="pace_delta", title=f"{code} — pace delta", markers=True)
    c3.plotly_chart(fig, use_container_width=True)

    if "tyre_life" in hist.columns:
        fig = px.line(hist, x="lap", y="tyre_life", color="compound", title=f"{code} — tyre age", markers=True)
        c4.plotly_chart(fig, use_container_width=True)


def render_alerts(r: redis.Redis) -> None:
    alerts = load_alerts(r, 50)
    st.subheader("Alerts")
    if alerts.empty:
        st.info("No alerts yet.")
        return
    cols = ["lap", "driver_code", "compound", "tyre_life", "risk_score", "reason", "timestamp"]
    cols = [c for c in cols if c in alerts.columns]
    st.dataframe(alerts[cols], use_container_width=True, height=300)


def main() -> None:
    st_autorefresh(interval=2000, key="refresh")

    st.title("F1 Tyre Degradation Monitor")
    r = get_redis()
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        st.error(f"Redis unreachable at {config.REDIS_HOST}:{config.REDIS_PORT}. Is docker stack up?")
        return

    drivers = load_drivers(r)
    race = load_race(r)

    render_header(race, drivers)
    st.divider()
    render_driver_cards(drivers)
    st.divider()
    render_driver_trend(r, drivers)
    st.divider()
    render_alerts(r)


if __name__ == "__main__":
    main()
