"""Phase 2: FastF1 lap data ingestion.

Loads one race session via FastF1, normalizes lap data, writes Parquet.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import fastf1
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from utils.logging import get_logger
from utils.time_utils import constructor_key, timedelta_to_seconds

log = get_logger("ingest")

LAP_COLUMNS_REQUIRED = [
    "Driver",
    "DriverNumber",
    "Team",
    "LapNumber",
    "LapTime",
    "Sector1Time",
    "Sector2Time",
    "Sector3Time",
    "Compound",
    "TyreLife",
    "Stint",
    "PitInTime",
    "PitOutTime",
    "TrackStatus",
    "IsAccurate",
    "LapStartTime",
]

WEATHER_COL_MAP = {
    "AirTemp": "air_temp_c",
    "TrackTemp": "track_temp_c",
    "Humidity": "humidity",
    "Pressure": "pressure",
    "Rainfall": "rainfall",
    "WindSpeed": "wind_speed",
    "WindDirection": "wind_direction",
}


def load_session(year: int, race: str, session_code: str):
    fastf1.Cache.enable_cache(str(config.FASTF1_CACHE_DIR))
    log.info("Loading FastF1 session: %s %s %s", year, race, session_code)
    session = fastf1.get_session(year, race, session_code)
    session.load(laps=True, telemetry=False, weather=True, messages=False)
    return session


def _attach_weather(df: pd.DataFrame, session) -> pd.DataFrame:
    """Asof-merge nearest weather sample before each lap start time."""
    weather = session.weather_data
    if weather is None or weather.empty:
        log.warning("No weather data for session; weather columns will be null.")
        for out_col in WEATHER_COL_MAP.values():
            df[out_col] = pd.NA
        return df

    w = weather[["Time"] + list(WEATHER_COL_MAP.keys())].copy()
    w = w.rename(columns=WEATHER_COL_MAP).sort_values("Time").reset_index(drop=True)

    df = df.copy()
    df["_lap_start_td"] = pd.to_timedelta(df["lap_start_time_sec"], unit="s")
    df_sorted = df.sort_values("_lap_start_td").reset_index()

    merged = pd.merge_asof(
        df_sorted, w,
        left_on="_lap_start_td", right_on="Time",
        direction="backward", allow_exact_matches=True,
    )
    merged = merged.sort_values("index").drop(columns=["index", "_lap_start_td", "Time"])
    return merged.reset_index(drop=True)


def laps_to_dataframe(session) -> pd.DataFrame:
    laps = session.laps.copy()
    missing = [c for c in LAP_COLUMNS_REQUIRED if c not in laps.columns]
    if missing:
        raise RuntimeError(f"Missing FastF1 lap columns: {missing}")

    laps = laps[LAP_COLUMNS_REQUIRED].copy()

    df = pd.DataFrame()
    df["year"] = [session.event["EventDate"].year] * len(laps)
    df["race_round"] = int(session.event["RoundNumber"])
    df["race_name"] = session.event["EventName"]
    df["session_name"] = session.name
    df["driver_code"] = laps["Driver"].astype(str)
    df["driver_number"] = laps["DriverNumber"].astype(str)
    df["team"] = laps["Team"].astype(str)
    df["constructor_key"] = laps["Team"].map(constructor_key)
    df["lap_number"] = laps["LapNumber"].astype("Int64")
    df["lap_time_sec"] = timedelta_to_seconds(laps["LapTime"])
    df["sector1_sec"] = timedelta_to_seconds(laps["Sector1Time"])
    df["sector2_sec"] = timedelta_to_seconds(laps["Sector2Time"])
    df["sector3_sec"] = timedelta_to_seconds(laps["Sector3Time"])
    df["compound"] = laps["Compound"].astype("string")
    df["tyre_life"] = pd.to_numeric(laps["TyreLife"], errors="coerce").astype("Float64")
    df["stint"] = pd.to_numeric(laps["Stint"], errors="coerce").astype("Int64")
    df["pit_in_time"] = timedelta_to_seconds(laps["PitInTime"])
    df["pit_out_time"] = timedelta_to_seconds(laps["PitOutTime"])
    df["track_status"] = laps["TrackStatus"].astype("string")
    df["is_accurate"] = laps["IsAccurate"].astype("boolean")
    df["lap_start_time_sec"] = timedelta_to_seconds(laps["LapStartTime"])
    return df


def report(df: pd.DataFrame) -> None:
    log.info("Total laps: %d", len(df))
    log.info("Drivers (%d): %s", df["driver_code"].nunique(), sorted(df["driver_code"].unique()))
    log.info("Missing LapTime: %d", df["lap_time_sec"].isna().sum())
    log.info("Missing Compound: %d", df["compound"].isna().sum())
    log.info("Missing TyreLife: %d", df["tyre_life"].isna().sum())
    log.info(
        "Lap range: %s..%s",
        df["lap_number"].min(),
        df["lap_number"].max(),
    )
    log.info("Compounds: %s", df["compound"].dropna().unique().tolist())
    if "track_temp_c" in df.columns:
        log.info(
            "Track temp range: %.1f..%.1f (missing: %d)",
            float(df["track_temp_c"].min()) if df["track_temp_c"].notna().any() else float("nan"),
            float(df["track_temp_c"].max()) if df["track_temp_c"].notna().any() else float("nan"),
            df["track_temp_c"].isna().sum(),
        )
        if "rainfall" in df.columns:
            log.info("Wet laps (rainfall=True): %d", int(df["rainfall"].astype("boolean").fillna(False).sum()))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest FastF1 lap data for one race.")
    p.add_argument("--year", type=int, default=config.DEMO_SEASON)
    p.add_argument("--race", type=str, default=config.DEMO_RACE)
    p.add_argument("--session", type=str, default=config.DEMO_SESSION)
    p.add_argument("--output", type=Path, default=None,
                   help="Override output path. Defaults to partitioned path data/raw/laps/year=YYYY/round=NN/laps.parquet.")
    p.add_argument("--skip-if-exists", action="store_true",
                   help="Skip ingestion if the partition output file already exists.")
    return p.parse_args()


def ingest_one(year: int, race: str, session_code: str, output: Path | None = None,
               skip_if_exists: bool = False) -> Path | None:
    """Ingest a single race session. Returns output path or None if skipped.

    If output is None and skip_if_exists is True, caller must pre-compute the
    partition path (round number requires loading the session). Use
    config.raw_laps_partition_path(year, race_round) when round number is known.
    """
    if output is not None and skip_if_exists and output.exists():
        log.info("Skip (exists): %s", output)
        return None

    session = load_session(year, race, session_code)
    if output is None:
        race_round = int(session.event["RoundNumber"])
        output = config.raw_laps_partition_path(year, race_round)
        if skip_if_exists and output.exists():
            log.info("Skip (exists): %s", output)
            return None

    df = laps_to_dataframe(session)
    df = _attach_weather(df, session)
    report(df)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output, index=False)
    log.info("Wrote %d rows -> %s", len(df), output)
    return output


def main() -> None:
    args = parse_args()
    ingest_one(args.year, args.race, args.session, args.output, args.skip_if_exists)


if __name__ == "__main__":
    main()
