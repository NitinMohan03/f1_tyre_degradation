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
]


def load_session(year: int, race: str, session_code: str):
    fastf1.Cache.enable_cache(str(config.FASTF1_CACHE_DIR))
    log.info("Loading FastF1 session: %s %s %s", year, race, session_code)
    session = fastf1.get_session(year, race, session_code)
    session.load(laps=True, telemetry=False, weather=False, messages=False)
    return session


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest FastF1 lap data for one race.")
    p.add_argument("--year", type=int, default=config.DEMO_SEASON)
    p.add_argument("--race", type=str, default=config.DEMO_RACE)
    p.add_argument("--session", type=str, default=config.DEMO_SESSION)
    p.add_argument("--output", type=Path, default=config.RAW_LAPS_PARQUET)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    session = load_session(args.year, args.race, args.session)
    df = laps_to_dataframe(session)
    report(df)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    log.info("Wrote %d rows -> %s", len(df), args.output)


if __name__ == "__main__":
    main()
