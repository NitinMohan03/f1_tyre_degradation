"""Phase 10: full-season ingestion sweep.

Loops every conventional race round in a season (or list of seasons) and calls
ingest_fastf1_laps.ingest_one for each. Idempotent via --skip-if-exists.

Run order: cache builds up over time, so re-runs are fast. Sleep between rounds
to be polite to the FastF1/Ergast backend.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import fastf1
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from ingestion.ingest_fastf1_laps import ingest_one
from utils.logging import get_logger

log = get_logger("sweep")

DEFAULT_SLEEP_SEC = 1.5


def race_rounds(year: int) -> list[tuple[int, str]]:
    """Return [(round_number, event_name), ...] for conventional rounds only.

    Skips testing events (RoundNumber=0). FastF1's get_event_schedule returns
    pre-season tests with RoundNumber=0 which we don't want.
    """
    fastf1.Cache.enable_cache(str(config.FASTF1_CACHE_DIR))
    schedule = fastf1.get_event_schedule(year, include_testing=False)
    rows: list[tuple[int, str]] = []
    for _, row in schedule.iterrows():
        rnd = int(row["RoundNumber"])
        if rnd <= 0:
            continue
        rows.append((rnd, str(row["EventName"])))
    return sorted(rows, key=lambda t: t[0])


def sweep(years: list[int], session_code: str, sleep_sec: float, skip_if_exists: bool) -> None:
    total_written = 0
    total_skipped = 0
    total_failed = 0
    for year in years:
        rounds = race_rounds(year)
        log.info("Year %d: %d rounds", year, len(rounds))
        for rnd, name in rounds:
            output = config.raw_laps_partition_path(year, rnd)
            log.info("---- %d R%02d %s ----", year, rnd, name)
            try:
                result = ingest_one(
                    year=year, race=name, session_code=session_code,
                    output=output, skip_if_exists=skip_if_exists,
                )
                if result is None:
                    total_skipped += 1
                else:
                    total_written += 1
            except Exception as e:
                log.error("Round %d %s failed: %s", rnd, name, e)
                total_failed += 1
            time.sleep(sleep_sec)
    log.info("Sweep done. written=%d skipped=%d failed=%d", total_written, total_skipped, total_failed)
    summarize_corpus()


def summarize_corpus() -> None:
    parts = list(config.RAW_LAPS_DIR.glob("year=*/round=*/laps.parquet"))
    if not parts:
        log.warning("No partitions found under %s", config.RAW_LAPS_DIR)
        return
    total_rows = 0
    per_year: dict[int, int] = {}
    for p in parts:
        df = pd.read_parquet(p, columns=["lap_number"])
        n = len(df)
        total_rows += n
        year = int(p.parent.parent.name.split("=")[1])
        per_year[year] = per_year.get(year, 0) + n
    log.info("Corpus: %d partitions, %d total rows", len(parts), total_rows)
    for y, n in sorted(per_year.items()):
        log.info("  year=%d rows=%d", y, n)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sweep FastF1 lap ingestion across one or more seasons.")
    p.add_argument("--years", type=int, nargs="+", default=config.INGEST_SEASONS)
    p.add_argument("--session", type=str, default=config.DEMO_SESSION)
    p.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_SEC)
    p.add_argument("--no-skip", action="store_true",
                   help="Re-ingest even if partition file already exists.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    sweep(
        years=args.years,
        session_code=args.session,
        sleep_sec=args.sleep,
        skip_if_exists=not args.no_skip,
    )


if __name__ == "__main__":
    main()
