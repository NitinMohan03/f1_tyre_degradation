from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FASTF1_CACHE_DIR = DATA_DIR / "cache"

for _d in (RAW_DIR, PROCESSED_DIR, FASTF1_CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

RAW_LAPS_PARQUET = RAW_DIR / "fastf1_laps.parquet"
LAP_FEATURES_PARQUET = PROCESSED_DIR / "lap_features.parquet"
DEGRADATION_BASELINES_PARQUET = PROCESSED_DIR / "degradation_baselines.parquet"
RACE_REPLAY_EVENTS_PARQUET = PROCESSED_DIR / "race_replay_events.parquet"

DEMO_SEASON = 2024
DEMO_RACE = "Bahrain Grand Prix"
DEMO_SESSION = "R"
BACKUP_RACES = ["Spanish Grand Prix", "Hungarian Grand Prix"]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "f1-lap-events"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

REDIS_KEY_DRIVER = "driver:{driver_code}"
REDIS_KEY_HISTORY = "history:{driver_code}"
REDIS_KEY_ALERTS = "alerts:log"
REDIS_KEY_RACE = "race:current"

CLEAN_LAP_MIN_LAP_NUMBER = 2
STINT_MIN_CLEAN_LAPS = 3
TYRE_AGE_BUCKETS = [(0, 5), (6, 10), (11, 15), (16, 20), (21, 999)]
TRACK_STATUS_EXCLUDE = {"4", "5", "6", "7"}

RISK_WEIGHT_PACE = 0.4
RISK_WEIGHT_SLOPE = 0.4
RISK_WEIGHT_FIELD = 0.2

RISK_THRESHOLD_MEDIUM = 0.40
RISK_THRESHOLD_HIGH = 0.70
ALERT_HIGH_CONSECUTIVE = 2
ALERT_FALLBACK_RISK = 0.80

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

DEFAULT_REPLAY_SPEED = 20
