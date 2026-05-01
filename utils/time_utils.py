import pandas as pd


def timedelta_to_seconds(series: pd.Series) -> pd.Series:
    return series.dt.total_seconds()


def constructor_key(team_name: str | None) -> str | None:
    if team_name is None or pd.isna(team_name):
        return None
    return (
        str(team_name)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )
