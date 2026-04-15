from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[5]


def get_data_dir() -> Path:
    data_dir = get_project_root() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_tse_common_stock_symbols_path() -> Path:
    return Path(
        os.environ.get(
            "STOCKANALYSE_TSE_COMMON_STOCK_SYMBOLS_PATH",
            get_data_dir() / "tse_common_stock_symbols.txt",
        )
    )


def get_local_csv_raw_dir() -> Path:
    return Path(
        os.environ.get(
            "STOCKANALYSE_LOCAL_CSV_RAW_DIR",
            get_data_dir() / "archive" / "local_seed_csv",
        )
    )


def get_database_path() -> Path:
    return Path(os.environ.get("STOCKANALYSE_DB_PATH", get_data_dir() / "stockanalyse.db"))


def get_database_url() -> str:
    database_url = os.environ.get("STOCKANALYSE_DATABASE_URL")
    if database_url:
        return database_url
    return f"sqlite:///{get_database_path()}"


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_auto_refresh_enabled() -> bool:
    return _env_flag("STOCKANALYSE_AUTO_REFRESH_ENABLED", True)


def get_auto_refresh_provider() -> str:
    return os.environ.get("STOCKANALYSE_AUTO_REFRESH_PROVIDER", "yahoo_finance_chart")


def get_auto_refresh_symbols_file() -> Path:
    return Path(
        os.environ.get(
            "STOCKANALYSE_AUTO_REFRESH_SYMBOLS_FILE",
            get_tse_common_stock_symbols_path(),
        )
    )


def get_auto_refresh_csv_dir() -> Path:
    return Path(
        os.environ.get(
            "STOCKANALYSE_AUTO_REFRESH_CSV_DIR",
            get_local_csv_raw_dir(),
        )
    )


def get_auto_refresh_fixture_path() -> Path:
    return Path(
        os.environ.get(
            "STOCKANALYSE_AUTO_REFRESH_FIXTURE_PATH",
            get_project_root() / "apps" / "api" / "tests" / "fixtures" / "japan_equity_eod_fixture.json",
        )
    )


def get_auto_refresh_commit_every() -> int:
    return int(os.environ.get("STOCKANALYSE_AUTO_REFRESH_COMMIT_EVERY", "100"))


def get_auto_refresh_check_interval_seconds() -> int:
    return int(os.environ.get("STOCKANALYSE_AUTO_REFRESH_CHECK_INTERVAL_SECONDS", "300"))


def get_auto_refresh_timezone() -> ZoneInfo:
    return ZoneInfo(os.environ.get("STOCKANALYSE_AUTO_REFRESH_TIMEZONE", "Asia/Tokyo"))
