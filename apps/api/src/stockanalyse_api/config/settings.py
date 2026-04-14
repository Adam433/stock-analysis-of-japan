from __future__ import annotations

import os
from pathlib import Path


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
