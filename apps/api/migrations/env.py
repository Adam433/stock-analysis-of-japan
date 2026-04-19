from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from stockanalyse_api.config.settings import get_database_url
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests import models as backtest_models  # noqa: F401
from stockanalyse_api.domain.fundamentals import models as fundamentals_models  # noqa: F401
from stockanalyse_api.domain.instruments import models as instrument_models  # noqa: F401
from stockanalyse_api.domain.indicators import models as indicator_models  # noqa: F401
from stockanalyse_api.domain.market_data import models as market_data_models  # noqa: F401
from stockanalyse_api.domain.operations import models as operations_models  # noqa: F401
from stockanalyse_api.domain.screens import models as screen_models  # noqa: F401
from stockanalyse_api.domain.watchlists import models as watchlist_models  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", get_database_url())

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=url.startswith("sqlite"),
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=connection.dialect.name == "sqlite",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
