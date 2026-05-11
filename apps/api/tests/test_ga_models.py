from __future__ import annotations

import os
import unittest
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.backtests.models  # noqa: F401
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
import stockanalyse_api.domain.operations.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
import stockanalyse_api.domain.watchlists.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.backtests.models import GaEvent, GaIndividual, GaRun
from stockanalyse_api.services.strategy_parameters import (
    STRATEGY_PARAMETER_SCHEMA_VERSION,
    dump_json,
    stable_parameter_hash,
)


class GaModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            future=True,
        )

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _build_alembic_config(self, database_url: str) -> Config:
        api_root = Path(__file__).resolve().parents[1]
        config = Config(str(api_root / "alembic.ini"))
        config.set_main_option("script_location", str(api_root / "migrations"))
        config.set_main_option("prepend_sys_path", str(api_root / "src"))
        config.set_main_option("sqlalchemy.url", database_url)
        return config

    def test_ga_run_individual_and_event_can_be_persisted(self) -> None:
        parameters = {"strategy_schema_version": STRATEGY_PARAMETER_SCHEMA_VERSION, "rps_threshold": 80}
        with self.session_factory() as session:
            run = GaRun(
                market="us",
                train_start_date=date(2020, 1, 1),
                train_end_date=date(2022, 12, 31),
                validation_start_date=date(2023, 1, 1),
                validation_end_date=date(2024, 12, 31),
                objective="spy_alpha",
                strategy_schema_version=STRATEGY_PARAMETER_SCHEMA_VERSION,
                population_size=12,
                max_generations=4,
                random_seed=7,
                gene_space_json=dump_json({"rps_threshold": [70, 80]}),
                fitness_config_json=dump_json({"require_complete_benchmark": True}),
                started_at=datetime.now(UTC),
            )
            session.add(run)
            session.flush()
            individual = GaIndividual(
                ga_run_id=run.id,
                generation=0,
                individual_index=0,
                parameter_hash=stable_parameter_hash(parameters),
                parameters_json=dump_json(parameters),
                fitness=Decimal("0.123456"),
                metrics_json=dump_json({"completed_trades": 10}),
                evaluation_json=dump_json({"status": "completed"}),
                mutation_json=dump_json({"source": "seed"}),
                status="completed",
                completed_at=datetime.now(UTC),
            )
            session.add(individual)
            session.flush()
            individual_id = individual.id
            run.best_individual_id = individual.id
            session.add(
                GaEvent(
                    ga_run_id=run.id,
                    generation=0,
                    event_type="selection",
                    event_json=dump_json({"elite_count": 1}),
                )
            )
            session.commit()

            persisted = session.execute(select(GaRun)).scalar_one()
            event = session.execute(select(GaEvent)).scalar_one()
            persisted_best_individual_id = persisted.best_individual_id
            persisted_completed_generations = persisted.completed_generations
            persisted_event_type = event.event_type

        self.assertEqual(persisted_best_individual_id, individual_id)
        self.assertEqual(persisted_completed_generations, 0)
        self.assertEqual(persisted_event_type, "selection")

    def test_ga_run_rejects_invalid_status(self) -> None:
        with self.session_factory() as session:
            session.add(
                GaRun(
                    market="us",
                    train_start_date=date(2020, 1, 1),
                    train_end_date=date(2022, 12, 31),
                    objective="spy_alpha",
                    strategy_schema_version=STRATEGY_PARAMETER_SCHEMA_VERSION,
                    population_size=12,
                    max_generations=4,
                    gene_space_json=dump_json({}),
                    fitness_config_json=dump_json({}),
                    status="invalid",
                    started_at=datetime.now(UTC),
                )
            )

            with self.assertRaises(IntegrityError):
                session.commit()

    def test_ga_migration_creates_experiment_tables(self) -> None:
        with TemporaryDirectory() as tmpdir:
            database_url = f"sqlite:///{Path(tmpdir) / 'migration.db'}"
            config = self._build_alembic_config(database_url)

            with patch.dict(os.environ, {"STOCKANALYSE_DATABASE_URL": database_url}, clear=False):
                command.upgrade(config, "20260511_0028")

            engine = create_engine(database_url, future=True)
            try:
                inspector = inspect(engine)
                self.assertIn("ga_runs", inspector.get_table_names())
                self.assertIn("ga_individuals", inspector.get_table_names())
                self.assertIn("ga_events", inspector.get_table_names())
                ga_run_columns = {column["name"] for column in inspector.get_columns("ga_runs")}
                self.assertIn("strategy_schema_version", ga_run_columns)
                self.assertIn("best_individual_id", ga_run_columns)
            finally:
                engine.dispose()
