"""add portfolio-return execution fields

Revision ID: 20260417_0019
Revises: 20260417_0018
Create Date: 2026-04-17 16:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_0019"
down_revision = "20260417_0018"
branch_labels = None
depends_on = None

NEW_STATUS_CONSTRAINT = (
    "status IN ('running', 'completed', 'failed', 'failed-recoverable', 'failed-data-insufficient')"
)
OLD_STATUS_CONSTRAINT = "status IN ('running', 'completed', 'failed', 'failed-recoverable')"
LIFECYCLE_CONSTRAINT = "backtest_lifecycle IN ('portfolio_return', 'legacy_condition_hit')"
TEMP_TABLE_NAME = "backtest_runs_portfolio_execution_tmp"


def _create_backtest_runs_table(*, table_name: str, include_execution_fields: bool, status_constraint: str) -> None:
    columns: list[sa.Column[object]] = [
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("strategy_configuration_id", sa.Integer(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("trade_dates_evaluated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_candidates_evaluated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("qualifying_observations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unique_qualified_instruments", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("first_qualified_trade_date", sa.Date(), nullable=True),
        sa.Column("last_qualified_trade_date", sa.Date(), nullable=True),
        sa.Column("result_checksum", sa.String(length=64), nullable=True),
        sa.Column("rps_definition_version", sa.String(length=64), nullable=True),
        sa.Column("dataset_trade_date_start", sa.Date(), nullable=True),
        sa.Column("dataset_trade_date_end", sa.Date(), nullable=True),
        sa.Column("dataset_checksum", sa.String(length=64), nullable=True),
        sa.Column("backtest_lifecycle", sa.String(length=32), nullable=False),
        sa.Column("source_screen_run_id", sa.Integer(), nullable=True),
        sa.Column("effective_holding_days", sa.Integer(), nullable=True),
        sa.Column("effective_stop_loss_pct", sa.Numeric(6, 4), nullable=True),
        sa.Column("effective_portfolio_cap", sa.Integer(), nullable=True),
        sa.Column("effective_entry_deferral_window_days", sa.Integer(), nullable=True),
    ]
    constraints: list[sa.Constraint] = [
        sa.CheckConstraint(status_constraint, name="ck_backtest_runs_backtest_runs_status"),
        sa.CheckConstraint(LIFECYCLE_CONSTRAINT, name="ck_backtest_runs_backtest_runs_lifecycle"),
        sa.ForeignKeyConstraint(
            ["strategy_configuration_id"],
            ["strategy_configurations.id"],
            name="fk_backtest_runs_strategy_configuration_id_strategy_configurations",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_screen_run_id"],
            ["screen_runs.id"],
            name="fk_backtest_runs_source_screen_run_id_screen_runs",
            ondelete="RESTRICT",
        ),
    ]

    if include_execution_fields:
        columns.extend(
            [
                sa.Column("ranking_policy_id", sa.String(length=64), nullable=True),
                sa.Column("excluded_securities_json", sa.Text(), nullable=True),
                sa.Column("portfolio_value", sa.Numeric(18, 6), nullable=True),
                sa.Column("position_count_after_exclusions", sa.Integer(), nullable=True),
                sa.Column("cumulative_return", sa.Numeric(18, 6), nullable=True),
                sa.Column("equity_curve_json", sa.Text(), nullable=True),
                sa.Column("per_security_returns_json", sa.Text(), nullable=True),
            ]
        )

    op.create_table(table_name, *columns, *constraints)


def upgrade() -> None:
    _create_backtest_runs_table(
        table_name=TEMP_TABLE_NAME,
        include_execution_fields=True,
        status_constraint=NEW_STATUS_CONSTRAINT,
    )
    op.execute(
        sa.text(
            f"""
            INSERT INTO {TEMP_TABLE_NAME} (
                id,
                strategy_configuration_id,
                start_date,
                end_date,
                started_at,
                completed_at,
                status,
                error_message,
                created_at,
                updated_at,
                trade_dates_evaluated,
                total_candidates_evaluated,
                qualifying_observations,
                unique_qualified_instruments,
                first_qualified_trade_date,
                last_qualified_trade_date,
                result_checksum,
                rps_definition_version,
                dataset_trade_date_start,
                dataset_trade_date_end,
                dataset_checksum,
                backtest_lifecycle,
                source_screen_run_id,
                effective_holding_days,
                effective_stop_loss_pct,
                effective_portfolio_cap,
                effective_entry_deferral_window_days,
                ranking_policy_id,
                excluded_securities_json,
                portfolio_value,
                position_count_after_exclusions,
                cumulative_return,
                equity_curve_json,
                per_security_returns_json
            )
            SELECT
                id,
                strategy_configuration_id,
                start_date,
                end_date,
                started_at,
                completed_at,
                status,
                error_message,
                created_at,
                updated_at,
                trade_dates_evaluated,
                total_candidates_evaluated,
                qualifying_observations,
                unique_qualified_instruments,
                first_qualified_trade_date,
                last_qualified_trade_date,
                result_checksum,
                rps_definition_version,
                dataset_trade_date_start,
                dataset_trade_date_end,
                dataset_checksum,
                backtest_lifecycle,
                source_screen_run_id,
                effective_holding_days,
                effective_stop_loss_pct,
                effective_portfolio_cap,
                effective_entry_deferral_window_days,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            FROM backtest_runs
            """
        )
    )
    op.drop_table("backtest_runs")
    op.rename_table(TEMP_TABLE_NAME, "backtest_runs")
    op.create_index("ix_backtest_runs_strategy_configuration_id", "backtest_runs", ["strategy_configuration_id"], unique=False)
    op.create_index("ix_backtest_runs_start_date", "backtest_runs", ["start_date"], unique=False)
    op.create_index("ix_backtest_runs_end_date", "backtest_runs", ["end_date"], unique=False)
    op.create_index("ix_backtest_runs_source_screen_run_id", "backtest_runs", ["source_screen_run_id"], unique=False)


def downgrade() -> None:
    _create_backtest_runs_table(
        table_name=TEMP_TABLE_NAME,
        include_execution_fields=False,
        status_constraint=OLD_STATUS_CONSTRAINT,
    )
    op.execute(
        sa.text(
            f"""
            INSERT INTO {TEMP_TABLE_NAME} (
                id,
                strategy_configuration_id,
                start_date,
                end_date,
                started_at,
                completed_at,
                status,
                error_message,
                created_at,
                updated_at,
                trade_dates_evaluated,
                total_candidates_evaluated,
                qualifying_observations,
                unique_qualified_instruments,
                first_qualified_trade_date,
                last_qualified_trade_date,
                result_checksum,
                rps_definition_version,
                dataset_trade_date_start,
                dataset_trade_date_end,
                dataset_checksum,
                backtest_lifecycle,
                source_screen_run_id,
                effective_holding_days,
                effective_stop_loss_pct,
                effective_portfolio_cap,
                effective_entry_deferral_window_days
            )
            SELECT
                id,
                strategy_configuration_id,
                start_date,
                end_date,
                started_at,
                completed_at,
                CASE
                    WHEN status = 'failed-data-insufficient' THEN 'failed-recoverable'
                    ELSE status
                END,
                error_message,
                created_at,
                updated_at,
                trade_dates_evaluated,
                total_candidates_evaluated,
                qualifying_observations,
                unique_qualified_instruments,
                first_qualified_trade_date,
                last_qualified_trade_date,
                result_checksum,
                rps_definition_version,
                dataset_trade_date_start,
                dataset_trade_date_end,
                dataset_checksum,
                backtest_lifecycle,
                source_screen_run_id,
                effective_holding_days,
                effective_stop_loss_pct,
                effective_portfolio_cap,
                effective_entry_deferral_window_days
            FROM backtest_runs
            """
        )
    )
    op.drop_table("backtest_runs")
    op.rename_table(TEMP_TABLE_NAME, "backtest_runs")
    op.create_index("ix_backtest_runs_strategy_configuration_id", "backtest_runs", ["strategy_configuration_id"], unique=False)
    op.create_index("ix_backtest_runs_start_date", "backtest_runs", ["start_date"], unique=False)
    op.create_index("ix_backtest_runs_end_date", "backtest_runs", ["end_date"], unique=False)
    op.create_index("ix_backtest_runs_source_screen_run_id", "backtest_runs", ["source_screen_run_id"], unique=False)
