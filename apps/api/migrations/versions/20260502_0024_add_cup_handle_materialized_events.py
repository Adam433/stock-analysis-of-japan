"""add cup handle materialized events

Revision ID: 20260502_0024
Revises: 20260501_0023
Create Date: 2026-05-02 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260502_0024"
down_revision = "20260501_0023"
branch_labels = None
depends_on = None


CUP_HANDLE_MATERIALIZATION_STATUS_CONSTRAINT = "status IN ('running', 'completed', 'failed')"
OPTIMIZATION_BEST_RESULT_FK = "fk_optimization_runs_best_result_id_optimization_results"


def _fk_exists(table_name: str, constraint_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(fk.get("name") == constraint_name for fk in inspector.get_foreign_keys(table_name))


def _ensure_optimization_best_result_fk() -> None:
    if _fk_exists("optimization_runs", OPTIMIZATION_BEST_RESULT_FK):
        return
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table("optimization_runs") as batch_op:
            batch_op.create_foreign_key(
                OPTIMIZATION_BEST_RESULT_FK,
                "optimization_results",
                ["best_result_id"],
                ["id"],
                ondelete="SET NULL",
            )
    else:
        op.create_foreign_key(
            OPTIMIZATION_BEST_RESULT_FK,
            "optimization_runs",
            "optimization_results",
            ["best_result_id"],
            ["id"],
            ondelete="SET NULL",
        )


def upgrade() -> None:
    _ensure_optimization_best_result_fk()

    op.create_table(
        "cup_handle_materialization_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_start_date", sa.Date(), nullable=False),
        sa.Column("source_end_date", sa.Date(), nullable=False),
        sa.Column("latest_market_data_date", sa.Date(), nullable=True),
        sa.Column("generation_bounds_json", sa.Text(), nullable=False),
        sa.Column("feature_windows_json", sa.Text(), nullable=False),
        sa.Column("detector_version", sa.String(length=64), nullable=False),
        sa.Column("events_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("symbols_processed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            CUP_HANDLE_MATERIALIZATION_STATUS_CONSTRAINT,
            name="cup_handle_materialization_runs_status",
        ),
    )
    op.create_index(
        "ix_cup_handle_materialization_runs_market",
        "cup_handle_materialization_runs",
        ["market"],
        unique=False,
    )
    op.create_index(
        "ix_cup_handle_materialization_runs_source_start_date",
        "cup_handle_materialization_runs",
        ["source_start_date"],
        unique=False,
    )
    op.create_index(
        "ix_cup_handle_materialization_runs_source_end_date",
        "cup_handle_materialization_runs",
        ["source_end_date"],
        unique=False,
    )

    op.create_table(
        "cup_handle_pattern_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("materialization_run_id", sa.Integer(), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("symbol_snapshot", sa.String(length=32), nullable=False),
        sa.Column("breakout_date", sa.Date(), nullable=False),
        sa.Column("left_lip_date", sa.Date(), nullable=False),
        sa.Column("cup_bottom_date", sa.Date(), nullable=False),
        sa.Column("right_lip_date", sa.Date(), nullable=False),
        sa.Column("handle_low_date", sa.Date(), nullable=False),
        sa.Column("cup_duration", sa.Integer(), nullable=False),
        sa.Column("handle_duration", sa.Integer(), nullable=False),
        sa.Column("total_duration", sa.Integer(), nullable=False),
        sa.Column("cup_depth_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("handle_depth_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("right_lip_delta_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("handle_low_position_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("handle_depth_to_cup_depth_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("handle_high_above_lip_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("bottom_dwell_days_zone_20", sa.Integer(), nullable=False),
        sa.Column("bottom_dwell_days_zone_35", sa.Integer(), nullable=False),
        sa.Column("bottom_span_pct_zone_20", sa.Numeric(8, 4), nullable=False),
        sa.Column("bottom_span_pct_zone_35", sa.Numeric(8, 4), nullable=False),
        sa.Column("left_side_duration_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("right_side_duration_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("prior_uptrend_pct_60", sa.Numeric(8, 4), nullable=True),
        sa.Column("prior_uptrend_pct_90", sa.Numeric(8, 4), nullable=True),
        sa.Column("prior_uptrend_pct_120", sa.Numeric(8, 4), nullable=True),
        sa.Column("prior_uptrend_pct_180", sa.Numeric(8, 4), nullable=True),
        sa.Column("breakout_volume_ratio_20", sa.Numeric(10, 4), nullable=True),
        sa.Column("breakout_volume_ratio_50", sa.Numeric(10, 4), nullable=True),
        sa.Column("breakout_volume_ratio_60", sa.Numeric(10, 4), nullable=True),
        sa.Column("breakout_close_over_resistance_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("data_start_date", sa.Date(), nullable=False),
        sa.Column("data_end_date", sa.Date(), nullable=False),
        sa.Column("detector_version", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["materialization_run_id"],
            ["cup_handle_materialization_runs.id"],
            name="fk_cup_handle_pattern_events_materialization_run_id_cup_handle_materialization_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_cup_handle_pattern_events_instrument_id_instruments",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_cup_handle_pattern_events_market",
        "cup_handle_pattern_events",
        ["market"],
        unique=False,
    )
    op.create_index(
        "ix_cup_handle_pattern_events_materialization_run_id",
        "cup_handle_pattern_events",
        ["materialization_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_cup_handle_pattern_events_instrument_id",
        "cup_handle_pattern_events",
        ["instrument_id"],
        unique=False,
    )
    op.create_index(
        "ix_cup_handle_pattern_events_breakout_date",
        "cup_handle_pattern_events",
        ["breakout_date"],
        unique=False,
    )
    op.create_index(
        "ix_cup_handle_pattern_events_market_breakout_date_instrument",
        "cup_handle_pattern_events",
        ["market", "breakout_date", "instrument_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_cup_handle_pattern_events_market_breakout_date_instrument",
        table_name="cup_handle_pattern_events",
    )
    op.drop_index("ix_cup_handle_pattern_events_breakout_date", table_name="cup_handle_pattern_events")
    op.drop_index("ix_cup_handle_pattern_events_instrument_id", table_name="cup_handle_pattern_events")
    op.drop_index(
        "ix_cup_handle_pattern_events_materialization_run_id",
        table_name="cup_handle_pattern_events",
    )
    op.drop_index("ix_cup_handle_pattern_events_market", table_name="cup_handle_pattern_events")
    op.drop_table("cup_handle_pattern_events")
    op.drop_index(
        "ix_cup_handle_materialization_runs_source_end_date",
        table_name="cup_handle_materialization_runs",
    )
    op.drop_index(
        "ix_cup_handle_materialization_runs_source_start_date",
        table_name="cup_handle_materialization_runs",
    )
    op.drop_index("ix_cup_handle_materialization_runs_market", table_name="cup_handle_materialization_runs")
    op.drop_table("cup_handle_materialization_runs")
