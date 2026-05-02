"""add optimization runs and strategy presets

Revision ID: 20260501_0023
Revises: 20260428_0022
Create Date: 2026-05-01 00:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260501_0023"
down_revision = "20260428_0022"
branch_labels = None
depends_on = None


OPTIMIZATION_RUN_STATUS_CONSTRAINT = (
    "status IN ('running', 'completed', 'failed', 'cancel_requested', 'cancelled')"
)
OPTIMIZATION_RESULT_STATUS_CONSTRAINT = "status IN ('completed', 'failed')"


def upgrade() -> None:
    op.create_table(
        "optimization_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("train_start_date", sa.Date(), nullable=False),
        sa.Column("train_end_date", sa.Date(), nullable=False),
        sa.Column("validation_start_date", sa.Date(), nullable=True),
        sa.Column("validation_end_date", sa.Date(), nullable=True),
        sa.Column("objective", sa.String(length=64), nullable=False, server_default="score"),
        sa.Column("parameter_space_json", sa.Text(), nullable=False),
        sa.Column("parameter_sets_json", sa.Text(), nullable=False),
        sa.Column("data_snapshot_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("total_parameter_sets", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_parameter_sets", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_parameter_sets", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("best_result_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            OPTIMIZATION_RUN_STATUS_CONSTRAINT,
            name="optimization_runs_status",
        ),
    )
    op.create_index("ix_optimization_runs_market", "optimization_runs", ["market"], unique=False)
    op.create_index(
        "ix_optimization_runs_train_start_date",
        "optimization_runs",
        ["train_start_date"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_runs_train_end_date",
        "optimization_runs",
        ["train_end_date"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_runs_validation_start_date",
        "optimization_runs",
        ["validation_start_date"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_runs_validation_end_date",
        "optimization_runs",
        ["validation_end_date"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_runs_best_result_id",
        "optimization_runs",
        ["best_result_id"],
        unique=False,
    )

    op.create_table(
        "optimization_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("optimization_run_id", sa.Integer(), nullable=False),
        sa.Column("parameter_hash", sa.String(length=64), nullable=False),
        sa.Column("parameters_json", sa.Text(), nullable=False),
        sa.Column("train_metrics_json", sa.Text(), nullable=True),
        sa.Column("validation_metrics_json", sa.Text(), nullable=True),
        sa.Column("score", sa.Numeric(18, 6), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="completed"),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            OPTIMIZATION_RESULT_STATUS_CONSTRAINT,
            name="optimization_results_status",
        ),
        sa.ForeignKeyConstraint(
            ["optimization_run_id"],
            ["optimization_runs.id"],
            name="fk_optimization_results_optimization_run_id_optimization_runs",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_optimization_results_optimization_run_id",
        "optimization_results",
        ["optimization_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_results_parameter_hash",
        "optimization_results",
        ["parameter_hash"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_results_score",
        "optimization_results",
        ["score"],
        unique=False,
    )
    op.create_index(
        "ix_optimization_results_rank",
        "optimization_results",
        ["rank"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_optimization_runs_best_result_id_optimization_results",
        "optimization_runs",
        "optimization_results",
        ["best_result_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_table(
        "strategy_presets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("parameters_hash", sa.String(length=64), nullable=False),
        sa.Column("parameters_json", sa.Text(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("source_optimization_run_id", sa.Integer(), nullable=True),
        sa.Column("source_optimization_result_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["source_optimization_run_id"],
            ["optimization_runs.id"],
            name="fk_strategy_presets_source_optimization_run_id_optimization_runs",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["source_optimization_result_id"],
            ["optimization_results.id"],
            name="fk_strategy_presets_source_optimization_result_id_optimization_results",
            ondelete="SET NULL",
        ),
    )
    op.create_index("ix_strategy_presets_market", "strategy_presets", ["market"], unique=False)
    op.create_index(
        "ix_strategy_presets_parameters_hash",
        "strategy_presets",
        ["parameters_hash"],
        unique=False,
    )
    op.create_index(
        "ix_strategy_presets_source_optimization_run_id",
        "strategy_presets",
        ["source_optimization_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_strategy_presets_source_optimization_result_id",
        "strategy_presets",
        ["source_optimization_result_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_strategy_presets_source_optimization_result_id", table_name="strategy_presets")
    op.drop_index("ix_strategy_presets_source_optimization_run_id", table_name="strategy_presets")
    op.drop_index("ix_strategy_presets_parameters_hash", table_name="strategy_presets")
    op.drop_index("ix_strategy_presets_market", table_name="strategy_presets")
    op.drop_table("strategy_presets")
    op.drop_constraint(
        "fk_optimization_runs_best_result_id_optimization_results",
        "optimization_runs",
        type_="foreignkey",
    )
    op.drop_index("ix_optimization_results_rank", table_name="optimization_results")
    op.drop_index("ix_optimization_results_score", table_name="optimization_results")
    op.drop_index("ix_optimization_results_parameter_hash", table_name="optimization_results")
    op.drop_index("ix_optimization_results_optimization_run_id", table_name="optimization_results")
    op.drop_table("optimization_results")
    op.drop_index("ix_optimization_runs_best_result_id", table_name="optimization_runs")
    op.drop_index("ix_optimization_runs_validation_end_date", table_name="optimization_runs")
    op.drop_index("ix_optimization_runs_validation_start_date", table_name="optimization_runs")
    op.drop_index("ix_optimization_runs_train_end_date", table_name="optimization_runs")
    op.drop_index("ix_optimization_runs_train_start_date", table_name="optimization_runs")
    op.drop_index("ix_optimization_runs_market", table_name="optimization_runs")
    op.drop_table("optimization_runs")
