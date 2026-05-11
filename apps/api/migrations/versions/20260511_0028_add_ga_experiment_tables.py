"""add ga experiment tables

Revision ID: 20260511_0028
Revises: 20260504_0027
Create Date: 2026-05-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260511_0028"
down_revision = "20260504_0027"
branch_labels = None
depends_on = None


GA_RUN_STATUS_CONSTRAINT = (
    "status IN ('running', 'completed', 'failed', 'cancel_requested', 'cancelled')"
)
GA_INDIVIDUAL_STATUS_CONSTRAINT = "status IN ('pending', 'completed', 'failed')"


def upgrade() -> None:
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite"
    ga_run_constraints = [
        sa.CheckConstraint(GA_RUN_STATUS_CONSTRAINT, name="ga_runs_status"),
    ]
    if is_sqlite:
        ga_run_constraints.append(
            sa.ForeignKeyConstraint(
                ["best_individual_id"],
                ["ga_individuals.id"],
                name="fk_ga_runs_best_individual_id_ga_individuals",
                ondelete="SET NULL",
            )
        )

    op.create_table(
        "ga_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("train_start_date", sa.Date(), nullable=False),
        sa.Column("train_end_date", sa.Date(), nullable=False),
        sa.Column("validation_start_date", sa.Date(), nullable=True),
        sa.Column("validation_end_date", sa.Date(), nullable=True),
        sa.Column("holdout_start_date", sa.Date(), nullable=True),
        sa.Column("holdout_end_date", sa.Date(), nullable=True),
        sa.Column("objective", sa.String(length=64), nullable=False, server_default="spy_alpha"),
        sa.Column("strategy_schema_version", sa.String(length=64), nullable=False),
        sa.Column("population_size", sa.Integer(), nullable=False),
        sa.Column("max_generations", sa.Integer(), nullable=False),
        sa.Column("random_seed", sa.Integer(), nullable=True),
        sa.Column("gene_space_json", sa.Text(), nullable=False),
        sa.Column("fitness_config_json", sa.Text(), nullable=False),
        sa.Column("initial_population_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column("total_generations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_generations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_individuals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completed_individuals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_individuals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("best_individual_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        *ga_run_constraints,
    )
    op.create_index("ix_ga_runs_market", "ga_runs", ["market"], unique=False)
    op.create_index("ix_ga_runs_train_start_date", "ga_runs", ["train_start_date"], unique=False)
    op.create_index("ix_ga_runs_train_end_date", "ga_runs", ["train_end_date"], unique=False)
    op.create_index(
        "ix_ga_runs_validation_start_date",
        "ga_runs",
        ["validation_start_date"],
        unique=False,
    )
    op.create_index(
        "ix_ga_runs_validation_end_date",
        "ga_runs",
        ["validation_end_date"],
        unique=False,
    )
    op.create_index("ix_ga_runs_holdout_start_date", "ga_runs", ["holdout_start_date"], unique=False)
    op.create_index("ix_ga_runs_holdout_end_date", "ga_runs", ["holdout_end_date"], unique=False)
    op.create_index("ix_ga_runs_best_individual_id", "ga_runs", ["best_individual_id"], unique=False)

    op.create_table(
        "ga_individuals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ga_run_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("individual_index", sa.Integer(), nullable=False),
        sa.Column("parameter_hash", sa.String(length=64), nullable=False),
        sa.Column("parameters_json", sa.Text(), nullable=False),
        sa.Column("fitness", sa.Numeric(18, 6), nullable=True),
        sa.Column("metrics_json", sa.Text(), nullable=True),
        sa.Column("evaluation_json", sa.Text(), nullable=True),
        sa.Column("source_optimization_result_id", sa.Integer(), nullable=True),
        sa.Column("parent_a_id", sa.Integer(), nullable=True),
        sa.Column("parent_b_id", sa.Integer(), nullable=True),
        sa.Column("mutation_json", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            GA_INDIVIDUAL_STATUS_CONSTRAINT,
            name="ga_individuals_status",
        ),
        sa.ForeignKeyConstraint(
            ["ga_run_id"],
            ["ga_runs.id"],
            name="fk_ga_individuals_ga_run_id_ga_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_optimization_result_id"],
            ["optimization_results.id"],
            name="fk_ga_individuals_source_optimization_result_id_optimization_results",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["parent_a_id"],
            ["ga_individuals.id"],
            name="fk_ga_individuals_parent_a_id_ga_individuals",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["parent_b_id"],
            ["ga_individuals.id"],
            name="fk_ga_individuals_parent_b_id_ga_individuals",
            ondelete="SET NULL",
        ),
    )
    op.create_index("ix_ga_individuals_ga_run_id", "ga_individuals", ["ga_run_id"], unique=False)
    op.create_index("ix_ga_individuals_generation", "ga_individuals", ["generation"], unique=False)
    op.create_index(
        "ix_ga_individuals_parameter_hash",
        "ga_individuals",
        ["parameter_hash"],
        unique=False,
    )
    op.create_index("ix_ga_individuals_fitness", "ga_individuals", ["fitness"], unique=False)
    op.create_index(
        "ix_ga_individuals_source_optimization_result_id",
        "ga_individuals",
        ["source_optimization_result_id"],
        unique=False,
    )
    op.create_index("ix_ga_individuals_parent_a_id", "ga_individuals", ["parent_a_id"], unique=False)
    op.create_index("ix_ga_individuals_parent_b_id", "ga_individuals", ["parent_b_id"], unique=False)

    if not is_sqlite:
        op.create_foreign_key(
            "fk_ga_runs_best_individual_id_ga_individuals",
            "ga_runs",
            "ga_individuals",
            ["best_individual_id"],
            ["id"],
            ondelete="SET NULL",
        )

    op.create_table(
        "ga_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ga_run_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["ga_run_id"],
            ["ga_runs.id"],
            name="fk_ga_events_ga_run_id_ga_runs",
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_ga_events_ga_run_id", "ga_events", ["ga_run_id"], unique=False)
    op.create_index("ix_ga_events_generation", "ga_events", ["generation"], unique=False)
    op.create_index("ix_ga_events_event_type", "ga_events", ["event_type"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite"
    op.drop_index("ix_ga_events_event_type", table_name="ga_events")
    op.drop_index("ix_ga_events_generation", table_name="ga_events")
    op.drop_index("ix_ga_events_ga_run_id", table_name="ga_events")
    op.drop_table("ga_events")
    if not is_sqlite:
        op.drop_constraint(
            "fk_ga_runs_best_individual_id_ga_individuals",
            "ga_runs",
            type_="foreignkey",
        )
    op.drop_index("ix_ga_individuals_parent_b_id", table_name="ga_individuals")
    op.drop_index("ix_ga_individuals_parent_a_id", table_name="ga_individuals")
    op.drop_index("ix_ga_individuals_source_optimization_result_id", table_name="ga_individuals")
    op.drop_index("ix_ga_individuals_fitness", table_name="ga_individuals")
    op.drop_index("ix_ga_individuals_parameter_hash", table_name="ga_individuals")
    op.drop_index("ix_ga_individuals_generation", table_name="ga_individuals")
    op.drop_index("ix_ga_individuals_ga_run_id", table_name="ga_individuals")
    op.drop_table("ga_individuals")
    op.drop_index("ix_ga_runs_best_individual_id", table_name="ga_runs")
    op.drop_index("ix_ga_runs_holdout_end_date", table_name="ga_runs")
    op.drop_index("ix_ga_runs_holdout_start_date", table_name="ga_runs")
    op.drop_index("ix_ga_runs_validation_end_date", table_name="ga_runs")
    op.drop_index("ix_ga_runs_validation_start_date", table_name="ga_runs")
    op.drop_index("ix_ga_runs_train_end_date", table_name="ga_runs")
    op.drop_index("ix_ga_runs_train_start_date", table_name="ga_runs")
    op.drop_index("ix_ga_runs_market", table_name="ga_runs")
    op.drop_table("ga_runs")
