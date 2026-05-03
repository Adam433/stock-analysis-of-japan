"""add optimization result detail cache

Revision ID: 20260503_0025
Revises: 20260502_0024
Create Date: 2026-05-03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260503_0025"
down_revision = "20260502_0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "optimization_result_detail_cache",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("optimization_result_id", sa.Integer(), nullable=False),
        sa.Column("max_trades_returned", sa.Integer(), nullable=False),
        sa.Column("train_result_json", sa.Text(), nullable=False),
        sa.Column("validation_result_json", sa.Text(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.ForeignKeyConstraint(
            ["optimization_result_id"],
            ["optimization_results.id"],
            name="fk_optimization_result_detail_cache_result_id_optimization_results",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_optimization_result_detail_cache"),
        sa.UniqueConstraint(
            "optimization_result_id",
            name="uq_optimization_result_detail_cache_result_id",
        ),
    )
    op.create_index(
        "ix_optimization_result_detail_cache_optimization_result_id",
        "optimization_result_detail_cache",
        ["optimization_result_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_optimization_result_detail_cache_optimization_result_id",
        table_name="optimization_result_detail_cache",
    )
    op.drop_table("optimization_result_detail_cache")
