"""add backtest run lifecycle

Revision ID: 20260417_0017
Revises: 20260415_0016
Create Date: 2026-04-17 09:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_0017"
down_revision = "20260415_0016"
branch_labels = None
depends_on = None

legacy_lifecycle = "legacy_condition_hit"


def upgrade() -> None:
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.add_column(
            sa.Column(
                "backtest_lifecycle",
                sa.String(length=32),
                nullable=True,
                server_default=legacy_lifecycle,
            )
        )

    op.execute(
        sa.text(
            "UPDATE backtest_runs "
            "SET backtest_lifecycle = :lifecycle "
            "WHERE backtest_lifecycle IS NULL"
        ).bindparams(lifecycle=legacy_lifecycle)
    )

    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.alter_column(
            "backtest_lifecycle",
            existing_type=sa.String(length=32),
            nullable=False,
            server_default=None,
        )
        batch_op.create_check_constraint(
            "backtest_runs_lifecycle",
            "backtest_lifecycle IN ('portfolio_return', 'legacy_condition_hit')",
        )


def downgrade() -> None:
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.drop_constraint("backtest_runs_lifecycle", type_="check")
        batch_op.drop_column("backtest_lifecycle")
