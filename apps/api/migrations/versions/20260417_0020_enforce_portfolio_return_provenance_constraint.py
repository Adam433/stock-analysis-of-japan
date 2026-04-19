"""enforce portfolio-return provenance constraint

Revision ID: 20260417_0020
Revises: 20260417_0019
Create Date: 2026-04-17 16:45:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260417_0020"
down_revision = "20260417_0019"
branch_labels = None
depends_on = None

CONSTRAINT_NAME = "backtest_runs_portfolio_return_provenance"
CONSTRAINT_SQL = (
    "(backtest_lifecycle = 'legacy_condition_hit') OR "
    "(backtest_lifecycle = 'portfolio_return' AND source_screen_run_id IS NOT NULL AND rps_definition_version IS NULL)"
)


def upgrade() -> None:
    connection = op.get_bind()
    invalid_rows = connection.execute(
        sa.text(
            """
            SELECT id
            FROM backtest_runs
            WHERE NOT (
                (backtest_lifecycle = 'legacy_condition_hit')
                OR (
                    backtest_lifecycle = 'portfolio_return'
                    AND source_screen_run_id IS NOT NULL
                    AND rps_definition_version IS NULL
                )
            )
            ORDER BY id
            LIMIT 5
            """
        )
    ).scalars().all()
    if invalid_rows:
        raise RuntimeError(
            "Cannot enforce portfolio-return provenance constraint; invalid backtest_runs ids: "
            + ", ".join(str(row_id) for row_id in invalid_rows)
        )

    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.create_check_constraint(CONSTRAINT_NAME, CONSTRAINT_SQL)


def downgrade() -> None:
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.drop_constraint(CONSTRAINT_NAME, type_="check")
