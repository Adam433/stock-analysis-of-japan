"""add strategy configuration rps window selection

Revision ID: 20260415_0016
Revises: 20260415_0015
Create Date: 2026-04-15 21:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260415_0016"
down_revision = "20260415_0015"
branch_labels = None
depends_on = None

default_selected_rps_windows = "50,120,250"


def upgrade() -> None:
    with op.batch_alter_table("strategy_configurations") as batch_op:
        batch_op.add_column(
            sa.Column(
                "selected_rps_windows",
                sa.String(length=64),
                nullable=False,
                server_default=default_selected_rps_windows,
            )
        )
        batch_op.add_column(
            sa.Column(
                "min_rps_lines_required",
                sa.Integer(),
                nullable=False,
                server_default="1",
            )
        )

    op.execute(
        sa.text(
            "UPDATE strategy_configurations "
            "SET selected_rps_windows = :windows, min_rps_lines_required = 1 "
            "WHERE selected_rps_windows IS NULL OR min_rps_lines_required IS NULL"
        ).bindparams(windows=default_selected_rps_windows)
    )


def downgrade() -> None:
    with op.batch_alter_table("strategy_configurations") as batch_op:
        batch_op.drop_column("min_rps_lines_required")
        batch_op.drop_column("selected_rps_windows")
