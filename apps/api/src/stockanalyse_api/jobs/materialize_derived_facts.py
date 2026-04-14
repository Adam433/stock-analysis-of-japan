from __future__ import annotations

# Ensure SQLAlchemy relationship targets are registered before job execution.
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts


def main() -> None:
    with SessionLocal() as session:
        result = materialize_derived_indicator_facts(session)

    print(result)


if __name__ == "__main__":
    main()
