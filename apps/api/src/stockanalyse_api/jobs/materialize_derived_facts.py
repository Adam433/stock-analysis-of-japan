from __future__ import annotations

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.factor_materialization import materialize_derived_indicator_facts


def main() -> None:
    with SessionLocal() as session:
        result = materialize_derived_indicator_facts(session)

    print(result)


if __name__ == "__main__":
    main()
