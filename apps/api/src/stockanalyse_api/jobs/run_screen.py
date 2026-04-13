from __future__ import annotations

from stockanalyse_api.db.session import SessionLocal
from stockanalyse_api.services.screening import execute_screen_run


def main() -> None:
    with SessionLocal() as session:
        result = execute_screen_run(session)

    print(result.to_dict())


if __name__ == "__main__":
    main()
