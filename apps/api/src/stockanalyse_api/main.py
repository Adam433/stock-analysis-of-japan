from __future__ import annotations

from fastapi import FastAPI

from stockanalyse_api.api.routes import health_router


def create_app() -> FastAPI:
    app = FastAPI(title="stockAnalyse API")
    app.include_router(health_router)
    return app


app = create_app()

def main() -> None:
    """Expose the ASGI application for local verification."""
    print("stockanalyse-api app is ready at /health/market-data")


if __name__ == "__main__":
    main()
