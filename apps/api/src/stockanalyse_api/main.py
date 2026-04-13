from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from stockanalyse_api.api.routes import health_router
from stockanalyse_api.api.routes import screening_router
from stockanalyse_api.api.routes import stocks_router
from stockanalyse_api.api.routes import strategy_config_router
from stockanalyse_api.api.routes import watchlist_router


def create_app() -> FastAPI:
    app = FastAPI(title="stockAnalyse API")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://127.0.0.1:3000",
            "http://localhost:3000",
            "http://127.0.0.1:3001",
            "http://localhost:3001",
        ],
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )
    app.include_router(health_router)
    app.include_router(strategy_config_router)
    app.include_router(screening_router)
    app.include_router(stocks_router)
    app.include_router(watchlist_router)
    return app


app = create_app()

def main() -> None:
    """Expose the ASGI application for local verification."""
    print("stockanalyse-api app is ready at /health/market-data")


if __name__ == "__main__":
    main()
