from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from stockanalyse_api.api.routes import backtests_router
from stockanalyse_api.api.routes import health_router
from stockanalyse_api.api.routes import screening_router
from stockanalyse_api.api.routes import stocks_router
from stockanalyse_api.api.routes import strategy_config_router
from stockanalyse_api.api.routes import watchlist_router
from stockanalyse_api.services.operations.auto_refresh import AutoRefreshRuntime


def create_app(*, auto_refresh_runtime: AutoRefreshRuntime | None = None) -> FastAPI:
    runtime = auto_refresh_runtime or AutoRefreshRuntime()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.auto_refresh_runtime = runtime
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="stockAnalyse API", lifespan=lifespan)
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
    app.include_router(backtests_router)
    return app


app = create_app()

def main() -> None:
    """Expose the ASGI application for local verification."""
    print("stockanalyse-api app is ready at /health/market-data")


if __name__ == "__main__":
    main()
