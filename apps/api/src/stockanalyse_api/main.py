from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logger = logging.getLogger("stockanalyse_api")

from stockanalyse_api.api.routes import backtests_router
from stockanalyse_api.api.routes import health_router
from stockanalyse_api.api.routes import screening_router
from stockanalyse_api.api.routes import stocks_router
from stockanalyse_api.api.routes import strategy_config_router
from stockanalyse_api.api.routes import watchlist_router
from stockanalyse_api.services.operations.auto_refresh import AutoRefreshRuntime

_DEFAULT_CORS_ORIGINS = [
    "http://127.0.0.1:3000",
    "http://localhost:3000",
    "http://127.0.0.1:3001",
    "http://localhost:3001",
]


def _get_cors_origins() -> list[str]:
    env_value = os.environ.get("STOCKANALYSE_CORS_ORIGINS")
    if env_value:
        return [origin.strip() for origin in env_value.split(",") if origin.strip()]
    return _DEFAULT_CORS_ORIGINS


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

    app = FastAPI(
        title="stockAnalyse API",
        description="日股筛选与回测平台 API",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_origins(),
        allow_credentials=False,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error."},
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
