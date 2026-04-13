"""API routes package."""
from stockanalyse_api.api.routes.backtests import router as backtests_router
from stockanalyse_api.api.routes.health import router as health_router
from stockanalyse_api.api.routes.screening import router as screening_router
from stockanalyse_api.api.routes.stocks import router as stocks_router
from stockanalyse_api.api.routes.strategy_config import router as strategy_config_router
from stockanalyse_api.api.routes.watchlist import router as watchlist_router

__all__ = ["backtests_router", "health_router", "strategy_config_router", "screening_router", "stocks_router", "watchlist_router"]
