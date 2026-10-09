from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.deps import authenticate
from app.api.error_handlers import register_error_handlers
from app.api.router import api_router
from app.api.routes.system import router as system_router
from app.core.config import Settings, get_settings
from app.db.session import create_database


def create_app(
    settings: Settings | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = None
        if session_factory is None:
            engine, app.state.session_factory = create_database(settings.database_url)
        else:
            app.state.session_factory = session_factory
        try:
            yield
        finally:
            if engine is not None:
                await engine.dispose()

    app = FastAPI(
        title="Payment Processing API",
        version="1.0.0",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    if session_factory is not None:
        app.state.session_factory = session_factory
    register_error_handlers(app)
    app.include_router(api_router)
    app.include_router(system_router)
    return app
