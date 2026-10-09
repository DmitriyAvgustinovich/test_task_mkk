import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import Settings
from app.main import create_app
from app.models import Base


@pytest.fixture
def settings():
    return Settings(
        api_key="test-key",
        database_url="sqlite+aiosqlite://",
        gateway_min_delay_seconds=0,
        gateway_max_delay_seconds=0,
    )


@pytest.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'payments.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
async def client(settings, sessions):
    app = create_app(settings, sessions)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as client:
        yield client


@pytest.fixture
def payment_body():
    return {
        "amount": "1250.50",
        "currency": "RUB",
        "description": "Test order",
        "metadata": {"order_id": 123},
        "webhook_url": "https://merchant.example/webhook",
    }
