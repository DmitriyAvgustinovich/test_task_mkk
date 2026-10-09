from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.health import HealthRepository


class HealthService:
    def __init__(self, session: AsyncSession):
        self.repository = HealthRepository(session)

    async def check(self) -> dict[str, str]:
        await self.repository.ping()
        return {"status": "ok"}
