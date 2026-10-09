from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.openapi.docs import get_swagger_ui_html

from app.api.deps import get_health_service
from app.services.health import HealthService

router = APIRouter(include_in_schema=False)


@router.get("/health")
async def health(service: Annotated[HealthService, Depends(get_health_service)]):
    return await service.check()


@router.get("/openapi.json")
async def openapi(request: Request):
    return request.app.openapi()


@router.get("/docs")
async def docs(request: Request):
    return get_swagger_ui_html(openapi_url="/openapi.json", title=request.app.title)
