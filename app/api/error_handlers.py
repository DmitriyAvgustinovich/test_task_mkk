from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.exceptions import IdempotencyConflict, PaymentNotFound, UnauthorizedError


def register_error_handlers(app: FastAPI) -> None:
    status_codes = {IdempotencyConflict: 409, PaymentNotFound: 404, UnauthorizedError: 401}

    async def handle_error(_: Request, error: Exception) -> JSONResponse:
        return JSONResponse(status_code=status_codes[type(error)], content={"detail": str(error)})

    for error_type in status_codes:
        app.add_exception_handler(error_type, handle_error)
