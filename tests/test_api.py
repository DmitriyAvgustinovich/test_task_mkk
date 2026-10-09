import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models import Outbox, Payment


async def test_create_get_and_idempotency(client, sessions, payment_body):
    first = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": "order-123"},
    )
    assert first.status_code == 202
    assert first.json()["status"] == "pending"
    second = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": "order-123"},
    )
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert second.json()["status"] == first.json()["status"]

    assert datetime.fromisoformat(second.json()["created_at"]).replace(tzinfo=UTC) == (
        datetime.fromisoformat(first.json()["created_at"]).replace(tzinfo=UTC)
    )
    detail = await client.get(f"/api/v1/payments/{first.json()['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == "1250.50"
    assert detail.json()["metadata"] == {"order_id": 123}
    assert detail.json()["processed_at"] is None
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        events = list(await session.scalars(select(Outbox)))
        assert len(events) == 1
        assert events[0].routing_key == "payments.new"
        assert events[0].payload["payment_id"] == first.json()["payment_id"]
        assert events[0].published_at is None


async def test_conflicting_idempotency_key(client, payment_body):
    await client.post("/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "key"})
    payment_body["amount"] = "99.99"
    response = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": "key"},
    )
    assert response.status_code == 409


async def test_equal_amounts_have_same_fingerprint(client, payment_body):
    payment_body["amount"] = "10"
    first = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": "key"},
    )
    payment_body["amount"] = "10.00"
    second = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": "key"},
    )
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_concurrent_creation(client, sessions, payment_body):
    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "parallel"}
            )
            for _ in range(8)
        ]
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize("path", ["/health", "/docs", "/openapi.json", "/api/v1/payments/abc"])
@pytest.mark.parametrize("key", [None, "wrong"])
async def test_auth_on_all_endpoints(client, path, key):
    client.headers.pop("X-API-Key")
    response = await client.get(path, headers={"X-API-Key": key} if key else {})
    assert response.status_code == 401


async def test_unauthorized_post(client, payment_body):
    client.headers.pop("X-API-Key")
    response = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": "key"},
    )
    assert response.status_code == 401


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "10000000000000000"},
        {"amount": "NaN"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://example.com/hook"},
        {"description": ""},
        {"unexpected": 1},
    ],
)
async def test_invalid_payment(client, payment_body, changes):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body | changes,
        headers={"Idempotency-Key": "key"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("key", [None, "", "   ", "x" * 256])
async def test_idempotency_key_required(client, payment_body, key):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body,
        headers={"Idempotency-Key": key} if key is not None else {},
    )
    assert response.status_code == 422


async def test_missing_payment_and_invalid_uuid(client):
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/invalid")).status_code == 422
