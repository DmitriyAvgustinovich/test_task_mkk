import argparse
import asyncio
import os
from contextlib import suppress
from uuid import uuid4

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response


async def main():
    parser = argparse.ArgumentParser(
        description="Exercise the running service and HTTP webhook delivery"
    )
    parser.add_argument("--api", default="http://api:8000")
    parser.add_argument("--callback-host", default="payments-smoke")
    args = parser.parse_args()
    receiver = FastAPI()
    calls: dict[str, list[dict]] = {}

    @receiver.post("/webhook")
    async def webhook(request: Request):
        body = await request.json()
        attempts = calls.setdefault(body["payment_id"], [])
        attempts.append(body)
        fail_first = body["metadata"]["fail_first"]
        return Response(status_code=503 if len(attempts) <= fail_first else 204)

    server = uvicorn.Server(
        uvicorn.Config(receiver, host="0.0.0.0", port=9000, log_level="warning")
    )
    task = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(10):
            while not server.started:
                await asyncio.sleep(0.05)
        async with httpx.AsyncClient(
            base_url=args.api,
            headers={"X-API-Key": os.environ["API_KEY"]},
            timeout=10,
        ) as client:

            async def exercise(fail_first):
                body = {
                    "amount": "1250.50",
                    "currency": "RUB",
                    "description": "Docker smoke test",
                    "metadata": {"fail_first": fail_first},
                    "webhook_url": f"http://{args.callback_host}:9000/webhook",
                }
                key = f"smoke-{uuid4()}"
                response = await client.post(
                    "/api/v1/payments",
                    json=body,
                    headers={"Idempotency-Key": key},
                )
                assert response.status_code == 202, response.text
                payment_id = response.json()["payment_id"]
                duplicate = await client.post(
                    "/api/v1/payments",
                    json=body,
                    headers={"Idempotency-Key": key},
                )
                assert duplicate.json()["payment_id"] == payment_id
                async with asyncio.timeout(60):
                    while True:
                        detail = (await client.get(f"/api/v1/payments/{payment_id}")).json()
                        if detail["webhook_sent_at"] or detail["processing_attempts"] == 3:
                            break
                        await asyncio.sleep(0.1)
                received = calls[payment_id]
                assert detail["status"] in ("succeeded", "failed")
                assert all(payload == received[0] for payload in received)
                if fail_first == 1:
                    assert len(received) == 2 and detail["webhook_sent_at"]
                    print(f"PASS: payment {payment_id}: HTTP webhook succeeded on attempt 2")
                else:
                    assert len(received) == 3 and detail["webhook_sent_at"] is None
                    assert detail["last_error"]

                    async with httpx.AsyncClient(
                        base_url="http://rabbitmq:15672",
                        auth=(os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASSWORD"]),
                    ) as admin:
                        async with asyncio.timeout(10):
                            while True:
                                response = await admin.post(
                                    "/api/queues/%2F/payments.dlq/get",
                                    json={
                                        "count": 100,
                                        "ackmode": "ack_requeue_true",
                                        "encoding": "auto",
                                    },
                                )
                                response.raise_for_status()
                                import json

                                if any(
                                    json.loads(item["payload"]).get("payment_id") == payment_id
                                    for item in response.json()
                                ):
                                    break
                                await asyncio.sleep(0.1)
                    print(f"PASS: payment {payment_id}: 3 HTTP failures, event in payments.dlq")

            await exercise(1)
            await exercise(99)
    finally:
        server.should_exit = True
        try:
            async with asyncio.timeout(5):
                await task
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    asyncio.run(main())
