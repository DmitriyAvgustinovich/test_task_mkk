import httpx


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def send(self, url: str, payload: dict) -> None:
        response = await self.client.post(
            url,
            headers={"Idempotency-Key": payload["event_id"]},
            json=payload,
        )

        response.raise_for_status()
