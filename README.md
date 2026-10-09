# Асинхронный сервис процессинга платежей

## Запуск

Команды выполняются из корня проекта. Скопируйте `.env.example` в `.env`:

```powershell
Copy-Item .env.example .env
```

В Linux/macOS используйте `cp .env.example .env`. Если `.env` уже настроен,
сохраните существующий файл.

### Docker

Нужны Docker Engine и Docker Compose v2.

```powershell
docker compose up -d --build
docker compose ps
```

- API: `http://localhost:8000`.
- RabbitMQ Management: `http://localhost:15672`, логин `payments`, пароль
  `rabbit-local-password` из `.env.example`.
- API key для примеров: `local-test-api-key`.

Просмотр логов и остановка:

```powershell
docker compose logs -f api consumer
docker compose down
```

### Python вне Docker

Нужен Python 3.12+. Остановите контейнеры API и consumer перед локальным запуском:

```powershell
docker compose stop api consumer
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose -f docker-compose.yml -f compose.infra.yaml up -d postgres rabbitmq
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\uvicorn.exe app.main:create_app --factory --host 127.0.0.1 --port 8000
```

Consumer запускается в другом терминале:

```powershell
.\.venv\Scripts\python.exe -m app.worker
```

## Примеры

### Получатель webhook

В отдельном терминале запустите демонстрационный получатель на компьютере:

```powershell
python scripts/webhook_receiver.py --port 9000 --fail-first 1
```

Он вернёт `503` при первой доставке каждого события, затем `200`. Для Docker Desktop
адрес компьютера из consumer — `host.docker.internal`. В Linux используйте адрес
доступного получателя или добавьте соответствующий `extra_hosts` в compose.

### Создание и получение платежа в PowerShell

Все запросы требуют `X-API-Key`, создание платежа также требует `Idempotency-Key`.

```powershell
$headers = @{
    "X-API-Key" = "local-test-api-key"
    "Idempotency-Key" = "order-123"
}
$body = @{
    amount = "1250.50"
    currency = "RUB"
    description = "Оплата заказа 123"
    metadata = @{ order_id = 123 }
    webhook_url = "http://host.docker.internal:9000/webhook"
} | ConvertTo-Json -Depth 10

$payment = Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8000/api/v1/payments" `
    -Headers $headers -ContentType "application/json" -Body $body
$payment

Invoke-RestMethod `
    -Uri "http://localhost:8000/api/v1/payments/$($payment.payment_id)" `
    -Headers @{ "X-API-Key" = "local-test-api-key" }
```

Ответ создания:

```json
{
  "payment_id": "e9c845a6-7d5d-4b5e-9246-bcc40496472f",
  "status": "pending",
  "created_at": "2026-10-08T15:00:00Z"
}
```

### Запросы через curl

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-test-api-key' \
  -H 'Idempotency-Key: order-456' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"1250.50","currency":"RUB","description":"Order 456","metadata":{"order_id":456},"webhook_url":"http://host.docker.internal:9000/webhook"}'

curl http://localhost:8000/api/v1/payments/PAYMENT_ID \
  -H 'X-API-Key: local-test-api-key'
```

### Уведомление webhook

```json
{
  "event_id": "payment:e9c845a6-7d5d-4b5e-9246-bcc40496472f:result",
  "event_type": "payment.processed",
  "payment_id": "e9c845a6-7d5d-4b5e-9246-bcc40496472f",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "metadata": {"order_id": 123},
  "processed_at": "2026-10-08T15:00:03Z"
}
```
