class IdempotencyConflict(Exception):
    def __init__(self):
        super().__init__("Idempotency-Key is already used for a different request")


class PaymentNotFound(Exception):
    def __init__(self):
        super().__init__("Payment not found")


class UnauthorizedError(Exception):
    def __init__(self):
        super().__init__("Invalid or missing API key")


class PermanentEventError(Exception):
    pass
