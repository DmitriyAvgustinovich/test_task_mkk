import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

parser = argparse.ArgumentParser(description="Local demo webhook receiver")
parser.add_argument("--port", type=int, default=9000)
parser.add_argument("--fail-first", type=int, default=0)
args = parser.parse_args()
attempts: dict[str, int] = {}
delivered: set[str] = set()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        event_id = payload["event_id"]
        attempts[event_id] = attempts.get(event_id, 0) + 1
        status = 503 if attempts[event_id] <= args.fail_first else 200
        duplicate = event_id in delivered
        if status == 200:
            delivered.add(event_id)
        print(
            json.dumps(
                {
                    "attempt": attempts[event_id],
                    "http_status": status,
                    "duplicate": duplicate,
                    **payload,
                }
            ),
            flush=True,
        )
        self.send_response(status)
        self.end_headers()
        self.wfile.write(b"ok" if status == 200 else b"retry later")


if __name__ == "__main__":
    print(f"Webhook receiver listening on port {args.port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", args.port), Receiver).serve_forever()
