"""TEMPORARY (BGG.CON 2026): the convention library recorder as a Cloud Run service.

Cloud Scheduler POSTs here once a minute, and each request records one minute
(``bggcon_recorder.run_once``). A Cloud Run *job* took 10-60 s just to start each run, so runs a
minute apart overlapped; a service starts in seconds, stays warm between calls a minute apart,
and with one instance taking one request at a time two runs can't overlap.

The clients (GCS, BigQuery) are made once per instance, not per request.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer

from src.pipeline import bggcon_recorder as job

logger = logging.getLogger(__name__)


def make_handler(run: Callable[[], str]) -> type[BaseHTTPRequestHandler]:
    """Records one minute per POST. A failure answers 500, so the scheduler logs it."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 (the http.server name)
            started = time.monotonic()
            try:
                result, status = run(), 200
            except Exception:
                logger.exception("BGG.CON recorder failed")
                result, status = "failed", 500
            logger.info("BGG.CON recorder: %s (%.1fs)", result, time.monotonic() - started)
            body = result.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            pass  # one line per run comes from do_POST

    return Handler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    deps = job.real_deps()
    alert_user = os.environ.get("BGGCON_ALERT_USER")
    test_run = os.environ.get("BGGCON_TEST_RUN") == "1"

    def run() -> str:
        return job.run_once(deps, alert_user=alert_user, test_run=test_run)

    port = int(os.environ.get("PORT", "8080"))
    HTTPServer(("", port), make_handler(run)).serve_forever()


if __name__ == "__main__":
    main()
