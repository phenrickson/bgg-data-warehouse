"""The BGG.CON recorder as a Cloud Run service: each scheduler POST records one minute."""

import threading
import urllib.error
import urllib.request
from http.server import HTTPServer

import pytest

from src.pipeline import bggcon_service as service

INTERNAL_ERROR = 500


@pytest.fixture
def serve():
    servers = []

    def start(run):
        server = HTTPServer(("127.0.0.1", 0), service.make_handler(run))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_port}/"

    yield start
    for s in servers:
        s.shutdown()


def post(url):
    with urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=5) as res:
        return res.status, res.read().decode()


def test_a_post_records_one_minute_and_answers_with_the_result(serve):
    calls = []
    url = serve(lambda: calls.append(1) or "recorded 2 events")
    assert post(url) == (200, "recorded 2 events")
    assert post(url) == (200, "recorded 2 events")
    assert len(calls) == 2  # noqa: PLR2004


def test_a_failed_run_answers_500_so_the_scheduler_logs_it(serve):
    def boom():
        raise RuntimeError("BigQuery insert failed")

    url = serve(boom)
    with pytest.raises(urllib.error.HTTPError) as e:
        post(url)
    assert e.value.code == INTERNAL_ERROR
